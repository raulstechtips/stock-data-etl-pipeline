"""
Celery task for processing stock data into a unified Delta Lake table.

This task is triggered after fetch_stock_data completes successfully.
It performs the following steps:
1. Validates the run is in a valid state (QUEUED_FOR_DELTA)
2. Downloads JSON data from S3/MinIO raw bucket
3. Transforms data using Polars DataFrames into a unified structure
4. Creates or merges data into a single Delta Lake stocks table
5. Updates the run state to DELTA_FINISHED on success or FAILED on error

The worker processes three types of data into a unified stocks table:
- Financial time series data (quarterly metrics over time) - record_type='financials'
- Metadata (company information, not time-series) - record_type='metadata'
- Trailing Twelve Month data (TTM rolling metrics) - record_type='ttm'

All data is stored in a single Delta Lake table (s3://bucket/stocks) with a composite
key of (ticker, record_type, period_end_date). This design significantly reduces
operational overhead for query engines like Trino compared to per-ticker table splits.

Note: This task is not safe for concurrent execution due to delta-rs limitations
with MinIO/S3 concurrent writes. Run with concurrency=1.
"""

import io
import json
import logging
import uuid
from typing import Any, Dict, List, NotRequired, TypedDict
from urllib.parse import urlparse

import polars as pl
from celery import shared_task
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from django.conf import settings
from django.db import DatabaseError
from minio import Minio
from minio.error import MinioException, S3Error

from api.models import IngestionState
from api.services.stock_ingestion_service import (
    IngestionRunNotFoundError,
    InvalidStateTransitionError,
    StockIngestionService,
)
from workers.exceptions import (
    DeltaLakeError,
    DeltaLakeMergeError,
    DeltaLakeReadError,
    DeltaLakeWriteError,
    InvalidDataFormatError,
    InvalidStateError,
    NonRetryableError,
    StorageAuthenticationError,
    StorageBucketNotFoundError,
    StorageConnectionError,
)
from workers.tasks.base import BaseTask


logger = logging.getLogger(__name__)


class ProcessDeltaLakeResult(TypedDict):
    """
    Result object returned by the process_delta_lake task.
    
    Attributes:
        run_id: UUID of the StockIngestionRun that was processed
        ticker: Stock ticker symbol
        state: Current state of the run after processing
        skipped: Whether the task was skipped (idempotency check)
        processed_uri: URI where the processed Delta Lake table is stored (optional)
        reason: Reason for skipping (optional, only present when skipped=True)
        records_processed: Number of records processed (optional)
    """
    run_id: str
    ticker: str
    state: str
    skipped: bool
    processed_uri: NotRequired[str]
    reason: NotRequired[str]
    records_processed: NotRequired[int]


@shared_task(bind=True, base=BaseTask, name='workers.tasks.process_delta_lake')
def process_delta_lake(self, run_id: str, ticker: str) -> ProcessDeltaLakeResult:
    """
    Process stock data into Delta Lake tables.
    
    This task implements the following workflow:
    1. Validate run state (must be QUEUED_FOR_DELTA)
    2. Transition to DELTA_RUNNING state
    3. Download JSON file from S3/MinIO raw bucket
    4. Transform data using Polars DataFrames
    5. Check if Delta Lake table exists
    6. Create new table or merge data into existing table
    7. Transition to DELTA_FINISHED state with processed URI
    
    On failure:
    - Non-retryable errors: Immediately transition to FAILED
    - Max retries exceeded: Transition to FAILED
    
    Args:
        run_id: UUID of the StockIngestionRun to process
        ticker: Stock ticker symbol (for logging and file paths)
        
    Returns:
        ProcessDeltaLakeResult: Result object with run_id, ticker, state, 
            skipped status, processed_uri, and records_processed
        
    Raises:
        NonRetryableError: For all errors
    """
    service = StockIngestionService()
    
    ticker = ticker.strip().upper()
    logger.info("Starting Delta Lake processing task", extra={"run_id": run_id, "ticker": ticker})
    
    try:
        # Validate and convert run_id to UUID
        try:
            run_uuid = uuid.UUID(run_id)
        except ValueError as e:
            logger.error(
                "Invalid run_id format - not a valid UUID",
                extra={"run_id": run_id, "ticker": ticker}
            )
            raise NonRetryableError(f"Invalid run_id format: {run_id}") from e
        
        # Step 1: Validate state and transition to DELTA_RUNNING
        try:
            run = service.get_run_by_id(run_uuid)
            
            # Idempotency check: If already DELTA_FINISHED or DONE, task is complete
            if run.state in [IngestionState.DELTA_FINISHED, IngestionState.DONE]:
                logger.info(
                    "Run already past QUEUED_FOR_DELTA, skipping Delta Lake processing",
                    extra={"run_id": run_id, "state": run.state}
                )
                return ProcessDeltaLakeResult(
                    run_id=str(run_id),
                    ticker=ticker,
                    state=run.state,
                    skipped=True,
                    processed_uri=run.processed_data_uri,
                    reason='already_processed'
                )
            
            # Check if in FAILED state
            if run.state == IngestionState.FAILED:
                logger.warning(
                    "Run is in FAILED state, cannot proceed with Delta Lake processing",
                    extra={"run_id": run_id}
                )
                raise InvalidStateError(
                    f"Run {run_id} is in FAILED state and cannot be retried"
                )
            
            # Must be in QUEUED_FOR_DELTA to start, or DELTA_RUNNING if this is a retry
            if run.state not in [IngestionState.QUEUED_FOR_DELTA, IngestionState.DELTA_RUNNING]:
                logger.error(
                    "Run is in invalid state for Delta Lake processing",
                    extra={"run_id": run_id, "state": run.state}
                )
                raise InvalidStateError(
                    f"Run {run_id} must be in QUEUED_FOR_DELTA or DELTA_RUNNING state, "
                    f"but is in {run.state}"
                )
            
            # Validate raw_data_uri exists
            if not run.raw_data_uri:
                logger.error(
                    "Run has no raw_data_uri - cannot process Delta Lake",
                    extra={"run_id": run_id}
                )
                _transition_to_failed(
                    service, run_uuid, "MISSING_RAW_DATA",
                    "No raw_data_uri found for run"
                )
                raise InvalidStateError(f"Run {run_id} has no raw_data_uri")
            
            # Transition to DELTA_RUNNING (if not already)
            if run.state == IngestionState.QUEUED_FOR_DELTA:
                service.update_run_state(
                    run_id=run_uuid,
                    new_state=IngestionState.DELTA_RUNNING
                )
                logger.info("Transitioned run to DELTA_RUNNING state", extra={"run_id": run_id})
        
        except IngestionRunNotFoundError as e:
            logger.exception("Ingestion run not found", extra={"run_id": str(run_id)})
            raise NonRetryableError(f"Ingestion run {run_id} not found") from e
        
        except (InvalidStateTransitionError, InvalidStateError) as e:
            logger.exception("Invalid state transition or invalid state", extra={"run_id": str(run_id)})
            raise NonRetryableError(str(e)) from e
        
        # Step 2: Download JSON data from S3/MinIO
        try:
            logger.info("Downloading raw data from S3/MinIO", extra={"ticker": ticker, "uri": run.raw_data_uri})
            
            json_data = _download_from_storage(run.raw_data_uri)
            
            logger.info(
                "Successfully downloaded raw data",
                extra={"ticker": ticker, "bytes": len(json_data)}
            )
        
        except (StorageAuthenticationError, StorageBucketNotFoundError, 
                StorageConnectionError, InvalidDataFormatError) as e:
            logger.exception("Storage error downloading raw data", extra={"ticker": ticker, "run_id": str(run_id)})
            _transition_to_failed(service, run_uuid, "STORAGE_ERROR", str(e))
            raise NonRetryableError(str(e)) from e
        
        # Step 3: Transform data using Polars
        try:
            logger.info("Transforming data with Polars", extra={"ticker": ticker})
            
            # Parse JSON
            data = json.loads(json_data)
            
            # Transform into unified DataFrame with all data types
            unified_df = _transform_data_to_polars(data, ticker)
            
            logger.info(
                "Successfully transformed data into unified DataFrame",
                extra={
                    "ticker": ticker,
                    "total_rows": len(unified_df),
                    "record_types": unified_df['record_type'].unique().to_list()
                }
            )
        
        except (InvalidDataFormatError, ValueError, KeyError) as e:
            logger.exception("Error transforming data", extra={"ticker": ticker, "run_id": str(run_id)})
            _transition_to_failed(service, run_uuid, "DATA_TRANSFORMATION_ERROR", str(e))
            raise NonRetryableError(str(e)) from e
        
        # Step 4: Process unified Delta Lake stocks table
        try:
            logger.info("Processing unified Delta Lake stocks table", extra={"ticker": ticker})
            
            # Build S3 storage options for Delta Lake
            storage_options = _build_storage_options()
            
            # Process all data into unified stocks table
            processed_uri = _process_stocks_table(
                ticker, unified_df, storage_options
            )
            
            total_records = len(unified_df)
            
            logger.info(
                "Successfully processed unified Delta Lake stocks table",
                extra={
                    "ticker": ticker,
                    "processed_uri": processed_uri,
                    "total_records": total_records,
                    "record_types": unified_df['record_type'].unique().to_list()
                }
            )
        
        except (DeltaLakeError, DeltaLakeWriteError, DeltaLakeMergeError, 
                DeltaLakeReadError, StorageAuthenticationError, 
                StorageBucketNotFoundError) as e:
            logger.exception("Delta Lake processing error", extra={"ticker": ticker, "run_id": str(run_id)})
            _transition_to_failed(service, run_uuid, "DELTA_LAKE_ERROR", str(e))
            raise NonRetryableError(str(e)) from e
        
        # Step 5: Transition to DELTA_FINISHED state
        try:
            service.update_run_state(
                run_id=run_uuid,
                new_state=IngestionState.DELTA_FINISHED,
                processed_data_uri=processed_uri
            )
            logger.info("Successfully completed Delta Lake processing", extra={"run_id": run_id, "ticker": ticker})
            
        except (InvalidStateTransitionError, IngestionRunNotFoundError, DatabaseError) as e:
            logger.exception("Failed to update run state", extra={"run_id": str(run_id)})
            raise NonRetryableError(f"Failed to update run state: {str(e)}") from e
    
        # Step 6: Transition to DONE and trigger metadata update task
        try:
            # Update state to DONE
            service.update_run_state(
                run_id=run_uuid,
                new_state=IngestionState.DONE
            )
            logger.info(
                "Transitioned to DONE state",
                extra={"run_id": run_id, "ticker": ticker}
            )
            
            # Queue metadata update task on queue_for_fetch (low priority)
            from workers.tasks.update_stock_metadata import update_stock_metadata
            
            update_stock_metadata.delay(ticker)
            
            logger.info(
                "Queued metadata update task",
                extra={"run_id": run_id, "ticker": ticker}
            )

            # Task already succeeded at DELTA_FINISHED, so return success
            return ProcessDeltaLakeResult(
                run_id=str(run_id),
                ticker=ticker,
                state=IngestionState.DONE,
                skipped=False,
                processed_uri=processed_uri,
                records_processed=total_records
            )
        except Exception as e:
            # Log error but don't fail the task - metadata update is not critical
            logger.exception(
                "Failed to transition to DONE or queue metadata task",
                extra={"run_id": run_id, "ticker": ticker}
            )
            # Return success with DELTA_FINISHED state since Delta processing succeeded
            return ProcessDeltaLakeResult(
                run_id=str(run_id),
                ticker=ticker,
                state=IngestionState.DELTA_FINISHED,
                skipped=False,
                processed_uri=processed_uri,
                records_processed=total_records
            )
            
    except NonRetryableError:
        raise
    
    except Exception as e:
        logger.exception(
            "Unexpected error in Delta Lake processing task",
            extra={"run_id": run_id, "ticker": ticker}
        )
        
        try:
            _transition_to_failed(
                service, run_uuid,
                "UNEXPECTED_ERROR",
                f"Unexpected error: {type(e).__name__}: {str(e)}"
            )
        except Exception:
            logger.exception("Failed to transition to FAILED state", extra={"run_id": str(run_id)})
        
        raise NonRetryableError(f"Unexpected error: {str(e)}") from e


def _download_from_storage(data_uri: str) -> bytes:
    """
    Download JSON file from S3/MinIO storage.
    
    Args:
        data_uri: S3 URI (e.g., s3://bucket/path/to/file.json)
        
    Returns:
        bytes: The JSON data as bytes
        
    Raises:
        StorageAuthenticationError: If S3/MinIO authentication fails
        StorageBucketNotFoundError: If bucket not found
        StorageConnectionError: If connection to S3/MinIO fails
        InvalidDataFormatError: If file is empty or invalid
    """
    try:
        # Parse S3 URI
        if not data_uri.startswith('s3://'):
            raise InvalidDataFormatError(f"Invalid S3 URI format: {data_uri}")
        
        # Extract bucket and key from URI
        uri_parts = data_uri[5:].split('/', 1)
        if len(uri_parts) != 2:
            raise InvalidDataFormatError(f"Invalid S3 URI format: {data_uri}")
        
        bucket_name, object_key = uri_parts
        
        # Initialize MinIO client
        parsed = urlparse(settings.AWS_S3_ENDPOINT_URL)
        endpoint = parsed.netloc or parsed.path
        secure = parsed.scheme == 'https'
        
        client = Minio(
            endpoint=endpoint,
            access_key=settings.AWS_ACCESS_KEY_ID,
            secret_key=settings.AWS_SECRET_ACCESS_KEY,
            secure=secure
        )
        
        # Ensure bucket exists
        if not client.bucket_exists(bucket_name):
            raise StorageBucketNotFoundError(f"Bucket {bucket_name} not found")
        
        # Download file
        response = client.get_object(bucket_name, object_key)
        
        try:
            file_data = response.read()
            
            if len(file_data) == 0:
                raise InvalidDataFormatError("Downloaded file is empty")
            
            return file_data
        
        finally:
            response.close()
            response.release_conn()
    
    except S3Error as e:
        if e.code in ['InvalidAccessKeyId', 'SignatureDoesNotMatch', 'AccessDenied']:
            raise StorageAuthenticationError(
                f"S3/MinIO authentication failed: {e.code}"
            ) from e
        elif e.code == 'NoSuchBucket':
            raise StorageBucketNotFoundError(f"Bucket not found: {e.code}") from e
        else:
            raise StorageConnectionError(f"S3/MinIO error: {e.code}") from e
    
    except MinioException as e:
        logger.exception("MinIO connection error")
        raise StorageConnectionError(f"MinIO connection error: {str(e)}") from e
    
    except Exception as e:
        logger.exception("Unexpected storage download error")
        raise StorageConnectionError(f"Unexpected error downloading from storage: {str(e)}") from e


def _transform_data_to_polars(data: Dict[str, Any], ticker: str) -> pl.DataFrame:
    """
    Transform JSON data into a unified Polars DataFrame for the stocks table.
    
    Combines three types of data into a single DataFrame:
    1. Financial time series data (quarterly metrics) - record_type='financials'
    2. Metadata (company information) - record_type='metadata' 
    3. Trailing Twelve Month (TTM) data - record_type='ttm'
    
    Each record includes:
    - ticker: Stock ticker symbol
    - record_type: Type of data ('financials', 'metadata', 'ttm')
    - period_end_date: Date for time-series data (null for metadata)
    - All metrics/fields (with nulls where not applicable)
    
    Args:
        data: Parsed JSON data
        ticker: Stock ticker symbol
        
    Returns:
        pl.DataFrame: Unified DataFrame with all data types
        
    Raises:
        InvalidDataFormatError: If data structure is invalid
        ValueError: If data cannot be transformed
    """
    # Define null string representations (common in financial data)
    # Using frozenset for O(1) lookup performance
    NULL_STRINGS = frozenset({"N/A", "NA", "NULL", "NONE", "-"})
    
    all_records = []
    
    # Validate data structure
    if not isinstance(data, dict):
        raise InvalidDataFormatError("Data must be a dictionary")
    
    if 'data' not in data:
        raise InvalidDataFormatError("Missing 'data' key in JSON")
    
    data_section = data['data']
    
    # Process financial time series data
    if 'financials' in data_section and 'quarterly' in data_section['financials']:
        quarterly = data_section['financials']['quarterly']
        
        # Validate period_end_date exists
        if 'period_end_date' not in quarterly:
            logger.warning("No period_end_date found in quarterly data", extra={"ticker": ticker})
        else:
            period_dates = quarterly['period_end_date']
            
            if len(period_dates) > 0:
                # Build records dynamically from all metrics
                for idx, period_date in enumerate(period_dates):
                    record = {
                        'ticker': ticker,
                        'record_type': 'financials',
                        'period_end_date': period_date,
                    }
                    
                    # Add all other metrics dynamically
                    for metric_name, metric_values in quarterly.items():
                        if metric_name != 'period_end_date' and metric_name != 'roic_5yr_avg':
                            # Handle case where array might be shorter than period_dates
                            if isinstance(metric_values, list) and idx < len(metric_values):
                                value = metric_values[idx]
                                # Normalize null strings to None during record building
                                # This prevents Polars schema inference errors (O(1) hash lookup)
                                if isinstance(value, str):
                                    stripped = value.strip().upper()
                                    record[metric_name] = None if stripped in NULL_STRINGS else value
                                else:
                                    record[metric_name] = value
                            else:
                                record[metric_name] = None
                    
                    all_records.append(record)
                
                logger.info(
                    "Transformed financial data",
                    extra={
                        "ticker": ticker,
                        "record_type": "financials",
                        "rows": len(period_dates)
                    }
                )
    
    # Process metadata
    if 'metadata' in data_section:
        metadata = data_section['metadata']
        
        if isinstance(metadata, dict) and len(metadata) > 0:
            # Add ticker, record_type, and null period_end_date for metadata
            # Normalize null strings during record building
            metadata_record = {
                'ticker': ticker,
                'record_type': 'metadata',
                'period_end_date': None,  # Metadata has no time dimension
            }
            
            # Add metadata fields, normalizing null strings
            for key, value in metadata.items():
                if isinstance(value, str):
                    stripped = value.strip().upper()
                    metadata_record[key] = None if stripped in NULL_STRINGS else value
                else:
                    metadata_record[key] = value
            
            all_records.append(metadata_record)
            
            logger.info(
                "Transformed metadata",
                extra={
                    "ticker": ticker,
                    "record_type": "metadata",
                    "fields": len(metadata)
                }
            )
    
    # Process Trailing Twelve Month (TTM) data
    if 'financials' in data_section and 'ttm' in data_section['financials']:
        ttm = data_section['financials']['ttm']
        
        if isinstance(ttm, dict) and len(ttm) > 0:
            # Get the latest quarterly period_end_date to replace "TTM" placeholder
            latest_period_date = None
            if ('quarterly' in data_section['financials'] and 
                'period_end_date' in data_section['financials']['quarterly']):
                period_dates = data_section['financials']['quarterly']['period_end_date']
                if isinstance(period_dates, list) and len(period_dates) > 0:
                    # Get the last (most recent) period_end_date
                    latest_period_date = period_dates[-1]
            
            if latest_period_date is None:
                logger.warning(
                    "No quarterly period_end_date found, cannot process TTM data",
                    extra={"ticker": ticker}
                )
            else:
                # Build TTM record with actual period_end_date
                ttm_record = {
                    'ticker': ticker,
                    'record_type': 'ttm'
                }
                
                for metric_name, metric_value in ttm.items():
                    # Replace "TTM" placeholder with actual latest quarterly date
                    if metric_name == 'period_end_date' and metric_value == "TTM":
                        ttm_record[metric_name] = latest_period_date
                    else:
                        # Normalize null strings during record building
                        if isinstance(metric_value, str):
                            stripped = metric_value.strip().upper()
                            ttm_record[metric_name] = None if stripped in NULL_STRINGS else metric_value
                        else:
                            ttm_record[metric_name] = metric_value
                
                all_records.append(ttm_record)
                
                logger.info(
                    "Transformed TTM data",
                    extra={
                        "ticker": ticker,
                        "record_type": "ttm",
                        "period_end_date": latest_period_date
                    }
                )
    
    if len(all_records) == 0:
        raise InvalidDataFormatError("No valid financial, metadata, or TTM data found in JSON")
    
    # Create unified DataFrame from all records
    # Null strings have been normalized to None during record building (Python preprocessing)
    # This allows fast default schema inference without type conflicts
    unified_df = pl.DataFrame(all_records)  # Uses default infer_schema_length=100 (fast)
    
    # Convert all integer numeric types to Float64 for consistency and decimal support
    # This prevents type casting errors when merging with Delta Lake tables
    # Float64 has sufficient precision (53 bits) for all financial metrics
    key_columns = {'ticker', 'record_type', 'period_end_date'}
    schema = unified_df.schema
    
    # Build type coercion expressions - convert all integer types to Float64
    # Also handle Null columns (all-null columns from normalized "N/A" strings)
    type_coercions = []
    for col in unified_df.columns:
        if col in key_columns:
            # Keep key columns as-is
            type_coercions.append(pl.col(col))
        else:
            col_dtype = schema[col]
            # Convert all integer types to Float64 for consistency and decimal support
            # This handles cases where some values are integers and others are decimals
            if col_dtype in (pl.Int64, pl.Int32, pl.Int16, pl.Int8, pl.UInt64, pl.UInt32, pl.UInt16, pl.UInt8):
                type_coercions.append(
                    pl.col(col).cast(pl.Float64, strict=False).alias(col)
                )
            elif col_dtype == pl.Null:
                # Handle all-null columns (Delta Lake doesn't support Null type)
                # Cast to String for safety - metadata fields may be null initially but
                # have string values in subsequent batches. String type prevents type
                # mismatch errors during Delta Lake merges (e.g., cusip, sector fields)
                type_coercions.append(
                    pl.col(col).cast(pl.Utf8, strict=False).alias(col)
                )
            else:
                # Keep other types (strings, booleans, etc.) as-is
                type_coercions.append(pl.col(col))
    
    # Apply type coercions in a single vectorized operation
    unified_df = unified_df.select(type_coercions)
    
    logger.info(
        "Created unified DataFrame for stocks table (nulls pre-normalized, numeric types cast to Float64, Null types cast to String)",
        extra={
            "ticker": ticker,
            "total_rows": len(unified_df),
            "columns": len(unified_df.columns),
            "record_types": unified_df['record_type'].unique().to_list()
        }
    )
    
    return unified_df


def _build_storage_options() -> Dict[str, str]:
    """
    Build storage options dictionary for Delta Lake S3 access.
    
    Returns:
        Dict with AWS credentials and endpoint configuration
    """
    parsed = urlparse(settings.AWS_S3_ENDPOINT_URL)
    endpoint = parsed.netloc or parsed.path
    
    # Build storage options for deltalake library
    storage_options = {
        'AWS_ACCESS_KEY_ID': settings.AWS_ACCESS_KEY_ID,
        'AWS_SECRET_ACCESS_KEY': settings.AWS_SECRET_ACCESS_KEY,
        'AWS_ENDPOINT_URL': settings.AWS_S3_ENDPOINT_URL,
        'AWS_REGION': settings.AWS_S3_REGION_NAME or 'us-east-1',
        'AWS_ALLOW_HTTP': 'true',  # Required for HTTP endpoints (MinIO)
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',  # Required for MinIO compatibility
        "conditional_put": "etag",
    }
    
    return storage_options


def _process_stocks_table(
    ticker: str,
    df: pl.DataFrame,
    storage_options: Dict[str, str]
) -> str:
    """
    Process stock data into the unified Delta Lake stocks table.
    
    Creates a new table on first write or merges data into existing table.
    The table contains all data types (financials, metadata, ttm) with a
    composite key of (ticker, record_type, period_end_date).
    
    Merge behavior by record_type:
    - financials: Match on ticker + record_type + period_end_date
    - ttm: Match on ticker + record_type + period_end_date  
    - metadata: Match on ticker + record_type (period_end_date is null)
    
    This unified approach significantly reduces operational overhead for
    query engines like Trino by consolidating per-ticker tables into a
    single queryable table.
    
    Args:
        ticker: Stock ticker symbol (for logging)
        df: Polars DataFrame with unified stock data (includes record_type column)
        storage_options: S3 storage options for Delta Lake
        
    Returns:
        str: S3 URI of the unified stocks Delta Lake table
        
    Raises:
        DeltaLakeWriteError: If table creation fails
        DeltaLakeMergeError: If merge operation fails
    """
    # Use single unified table path
    table_path = f"s3://{settings.STOCK_DELTA_LAKE_BUCKET}/stocks"
    
    try:
        # Check if table exists
        table_exists = False
        try:
            dt = DeltaTable(table_path, storage_options=storage_options)
            table_exists = True
            logger.info(
                "Unified stocks table exists, will merge data",
                extra={"ticker": ticker, "table_path": table_path}
            )
        except TableNotFoundError:
            table_exists = False
            logger.info(
                "Unified stocks table does not exist, will create new",
                extra={"ticker": ticker, "table_path": table_path}
            )
        
        # Convert Polars to PyArrow for deltalake library
        arrow_table = df.to_arrow()
        
        if table_exists:
            # Merge data into existing table
            # Composite key: ticker + record_type + period_end_date
            # For metadata records (period_end_date is null), the merge still works
            # because SQL handles null equality in the predicate
            merge_predicate = (
                "target.ticker = source.ticker AND "
                "target.record_type = source.record_type AND "
                "(target.period_end_date = source.period_end_date OR "
                "(target.period_end_date IS NULL AND source.period_end_date IS NULL))"
            )
            
            (
                dt.merge(
                    source=arrow_table,
                    predicate=merge_predicate,
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            
            logger.info(
                "Successfully merged data into unified stocks table",
                extra={
                    "ticker": ticker,
                    "rows": len(df),
                    "record_types": df['record_type'].unique().to_list()
                }
            )
        else:
            # Create new table
            write_deltalake(
                table_path,
                arrow_table,
                mode="error",  # Fail if table exists (shouldn't happen due to check above)
                storage_options=storage_options,
            )
            
            logger.info(
                "Successfully created unified stocks table",
                extra={
                    "ticker": ticker,
                    "rows": len(df),
                    "record_types": df['record_type'].unique().to_list()
                }
            )
        
        return table_path
    
    except TableNotFoundError as e:
        raise DeltaLakeError(f"Delta Lake table error: {str(e)}") from e
    
    except Exception as e:
        if table_exists:
            raise DeltaLakeMergeError(
                f"Failed to merge data into unified stocks table: {str(e)}"
            ) from e
        else:
            raise DeltaLakeWriteError(
                f"Failed to create unified stocks table: {str(e)}"
            ) from e


def _transition_to_failed(
    service: StockIngestionService,
    run_id: uuid.UUID,
    error_code: str,
    error_message: str
) -> None:
    """
    Transition a run to FAILED state.
    
    Args:
        service: StockIngestionService instance
        run_id: UUID of the run
        error_code: Error code for the failure
        error_message: Detailed error message
    """
    try:
        service.update_run_state(
            run_id=run_id,
            new_state=IngestionState.FAILED,
            error_code=error_code,
            error_message=error_message
        )
        logger.info("Transitioned run to FAILED", extra={"run_id": str(run_id), "error_code": error_code})
    except InvalidStateTransitionError:
        logger.warning("Could not transition run to FAILED (already in terminal state?)", extra={"run_id": str(run_id)})
    except Exception:
        logger.exception("Failed to transition to FAILED state", extra={"run_id": str(run_id)})

