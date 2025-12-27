"""
Celery task for updating Stock model metadata from Delta Lake.

This task is triggered after process_delta_lake completes successfully.
It performs the following steps:
1. Reads metadata from Delta Lake using Polars
2. Uses transactions with select_for_update to lock the Stock row
3. Updates the Stock model with metadata fields
4. Handles RetryableError for database lock timeouts

This task runs on the queue_for_fetch queue as it's not a priority operation
and can be processed asynchronously after the main ingestion pipeline completes.
"""

import logging
import uuid
from typing import Dict, Any, TypedDict, NotRequired

import polars as pl
from celery import shared_task
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from django.conf import settings
from django.db import DatabaseError, transaction
from django.db.utils import OperationalError

from api.models import Exchange, Stock
from api.services.stock_ingestion_service import StockNotFoundError
from workers.exceptions import (
    DeltaLakeReadError,
    InvalidDataFormatError,
    NonRetryableError,
    RetryableError,
    StorageAuthenticationError,
)
from workers.tasks.base import BaseTask


logger = logging.getLogger(__name__)


class UpdateStockMetadataResult(TypedDict):
    """
    Result object returned by the update_stock_metadata task.
    
    Attributes:
        stock_id: UUID of the Stock that was updated
        ticker: Stock ticker symbol
        updated: Whether metadata was successfully updated
        skipped: Whether the task was skipped (e.g., no metadata found)
        reason: Reason for skipping (optional, only present when skipped=True)
        fields_updated: List of fields that were updated (optional)
    """
    stock_id: str
    ticker: str
    updated: bool
    skipped: bool
    reason: NotRequired[str]
    fields_updated: NotRequired[list[str]]


@shared_task(bind=True, base=BaseTask, name='workers.tasks.update_stock_metadata')
def update_stock_metadata(self, ticker: str) -> UpdateStockMetadataResult:
    """
    Update Stock model metadata from Delta Lake.
    
    This task implements the following workflow:
    1. Validate ticker exists in database
    2. Read metadata from Delta Lake using Polars
    3. Use atomic transaction with select_for_update to lock the Stock row
    4. Update metadata fields on Stock model
    
    On database lock timeout:
    - Raises RetryableError to trigger automatic retry with backoff
    
    On other failures:
    - Non-retryable errors: Do not retry (log error and return failure result)
    
    Args:
        ticker: Stock ticker symbol to update metadata for
        
    Returns:
        UpdateStockMetadataResult: Result object with stock_id, ticker, updated status,
            skipped status, reason (optional), and fields_updated (optional)
        
    Raises:
        RetryableError: For database lock timeouts (will trigger automatic retry)
        NonRetryableError: For all other errors
    """
    ticker = ticker.strip().upper()
    logger.info("Starting metadata update task", extra={"ticker": ticker})
    
    try:
        # Step 1: Validate stock exists and get ID
        try:
            # Don't lock here - just check existence
            stock = Stock.objects.get(ticker=ticker)
            stock_id = stock.id
            
            logger.info(
                "Found stock for metadata update",
                extra={"ticker": ticker, "stock_id": str(stock_id)}
            )
        
        except Stock.DoesNotExist as e:
            logger.error("Stock not found", extra={"ticker": ticker})
            raise NonRetryableError(f"Stock {ticker} not found in database") from e
        
        # Step 2: Read metadata from Delta Lake
        try:
            logger.info("Reading metadata from Delta Lake", extra={"ticker": ticker})
            
            metadata_dict = _read_metadata_from_delta_lake(ticker)
            
            if not metadata_dict:
                logger.info(
                    "No metadata found in Delta Lake for ticker",
                    extra={"ticker": ticker}
                )
                return UpdateStockMetadataResult(
                    stock_id=str(stock_id),
                    ticker=ticker,
                    updated=False,
                    skipped=True,
                    reason='no_metadata_in_delta_lake'
                )
            
            logger.info(
                "Successfully read metadata from Delta Lake",
                extra={"ticker": ticker, "fields": list(metadata_dict.keys())}
            )
        
        except (DeltaLakeReadError, StorageAuthenticationError, InvalidDataFormatError) as e:
            logger.exception("Error reading metadata from Delta Lake", extra={"ticker": ticker})
            raise NonRetryableError(str(e)) from e
        
        # Step 3: Update Stock model with metadata using transaction and row locking
        try:
            logger.info("Updating Stock metadata with transaction", extra={"ticker": ticker})
            
            fields_updated = _update_stock_with_metadata(stock_id, metadata_dict)
            
            logger.info(
                "Successfully updated Stock metadata",
                extra={
                    "ticker": ticker,
                    "stock_id": str(stock_id),
                    "fields_updated": fields_updated
                }
            )
            
            return UpdateStockMetadataResult(
                stock_id=str(stock_id),
                ticker=ticker,
                updated=True,
                skipped=False,
                fields_updated=fields_updated
            )
        
        except OperationalError as e:
            # Database lock timeout - this is retryable
            if 'lock' in str(e).lower() or 'timeout' in str(e).lower():
                logger.warning(
                    "Database lock timeout, will retry",
                    extra={"ticker": ticker, "stock_id": str(stock_id)}
                )
                raise RetryableError(
                    f"Database lock timeout updating Stock {ticker}: {str(e)}"
                ) from e
            else:
                # Other operational errors are not retryable
                logger.exception("Operational error updating Stock", extra={"ticker": ticker})
                raise NonRetryableError(f"Operational error: {str(e)}") from e
        
        except DatabaseError as e:
            logger.exception("Database error updating Stock", extra={"ticker": ticker})
            raise NonRetryableError(f"Database error: {str(e)}") from e
    
    except RetryableError:
        # Re-raise retryable errors to trigger Celery retry
        raise
    
    except NonRetryableError:
        # Don't retry non-retryable errors
        raise
    
    except Exception as e:
        logger.exception(
            "Unexpected error in metadata update task",
            extra={"ticker": ticker}
        )
        raise NonRetryableError(f"Unexpected error: {str(e)}") from e


def _read_metadata_from_delta_lake(ticker: str) -> Dict[str, Any] | None:
    """
    Read metadata for a ticker from the Delta Lake stocks table.
    
    Queries the unified stocks table for records with:
    - ticker = <ticker>
    - record_type = 'metadata'
    
    Args:
        ticker: Stock ticker symbol
        
    Returns:
        Dict with metadata fields, or None if no metadata found
        
    Raises:
        DeltaLakeReadError: If Delta Lake read fails
        StorageAuthenticationError: If authentication fails
        InvalidDataFormatError: If data format is invalid
    """
    # Build storage options
    storage_options = {
        'AWS_ACCESS_KEY_ID': settings.AWS_ACCESS_KEY_ID,
        'AWS_SECRET_ACCESS_KEY': settings.AWS_SECRET_ACCESS_KEY,
        'AWS_ENDPOINT_URL': settings.AWS_S3_ENDPOINT_URL,
        'AWS_REGION': settings.AWS_S3_REGION_NAME or 'us-east-1',
        'AWS_ALLOW_HTTP': 'true',
        'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
        "conditional_put": "etag",
    }
    
    # Path to unified stocks table
    table_path = f"s3://{settings.STOCK_DELTA_LAKE_BUCKET}/stocks"
    
    try:
        # Read table into Polars DataFrame with predicate pushdown
        # Using scan_delta (lazy) + filter + collect enables predicate pushdown
        # to Delta Lake, avoiding loading the entire table into memory
        metadata_df = (
            pl.scan_delta(table_path, storage_options=storage_options)
            .filter(
                (pl.col("ticker") == ticker) & (pl.col("record_type") == "metadata")
            )
            .collect()
        )
        
        if len(metadata_df) == 0:
            logger.info(
                "No metadata record found for ticker in Delta Lake",
                extra={"ticker": ticker}
            )
            return None
        
        if len(metadata_df) > 1:
            logger.warning(
                "Multiple metadata records found for ticker, using first",
                extra={"ticker": ticker, "count": len(metadata_df)}
            )
        
        # Get first record as dict
        metadata_record = metadata_df.row(0, named=True)
        
        # Extract only the metadata fields we want to update
        # Exclude ticker, record_type, and period_end_date
        metadata_fields = {
            'sector': metadata_record.get('sector'),
            'name': metadata_record.get('name'),
            'exchange': metadata_record.get('exchange'),
            'country': metadata_record.get('country'),
            'subindustry': metadata_record.get('subindustry'),
            'morningstar_sector': metadata_record.get('morningstar_sector'),
            'morningstar_industry': metadata_record.get('morningstar_industry'),
            'industry': metadata_record.get('industry'),
            'description': metadata_record.get('description'),
        }
        
        # Remove None values (fields not present in Delta Lake)
        metadata_fields = {k: v for k, v in metadata_fields.items() if v is not None}
        
        logger.info(
            "Read metadata from Delta Lake",
            extra={"ticker": ticker, "fields": list(metadata_fields.keys())}
        )
        
        return metadata_fields
    
    except TableNotFoundError as e:
        logger.warning(
            "Delta Lake stocks table not found",
            extra={"ticker": ticker, "table_path": table_path}
        )
        raise DeltaLakeReadError(f"Delta Lake table not found: {table_path}") from e
    
    except Exception as e:
        logger.exception("Error reading from Delta Lake", extra={"ticker": ticker})
        raise DeltaLakeReadError(f"Failed to read metadata from Delta Lake: {str(e)}") from e


def _update_stock_with_metadata(
    stock_id: uuid.UUID,
    metadata_dict: Dict[str, Any]
) -> list[str]:
    """
    Update Stock model with metadata using atomic transaction and row locking.
    
    Uses select_for_update to acquire a row-level lock, preventing concurrent
    updates to the same Stock record. If the lock cannot be acquired (e.g., another
    task is updating the same Stock), an OperationalError will be raised which
    should be caught and converted to RetryableError by the caller.
    
    For the 'exchange' field, this function:
    1. Extracts the exchange name from metadata_dict
    2. Normalizes it (strip and uppercase)
    3. Uses Exchange.objects.get_or_create() to get or create the Exchange
    4. Sets stock.exchange to the Exchange instance (ForeignKey)
    
    Args:
        stock_id: UUID of the Stock to update
        metadata_dict: Dictionary of metadata fields to update
        
    Returns:
        List of field names that were updated
        
    Raises:
        OperationalError: If database lock cannot be acquired (retryable)
        DatabaseError: For other database errors (non-retryable)
    """
    fields_updated = []
    
    with transaction.atomic():
        # Acquire row-level lock
        # If lock cannot be acquired, this will raise OperationalError
        stock = Stock.objects.select_for_update().get(id=stock_id)
        
        # Update fields that are present in metadata_dict
        for field_name, value in metadata_dict.items():
            # Special handling for exchange field (ForeignKey to Exchange model)
            if field_name == 'exchange' and value:
                # Normalize exchange name (strip and uppercase)
                normalized_exchange_name = value.strip().upper()
                
                # Get or create Exchange instance
                # get_or_create is atomic and handles race conditions
                exchange, created = Exchange.objects.get_or_create(
                    name=normalized_exchange_name
                )
                
                logger.info(
                    "Exchange get_or_create completed",
                    extra={
                        "exchange_name": normalized_exchange_name,
                        "exchange_id": str(exchange.id),
                        "exchange_created": created,
                        "stock_id": str(stock_id)
                    }
                )
                
                # Set the ForeignKey
                stock.exchange = exchange
                fields_updated.append(field_name)
            elif hasattr(stock, field_name):
                setattr(stock, field_name, value)
                fields_updated.append(field_name)
            else:
                logger.warning(
                    "Field not found on Stock model",
                    extra={"field": field_name, "stock_id": str(stock_id)}
                )
        
        if fields_updated:
            stock.save(update_fields=fields_updated + ['updated_at'])
            logger.info(
                "Updated Stock fields",
                extra={
                    "stock_id": str(stock_id),
                    "ticker": stock.ticker,
                    "fields": fields_updated
                }
            )
        else:
            logger.info(
                "No fields to update",
                extra={"stock_id": str(stock_id), "ticker": stock.ticker}
            )
    
    return fields_updated

