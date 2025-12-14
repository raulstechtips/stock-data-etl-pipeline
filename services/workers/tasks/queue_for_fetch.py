"""
Celery task for fetching stock data from external API and uploading to S3/MinIO.

This task is triggered by the API service after a stock ingestion run is
queued. It performs the following steps:
1. Validates the run is in a valid state to begin processing
2. Fetches stock data from an external API (returns JSON data)
3. Uploads the JSON file to S3/MinIO storage
4. Updates the run state to FETCHED on success or FAILED on error
"""

import io
import logging
import uuid
from dataclasses import dataclass
from typing import Optional

import requests
from celery import Task, shared_task
from django.conf import settings
from django.db import DatabaseError
from minio import Minio
from minio.error import MinioException, S3Error
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout

from api.models import IngestionState
from api.services.stock_ingestion_service import (
    IngestionRunNotFoundError,
    InvalidStateTransitionError,
    StockIngestionService,
)
from workers.exceptions import (
    APIAuthenticationError,
    APIFetchError,
    APINotFoundError,
    APIRateLimitError,
    APITimeoutError,
    InvalidDataFormatError,
    InvalidStateError,
    NonRetryableError,
    RetryableError,
    StorageAuthenticationError,
    StorageConnectionError,
    StorageUploadError,
    StorageBucketNotFoundError,
)


logger = logging.getLogger(__name__)


@dataclass
class FetchStockDataResult:
    """
    Result object returned by the fetch_stock_data task.
    
    Attributes:
        run_id: UUID of the StockIngestionRun that was processed
        ticker: Stock ticker symbol
        state: Current state of the run after processing
        skipped: Whether the task was skipped (idempotency check)
        data_uri: URI where the raw data was stored (optional)
        reason: Reason for skipping (optional, only present when skipped=True)
    """
    run_id: str
    ticker: str
    state: str
    skipped: bool
    data_uri: Optional[str] = None
    reason: Optional[str] = None


class FetchTask(Task):
    """
    Custom Celery task class with retry configuration.
    
    This provides a base class for fetch tasks with automatic retry
    logic and proper error handling.
    """
    
    # Retry configuration
    autoretry_for = (RetryableError,)
    retry_kwargs = {'max_retries': 3}
    retry_backoff = True  # Exponential backoff
    retry_backoff_max = 600  # Max 10 minutes between retries
    retry_jitter = True  # Add randomness to prevent thundering herd


@shared_task(bind=True, base=FetchTask, name='workers.tasks.fetch_stock_data')
def fetch_stock_data(self, run_id: str, ticker: str) -> FetchStockDataResult:
    """
    Fetch stock data from external API and upload to S3/MinIO.
    
    This task implements the following workflow:
    1. Validate run state (must be QUEUED_FOR_FETCH)
    2. Transition to FETCHING state
    3. Fetch .xlsx file from external API
    4. Upload file to S3/MinIO
    5. Transition to FETCHED state with data URI
    
    On failure:
    - Retryable errors: Automatically retry up to 3 times
    - Non-retryable errors: Immediately transition to FAILED
    - Max retries exceeded: Transition to FAILED
    
    Args:
        run_id: UUID of the StockIngestionRun to process
        ticker: Stock ticker symbol (for logging and API calls)
        
    Returns:
        FetchStockDataResult: Result object with run_id, ticker, state, skipped status,
            data_uri (optional), and reason (optional)
        
    Raises:
        RetryableError: For transient errors that should be retried
        NonRetryableError: For permanent errors that should not be retried
    """
    service = StockIngestionService()
    run_uuid = uuid.UUID(run_id)
    
    logger.info(f"Starting fetch task for run {run_id}, ticker {ticker}")
    
    try:
        # Step 1: Validate state and transition to FETCHING
        try:
            run = service.get_run_by_id(run_uuid)
            
            # Idempotency check: If already FETCHED or beyond, task is complete
            if run.state in [IngestionState.FETCHED, IngestionState.QUEUED_FOR_SPARK,
                            IngestionState.SPARK_RUNNING, IngestionState.SPARK_FINISHED,
                            IngestionState.DONE]:
                logger.info(
                    f"Run {run_id} already in state {run.state}, skipping fetch. "
                    f"This is likely a duplicate task execution."
                )
                return FetchStockDataResult(
                    run_id=str(run_id),
                    ticker=ticker,
                    state=run.state,
                    skipped=True,
                    data_uri=run.raw_data_uri,
                    reason='already_processed'
                )
            
            # Check if in FAILED state (should not retry from API)
            if run.state == IngestionState.FAILED:
                logger.warning(
                    f"Run {run_id} is in FAILED state, cannot proceed with fetch"
                )
                raise InvalidStateError(
                    f"Run {run_id} is in FAILED state and cannot be retried"
                )
            
            # Must be in QUEUED_FOR_FETCH to start, or FETCHING if this is a retry
            if run.state not in [IngestionState.QUEUED_FOR_FETCH, IngestionState.FETCHING]:
                logger.error(
                    f"Run {run_id} is in invalid state {run.state} for fetch task"
                )
                raise InvalidStateError(
                    f"Run {run_id} must be in QUEUED_FOR_FETCH or FETCHING state, "
                    f"but is in {run.state}"
                )
            
            # Transition to FETCHING (if not already)
            if run.state == IngestionState.QUEUED_FOR_FETCH:
                service.update_run_state(
                    run_id=run_uuid,
                    new_state=IngestionState.FETCHING
                )
                logger.info(f"Transitioned run {run_id} to FETCHING state")
        
        except IngestionRunNotFoundError as e:
            logger.exception("ingestion_run_not_found", extra={"run_id": str(run_id)})
            raise NonRetryableError(f"Ingestion run {run_id} not found") from e
        
        except (InvalidStateTransitionError, InvalidStateError) as e:
            logger.exception("(invalid_state_transition_error, invalid_state)", extra={"run_id": str(run_id)})
            raise NonRetryableError(str(e)) from e
        
        # Step 2: Fetch data from external API
        try:
            logger.info(f"Fetching stock data for {ticker} from {settings.STOCK_DATA_API_URL}")
            
            file_data = _fetch_from_api(ticker)
            
            logger.info(
                f"Successfully fetched {len(file_data)} bytes for {ticker}"
            )
        
        except (APIAuthenticationError, APINotFoundError, InvalidDataFormatError) as e:
            # Non-retryable API errors
            logger.exception("(api_authentication_error, api_not_found, invalid_data_format)", extra={"ticker": str(ticker)})
            _transition_to_failed(service, run_uuid, "API_ERROR", str(e))
            raise NonRetryableError(str(e)) from e
        
        except (APITimeoutError, APIFetchError, APIRateLimitError) as e:
            # Retryable API errors - will be caught by autoretry_for
            logger.warning(
                f"Retryable API error for {ticker} (attempt {self.request.retries + 1}/3): {e}"
            )
            # Check if this is the last retry
            if self.request.retries >= 2:  # 0-indexed, so 2 = 3rd attempt
                logger.error(f"Max retries exceeded for run {run_id}, transitioning to FAILED")
                _transition_to_failed(
                    service, run_uuid,
                    "MAX_RETRIES_EXCEEDED",
                    f"Failed after 3 attempts: {str(e)}"
                )
            raise  # Re-raise to trigger Celery retry
        
        # Step 3: Upload to S3/MinIO
        try:
            logger.info(f"Uploading data for {ticker} to S3/MinIO")
            
            data_uri = _upload_to_storage(ticker, run_id, file_data)
            
            logger.info(f"Successfully uploaded data for {ticker} to {data_uri}")
        
        except StorageAuthenticationError as e:
            # Non-retryable storage errors
            logger.exception("storage_authentication_error", extra={"ticker": str(ticker)})
            _transition_to_failed(service, run_uuid, "STORAGE_AUTH_ERROR", str(e))
            raise NonRetryableError(str(e)) from e
        
        except StorageBucketNotFoundError as e:
            # Non-retryable storage errors
            logger.exception("storage_bucket_not_found", extra={"ticker": str(ticker)})
            _transition_to_failed(service, run_uuid, "STORAGE_BUCKET_NOT_FOUND", str(e))
            raise NonRetryableError(str(e)) from e
        except (StorageConnectionError, StorageUploadError) as e:
            # Retryable storage errors
            logger.warning(
                f"Retryable storage error for {ticker} (attempt {self.request.retries + 1}/3): {e}"
            )
            if self.request.retries >= 2:
                logger.error(f"Max retries exceeded for run {run_id}, transitioning to FAILED")
                _transition_to_failed(
                    service, run_uuid,
                    "MAX_RETRIES_EXCEEDED",
                    f"Failed after 3 attempts: {str(e)}"
                )
            raise  # Re-raise to trigger Celery retry
        
        # Step 4: Transition to FETCHED state
        try:
            service.update_run_state(
                run_id=run_uuid,
                new_state=IngestionState.FETCHED,
                raw_data_uri=data_uri
            )
            logger.info(f"Successfully completed fetch for run {run_id}, ticker {ticker}")
            
            return FetchStockDataResult(
                run_id=str(run_id),
                ticker=ticker,
                state=IngestionState.FETCHED,
                skipped=False,
                data_uri=data_uri
            )
        
        except (InvalidStateTransitionError, IngestionRunNotFoundError, DatabaseError) as e:
            # Database errors during final state transition
            logger.exception("(invalid_state_transition_error, ingestion_run_not_found, database_error)", extra={"run_id": str(run_id)})
            # This is a critical error but shouldn't retry the entire fetch
            raise NonRetryableError(f"Failed to update run state: {str(e)}") from e
    
    except RetryableError:
        # Let retryable errors propagate so Celery can retry
        raise
    
    except NonRetryableError:
        # Don't retry non-retryable errors
        raise
    
    except Exception as e:
        # Catch any unexpected errors and transition to FAILED
        logger.exception(f"Unexpected error in fetch task for run {run_id}: {e}")
        try:
            _transition_to_failed(
                service, run_uuid,
                "UNEXPECTED_ERROR",
                f"Unexpected error: {type(e).__name__}: {str(e)}"
            )
        except Exception as state_error:
            logger.exception("failed_to_transition_to_failed", extra={"run_id": str(run_id)})
        
        raise NonRetryableError(f"Unexpected error: {str(e)}") from e


def _fetch_from_api(ticker: str) -> bytes:
    """
    Fetch stock data from external API.
    
    Args:
        ticker: Stock ticker symbol
        
    Returns:
        bytes: The JSON data as bytes
        
    Raises:
        APIAuthenticationError: If authentication fails
        APINotFoundError: If the ticker is not found
        APITimeoutError: If the request times out
        APIRateLimitError: If rate limit is exceeded
        APIFetchError: For other API errors
        InvalidDataFormatError: If response is not valid JSON data
    """
    try:
        # Build request
        url = settings.STOCK_DATA_API_URL
        headers = {}
        
        if settings.STOCK_DATA_API_KEY:
            headers['Authorization'] = f'Bearer {settings.STOCK_DATA_API_KEY}'
        
        params = {'ticker': ticker}
        
        # Make request with timeout
        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=settings.STOCK_DATA_API_TIMEOUT,  # 30 second timeout
        )
        
        # Check for specific error status codes
        if response.status_code == 401:
            raise APIAuthenticationError(
                f"API authentication failed for {ticker}"
            )
        
        if response.status_code == 404:
            raise APINotFoundError(
                f"Ticker {ticker} not found in API"
            )
        
        if response.status_code == 429:
            raise APIRateLimitError(
                f"API rate limit exceeded for {ticker}"
            )
        
        # Raise for other HTTP errors
        response.raise_for_status()
        
        # Get JSON data
        try:
            # Validate that response is valid JSON by attempting to parse it
            response.json()
        except ValueError as e:
            logger.exception("invalid_data_format_error", extra={"ticker": str(ticker)})
            raise InvalidDataFormatError(
                f"Received data is not valid JSON: {str(e)}"
            ) from e
        
        # Return raw bytes for storage
        json_data = response.content
        
        # Validate data is not empty
        if len(json_data) == 0:
            logger.exception("invalid_data_format_error", extra={"ticker": str(ticker)})
            raise InvalidDataFormatError("Received empty response from API")
        
        return json_data
    
    except Timeout as e:
        logger.exception("api_timeout_error", extra={"ticker": str(ticker)})
        raise APITimeoutError(f"API request timed out for {ticker}") from e
    
    except ConnectionError as e:
        logger.exception("api_fetch_error", extra={"ticker": str(ticker)})
        raise APIFetchError(f"Connection error fetching data for {ticker}") from e
    
    except HTTPError as e:
        if e.response.status_code >= 500:
            # Server errors are retryable
            logger.exception("api_fetch_error", extra={"ticker": str(ticker)})
            raise APIFetchError(f"API server error for {ticker}: {e}") from e
        else:
            # Client errors are not retryable (except specific ones handled above)
            logger.exception("api_fetch_error", extra={"ticker": str(ticker)})
            raise APIFetchError(f"API client error for {ticker}: {e}") from e
    
    except RequestException as e:
        logger.exception("api_fetch_error", extra={"ticker": str(ticker)})
        raise APIFetchError(f"Error fetching data for {ticker}: {e}") from e


def _upload_to_storage(ticker: str, run_id: str, file_data: bytes) -> str:
    """
    Upload JSON file to S3/MinIO storage.
    
    Args:
        ticker: Stock ticker symbol
        run_id: UUID of the ingestion run
        file_data: The JSON file data to upload (as bytes)
        
    Returns:
        str: The S3/MinIO URI of the uploaded file
        
    Raises:
        StorageAuthenticationError: If S3/MinIO authentication fails
        StorageConnectionError: If connection to S3/MinIO fails
        StorageUploadError: For other upload errors
    """
    try:
        # Initialize MinIO client
        client = Minio(
            endpoint=settings.AWS_S3_ENDPOINT_URL.replace('http://', '').replace('https://', ''),
            access_key=settings.AWS_ACCESS_KEY_ID,
            secret_key=settings.AWS_SECRET_ACCESS_KEY,
            secure=settings.AWS_S3_ENDPOINT_URL.startswith('https://')
        )
        
        # Ensure bucket exists
        bucket_name = settings.STOCK_RAW_DATA_BUCKET
        if not client.bucket_exists(bucket_name):
            raise StorageBucketNotFoundError(f"Bucket {bucket_name} not found")
        
        # Generate object key for JSON file
        object_key = f"{ticker}/{run_id}.json"
        
        # Upload JSON file
        file_stream = io.BytesIO(file_data)
        client.put_object(
            bucket_name,
            object_key,
            file_stream,
            length=len(file_data),
            content_type='application/json'
        )
        
        # Build URI
        data_uri = f"s3://{bucket_name}/{object_key}"
        
        return data_uri
    
    except S3Error as e:
        if e.code in ['InvalidAccessKeyId', 'SignatureDoesNotMatch', 'AccessDenied']:
            raise StorageAuthenticationError(
                f"S3/MinIO authentication failed: {e.code}"
            ) from e
        else:
            raise StorageUploadError(f"S3/MinIO error uploading file: {e.code}") from e
    
    except MinioException as e:
        logger.exception("storage_connection_error", extra={"ticker": str(ticker)})
        raise StorageConnectionError(f"MinIO connection error: {str(e)}") from e
    
    except Exception as e:
        logger.exception("storage_upload_error", extra={"ticker": str(ticker)})
        raise StorageUploadError(f"Unexpected error uploading to storage: {str(e)}") from e


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
        logger.info(f"Transitioned run {run_id} to FAILED: {error_code}")
    except InvalidStateTransitionError:
        # Run might already be in FAILED state from another process
        logger.warning(f"Could not transition run {run_id} to FAILED (already in terminal state?)")
    except Exception as e:
        logger.exception("failed_to_transition_to_failed", extra={"run_id": str(run_id)})
