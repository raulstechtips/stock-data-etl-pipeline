"""
Tests for Celery worker tasks.

This module contains comprehensive tests for:
- fetch_stock_data task
- Error handling and retry logic
- State transitions
- Idempotency checks
"""

import io
import uuid
from unittest.mock import Mock, MagicMock, patch

from celery.exceptions import Retry
from django.test import TestCase, TransactionTestCase, override_settings
from requests.exceptions import ConnectionError, HTTPError, Timeout

from api.models import IngestionState, Stock, StockIngestionRun
from api.services.stock_ingestion_service import StockIngestionService
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
    StorageBucketNotFoundError,
    StorageConnectionError,
    StorageUploadError,
)
from workers.tasks.queue_for_fetch import fetch_stock_data


# =============================================================================
# Main Task Tests
# =============================================================================

class FetchStockDataTaskTest(TransactionTestCase):
    """Tests for the fetch_stock_data Celery task."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_successful_task_execution(self, mock_fetch, mock_upload):
        """Test successful task execution from QUEUED_FOR_FETCH to FETCHED."""
        # Create run
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock successful API fetch and upload
        mock_fetch.return_value = b'{"ticker": "AAPL", "data": [{"date": "2025-01-01", "price": 150.00}]}'
        mock_upload.return_value = f's3://bucket/AAPL/{run.id}.json'
        
        # Execute task
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify result
        self.assertEqual(result['run_id'], str(run.id))
        self.assertEqual(result['ticker'], 'AAPL')
        self.assertEqual(result['state'], IngestionState.FETCHED)
        self.assertFalse(result['skipped'])
        
        # Verify run state
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FETCHED)
        self.assertIsNotNone(run.raw_data_uri)
        self.assertIsNotNone(run.fetching_started_at)
        self.assertIsNotNone(run.fetching_finished_at)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_fetching_state_retry_proceeds(self, mock_fetch, mock_upload):
        """Test that a run in FETCHING state (from a previous retry) can proceed."""
        # Create run already in FETCHING state (from a previous retry attempt)
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        
        # Mock successful API fetch and upload
        mock_fetch.return_value = b'{"ticker": "AAPL", "data": [{"date": "2025-01-01", "price": 150.00}]}'
        mock_upload.return_value = f's3://bucket/AAPL/{run.id}.json'
        
        # Execute task
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify result
        self.assertEqual(result['run_id'], str(run.id))
        self.assertEqual(result['ticker'], 'AAPL')
        self.assertEqual(result['state'], IngestionState.FETCHED)
        self.assertFalse(result['skipped'])
        
        # Verify run state transitioned to FETCHED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FETCHED)
        self.assertIsNotNone(run.raw_data_uri)
        self.assertIsNotNone(run.fetching_finished_at)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_idempotency_already_fetched(self, mock_fetch, mock_upload):
        """Test that task is idempotent when run is already FETCHED."""
        # Create run that's already FETCHED
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHED,
            raw_data_uri='s3://bucket/AAPL/existing.json'
        )
        
        # Execute task
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify task was skipped
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'already_processed')
        
        # Verify API was not called
        mock_fetch.assert_not_called()
        mock_upload.assert_not_called()
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_idempotency_queued_for_spark(self, mock_fetch, mock_upload):
        """Test that task is idempotent when run is already QUEUED_FOR_SPARK."""
        # Create run that's already QUEUED_FOR_SPARK
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_SPARK,
            raw_data_uri='s3://bucket/AAPL/existing.json'
        )
        
        # Execute task
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify task was skipped
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'already_processed')
        self.assertEqual(result['state'], IngestionState.QUEUED_FOR_SPARK)
        
        # Verify API was not called
        mock_fetch.assert_not_called()
        mock_upload.assert_not_called()
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_idempotency_spark_running(self, mock_fetch, mock_upload):
        """Test that task is idempotent when run is already SPARK_RUNNING."""
        # Create run that's already SPARK_RUNNING
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.SPARK_RUNNING,
            raw_data_uri='s3://bucket/AAPL/existing.json'
        )
        
        # Execute task
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify task was skipped
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'already_processed')
        self.assertEqual(result['state'], IngestionState.SPARK_RUNNING)
        
        # Verify API was not called
        mock_fetch.assert_not_called()
        mock_upload.assert_not_called()
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_idempotency_spark_finished(self, mock_fetch, mock_upload):
        """Test that task is idempotent when run is already SPARK_FINISHED."""
        # Create run that's already SPARK_FINISHED
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.SPARK_FINISHED,
            raw_data_uri='s3://bucket/AAPL/existing.json'
        )
        
        # Execute task
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify task was skipped
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'already_processed')
        self.assertEqual(result['state'], IngestionState.SPARK_FINISHED)
        
        # Verify API was not called
        mock_fetch.assert_not_called()
        mock_upload.assert_not_called()
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_idempotency_done(self, mock_fetch, mock_upload):
        """Test that task is idempotent when run is already DONE."""
        # Create run that's already DONE
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://bucket/AAPL/existing.json'
        )
        
        # Execute task
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify task was skipped
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'already_processed')
        self.assertEqual(result['state'], IngestionState.DONE)
        
        # Verify API was not called
        mock_fetch.assert_not_called()
        mock_upload.assert_not_called()
    
    def test_failed_state_raises_non_retryable_error(self):
        """Test that attempting to fetch a run already in FAILED state raises NonRetryableError."""
        # Create run that's already FAILED
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FAILED,
            error_code='API_ERROR',
            error_message='Previous failure'
        )
        
        # Execute task - should raise NonRetryableError
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run state remains FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
    
    def test_run_not_found(self):
        """Test that task fails if run doesn't exist."""
        fake_id = str(uuid.uuid4())
        
        # Execute task - should raise NonRetryableError
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(fake_id, 'AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_api_authentication_error_transitions_to_failed(self, mock_fetch, mock_upload):
        """Test that API authentication errors transition run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock API authentication error
        mock_fetch.side_effect = APIAuthenticationError("Invalid API key")
        
        # Execute task - should raise NonRetryableError
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'API_ERROR')
        self.assertIn('Invalid API key', run.error_message)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_api_not_found_error_transitions_to_failed(self, mock_fetch, mock_upload):
        """Test that API not found errors transition run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock API not found error
        mock_fetch.side_effect = APINotFoundError("Ticker not found")
        
        # Execute task
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_storage_auth_error_transitions_to_failed(self, mock_fetch, mock_upload):
        """Test that storage auth errors transition run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock successful fetch but storage auth error
        mock_fetch.return_value = b'{"ticker": "AAPL", "data": []}'
        mock_upload.side_effect = StorageAuthenticationError("Invalid S3 credentials")
        
        # Execute task
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'STORAGE_AUTH_ERROR')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_storage_bucket_not_found_transitions_to_failed(self, mock_fetch, mock_upload):
        """Test that StorageBucketNotFoundError transitions run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock successful fetch but bucket not found error
        mock_fetch.return_value = b'{"ticker": "AAPL", "data": []}'
        mock_upload.side_effect = StorageBucketNotFoundError("Bucket not found")
        
        # Execute task
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'STORAGE_BUCKET_NOT_FOUND')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_api_rate_limit_error_retries(self, mock_fetch, mock_upload):
        """Test that API rate limit (429) errors trigger retry."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock API rate limit error
        mock_fetch.side_effect = APIRateLimitError("Rate limit exceeded")
        
        # Execute task - should raise retryable error
        with self.assertRaises(APIRateLimitError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run is still in FETCHING state (not FAILED yet)
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FETCHING)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_api_connection_error_retries(self, mock_fetch, mock_upload):
        """Test that API connection errors trigger retry."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock API fetch error (wraps connection errors)
        mock_fetch.side_effect = APIFetchError("Connection failed")
        
        # Execute task - should raise retryable error
        with self.assertRaises(APIFetchError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run is still in FETCHING state (not FAILED yet)
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FETCHING)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_api_server_error_retries(self, mock_fetch, mock_upload):
        """Test that API server errors (500+) trigger retry."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock API server error
        mock_fetch.side_effect = APIFetchError("Server error: 500")
        
        # Execute task - should raise retryable error
        with self.assertRaises(APIFetchError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run is still in FETCHING state (not FAILED yet)
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FETCHING)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_empty_file_error_transitions_to_failed(self, mock_fetch, mock_upload):
        """Test that empty file errors transition run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock invalid data format error (empty file)
        mock_fetch.side_effect = InvalidDataFormatError("Received empty file from API")
        
        # Execute task - should raise non-retryable error
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'API_ERROR')
        self.assertIn('empty file', run.error_message)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_invalid_json_format_transitions_to_failed(self, mock_fetch, mock_upload):
        """Test that invalid JSON format errors transition run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock invalid data format error (not JSON)
        mock_fetch.side_effect = InvalidDataFormatError(
            "Received data is not valid JSON"
        )
        
        # Execute task - should raise non-retryable error
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'API_ERROR')
        self.assertIn('not valid JSON', run.error_message)

@patch('workers.tasks.send_discord_notification.send_discord_notification.delay')
class FetchStockDataInvalidInputTest(TransactionTestCase):
    """Tests for invalid input handling in fetch_stock_data task."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_malformed_uuid_raises_non_retryable_error(self, mock_fetch, mock_upload, mock_discord_delay):
        """Test that a malformed run_id (invalid UUID) raises NonRetryableError."""
        # Execute task with malformed UUID
        malformed_run_id = 'not-a-valid-uuid'
        
        with self.assertRaises(NonRetryableError) as context:
            fetch_stock_data(malformed_run_id, 'AAPL')
        
        # Verify error message mentions invalid format
        self.assertIn('Invalid run_id format', str(context.exception))
        self.assertIn(malformed_run_id, str(context.exception))
        
        # Verify that API and storage methods were never called
        mock_fetch.assert_not_called()
        mock_upload.assert_not_called()
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_malformed_uuid_does_not_crash_with_various_formats(self, mock_fetch, mock_upload, mock_discord_delay):
        """Test that various malformed UUID formats are handled gracefully."""
        malformed_ids = [
            'not-a-uuid',
            '12345',
            'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
            '',
            'null',
            '{}',
            '[]',
        ]
        
        for malformed_id in malformed_ids:
            with self.subTest(malformed_id=malformed_id):
                with self.assertRaises(NonRetryableError) as context:
                    fetch_stock_data(malformed_id, 'AAPL')
                
                self.assertIn('Invalid run_id format', str(context.exception))
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_valid_uuid_proceeds_normally(self, mock_fetch, mock_upload, mock_discord_delay):
        """Test that a valid UUID proceeds normally after the fix."""
        # Create run with valid UUID
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock successful API fetch and upload
        mock_fetch.return_value = b'{"ticker": "AAPL", "data": []}'
        mock_upload.return_value = f's3://bucket/AAPL/{run.id}.json'
        
        # Execute task with valid UUID string
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify successful execution
        self.assertEqual(result['run_id'], str(run.id))
        self.assertEqual(result['state'], IngestionState.FETCHED)
        self.assertFalse(result['skipped'])


class FetchStockDataRetryTest(TransactionTestCase):
    """Tests for retry logic in fetch_stock_data task."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_api_timeout_retries(self, mock_fetch, mock_upload):
        """Test that API timeout errors trigger retry."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock API timeout error
        mock_fetch.side_effect = APITimeoutError("Request timed out")
        
        # Create a mock task with request context
        task = fetch_stock_data
        task.request.retries = 0  # First attempt
        
        # Execute task - should raise the retryable error
        with self.assertRaises(APITimeoutError):
            fetch_stock_data(str(run.id), 'AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_storage_upload_error_retries(self, mock_fetch, mock_upload):
        """Test that storage upload errors trigger retry."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock successful fetch but upload error
        mock_fetch.return_value = b'{"ticker": "AAPL", "data": []}'
        mock_upload.side_effect = StorageUploadError("Upload failed")
        
        # Create a mock task with request context
        task = fetch_stock_data
        task.request.retries = 0
        
        # Execute task - should raise the retryable error
        with self.assertRaises(StorageUploadError):
            fetch_stock_data(str(run.id), 'AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_max_retries_transitions_to_failed(self, mock_fetch, mock_upload):
        """Test that exceeding max retries transitions to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock API timeout error
        mock_fetch.side_effect = APITimeoutError("Request timed out")
        
        # Create a mock task with max retries
        task = fetch_stock_data
        task.request.retries = 3  # Third attempt (0-indexed)
        
        # Execute task
        with self.assertRaises(APITimeoutError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'MAX_RETRIES_EXCEEDED')
