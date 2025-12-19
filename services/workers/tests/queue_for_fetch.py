"""
Tests for the fetch_stock_data Celery task.

This module contains comprehensive tests for:
- fetch_stock_data task
- Error handling and retry logic
- State transitions
- Idempotency checks
"""

import uuid
from unittest.mock import patch

from django.test import TransactionTestCase

from api.models import IngestionState, Stock, StockIngestionRun
from api.services.stock_ingestion_service import StockIngestionService
from workers.exceptions import (
    APIAuthenticationError,
    APIFetchError,
    APINotFoundError,
    APIRateLimitError,
    InvalidDataFormatError,
    NonRetryableError,
    StorageAuthenticationError,
    StorageBucketNotFoundError,
)
from workers.tasks.queue_for_fetch import fetch_stock_data


@patch('workers.tasks.queue_for_delta.process_delta_lake.delay')
@patch('workers.tasks.send_discord_notification.send_discord_notification.delay')
class FetchStockDataTaskTest(TransactionTestCase):
    """Tests for the fetch_stock_data Celery task."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_successful_task_execution(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
        self.assertEqual(result['state'], IngestionState.QUEUED_FOR_DELTA)
        self.assertFalse(result['skipped'])
        
        # Verify run state
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.QUEUED_FOR_DELTA)
        self.assertIsNotNone(run.raw_data_uri)
        self.assertIsNotNone(run.fetching_started_at)
        self.assertIsNotNone(run.fetching_finished_at)
        
        # Verify Delta Lake task was queued
        mock_delta_delay.assert_called_once_with(str(run.id), 'AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_fetching_state_retry_proceeds(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
        self.assertEqual(result['state'], IngestionState.QUEUED_FOR_DELTA)
        self.assertFalse(result['skipped'])
        
        # Verify run state transitioned to QUEUED_FOR_DELTA
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.QUEUED_FOR_DELTA)
        self.assertIsNotNone(run.raw_data_uri)
        self.assertIsNotNone(run.fetching_finished_at)
        
        # Verify Delta Lake task was queued
        mock_delta_delay.assert_called_once_with(str(run.id), 'AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_idempotency_already_fetched(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
    def test_idempotency_queued_for_delta(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
        """Test that task is idempotent when run is already QUEUED_FOR_DELTA."""
        # Create run that's already QUEUED_FOR_DELTA
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri='s3://bucket/AAPL/existing.json'
        )
        
        # Execute task
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify task was skipped
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'already_processed')
        self.assertEqual(result['state'], IngestionState.QUEUED_FOR_DELTA)
        
        # Verify API was not called
        mock_fetch.assert_not_called()
        mock_upload.assert_not_called()
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_idempotency_delta_running(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
        """Test that task is idempotent when run is already DELTA_RUNNING."""
        # Create run that's already DELTA_RUNNING
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DELTA_RUNNING,
            raw_data_uri='s3://bucket/AAPL/existing.json'
        )
        
        # Execute task
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify task was skipped
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'already_processed')
        self.assertEqual(result['state'], IngestionState.DELTA_RUNNING)
        
        # Verify API was not called
        mock_fetch.assert_not_called()
        mock_upload.assert_not_called()
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_idempotency_delta_finished(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
        """Test that task is idempotent when run is already DELTA_FINISHED."""
        # Create run that's already DELTA_FINISHED
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DELTA_FINISHED,
            raw_data_uri='s3://bucket/AAPL/existing.json'
        )
        
        # Execute task
        result = fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify task was skipped
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'already_processed')
        self.assertEqual(result['state'], IngestionState.DELTA_FINISHED)
        
        # Verify API was not called
        mock_fetch.assert_not_called()
        mock_upload.assert_not_called()
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_idempotency_done(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
    
    def test_failed_state_raises_non_retryable_error(self, mock_discord_delay, mock_delta_delay):
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
    
    def test_run_not_found(self, mock_discord_delay, mock_delta_delay):
        """Test that task fails if run doesn't exist."""
        fake_id = str(uuid.uuid4())
        
        # Execute task - should raise NonRetryableError
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(fake_id, 'AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_api_authentication_error_transitions_to_failed(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
    def test_api_not_found_error_transitions_to_failed(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
    def test_storage_auth_error_transitions_to_failed(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
    def test_storage_bucket_not_found_transitions_to_failed(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
    def test_api_rate_limit_error_transitions_to_failed(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
        """Test that API rate limit (429) errors transitions to failed state."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock API rate limit error
        mock_fetch.side_effect = APIRateLimitError("Rate limit exceeded")
        
        # Execute task - should raise non-retryable error
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run has failed state
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_api_connection_error_transitions_to_failed(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
        """Test that API connection errors transitions to failed state."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock API fetch error (wraps connection errors)
        mock_fetch.side_effect = APIFetchError("Connection failed")
        
        # Execute task - should raise non-retryable error
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run has failed state
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_api_server_error_transitions_to_failed(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
        """Test that API server errors (500+) transitions to failed state."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Mock API server error
        mock_fetch.side_effect = APIFetchError("Server error: 500")
        
        # Execute task - should raise non-retryable error
        with self.assertRaises(NonRetryableError):
            fetch_stock_data(str(run.id), 'AAPL')
        
        # Verify run has failed state
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_empty_file_error_transitions_to_failed(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
    def test_invalid_json_format_transitions_to_failed(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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

@patch('workers.tasks.queue_for_delta.process_delta_lake.delay')
@patch('workers.tasks.send_discord_notification.send_discord_notification.delay')
class FetchStockDataInvalidInputTest(TransactionTestCase):
    """Tests for invalid input handling in fetch_stock_data task."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')
    
    @patch('workers.tasks.queue_for_fetch._upload_to_storage')
    @patch('workers.tasks.queue_for_fetch._fetch_from_api')
    def test_malformed_uuid_raises_non_retryable_error(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
    def test_malformed_uuid_does_not_crash_with_various_formats(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
    def test_valid_uuid_proceeds_normally(self, mock_fetch, mock_upload, mock_discord_delay, mock_delta_delay):
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
        self.assertEqual(result['state'], IngestionState.QUEUED_FOR_DELTA)
        self.assertFalse(result['skipped'])
        
        # Verify Delta Lake task was queued
        mock_delta_delay.assert_called_once_with(str(run.id), 'AAPL')
