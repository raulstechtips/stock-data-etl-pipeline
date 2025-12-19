"""
Tests for the process_delta_lake Celery task.

This module contains comprehensive tests for:
- process_delta_lake task
- Error handling and retry logic
- State transitions
- Idempotency checks
- Delta Lake table creation and merging
- Data transformation with Polars
"""

import json
import uuid
from unittest.mock import MagicMock, Mock, patch

from django.test import TransactionTestCase

from api.models import IngestionState, Stock, StockIngestionRun
from api.services.stock_ingestion_service import StockIngestionService
from workers.exceptions import (
    DeltaLakeError,
    DeltaLakeMergeError,
    DeltaLakeWriteError,
    InvalidDataFormatError,
    InvalidStateError,
    NonRetryableError,
    StorageAuthenticationError,
    StorageBucketNotFoundError,
    StorageConnectionError,
)
from workers.tasks.queue_for_delta import process_delta_lake


# Sample JSON data matching the structure from aapl-quarterly.json
SAMPLE_STOCK_DATA = {
    "data": {
        "financials": {
            "quarterly": {
                "period_end_date": ["2024-03", "2024-06", "2024-09"],
                "revenue": [90753000000, 85777000000, 94930000000],
                "cogs": [54428000000, 52498000000, 55800000000],
                "gross_profit": [36325000000, 33279000000, 39130000000],
            }
        },
        "metadata": {
            "sector": "Information Technology",
            "name": "Apple Inc",
            "exchange": "NASDAQ",
            "symbol": "AAPL",
            "country": "US",
            "currency": "USD",
        }
    }
}


@patch('workers.tasks.send_discord_notification.send_discord_notification.delay')
class ProcessDeltaLakeTaskTest(TransactionTestCase):
    """Tests for the process_delta_lake Celery task."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')
    
    @patch('workers.tasks.queue_for_delta._process_stocks_table')
    @patch('workers.tasks.queue_for_delta._transform_data_to_polars')
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_successful_task_execution(
        self, mock_download, mock_transform, mock_process_stocks, mock_discord_delay
    ):
        """Test successful task execution from QUEUED_FOR_DELTA to DELTA_FINISHED."""
        # Create run in QUEUED_FOR_DELTA state
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri='s3://stock-raw-data/AAPL.json'
        )
        
        # Mock successful operations
        mock_download.return_value = json.dumps(SAMPLE_STOCK_DATA).encode('utf-8')
        
        # Create mock unified DataFrame
        import polars as pl
        unified_df = pl.DataFrame({
            'ticker': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
            'record_type': ['financials', 'financials', 'financials', 'metadata'],
            'period_end_date': ['2024-03', '2024-06', '2024-09', None],
            'revenue': [90753000000, 85777000000, 94930000000, None],
            'sector': [None, None, None, 'Information Technology'],
            'name': [None, None, None, 'Apple Inc'],
        })
        
        mock_transform.return_value = unified_df
        mock_process_stocks.return_value = 's3://stock-delta-lake/stocks'
        
        # Execute task
        result = process_delta_lake(str(run.id), 'AAPL')
        
        # Verify result
        self.assertEqual(result['run_id'], str(run.id))
        self.assertEqual(result['ticker'], 'AAPL')
        self.assertEqual(result['state'], IngestionState.DELTA_FINISHED)
        self.assertFalse(result['skipped'])
        self.assertEqual(result['processed_uri'], 's3://stock-delta-lake/stocks')
        self.assertEqual(result['records_processed'], 4)  # 3 financials + 1 metadata
        
        # Verify run state
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.DELTA_FINISHED)
        self.assertIsNotNone(run.processed_data_uri)
        self.assertIsNotNone(run.delta_started_at)
        self.assertIsNotNone(run.delta_finished_at)
    
    @patch('workers.tasks.queue_for_delta._process_stocks_table')
    @patch('workers.tasks.queue_for_delta._transform_data_to_polars')
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_delta_running_state_retry_proceeds(
        self, mock_download, mock_transform, mock_process_stocks, mock_discord_delay
    ):
        """Test that a run in DELTA_RUNNING state (from a previous retry) can proceed."""
        # Create run already in DELTA_RUNNING state
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DELTA_RUNNING,
            raw_data_uri='s3://stock-raw-data/AAPL.json'
        )
        
        # Mock successful operations
        mock_download.return_value = json.dumps(SAMPLE_STOCK_DATA).encode('utf-8')
        
        import polars as pl
        unified_df = pl.DataFrame({
            'ticker': ['AAPL'],
            'record_type': ['financials'],
            'period_end_date': ['2024-03'],
            'revenue': [90753000000],
        })
        
        mock_transform.return_value = unified_df
        mock_process_stocks.return_value = 's3://stock-delta-lake/stocks'
        
        # Execute task
        result = process_delta_lake(str(run.id), 'AAPL')
        
        # Verify result
        self.assertEqual(result['state'], IngestionState.DELTA_FINISHED)
        self.assertFalse(result['skipped'])
        
        # Verify run state transitioned to DELTA_FINISHED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.DELTA_FINISHED)
    
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_idempotency_already_delta_finished(self, mock_download, mock_discord_delay):
        """Test that task is idempotent when run is already DELTA_FINISHED."""
        # Create run that's already DELTA_FINISHED
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DELTA_FINISHED,
            raw_data_uri='s3://stock-raw-data/AAPL.json',
            processed_data_uri='s3://stock-delta-lake/AAPL/financials'
        )
        
        # Execute task
        result = process_delta_lake(str(run.id), 'AAPL')
        
        # Verify task was skipped
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'already_processed')
        self.assertEqual(result['state'], IngestionState.DELTA_FINISHED)
        
        # Verify download was not called
        mock_download.assert_not_called()
    
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_idempotency_already_done(self, mock_download, mock_discord_delay):
        """Test that task is idempotent when run is already DONE."""
        # Create run that's already DONE
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://stock-raw-data/AAPL.json',
            processed_data_uri='s3://stock-delta-lake/AAPL/financials'
        )
        
        # Execute task
        result = process_delta_lake(str(run.id), 'AAPL')
        
        # Verify task was skipped
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'already_processed')
        
        # Verify download was not called
        mock_download.assert_not_called()
    
    def test_failed_state_raises_non_retryable_error(self, mock_discord_delay):
        """Test that attempting to process a run in FAILED state raises NonRetryableError."""
        # Create run that's already FAILED
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FAILED,
            raw_data_uri='s3://stock-raw-data/AAPL.json',
            error_code='API_ERROR',
            error_message='Previous failure'
        )
        
        # Execute task - should raise NonRetryableError
        with self.assertRaises(NonRetryableError):
            process_delta_lake(str(run.id), 'AAPL')
        
        # Verify run state remains FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
    
    def test_run_not_found(self, mock_discord_delay):
        """Test that task fails if run doesn't exist."""
        fake_id = str(uuid.uuid4())
        
        # Execute task - should raise NonRetryableError
        with self.assertRaises(NonRetryableError):
            process_delta_lake(fake_id, 'AAPL')
    
    def test_missing_raw_data_uri_transitions_to_failed(self, mock_discord_delay):
        """Test that missing raw_data_uri transitions run to FAILED."""
        # Create run without raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri=None  # Missing!
        )
        
        # Execute task - should raise NonRetryableError
        with self.assertRaises(NonRetryableError):
            process_delta_lake(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'MISSING_RAW_DATA')
    
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_storage_authentication_error_transitions_to_failed(
        self, mock_download, mock_discord_delay
    ):
        """Test that storage authentication errors transition run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri='s3://stock-raw-data/AAPL.json'
        )
        
        # Mock storage authentication error
        mock_download.side_effect = StorageAuthenticationError("Invalid credentials")
        
        # Execute task - should raise NonRetryableError
        with self.assertRaises(NonRetryableError):
            process_delta_lake(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'STORAGE_ERROR')
    
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_storage_bucket_not_found_transitions_to_failed(
        self, mock_download, mock_discord_delay
    ):
        """Test that StorageBucketNotFoundError transitions run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri='s3://stock-raw-data/AAPL.json'
        )
        
        # Mock bucket not found error
        mock_download.side_effect = StorageBucketNotFoundError("Bucket not found")
        
        # Execute task
        with self.assertRaises(NonRetryableError):
            process_delta_lake(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'STORAGE_ERROR')
    
    @patch('workers.tasks.queue_for_delta._transform_data_to_polars')
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_invalid_data_format_transitions_to_failed(
        self, mock_download, mock_transform, mock_discord_delay
    ):
        """Test that invalid data format errors transition run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri='s3://stock-raw-data/AAPL.json'
        )
        
        # Mock successful download but invalid data format
        mock_download.return_value = b'{"invalid": "structure"}'
        mock_transform.side_effect = InvalidDataFormatError("Missing 'data' key in JSON")
        
        # Execute task
        with self.assertRaises(NonRetryableError):
            process_delta_lake(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'DATA_TRANSFORMATION_ERROR')
    
    @patch('workers.tasks.queue_for_delta._process_stocks_table')
    @patch('workers.tasks.queue_for_delta._transform_data_to_polars')
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_delta_lake_write_error_transitions_to_failed(
        self, mock_download, mock_transform, mock_process_stocks, mock_discord_delay
    ):
        """Test that Delta Lake write errors transition run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri='s3://stock-raw-data/AAPL.json'
        )
        
        # Mock successful download and transform
        mock_download.return_value = json.dumps(SAMPLE_STOCK_DATA).encode('utf-8')
        
        import polars as pl
        unified_df = pl.DataFrame({
            'ticker': ['AAPL'],
            'record_type': ['financials'],
            'period_end_date': ['2024-03'],
            'revenue': [90753000000],
        })
        mock_transform.return_value = unified_df
        
        # Mock Delta Lake write error
        mock_process_stocks.side_effect = DeltaLakeWriteError("Failed to write table")
        
        # Execute task
        with self.assertRaises(NonRetryableError):
            process_delta_lake(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'DELTA_LAKE_ERROR')
    
    @patch('workers.tasks.queue_for_delta._process_stocks_table')
    @patch('workers.tasks.queue_for_delta._transform_data_to_polars')
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_delta_lake_merge_error_transitions_to_failed(
        self, mock_download, mock_transform, mock_process_stocks, mock_discord_delay
    ):
        """Test that Delta Lake merge errors transition run to FAILED."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri='s3://stock-raw-data/AAPL.json'
        )
        
        # Mock successful download and transform
        mock_download.return_value = json.dumps(SAMPLE_STOCK_DATA).encode('utf-8')
        
        import polars as pl
        unified_df = pl.DataFrame({
            'ticker': ['AAPL'],
            'record_type': ['financials'],
            'period_end_date': ['2024-03'],
            'revenue': [90753000000],
        })
        mock_transform.return_value = unified_df
        
        # Mock Delta Lake merge error
        mock_process_stocks.side_effect = DeltaLakeMergeError("Failed to merge data")
        
        # Execute task
        with self.assertRaises(NonRetryableError):
            process_delta_lake(str(run.id), 'AAPL')
        
        # Verify run transitioned to FAILED
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'DELTA_LAKE_ERROR')


@patch('workers.tasks.send_discord_notification.send_discord_notification.delay')
class ProcessDeltaLakeInvalidInputTest(TransactionTestCase):
    """Tests for invalid input handling in process_delta_lake task."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')
    
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_malformed_uuid_raises_non_retryable_error(self, mock_download, mock_discord_delay):
        """Test that a malformed run_id (invalid UUID) raises NonRetryableError."""
        # Execute task with malformed UUID
        malformed_run_id = 'not-a-valid-uuid'
        
        with self.assertRaises(NonRetryableError) as context:
            process_delta_lake(malformed_run_id, 'AAPL')
        
        # Verify error message mentions invalid format
        self.assertIn('Invalid run_id format', str(context.exception))
        self.assertIn(malformed_run_id, str(context.exception))
        
        # Verify that download was never called
        mock_download.assert_not_called()
    
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_malformed_uuid_does_not_crash_with_various_formats(
        self, mock_download, mock_discord_delay
    ):
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
                    process_delta_lake(malformed_id, 'AAPL')
                
                self.assertIn('Invalid run_id format', str(context.exception))
    
    @patch('workers.tasks.queue_for_delta._process_stocks_table')
    @patch('workers.tasks.queue_for_delta._transform_data_to_polars')
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_valid_uuid_proceeds_normally(
        self, mock_download, mock_transform, mock_process_stocks, mock_discord_delay
    ):
        """Test that a valid UUID proceeds normally."""
        # Create run with valid UUID
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri='s3://stock-raw-data/AAPL.json'
        )
        
        # Mock successful operations
        mock_download.return_value = json.dumps(SAMPLE_STOCK_DATA).encode('utf-8')
        
        import polars as pl
        unified_df = pl.DataFrame({
            'ticker': ['AAPL'],
            'record_type': ['financials'],
            'period_end_date': ['2024-03'],
            'revenue': [90753000000],
        })
        mock_transform.return_value = unified_df
        mock_process_stocks.return_value = 's3://stock-delta-lake/stocks'
        
        # Execute task with valid UUID string
        result = process_delta_lake(str(run.id), 'AAPL')
        
        # Verify successful execution
        self.assertEqual(result['run_id'], str(run.id))
        self.assertEqual(result['state'], IngestionState.DELTA_FINISHED)
        self.assertFalse(result['skipped'])


@patch('workers.tasks.send_discord_notification.send_discord_notification.delay')
class TTMDataProcessingTest(TransactionTestCase):
    """Tests for Trailing Twelve Month (TTM) data processing."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')
    
    @patch('workers.tasks.queue_for_delta._process_stocks_table')
    @patch('workers.tasks.queue_for_delta._transform_data_to_polars')
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_ttm_data_transformation_with_period_replacement(
        self, mock_download, mock_transform, mock_process_stocks, mock_discord_delay
    ):
        """Test that TTM data is processed and period_end_date is replaced with latest quarterly date."""
        # Create run in QUEUED_FOR_DELTA state
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri='s3://stock-raw-data/AAPL.json'
        )
        
        # Sample data with TTM
        sample_data = {
            "data": {
                "financials": {
                    "quarterly": {
                        "period_end_date": ["2024-03", "2024-06", "2024-09"],
                        "revenue": [90753000000, 85777000000, 94930000000],
                    },
                    "ttm": {
                        "period_end_date": "TTM",
                        "revenue": 416161000000,
                        "cogs": 220960000000,
                        "gross_profit": 195201000000,
                    }
                },
                "metadata": {
                    "sector": "Information Technology",
                    "name": "Apple Inc",
                }
            }
        }
        
        mock_download.return_value = json.dumps(sample_data).encode('utf-8')
        
        # Create mock unified DataFrame
        import polars as pl
        unified_df = pl.DataFrame({
            'ticker': ['AAPL', 'AAPL', 'AAPL', 'AAPL', 'AAPL'],
            'record_type': ['financials', 'financials', 'financials', 'metadata', 'ttm'],
            'period_end_date': ['2024-03', '2024-06', '2024-09', None, '2024-09'],
            'revenue': [90753000000, 85777000000, 94930000000, None, 416161000000],
            'cogs': [None, None, None, None, 220960000000],
            'gross_profit': [None, None, None, None, 195201000000],
            'sector': [None, None, None, 'Information Technology', None],
            'name': [None, None, None, 'Apple Inc', None],
        })
        
        mock_transform.return_value = unified_df
        mock_process_stocks.return_value = 's3://stock-delta-lake/stocks'
        
        # Execute task
        result = process_delta_lake(str(run.id), 'AAPL')
        
        # Verify successful execution
        self.assertEqual(result['run_id'], str(run.id))
        self.assertEqual(result['state'], IngestionState.DELTA_FINISHED)
        self.assertFalse(result['skipped'])
        self.assertEqual(result['records_processed'], 5)  # 3 financials + 1 metadata + 1 ttm
        
        # Verify stocks table was processed
        mock_process_stocks.assert_called_once()
        call_args = mock_process_stocks.call_args
        self.assertEqual(call_args[0][0], 'AAPL')  # ticker
        # Verify the DataFrame has correct data
        unified_df_arg = call_args[0][1]
        self.assertEqual(len(unified_df_arg), 5)
        # Verify TTM record exists
        ttm_rows = unified_df_arg.filter(pl.col('record_type') == 'ttm')
        self.assertEqual(len(ttm_rows), 1)
        self.assertEqual(ttm_rows['ticker'][0], 'AAPL')
        self.assertEqual(ttm_rows['period_end_date'][0], '2024-09')
    
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_ttm_transformation_without_quarterly_data(
        self, mock_download, mock_discord_delay
    ):
        """Test that TTM data is skipped when there's no quarterly period_end_date."""
        # Sample data with TTM but no quarterly data
        sample_data = {
            "data": {
                "financials": {
                    "ttm": {
                        "period_end_date": "TTM",
                        "revenue": 416161000000,
                    }
                },
                "metadata": {
                    "sector": "Information Technology",
                }
            }
        }
        
        mock_download.return_value = json.dumps(sample_data).encode('utf-8')
        
        # The transformation will log a warning and skip TTM
        from workers.tasks.queue_for_delta import _transform_data_to_polars
        
        data = json.loads(mock_download.return_value)
        unified_df = _transform_data_to_polars(data, 'AAPL')
        
        # Verify TTM is not in the unified DataFrame (skipped due to missing quarterly data)
        # Only metadata should be present
        self.assertIn('metadata', unified_df['record_type'].to_list())
        self.assertNotIn('ttm', unified_df['record_type'].to_list())
    
    def test_ttm_transformation_with_real_data_structure(self, mock_discord_delay):
        """Test TTM transformation with real AAPL JSON structure."""
        from workers.tasks.queue_for_delta import _transform_data_to_polars
        import polars as pl
        
        # Use realistic data structure from aapl.json
        sample_data = {
            "data": {
                "financials": {
                    "quarterly": {
                        "period_end_date": ["2002-12"],
                        "revenue": [1472000000],
                        "cogs": [1066000000],
                    },
                    "ttm": {
                        "period_end_date": "TTM",
                        "revenue": 416161000000,
                        "cogs": 220960000000,
                        "gross_profit": 195201000000,
                        "ebitda": 144748000000,
                        "fcf": 98767000000,
                    }
                },
                "metadata": {
                    "sector": "Information Technology",
                    "name": "Apple Inc",
                }
            }
        }
        
        unified_df = _transform_data_to_polars(sample_data, 'AAPL')
        
        # Verify TTM record exists in unified DataFrame
        ttm_rows = unified_df.filter(pl.col('record_type') == 'ttm')
        self.assertEqual(len(ttm_rows), 1)
        self.assertEqual(ttm_rows['ticker'][0], 'AAPL')
        # Verify period_end_date was replaced with latest quarterly date
        self.assertEqual(ttm_rows['period_end_date'][0], '2002-12')
        # Verify all TTM metrics are present
        self.assertEqual(ttm_rows['revenue'][0], 416161000000)
        self.assertEqual(ttm_rows['cogs'][0], 220960000000)
        self.assertEqual(ttm_rows['gross_profit'][0], 195201000000)
        self.assertEqual(ttm_rows['ebitda'][0], 144748000000)
        self.assertEqual(ttm_rows['fcf'][0], 98767000000)
    
    @patch('workers.tasks.queue_for_delta._process_stocks_table')
    @patch('workers.tasks.queue_for_delta._transform_data_to_polars')
    @patch('workers.tasks.queue_for_delta._download_from_storage')
    def test_ttm_only_data_processing(
        self, mock_download, mock_transform, mock_process_stocks, mock_discord_delay
    ):
        """Test processing when only TTM data is available (no financials or metadata)."""
        # Create run in QUEUED_FOR_DELTA state
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_DELTA,
            raw_data_uri='s3://stock-raw-data/AAPL.json'
        )
        
        # Mock: only TTM data returned
        mock_download.return_value = json.dumps({}).encode('utf-8')
        
        import polars as pl
        unified_df = pl.DataFrame({
            'ticker': ['AAPL'],
            'record_type': ['ttm'],
            'period_end_date': ['2024-09'],
            'revenue': [416161000000],
        })
        
        mock_transform.return_value = unified_df
        mock_process_stocks.return_value = 's3://stock-delta-lake/stocks'
        
        # Execute task
        result = process_delta_lake(str(run.id), 'AAPL')
        
        # Verify successful execution with only TTM data
        self.assertEqual(result['run_id'], str(run.id))
        self.assertEqual(result['state'], IngestionState.DELTA_FINISHED)
        self.assertFalse(result['skipped'])
        self.assertEqual(result['records_processed'], 1)
        self.assertEqual(result['processed_uri'], 's3://stock-delta-lake/stocks')
        
        # Verify stocks table was processed
        mock_process_stocks.assert_called_once()
    
    def test_ttm_transformation_empty_quarterly_array(self, mock_discord_delay):
        """Test TTM transformation when quarterly period_end_date array is empty."""
        from workers.tasks.queue_for_delta import _transform_data_to_polars
        import polars as pl
        
        sample_data = {
            "data": {
                "financials": {
                    "quarterly": {
                        "period_end_date": [],  # Empty array
                    },
                    "ttm": {
                        "period_end_date": "TTM",
                        "revenue": 416161000000,
                    }
                },
                "metadata": {
                    "sector": "Information Technology",
                }
            }
        }
        
        unified_df = _transform_data_to_polars(sample_data, 'AAPL')
        
        # Verify TTM is not in the unified DataFrame (skipped due to empty quarterly data)
        # But metadata should be present
        self.assertNotIn('ttm', unified_df['record_type'].to_list())
        self.assertIn('metadata', unified_df['record_type'].to_list())
    
    def test_ttm_transformation_multiple_quarters(self, mock_discord_delay):
        """Test TTM transformation uses the last (most recent) quarterly date."""
        from workers.tasks.queue_for_delta import _transform_data_to_polars
        import polars as pl
        
        sample_data = {
            "data": {
                "financials": {
                    "quarterly": {
                        "period_end_date": ["2023-12", "2024-03", "2024-06", "2024-09"],
                        "revenue": [1000000000, 2000000000, 3000000000, 4000000000],
                    },
                    "ttm": {
                        "period_end_date": "TTM",
                        "revenue": 10000000000,
                    }
                }
            }
        }
        
        unified_df = _transform_data_to_polars(sample_data, 'AAPL')
        
        # Verify TTM uses the last quarterly date (2024-09)
        ttm_rows = unified_df.filter(pl.col('record_type') == 'ttm')
        self.assertEqual(len(ttm_rows), 1)
        self.assertEqual(ttm_rows['period_end_date'][0], '2024-09')

