"""
Tests for the update_stock_metadata Celery task.

These tests verify:
1. Successful metadata update from Delta Lake
2. Handling of missing metadata in Delta Lake
3. Database lock timeout retry behavior
4. Transaction rollback on errors
5. Non-retryable error handling

Testing Strategy:
- Test exception types, not exception messages (messages are implementation details)
- Test behavior and side effects (database state, logging, return values)
- Test system state after errors occur
- Use descriptive test names that explain the scenario
"""

import uuid
from unittest.mock import MagicMock, Mock, patch, call

import polars as pl
from celery.exceptions import Retry
from django.db import DatabaseError, OperationalError, transaction
from django.test import TransactionTestCase

from api.models import Stock
from workers.exceptions import (
    DeltaLakeReadError,
    InvalidDataFormatError,
    NonRetryableError,
    RetryableError,
    StorageAuthenticationError,
)
from workers.tasks.update_stock_metadata import (
    UpdateStockMetadataResult,
    _read_metadata_from_delta_lake,
    _update_stock_with_metadata,
    update_stock_metadata,
)


class UpdateStockMetadataTaskTests(TransactionTestCase):
    """Tests for the update_stock_metadata Celery task."""

    def setUp(self):
        """Create test stock."""
        self.stock = Stock.objects.create(ticker='AAPL')
        self.ticker = self.stock.ticker

    @patch('workers.tasks.update_stock_metadata._read_metadata_from_delta_lake')
    @patch('workers.tasks.update_stock_metadata._update_stock_with_metadata')
    def test_successful_metadata_update(self, mock_update, mock_read):
        """Test successful metadata update from Delta Lake."""
        # Arrange
        metadata_dict = {
            'sector': 'Information Technology',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'country': 'US',
            'subindustry': 'Technology Hardware',
            'morningstar_sector': 'Technology',
            'morningstar_industry': 'Consumer Electronics',
            'industry': 'Technology Hardware',
            'description': 'Apple Inc. designs and manufactures...',
        }
        mock_read.return_value = metadata_dict
        mock_update.return_value = list(metadata_dict.keys())

        # Act
        result = update_stock_metadata(self.ticker)

        # Assert - Test behavior and return value structure
        self.assertTrue(result['updated'])
        self.assertFalse(result['skipped'])
        self.assertEqual(result['ticker'], self.ticker)
        self.assertEqual(result['stock_id'], str(self.stock.id))
        self.assertEqual(set(result['fields_updated']), set(metadata_dict.keys()))
        
        # Verify correct function calls
        mock_read.assert_called_once_with(self.ticker)
        mock_update.assert_called_once_with(self.stock.id, metadata_dict)

    @patch('workers.tasks.update_stock_metadata._read_metadata_from_delta_lake')
    def test_no_metadata_in_delta_lake_returns_skipped_result(self, mock_read):
        """Test that task gracefully skips when no metadata exists in Delta Lake."""
        # Arrange
        mock_read.return_value = None

        # Act
        result = update_stock_metadata(self.ticker)

        # Assert - Test behavior: task should skip, not fail
        self.assertFalse(result['updated'])
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'no_metadata_in_delta_lake')
        self.assertEqual(result['ticker'], self.ticker)
        
        # Verify Stock was not modified
        self.stock.refresh_from_db()
        self.assertIsNone(self.stock.sector)

    def test_stock_not_found_raises_non_retryable_error(self):
        """Test that NonRetryableError is raised when stock doesn't exist in database."""
        # Arrange
        nonexistent_ticker = 'NOTFOUND'

        # Act & Assert - Test exception type only
        with self.assertRaises(NonRetryableError):
            update_stock_metadata(nonexistent_ticker)
        
        # Verify stock was never created as side effect
        self.assertFalse(Stock.objects.filter(ticker=nonexistent_ticker).exists())
    
    def test_stock_not_found_exception_contains_ticker_info(self):
        """Test that exception includes ticker for debugging (capture exception pattern)."""
        # Arrange
        nonexistent_ticker = 'NOTFOUND'

        # Act - Capture exception to inspect its contents
        with self.assertRaises(NonRetryableError) as context:
            update_stock_metadata(nonexistent_ticker)
        
        # Assert - Verify exception contains helpful debugging info
        exception_message = str(context.exception)
        self.assertIn(nonexistent_ticker, exception_message, 
                     "Exception should contain ticker for debugging")
    
    @patch('workers.tasks.update_stock_metadata.logger')
    def test_stock_not_found_logs_error(self, mock_logger):
        """Test that error is properly logged for debugging (logging verification pattern)."""
        # Arrange
        nonexistent_ticker = 'NOTFOUND'

        # Act
        with self.assertRaises(NonRetryableError):
            update_stock_metadata(nonexistent_ticker)
        
        # Assert - Verify appropriate logging occurred
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        self.assertIn(nonexistent_ticker, str(call_args))

    @patch('workers.tasks.update_stock_metadata._read_metadata_from_delta_lake')
    @patch('workers.tasks.update_stock_metadata._update_stock_with_metadata')
    def test_database_lock_timeout_raises_retryable_error(self, mock_update, mock_read):
        """Test that database lock timeout raises RetryableError (allows Celery to retry)."""
        # Arrange
        metadata_dict = {'sector': 'Technology'}
        mock_read.return_value = metadata_dict
        mock_update.side_effect = OperationalError('database is locked')

        # Act & Assert - Test that correct exception type is raised
        with self.assertRaises(RetryableError):
            update_stock_metadata(self.ticker)
        
        # Verify behavior: Stock should remain unchanged after failed update
        self.stock.refresh_from_db()
        self.assertIsNone(self.stock.sector)
    
    @patch('workers.tasks.update_stock_metadata._read_metadata_from_delta_lake')
    @patch('workers.tasks.update_stock_metadata._update_stock_with_metadata')
    def test_operational_error_without_lock_is_not_retryable(self, mock_update, mock_read):
        """Test that non-lock OperationalErrors are non-retryable (data corruption)."""
        # Arrange
        metadata_dict = {'sector': 'Technology'}
        mock_read.return_value = metadata_dict
        # Simulate an operational error that's NOT a lock timeout
        mock_update.side_effect = OperationalError('disk I/O error')

        # Act & Assert - Should raise NonRetryableError, not RetryableError
        with self.assertRaises(NonRetryableError):
            update_stock_metadata(self.ticker)
        
        # Verify Stock was not modified
        self.stock.refresh_from_db()
        self.assertIsNone(self.stock.sector)

    @patch('workers.tasks.update_stock_metadata._read_metadata_from_delta_lake')
    def test_delta_lake_read_error_is_not_retryable(self, mock_read):
        """Test that Delta Lake read errors are non-retryable (configuration issue)."""
        # Arrange
        mock_read.side_effect = DeltaLakeReadError('Failed to read Delta table')

        # Act & Assert - Test exception type
        with self.assertRaises(NonRetryableError):
            update_stock_metadata(self.ticker)
        
        # Verify Stock was not modified
        self.stock.refresh_from_db()
        self.assertIsNone(self.stock.sector)
        self.assertIsNone(self.stock.name)

    @patch('workers.tasks.update_stock_metadata._read_metadata_from_delta_lake')
    def test_storage_authentication_error_is_not_retryable(self, mock_read):
        """Test that storage authentication errors are non-retryable (credentials issue)."""
        # Arrange
        mock_read.side_effect = StorageAuthenticationError('Authentication failed')

        # Act & Assert - Test exception type
        with self.assertRaises(NonRetryableError):
            update_stock_metadata(self.ticker)
        
        # Verify no partial updates occurred
        self.stock.refresh_from_db()
        self.assertIsNone(self.stock.sector)
        self.assertIsNone(self.stock.name)
        self.assertIsNone(self.stock.exchange)


class ReadMetadataFromDeltaLakeTests(TransactionTestCase):
    """Tests for reading metadata from Delta Lake."""

    @patch('workers.tasks.update_stock_metadata.pl.read_delta')
    @patch('workers.tasks.update_stock_metadata.DeltaTable')
    def test_read_metadata_success_returns_clean_dict(self, mock_delta_table, mock_read_delta):
        """Test successful metadata read from Delta Lake returns cleaned dictionary."""
        # Arrange
        ticker = 'AAPL'
        
        # Create mock DataFrame with metadata record
        metadata_data = {
            'ticker': ['AAPL'],
            'record_type': ['metadata'],
            'period_end_date': [None],
            'sector': ['Information Technology'],
            'name': ['Apple Inc.'],
            'exchange': ['NASDAQ'],
            'country': ['US'],
            'subindustry': ['Technology Hardware'],
            'morningstar_sector': ['Technology'],
            'morningstar_industry': ['Consumer Electronics'],
            'industry': ['Technology Hardware'],
            'description': ['Apple Inc. designs...'],
        }
        mock_df = pl.DataFrame(metadata_data)
        mock_read_delta.return_value = mock_df

        # Act
        result = _read_metadata_from_delta_lake(ticker)

        # Assert - Test return value structure and content
        self.assertIsNotNone(result)
        self.assertEqual(result['sector'], 'Information Technology')
        self.assertEqual(result['name'], 'Apple Inc.')
        self.assertEqual(result['exchange'], 'NASDAQ')
        self.assertEqual(result['country'], 'US')
        
        # Verify metadata fields are excluded (only Stock model fields returned)
        self.assertNotIn('ticker', result)
        self.assertNotIn('record_type', result)
        self.assertNotIn('period_end_date', result)

    @patch('workers.tasks.update_stock_metadata.pl.read_delta')
    @patch('workers.tasks.update_stock_metadata.DeltaTable')
    def test_no_metadata_record_found_returns_none(self, mock_delta_table, mock_read_delta):
        """Test that None is returned when no metadata record exists for ticker."""
        # Arrange
        ticker = 'AAPL'
        
        # Create empty DataFrame (no matching records)
        mock_df = pl.DataFrame({
            'ticker': [],
            'record_type': [],
            'sector': [],
        })
        mock_read_delta.return_value = mock_df

        # Act
        result = _read_metadata_from_delta_lake(ticker)

        # Assert - Test behavior: should return None, not raise exception
        self.assertIsNone(result)

    @patch('workers.tasks.update_stock_metadata.pl.read_delta')
    @patch('workers.tasks.update_stock_metadata.DeltaTable')
    def test_multiple_metadata_records_uses_first(self, mock_delta_table, mock_read_delta):
        """Test that first record is used when multiple metadata records exist (deduplication)."""
        # Arrange
        ticker = 'AAPL'
        
        # Create DataFrame with multiple metadata records
        metadata_data = {
            'ticker': ['AAPL', 'AAPL'],
            'record_type': ['metadata', 'metadata'],
            'period_end_date': [None, None],
            'sector': ['Technology', 'Finance'],  # Different values
            'name': ['Apple Inc.', 'Apple Corp.'],
        }
        mock_df = pl.DataFrame(metadata_data)
        mock_read_delta.return_value = mock_df

        # Act
        result = _read_metadata_from_delta_lake(ticker)

        # Assert - Test behavior: should use first record (deterministic)
        self.assertIsNotNone(result)
        self.assertEqual(result['sector'], 'Technology')  # First record
        self.assertEqual(result['name'], 'Apple Inc.')  # First record

    @patch('workers.tasks.update_stock_metadata.DeltaTable')
    def test_delta_table_not_found_raises_read_error(self, mock_delta_table):
        """Test that missing Delta Lake table raises DeltaLakeReadError."""
        # Arrange
        from deltalake.exceptions import TableNotFoundError
        ticker = 'AAPL'
        mock_delta_table.side_effect = TableNotFoundError('Table not found')

        # Act & Assert - Test exception type (message is implementation detail)
        with self.assertRaises(DeltaLakeReadError):
            _read_metadata_from_delta_lake(ticker)


class UpdateStockWithMetadataTests(TransactionTestCase):
    """Tests for updating Stock model with metadata using transactions."""

    def setUp(self):
        """Create test stock."""
        self.stock = Stock.objects.create(ticker='AAPL')

    def test_update_stock_metadata_success(self):
        """Test successful update of stock metadata fields."""
        # Arrange
        metadata_dict = {
            'sector': 'Information Technology',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'country': 'US',
        }

        # Act
        fields_updated = _update_stock_with_metadata(self.stock.id, metadata_dict)

        # Assert - Test return value
        self.assertEqual(set(fields_updated), set(metadata_dict.keys()))
        
        # Verify database state was updated correctly
        self.stock.refresh_from_db()
        self.assertEqual(self.stock.sector, 'Information Technology')
        self.assertEqual(self.stock.name, 'Apple Inc.')
        self.assertEqual(self.stock.exchange, 'NASDAQ')
        self.assertEqual(self.stock.country, 'US')

    def test_partial_metadata_update_leaves_other_fields_unchanged(self):
        """Test updating only a subset of metadata fields (partial update)."""
        # Arrange
        metadata_dict = {
            'sector': 'Technology',
            'name': 'Apple Inc.',
        }

        # Act
        fields_updated = _update_stock_with_metadata(self.stock.id, metadata_dict)

        # Assert - Test return value
        self.assertEqual(len(fields_updated), 2)
        self.assertIn('sector', fields_updated)
        self.assertIn('name', fields_updated)
        
        # Verify behavior: only specified fields updated, others unchanged
        self.stock.refresh_from_db()
        self.assertEqual(self.stock.sector, 'Technology')
        self.assertEqual(self.stock.name, 'Apple Inc.')
        self.assertIsNone(self.stock.exchange)  # Not updated

    def test_empty_metadata_dict_is_noop(self):
        """Test that empty metadata dict is handled gracefully (no-op)."""
        # Arrange
        metadata_dict = {}

        # Act
        fields_updated = _update_stock_with_metadata(self.stock.id, metadata_dict)

        # Assert - Test behavior: should succeed with no updates
        self.assertEqual(fields_updated, [])

    def test_transaction_rollback_on_error_prevents_partial_updates(self):
        """Test that transaction rolls back completely on error (no partial updates)."""
        # Arrange
        metadata_dict = {
            'sector': 'Technology',
            'name': 'Apple Inc.',
        }
        original_sector = self.stock.sector
        original_name = self.stock.name

        # Act - Simulate failure during save
        with patch.object(Stock, 'save', side_effect=DatabaseError('Constraint violation')):
            with self.assertRaises(Exception):
                _update_stock_with_metadata(self.stock.id, metadata_dict)

        # Assert - Verify transaction rollback: no changes were committed
        self.stock.refresh_from_db()
        self.assertEqual(self.stock.sector, original_sector)
        self.assertEqual(self.stock.name, original_name)

    def test_select_for_update_acquires_lock(self):
        """Test that select_for_update properly acquires row lock (transaction safety)."""
        # Arrange
        metadata_dict = {'sector': 'Technology'}

        # Act
        with transaction.atomic():
            # Start a transaction and lock the row
            locked_stock = Stock.objects.select_for_update().get(id=self.stock.id)
            
            # Verify we can update within same transaction
            fields_updated = _update_stock_with_metadata(self.stock.id, metadata_dict)
            
            # Assert - Test behavior: update succeeds within same transaction
            self.assertIn('sector', fields_updated)

    def test_invalid_field_in_metadata_dict_is_skipped(self):
        """Test that invalid field names are gracefully skipped (defensive programming)."""
        # Arrange
        metadata_dict = {
            'sector': 'Technology',
            'invalid_field': 'Some value',  # Not a Stock model field
        }

        # Act
        fields_updated = _update_stock_with_metadata(self.stock.id, metadata_dict)

        # Assert - Test behavior: only valid fields updated, invalid ones skipped
        self.assertIn('sector', fields_updated)
        self.assertNotIn('invalid_field', fields_updated)
        
        # Verify database state
        self.stock.refresh_from_db()
        self.assertEqual(self.stock.sector, 'Technology')
        self.assertFalse(hasattr(self.stock, 'invalid_field'))


class MetadataWorkerIntegrationTests(TransactionTestCase):
    """Integration tests for the complete metadata worker flow."""

    def setUp(self):
        """Create test stock."""
        self.stock = Stock.objects.create(ticker='AAPL')

    @patch('workers.tasks.update_stock_metadata._read_metadata_from_delta_lake')
    def test_end_to_end_metadata_update(self, mock_read):
        """Test complete flow from Delta Lake read to Stock update (integration test)."""
        # Arrange
        metadata_dict = {
            'sector': 'Information Technology',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'country': 'US',
            'subindustry': 'Technology Hardware, Storage & Peripherals',
            'morningstar_sector': 'Technology',
            'morningstar_industry': 'Consumer Electronics',
            'industry': 'Technology Hardware',
            'description': 'Apple Inc. designs, manufactures, and markets smartphones...',
        }
        mock_read.return_value = metadata_dict

        # Act
        result = update_stock_metadata(self.stock.ticker)

        # Assert - Test result structure
        self.assertTrue(result['updated'])
        self.assertFalse(result['skipped'])
        
        # Verify complete database state after integration
        self.stock.refresh_from_db()
        self.assertEqual(self.stock.sector, 'Information Technology')
        self.assertEqual(self.stock.name, 'Apple Inc.')
        self.assertEqual(self.stock.exchange, 'NASDAQ')
        self.assertEqual(self.stock.country, 'US')
        self.assertEqual(self.stock.subindustry, 'Technology Hardware, Storage & Peripherals')
        self.assertEqual(self.stock.morningstar_sector, 'Technology')
        self.assertEqual(self.stock.morningstar_industry, 'Consumer Electronics')
        self.assertEqual(self.stock.industry, 'Technology Hardware')
        self.assertIn('Apple Inc. designs', self.stock.description)

