"""
Tests for the queue_all_stocks_for_fetch Celery task.

This module contains comprehensive tests for:
- queue_all_stocks_for_fetch task
- BulkQueueRun statistics tracking
- Error handling and individual stock failures
- Foreign key linking between StockIngestionRun and BulkQueueRun
- Idempotency checks (skipping active runs)
"""

import uuid
from unittest.mock import patch, call

from django.test import TransactionTestCase
from django.utils import timezone

from api.models import BulkQueueRun, Exchange, IngestionState, Stock, StockIngestionRun
from workers.exceptions import NonRetryableError
from workers.tasks.queue_all_stocks_for_fetch import queue_all_stocks_for_fetch


@patch('workers.tasks.queue_for_fetch.fetch_stock_data.delay')
class QueueAllStocksForFetchTaskTest(TransactionTestCase):
    """Tests for the queue_all_stocks_for_fetch Celery task."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create test stocks
        self.stock1 = Stock.objects.create(ticker='AAPL')
        self.stock2 = Stock.objects.create(ticker='GOOGL')
        self.stock3 = Stock.objects.create(ticker='MSFT')
        
        # Create a BulkQueueRun
        self.bulk_queue_run = BulkQueueRun.objects.create(
            requested_by='test-user'
        )
    
    def test_successful_task_execution_all_stocks_queued(self, mock_fetch_delay):
        """Test successful task execution that queues all stocks."""
        # Execute task
        result = queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Verify result
        self.assertEqual(result['bulk_queue_run_id'], str(self.bulk_queue_run.id))
        self.assertEqual(result['total_stocks'], 3)
        self.assertEqual(result['queued_count'], 3)
        self.assertEqual(result['skipped_count'], 0)
        self.assertEqual(result['error_count'], 0)
        self.assertTrue(result['success'])
        
        # Verify BulkQueueRun was updated
        self.bulk_queue_run.refresh_from_db()
        self.assertEqual(self.bulk_queue_run.total_stocks, 3)
        self.assertEqual(self.bulk_queue_run.queued_count, 3)
        self.assertEqual(self.bulk_queue_run.skipped_count, 0)
        self.assertEqual(self.bulk_queue_run.error_count, 0)
        self.assertIsNotNone(self.bulk_queue_run.started_at)
        self.assertIsNotNone(self.bulk_queue_run.completed_at)
        self.assertGreaterEqual(
            self.bulk_queue_run.completed_at,
            self.bulk_queue_run.started_at
        )
        
        # Verify fetch_stock_data.delay was called for each stock
        self.assertEqual(mock_fetch_delay.call_count, 3)
        
        # Verify StockIngestionRun instances were created and linked
        runs = StockIngestionRun.objects.filter(bulk_queue_run=self.bulk_queue_run)
        self.assertEqual(runs.count(), 3)
        
        # Verify all runs are in QUEUED_FOR_FETCH state
        for run in runs:
            self.assertEqual(run.state, IngestionState.QUEUED_FOR_FETCH)
            self.assertEqual(run.bulk_queue_run_id, self.bulk_queue_run.id)
            self.assertEqual(run.requested_by, 'test-user')
            self.assertEqual(run.request_id, f"bulk-queue-{self.bulk_queue_run.id}")
    
    def test_started_at_updated_when_processing_begins(self, mock_fetch_delay):
        """Test that BulkQueueRun.started_at is updated when processing begins."""
        # Verify started_at is initially None
        self.assertIsNone(self.bulk_queue_run.started_at)
        
        # Execute task
        queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Verify started_at was set
        self.bulk_queue_run.refresh_from_db()
        self.assertIsNotNone(self.bulk_queue_run.started_at)
        self.assertLessEqual(self.bulk_queue_run.started_at, timezone.now())
    
    def test_completed_at_updated_when_processing_finishes(self, mock_fetch_delay):
        """Test that BulkQueueRun.completed_at is updated when processing finishes."""
        # Verify completed_at is initially None
        self.assertIsNone(self.bulk_queue_run.completed_at)
        
        # Execute task
        queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Verify completed_at was set
        self.bulk_queue_run.refresh_from_db()
        self.assertIsNotNone(self.bulk_queue_run.completed_at)
        self.assertLessEqual(self.bulk_queue_run.completed_at, timezone.now())
    
    def test_existing_active_runs_are_skipped(self, mock_fetch_delay):
        """Test that stocks with existing active runs are skipped (idempotency)."""
        # Create an existing active run for AAPL
        existing_run = StockIngestionRun.objects.create(
            stock=self.stock1,
            state=IngestionState.FETCHING
        )
        
        # Execute task
        result = queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Verify result
        self.assertEqual(result['total_stocks'], 3)
        self.assertEqual(result['queued_count'], 2)  # GOOGL and MSFT
        self.assertEqual(result['skipped_count'], 1)  # AAPL
        self.assertEqual(result['error_count'], 0)
        
        # Verify BulkQueueRun statistics
        self.bulk_queue_run.refresh_from_db()
        self.assertEqual(self.bulk_queue_run.queued_count, 2)
        self.assertEqual(self.bulk_queue_run.skipped_count, 1)
        
        # Verify fetch_stock_data.delay was only called twice (not for AAPL)
        self.assertEqual(mock_fetch_delay.call_count, 2)
        
        # Verify the existing run was still linked to the BulkQueueRun
        existing_run.refresh_from_db()
        self.assertEqual(existing_run.bulk_queue_run_id, self.bulk_queue_run.id)
        
        # Verify new runs were created for the other stocks
        new_runs = StockIngestionRun.objects.filter(
            bulk_queue_run=self.bulk_queue_run,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        self.assertEqual(new_runs.count(), 2)
    
    def test_error_handling_for_individual_stock_failures(self, mock_fetch_delay):
        """Test that individual stock failures are handled gracefully."""
        # Mock queue_for_fetch to raise an exception for GOOGL
        with patch('workers.tasks.queue_all_stocks_for_fetch.StockIngestionService') as mock_service:
            service_instance = mock_service.return_value
            
            def queue_for_fetch_side_effect(ticker, **kwargs):
                if ticker == 'GOOGL':
                    raise Exception("Database error for GOOGL")
                
                # Create a real run for other stocks
                stock = Stock.objects.get(ticker=ticker)
                run = StockIngestionRun.objects.create(
                    stock=stock,
                    state=IngestionState.QUEUED_FOR_FETCH,
                    requested_by=kwargs.get('requested_by'),
                    request_id=kwargs.get('request_id')
                )
                return run, True
            
            service_instance.queue_for_fetch.side_effect = queue_for_fetch_side_effect
            
            # Execute task
            result = queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
            
            # Verify result - should have 1 error, 2 queued
            self.assertEqual(result['total_stocks'], 3)
            self.assertEqual(result['queued_count'], 2)
            self.assertEqual(result['skipped_count'], 0)
            self.assertEqual(result['error_count'], 1)
            self.assertTrue(result['success'])  # Task completes even with individual failures
            
            # Verify BulkQueueRun statistics
            self.bulk_queue_run.refresh_from_db()
            self.assertEqual(self.bulk_queue_run.error_count, 1)
    
    def test_ingestion_runs_properly_linked_to_bulk_queue_run(self, mock_fetch_delay):
        """Test that StockIngestionRun instances are properly linked to BulkQueueRun."""
        # Execute task
        queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Verify all runs are linked to the BulkQueueRun
        runs = StockIngestionRun.objects.filter(bulk_queue_run=self.bulk_queue_run)
        self.assertEqual(runs.count(), 3)
        
        for run in runs:
            self.assertEqual(run.bulk_queue_run_id, self.bulk_queue_run.id)
            self.assertEqual(run.bulk_queue_run, self.bulk_queue_run)
    
    def test_queued_count_increments_for_successfully_queued_stocks(self, mock_fetch_delay):
        """Test that queued_count increments correctly for each successfully queued stock."""
        # Execute task
        queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Verify queued_count
        self.bulk_queue_run.refresh_from_db()
        self.assertEqual(self.bulk_queue_run.queued_count, 3)
        
        # Verify fetch_stock_data.delay was called for each queued stock
        self.assertEqual(mock_fetch_delay.call_count, 3)
    
    def test_query_failed_stocks_by_bulk_queue_run_and_state(self, mock_fetch_delay):
        """Test querying failed stocks by filtering StockIngestionRun by bulk_queue_run and state=FAILED."""
        # Execute task to create runs
        queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Manually mark some runs as FAILED (simulating failures)
        run1 = StockIngestionRun.objects.get(stock=self.stock1, bulk_queue_run=self.bulk_queue_run)
        run1.state = IngestionState.FAILED
        run1.error_code = 'API_ERROR'
        run1.error_message = 'Failed to fetch data'
        run1.save()
        
        run2 = StockIngestionRun.objects.get(stock=self.stock2, bulk_queue_run=self.bulk_queue_run)
        run2.state = IngestionState.FAILED
        run2.error_code = 'STORAGE_ERROR'
        run2.error_message = 'Failed to upload'
        run2.save()
        
        # Query failed stocks
        failed_runs = StockIngestionRun.objects.filter(
            bulk_queue_run=self.bulk_queue_run,
            state=IngestionState.FAILED
        )
        
        # Verify we can find the failed runs
        self.assertEqual(failed_runs.count(), 2)
        failed_tickers = [run.stock.ticker for run in failed_runs]
        self.assertIn('AAPL', failed_tickers)
        self.assertIn('GOOGL', failed_tickers)
    
    def test_empty_stock_database(self, mock_fetch_delay):
        """Test with empty stock database (verify all counts are 0, completed_at is set)."""
        # Delete all stocks
        Stock.objects.all().delete()
        
        # Execute task
        result = queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Verify result
        self.assertEqual(result['total_stocks'], 0)
        self.assertEqual(result['queued_count'], 0)
        self.assertEqual(result['skipped_count'], 0)
        self.assertEqual(result['error_count'], 0)
        self.assertTrue(result['success'])
        
        # Verify BulkQueueRun was updated
        self.bulk_queue_run.refresh_from_db()
        self.assertEqual(self.bulk_queue_run.total_stocks, 0)
        self.assertEqual(self.bulk_queue_run.queued_count, 0)
        self.assertIsNotNone(self.bulk_queue_run.started_at)
        self.assertIsNotNone(self.bulk_queue_run.completed_at)
        
        # Verify no tasks were queued
        mock_fetch_delay.assert_not_called()
    
    def test_bulk_queue_run_not_found(self, mock_fetch_delay):
        """Test error handling when BulkQueueRun is not found."""
        fake_id = str(uuid.uuid4())
        
        # Execute task - should raise NonRetryableError
        with self.assertRaises(NonRetryableError) as cm:
            queue_all_stocks_for_fetch(fake_id)
        
        # Verify error message
        self.assertIn('BulkQueueRun not found', str(cm.exception))
        
        # Verify no tasks were queued
        mock_fetch_delay.assert_not_called()
    
    def test_invalid_uuid_format(self, mock_fetch_delay):
        """Test error handling when bulk_queue_run_id is not a valid UUID."""
        invalid_id = 'not-a-uuid'
        
        # Execute task - should raise NonRetryableError
        with self.assertRaises(NonRetryableError) as cm:
            queue_all_stocks_for_fetch(invalid_id)
        
        # Verify error message
        self.assertIn('BulkQueueRun not found', str(cm.exception))
    
    def test_multiple_stocks_with_mixed_states(self, mock_fetch_delay):
        """Test processing with a mix of new stocks and stocks with existing runs in various states."""
        # Create existing runs with different states
        StockIngestionRun.objects.create(
            stock=self.stock1,
            state=IngestionState.FETCHING  # Active - should be skipped
        )
        
        StockIngestionRun.objects.create(
            stock=self.stock2,
            state=IngestionState.DONE  # Terminal - should create new run
        )
        
        # stock3 has no existing run - should create new run
        
        # Execute task
        result = queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Verify result
        self.assertEqual(result['total_stocks'], 3)
        self.assertEqual(result['queued_count'], 2)  # GOOGL (after DONE), MSFT (new)
        self.assertEqual(result['skipped_count'], 1)  # AAPL (FETCHING)
        self.assertEqual(result['error_count'], 0)
        
        # Verify BulkQueueRun statistics
        self.bulk_queue_run.refresh_from_db()
        self.assertEqual(self.bulk_queue_run.queued_count, 2)
        self.assertEqual(self.bulk_queue_run.skipped_count, 1)
    
    def test_fetch_task_queueing_failure_increments_error_count(self, mock_fetch_delay):
        """Test that failures to queue fetch_stock_data tasks are counted as errors."""
        # Mock fetch_stock_data.delay to raise an exception
        mock_fetch_delay.side_effect = Exception("RabbitMQ connection error")
        
        # Execute task
        result = queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Verify all stocks failed to queue due to RabbitMQ error
        self.assertEqual(result['total_stocks'], 3)
        self.assertEqual(result['queued_count'], 0)  # None successfully queued
        self.assertEqual(result['error_count'], 3)  # All failed to queue
        
        # Verify BulkQueueRun statistics
        self.bulk_queue_run.refresh_from_db()
        self.assertEqual(self.bulk_queue_run.error_count, 3)
    
    def test_progress_logging_with_many_stocks(self, mock_fetch_delay):
        """Test that progress is logged at appropriate intervals (every 100 stocks)."""
        # Create many stocks (150 total)
        for i in range(147):  # Already have 3 stocks from setUp
            Stock.objects.create(ticker=f'STOCK{i:04d}')
        
        # Verify we have 150 stocks
        self.assertEqual(Stock.objects.count(), 150)
        
        # Execute task (with logging - we won't assert on logs, just verify it completes)
        result = queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Verify result
        self.assertEqual(result['total_stocks'], 150)
        self.assertEqual(result['queued_count'], 150)
        self.assertEqual(result['error_count'], 0)
        
        # Verify all fetch tasks were queued
        self.assertEqual(mock_fetch_delay.call_count, 150)
    
    def test_runs_created_in_correct_order(self, mock_fetch_delay):
        """Test that stocks are processed in alphabetical order by ticker."""
        # Execute task
        queue_all_stocks_for_fetch(str(self.bulk_queue_run.id))
        
        # Get all runs for this bulk queue run, ordered by creation
        runs = StockIngestionRun.objects.filter(
            bulk_queue_run=self.bulk_queue_run
        ).select_related('stock').order_by('created_at')
        
        # Verify tickers are in alphabetical order (AAPL, GOOGL, MSFT)
        tickers = [run.stock.ticker for run in runs]
        self.assertEqual(tickers, ['AAPL', 'GOOGL', 'MSFT'])
    
    def test_queue_all_stocks_with_exchange_filter(self, mock_fetch_delay):
        """Test queuing all stocks with exchange_name parameter filters stocks correctly."""
        # Create exchanges
        nasdaq = Exchange.objects.create(name='NASDAQ')
        nyse = Exchange.objects.create(name='NYSE')
        
        # Update stocks with exchanges
        self.stock1.exchange = nasdaq
        self.stock1.save()
        self.stock2.exchange = nasdaq
        self.stock2.save()
        self.stock3.exchange = nyse
        self.stock3.save()
        
        # Execute task with exchange_name='NASDAQ'
        result = queue_all_stocks_for_fetch(
            str(self.bulk_queue_run.id),
            exchange_name='NASDAQ'
        )
        
        # Verify result
        self.assertEqual(result['bulk_queue_run_id'], str(self.bulk_queue_run.id))
        self.assertEqual(result['total_stocks'], 2)  # Only NASDAQ stocks
        self.assertEqual(result['queued_count'], 2)
        self.assertEqual(result['skipped_count'], 0)
        self.assertEqual(result['error_count'], 0)
        self.assertTrue(result['success'])
        
        # Verify BulkQueueRun was updated with filtered count
        self.bulk_queue_run.refresh_from_db()
        self.assertEqual(self.bulk_queue_run.total_stocks, 2)
        self.assertEqual(self.bulk_queue_run.queued_count, 2)
        
        # Verify only NASDAQ stocks were queued
        self.assertEqual(mock_fetch_delay.call_count, 2)
        
        # Verify StockIngestionRun instances were created only for NASDAQ stocks
        runs = StockIngestionRun.objects.filter(bulk_queue_run=self.bulk_queue_run)
        self.assertEqual(runs.count(), 2)
        
        # Verify the correct stocks were queued (AAPL and GOOGL, not MSFT)
        queued_tickers = set(run.stock.ticker for run in runs)
        self.assertEqual(queued_tickers, {'AAPL', 'GOOGL'})
    
    def test_queue_all_stocks_without_exchange_filter(self, mock_fetch_delay):
        """Test queuing all stocks without exchange_name parameter processes all stocks."""
        # Create exchanges
        nasdaq = Exchange.objects.create(name='NASDAQ')
        nyse = Exchange.objects.create(name='NYSE')
        
        # Update stocks with exchanges
        self.stock1.exchange = nasdaq
        self.stock1.save()
        self.stock2.exchange = nasdaq
        self.stock2.save()
        self.stock3.exchange = nyse
        self.stock3.save()
        
        # Execute task without exchange_name (None)
        result = queue_all_stocks_for_fetch(
            str(self.bulk_queue_run.id),
            exchange_name=None
        )
        
        # Verify result includes all stocks
        self.assertEqual(result['total_stocks'], 3)
        self.assertEqual(result['queued_count'], 3)
        
        # Verify all stocks were queued
        self.assertEqual(mock_fetch_delay.call_count, 3)
        
        # Verify StockIngestionRun instances were created for all stocks
        runs = StockIngestionRun.objects.filter(bulk_queue_run=self.bulk_queue_run)
        self.assertEqual(runs.count(), 3)
    
    def test_queue_all_stocks_with_non_existent_exchange(self, mock_fetch_delay):
        """Test handling of non-existent exchange in worker task."""
        # Execute task with non-existent exchange
        result = queue_all_stocks_for_fetch(
            str(self.bulk_queue_run.id),
            exchange_name='NONEXISTENT'
        )
        
        # Verify result shows failure
        self.assertEqual(result['bulk_queue_run_id'], str(self.bulk_queue_run.id))
        self.assertEqual(result['total_stocks'], 0)
        self.assertEqual(result['queued_count'], 0)
        self.assertEqual(result['skipped_count'], 0)
        self.assertEqual(result['error_count'], 0)
        self.assertFalse(result['success'])
        
        # Verify BulkQueueRun was updated
        self.bulk_queue_run.refresh_from_db()
        self.assertIsNotNone(self.bulk_queue_run.started_at)
        self.assertIsNotNone(self.bulk_queue_run.completed_at)
        
        # Verify no stocks were queued
        mock_fetch_delay.assert_not_called()
        
        # Verify no StockIngestionRun instances were created
        runs = StockIngestionRun.objects.filter(bulk_queue_run=self.bulk_queue_run)
        self.assertEqual(runs.count(), 0)
    
    def test_queue_all_stocks_exchange_name_normalization(self, mock_fetch_delay):
        """Test that exchange name is normalized in worker task."""
        # Create exchange with uppercase name
        nasdaq = Exchange.objects.create(name='NASDAQ')
        
        # Update stocks with exchange
        self.stock1.exchange = nasdaq
        self.stock1.save()
        self.stock2.exchange = nasdaq
        self.stock2.save()
        
        # Execute task with lowercase and whitespace
        result = queue_all_stocks_for_fetch(
            str(self.bulk_queue_run.id),
            exchange_name='  nasdaq  '
        )
        
        # Verify stocks were filtered correctly (normalization worked)
        self.assertEqual(result['total_stocks'], 2)
        self.assertEqual(result['queued_count'], 2)
        self.assertTrue(result['success'])
        
        # Verify correct stocks were queued
        runs = StockIngestionRun.objects.filter(bulk_queue_run=self.bulk_queue_run)
        self.assertEqual(runs.count(), 2)
        queued_tickers = set(run.stock.ticker for run in runs)
        self.assertEqual(queued_tickers, {'AAPL', 'GOOGL'})
    
    def test_queue_all_stocks_exchange_filter_with_skipped_runs(self, mock_fetch_delay):
        """Test exchange filtering with some stocks already having active runs."""
        # Create exchange
        nasdaq = Exchange.objects.create(name='NASDAQ')
        nyse = Exchange.objects.create(name='NYSE')
        
        # Update stocks with exchanges
        self.stock1.exchange = nasdaq
        self.stock1.save()
        self.stock2.exchange = nasdaq
        self.stock2.save()
        self.stock3.exchange = nyse
        self.stock3.save()
        
        # Create an existing active run for stock1 (AAPL, NASDAQ)
        existing_run = StockIngestionRun.objects.create(
            stock=self.stock1,
            state=IngestionState.FETCHING
        )
        
        # Execute task with exchange_name='NASDAQ'
        result = queue_all_stocks_for_fetch(
            str(self.bulk_queue_run.id),
            exchange_name='NASDAQ'
        )
        
        # Verify result
        self.assertEqual(result['total_stocks'], 2)  # AAPL and GOOGL
        self.assertEqual(result['queued_count'], 1)  # Only GOOGL queued
        self.assertEqual(result['skipped_count'], 1)  # AAPL skipped
        self.assertEqual(result['error_count'], 0)
        self.assertTrue(result['success'])
        
        # Verify only 1 new task was queued (for GOOGL)
        self.assertEqual(mock_fetch_delay.call_count, 1)
        
        # Verify MSFT (NYSE) was not processed at all
        msft_runs = StockIngestionRun.objects.filter(stock=self.stock3)
        self.assertEqual(msft_runs.count(), 0)
    
    def test_queue_all_stocks_exchange_filter_empty_result(self, mock_fetch_delay):
        """Test exchange filtering when no stocks belong to the exchange."""
        # Create exchange
        nasdaq = Exchange.objects.create(name='NASDAQ')
        nyse = Exchange.objects.create(name='NYSE')
        
        # All stocks belong to NYSE
        self.stock1.exchange = nyse
        self.stock1.save()
        self.stock2.exchange = nyse
        self.stock2.save()
        self.stock3.exchange = nyse
        self.stock3.save()
        
        # Execute task with exchange_name='NASDAQ' (no stocks have this exchange)
        result = queue_all_stocks_for_fetch(
            str(self.bulk_queue_run.id),
            exchange_name='NASDAQ'
        )
        
        # Verify result shows 0 stocks
        self.assertEqual(result['total_stocks'], 0)
        self.assertEqual(result['queued_count'], 0)
        self.assertEqual(result['skipped_count'], 0)
        self.assertEqual(result['error_count'], 0)
        self.assertTrue(result['success'])
        
        # Verify no tasks were queued
        mock_fetch_delay.assert_not_called()
        
        # Verify BulkQueueRun reflects 0 stocks
        self.bulk_queue_run.refresh_from_db()
        self.assertEqual(self.bulk_queue_run.total_stocks, 0)
    
    def test_queue_all_stocks_exchange_filter_with_null_exchange_stocks(self, mock_fetch_delay):
        """Test that stocks with null exchange are not included in exchange filter."""
        # Create exchange
        nasdaq = Exchange.objects.create(name='NASDAQ')
        
        # Only stock1 has NASDAQ exchange, others have null
        self.stock1.exchange = nasdaq
        self.stock1.save()
        # stock2 and stock3 have exchange=None
        
        # Execute task with exchange_name='NASDAQ'
        result = queue_all_stocks_for_fetch(
            str(self.bulk_queue_run.id),
            exchange_name='NASDAQ'
        )
        
        # Verify only stock1 was processed
        self.assertEqual(result['total_stocks'], 1)
        self.assertEqual(result['queued_count'], 1)
        
        # Verify only 1 task was queued
        self.assertEqual(mock_fetch_delay.call_count, 1)
        
        # Verify only AAPL was queued
        runs = StockIngestionRun.objects.filter(bulk_queue_run=self.bulk_queue_run)
        self.assertEqual(runs.count(), 1)
        self.assertEqual(runs.first().stock.ticker, 'AAPL')
