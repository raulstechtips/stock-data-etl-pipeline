"""
Tests for the StockIngestionService.

This test module covers:
- Retrieving stock status for existing and missing stocks
- Creating and updating StockIngestionRun entries via service methods
- Validating state transitions and updating run timestamps
- Handling invalid or terminal state transitions with appropriate errors
- Ensuring correct handling of concurrent actions and database errors
- Verifying state transitions for both success and failure scenarios
"""

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch


from django.test import TestCase, TransactionTestCase
from django.db import close_old_connections
from django.db.utils import DatabaseError

from api.models import IngestionState, Stock, StockIngestionRun
from api.services import StockIngestionService
from api.services.stock_ingestion_service import (
    IngestionRunNotFoundError,
    InvalidStateTransitionError,
    StockNotFoundError,
)


class StockIngestionServiceTest(TestCase):
    """Tests for StockIngestionService business logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')

    def test_get_stock_status_existing_stock_with_run(self):
        """Test getting status for an existing stock with a run."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        
        result = self.service.get_stock_status('AAPL')
        
        self.assertEqual(result.ticker, 'AAPL')
        self.assertEqual(result.stock_id, self.stock.id)
        self.assertEqual(result.run_id, run.id)
        self.assertEqual(result.state, IngestionState.FETCHING)

    def test_get_stock_status_existing_stock_no_runs(self):
        """Test getting status for a stock with no runs."""
        result = self.service.get_stock_status('AAPL')
        
        self.assertEqual(result.ticker, 'AAPL')
        self.assertEqual(result.stock_id, self.stock.id)
        self.assertIsNone(result.run_id)
        self.assertIsNone(result.state)

    def test_get_stock_status_not_found(self):
        """Test getting status for a non-existent stock."""
        with self.assertRaises(StockNotFoundError):
            self.service.get_stock_status('NONEXISTENT')

    def test_get_stock_status_case_insensitive(self):
        """Test that get_stock_status is case-insensitive."""
        result = self.service.get_stock_status('aapl')
        self.assertEqual(result.ticker, 'AAPL')

    def test_get_or_create_stock_creates_new(self):
        """Test creating a new stock."""
        stock, created = self.service.get_or_create_stock('GOOGL')
        
        self.assertTrue(created)
        self.assertEqual(stock.ticker, 'GOOGL')

    def test_get_or_create_stock_returns_existing(self):
        """Test returning an existing stock."""
        stock, created = self.service.get_or_create_stock('AAPL')
        
        self.assertFalse(created)
        self.assertEqual(stock.id, self.stock.id)

    def test_queue_for_fetch_creates_new_run(self):
        """Test queuing creates a new run when no active run exists."""
        run, created = self.service.queue_for_fetch(
            ticker='AAPL',
            requested_by='test-service',
            request_id='test-123'
        )
        
        self.assertTrue(created)
        self.assertEqual(run.stock, self.stock)
        self.assertEqual(run.state, IngestionState.QUEUED_FOR_FETCH)
        self.assertEqual(run.requested_by, 'test-service')
        self.assertEqual(run.request_id, 'test-123')
        self.assertIsNotNone(run.queued_for_fetch_at)

    def test_queue_for_fetch_returns_existing_active_run(self):
        """Test that queuing returns existing active run instead of creating new."""
        existing_run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        
        run, created = self.service.queue_for_fetch(ticker='AAPL')
        
        self.assertFalse(created)
        self.assertEqual(run.id, existing_run.id)

    def test_queue_for_fetch_creates_run_when_terminal_exists(self):
        """Test that queuing creates new run when only terminal runs exist."""
        StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE
        )
        
        run, created = self.service.queue_for_fetch(ticker='AAPL')
        
        self.assertTrue(created)
        self.assertEqual(run.state, IngestionState.QUEUED_FOR_FETCH)

    def test_queue_for_fetch_creates_stock_if_not_exists(self):
        """Test that queuing creates the stock if it doesn't exist."""
        _run, created = self.service.queue_for_fetch(ticker='NEWSTOCK')
        
        self.assertTrue(created)
        self.assertTrue(Stock.objects.filter(ticker='NEWSTOCK').exists())

    def test_queue_for_fetch_generates_request_id(self):
        """Test that request_id is auto-generated if not provided."""
        run, _ = self.service.queue_for_fetch(ticker='AAPL')
        
        self.assertIsNotNone(run.request_id)

    def test_update_run_state_valid_transition(self):
        """Test updating run state with valid transition."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        updated_run = self.service.update_run_state(
            run_id=run.id,
            new_state=IngestionState.FETCHING
        )
        
        self.assertEqual(updated_run.state, IngestionState.FETCHING)
        self.assertIsNotNone(updated_run.fetching_started_at)

    def test_update_run_state_invalid_transition(self):
        """Test that invalid state transitions raise an error."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        with self.assertRaises(InvalidStateTransitionError):
            self.service.update_run_state(
                run_id=run.id,
                new_state=IngestionState.DONE  # Invalid: QUEUED_FOR_FETCH -> DONE
            )

    def test_update_run_state_to_failed(self):
        """Test updating run state to FAILED with error info."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        
        updated_run = self.service.update_run_state(
            run_id=run.id,
            new_state=IngestionState.FAILED,
            error_code='FETCH_ERROR',
            error_message='Connection timeout'
        )
        
        self.assertEqual(updated_run.state, IngestionState.FAILED)
        self.assertEqual(updated_run.error_code, 'FETCH_ERROR')
        self.assertEqual(updated_run.error_message, 'Connection timeout')

    def test_update_run_state_not_found(self):
        """Test updating non-existent run raises error."""
        fake_id = uuid.uuid4()
        
        with self.assertRaises(IngestionRunNotFoundError):
            self.service.update_run_state(
                run_id=fake_id,
                new_state=IngestionState.FETCHING
            )

    def test_update_run_state_with_data_uris(self):
        """Test updating run state with data URIs."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        
        updated_run = self.service.update_run_state(
            run_id=run.id,
            new_state=IngestionState.FETCHED,
            raw_data_uri='s3://bucket/raw/AAPL'
        )
        
        self.assertEqual(updated_run.raw_data_uri, 's3://bucket/raw/AAPL')

    def test_get_run_by_id(self):
        """Test getting a run by its ID."""
        run = StockIngestionRun.objects.create(stock=self.stock)
        
        retrieved_run = self.service.get_run_by_id(run.id)
        
        self.assertEqual(retrieved_run.id, run.id)

    def test_get_run_by_id_not_found(self):
        """Test getting a non-existent run raises error."""
        fake_id = uuid.uuid4()
        
        with self.assertRaises(IngestionRunNotFoundError):
            self.service.get_run_by_id(fake_id)



class StockIngestionServiceTransactionTest(TransactionTestCase):
    """
    Transaction-specific tests for StockIngestionService.
    
    Uses TransactionTestCase for testing atomic operations and
    row-level locking behavior.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')

    def test_queue_for_fetch_atomic_rollback(self):
        """Test that queue_for_fetch rolls back on error."""
        initial_run_count = StockIngestionRun.objects.count()
        
        # Mock an error during the operation
        with patch.object(
            StockIngestionRun.objects,
            'create',
            side_effect=DatabaseError('Database error')
        ):
            with self.assertRaises(DatabaseError):
                self.service.queue_for_fetch(ticker='NEWSTOCK')
        
        # Verify no run was created
        self.assertEqual(StockIngestionRun.objects.count(), initial_run_count)
        self.assertFalse(Stock.objects.filter(ticker='NEWSTOCK').exists())
    
    @patch('workers.tasks.send_discord_notification.send_discord_notification.delay')
    def test_update_run_state_concurrent_updates(self, mock_discord_delay):
        """Test that concurrent state updates are handled correctly."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        def update_state():
            close_old_connections()
            service = StockIngestionService()
            return service.update_run_state(
                run_id=run.id,
                new_state=IngestionState.FETCHING,
            )

        results = []
        errors = []
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = [ex.submit(update_state) for _ in range(3)]
            for f in as_completed(futures, timeout=5):
                try:
                    results.append(f.result())
                except InvalidStateTransitionError as e:
                    errors.append(e)
        # Refresh and verify state
        run.refresh_from_db()
        self.assertEqual(run.state, IngestionState.FETCHING)
        
        # At least one should succeed, others may fail due to invalid transition
        self.assertGreaterEqual(len(results), 1)


class StateTransitionTest(TestCase):
    """Tests for valid state transitions in the ETL pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')

    def test_full_successful_pipeline_flow(self):
        """Test a complete successful flow through the pipeline."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Progress through all states
        transitions = [
            (IngestionState.FETCHING, 'fetching_started_at'),
            (IngestionState.FETCHED, 'fetching_finished_at'),
            (IngestionState.QUEUED_FOR_DELTA, 'queued_for_delta_at'),
            (IngestionState.DELTA_RUNNING, 'delta_started_at'),
            (IngestionState.DELTA_FINISHED, 'delta_finished_at'),
            (IngestionState.DONE, 'done_at'),
        ]
        
        for new_state, timestamp_field in transitions:
            run = self.service.update_run_state(
                run_id=run.id,
                new_state=new_state
            )
            self.assertEqual(run.state, new_state)
            self.assertIsNotNone(getattr(run, timestamp_field))
        
        self.assertTrue(run.is_terminal)

    def test_failure_from_any_active_state(self):
        """Test that any active state can transition to FAILED."""
        active_states = [
            IngestionState.QUEUED_FOR_FETCH,
            IngestionState.FETCHING,
            IngestionState.FETCHED,
            IngestionState.QUEUED_FOR_DELTA,
            IngestionState.DELTA_RUNNING,
            IngestionState.DELTA_FINISHED,
        ]
        
        for state in active_states:
            run = StockIngestionRun.objects.create(
                stock=self.stock,
                state=state
            )
            
            updated = self.service.update_run_state(
                run_id=run.id,
                new_state=IngestionState.FAILED,
                error_code='TEST_ERROR',
                error_message='Test failure'
            )
            
            self.assertEqual(updated.state, IngestionState.FAILED)
            self.assertIsNotNone(updated.failed_at, "failed_at timestamp should be set when transitioning to FAILED")
