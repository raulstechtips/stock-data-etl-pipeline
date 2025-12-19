"""
Tests for Stock and StockIngestionRun models.

This file contains unit tests for:
- Stock model: creation, uniqueness, normalization, and case insensitivity of tickers.
- StockIngestionRun model: state and timestamp behaviors, related stock usage, and custom manager methods.
"""


from django.test import TestCase
from django.db import IntegrityError

from api.models import IngestionState, Stock, StockIngestionRun


class StockModelTest(TestCase):
    """Tests for the Stock model."""

    def test_create_stock(self):
        """Test creating a stock with valid data."""
        stock = Stock.objects.create(ticker='AAPL')
        
        self.assertIsNotNone(stock.id)
        self.assertEqual(stock.ticker, 'AAPL')
        self.assertIsNotNone(stock.created_at)
        self.assertIsNotNone(stock.updated_at)

    def test_ticker_uniqueness(self):
        """Test that ticker symbols must be unique."""
        Stock.objects.create(ticker='AAPL')
        
        with self.assertRaises(IntegrityError):
            Stock.objects.create(ticker='AAPL')

    def test_ticker_normalization_to_uppercase(self):
        """Test that ticker symbols are normalized to uppercase on save."""
        # Create stock with lowercase ticker
        stock = Stock.objects.create(ticker='aapl')
        
        # Verify it's stored as uppercase
        self.assertEqual(stock.ticker, 'AAPL')
        
        # Verify it can be retrieved with uppercase
        retrieved = Stock.objects.get(ticker='AAPL')
        self.assertEqual(retrieved.id, stock.id)

    def test_ticker_case_insensitive_uniqueness_uppercase(self):
        """Test that creating with uppercase after lowercase raises IntegrityError."""
        # Create stock with lowercase
        Stock.objects.create(ticker='aapl')
        
        # Try to create with uppercase - should raise IntegrityError
        with self.assertRaises(IntegrityError):
            Stock.objects.create(ticker='AAPL')

    def test_ticker_case_insensitive_uniqueness_mixed_case(self):
        """Test that creating with mixed case after lowercase raises IntegrityError."""
        # Create stock with lowercase
        Stock.objects.create(ticker='aapl')
        
        # Try to create with mixed case - should raise IntegrityError
        with self.assertRaises(IntegrityError):
            Stock.objects.create(ticker='AaPl')

    def test_ticker_normalization_with_whitespace(self):
        """Test that ticker symbols are stripped of whitespace."""
        stock = Stock.objects.create(ticker='  aapl  ')
        
        # Verify it's stored as uppercase and trimmed
        self.assertEqual(stock.ticker, 'AAPL')

    def test_stock_str_representation(self):
        """Test the string representation of a stock."""
        stock = Stock.objects.create(ticker='GOOGL')
        self.assertEqual(str(stock), 'GOOGL')

    def test_stock_repr(self):
        """Test the repr of a stock."""
        stock = Stock.objects.create(ticker='MSFT')
        self.assertIn('MSFT', repr(stock))


class StockIngestionRunModelTest(TestCase):
    """Tests for the StockIngestionRun model."""

    def setUp(self):
        """Set up test fixtures."""
        self.stock = Stock.objects.create(ticker='AAPL')

    def test_create_run_with_defaults(self):
        """Test creating a run with default values."""
        run = StockIngestionRun.objects.create(stock=self.stock)
        
        self.assertIsNotNone(run.id)
        self.assertEqual(run.stock, self.stock)
        self.assertEqual(run.state, IngestionState.QUEUED_FOR_FETCH)
        self.assertIsNone(run.requested_by)
        self.assertIsNone(run.request_id)

    def test_is_terminal_for_done_state(self):
        """Test is_terminal property for DONE state."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE
        )
        self.assertTrue(run.is_terminal)
        self.assertFalse(run.is_in_progress)

    def test_is_terminal_for_failed_state(self):
        """Test is_terminal property for FAILED state."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FAILED
        )
        self.assertTrue(run.is_terminal)
        self.assertFalse(run.is_in_progress)

    def test_is_in_progress_for_active_states(self):
        """Test is_in_progress for non-terminal states."""
        active_states = [
            IngestionState.QUEUED_FOR_FETCH,
            IngestionState.FETCHING,
            IngestionState.FETCHED,
            IngestionState.QUEUED_FOR_DELTA,
            IngestionState.DELTA_RUNNING,
            IngestionState.DELTA_FINISHED,
        ]
        
        # Use a different stock for each state to avoid unique constraint violation
        for i, state in enumerate(active_states):
            stock = Stock.objects.create(ticker=f'TEST{i}')
            run = StockIngestionRun.objects.create(
                stock=stock,
                state=state
            )
            self.assertTrue(run.is_in_progress, f"State {state} should be in progress")
            self.assertFalse(run.is_terminal, f"State {state} should not be terminal")

    def test_run_str_representation(self):
        """Test the string representation of a run."""
        run = StockIngestionRun.objects.create(stock=self.stock)
        str_repr = str(run)
        
        self.assertIn('AAPL', str_repr)
        self.assertIn('QUEUED_FOR_FETCH', str_repr)

    def test_unique_constraint_prevents_multiple_active_runs(self):
        """Test that the unique constraint prevents multiple active runs for the same stock."""
        
        # Create first active run
        StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Attempt to create second active run for same stock should fail
        with self.assertRaises(IntegrityError):
            StockIngestionRun.objects.create(
                stock=self.stock,
                state=IngestionState.FETCHING
            )

    def test_unique_constraint_allows_terminal_and_active_runs(self):
        """Test that terminal runs don't conflict with active runs."""
        # Create a terminal run
        StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE
        )
        
        # Should be able to create an active run for the same stock
        active_run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        self.assertEqual(active_run.state, IngestionState.QUEUED_FOR_FETCH)

    def test_unique_constraint_allows_multiple_terminal_runs(self):
        """Test that multiple terminal runs can exist for the same stock."""
        # Create first terminal run
        run1 = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE
        )
        
        # Should be able to create another terminal run
        run2 = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FAILED
        )
        
        self.assertEqual(run1.state, IngestionState.DONE)
        self.assertEqual(run2.state, IngestionState.FAILED)

    def test_unique_constraint_released_after_state_transition_to_terminal(self):
        """Test that transitioning to terminal state releases the constraint."""
        # Create an active run
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        
        # Transition to terminal state
        run.state = IngestionState.DONE
        run.save()
        
        # Should now be able to create a new active run
        new_run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        self.assertEqual(new_run.state, IngestionState.QUEUED_FOR_FETCH)


class StockIngestionRunManagerTest(TestCase):
    """Tests for the StockIngestionRunManager custom manager."""

    def setUp(self):
        """Set up test fixtures."""
        self.stock = Stock.objects.create(ticker='AAPL')

    def test_get_latest_for_stock(self):
        """Test getting the latest run for a stock."""
        # Create multiple runs
        _older_run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE
        )
        newer_run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        latest = StockIngestionRun.objects.get_latest_for_stock(self.stock.id)
        
        self.assertEqual(latest.id, newer_run.id)

    def test_get_latest_for_stock_no_runs(self):
        """Test get_latest_for_stock when no runs exist."""
        latest = StockIngestionRun.objects.get_latest_for_stock(self.stock.id)
        self.assertIsNone(latest)

    def test_get_latest_by_ticker(self):
        """Test getting the latest run by ticker symbol."""
        run = StockIngestionRun.objects.create(stock=self.stock)
        
        latest = StockIngestionRun.objects.get_latest_by_ticker('AAPL')
        
        self.assertEqual(latest.id, run.id)
        self.assertEqual(latest.stock.ticker, 'AAPL')

    def test_get_latest_by_ticker_case_insensitive(self):
        """Test that get_latest_by_ticker is case-insensitive."""
        run = StockIngestionRun.objects.create(stock=self.stock)
        
        # Test various case combinations
        for ticker in ['aapl', 'Aapl', 'aApL']:
            latest = StockIngestionRun.objects.get_latest_by_ticker(ticker)
            self.assertEqual(latest.id, run.id)

    def test_get_active_runs(self):
        """Test getting all active (non-terminal) runs."""
        # Create terminal runs
        StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE
        )
        StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FAILED
        )
        
        # Create active runs
        active_run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        
        active_runs = StockIngestionRun.objects.get_active_runs()
        
        self.assertEqual(active_runs.count(), 1)
        self.assertEqual(active_runs.first().id, active_run.id)
