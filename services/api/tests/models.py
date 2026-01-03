"""
Tests for Stock and StockIngestionRun models.

This file contains unit tests for:
- Stock model: creation, uniqueness, normalization, and case insensitivity of tickers.
- StockIngestionRun model: state and timestamp behaviors, related stock usage, and custom manager methods.
"""


from django.test import TestCase
from django.db import IntegrityError

from api.models import BulkQueueRun, Exchange, IngestionState, Sector, Stock, StockIngestionRun


class ExchangeModelTest(TestCase):
    """Tests for the Exchange model."""

    def test_create_exchange(self):
        """Test creating an exchange with valid data."""
        exchange = Exchange.objects.create(name='NASDAQ')
        
        self.assertIsNotNone(exchange.id)
        self.assertEqual(exchange.name, 'NASDAQ')
        self.assertIsNotNone(exchange.created_at)
        self.assertIsNotNone(exchange.updated_at)

    def test_exchange_name_uniqueness(self):
        """Test that exchange names must be unique."""
        Exchange.objects.create(name='NASDAQ')
        
        with self.assertRaises(IntegrityError):
            Exchange.objects.create(name='NASDAQ')

    def test_exchange_name_normalization_to_uppercase(self):
        """Test that exchange names are normalized to uppercase on save."""
        # Create exchange with lowercase name
        exchange = Exchange.objects.create(name='nasdaq')
        
        # Verify it's stored as uppercase
        self.assertEqual(exchange.name, 'NASDAQ')
        
        # Verify it can be retrieved with uppercase
        retrieved = Exchange.objects.get(name='NASDAQ')
        self.assertEqual(retrieved.id, exchange.id)

    def test_exchange_name_case_insensitive_uniqueness_uppercase(self):
        """Test that creating with uppercase after lowercase raises IntegrityError."""
        # Create exchange with lowercase
        Exchange.objects.create(name='nasdaq')
        
        # Try to create with uppercase - should raise IntegrityError
        with self.assertRaises(IntegrityError):
            Exchange.objects.create(name='NASDAQ')

    def test_exchange_name_case_insensitive_uniqueness_mixed_case(self):
        """Test that creating with mixed case after lowercase raises IntegrityError."""
        # Create exchange with lowercase
        Exchange.objects.create(name='nasdaq')
        
        # Try to create with mixed case - should raise IntegrityError
        with self.assertRaises(IntegrityError):
            Exchange.objects.create(name='NasDaq')

    def test_exchange_name_normalization_with_whitespace(self):
        """Test that exchange names are stripped of whitespace."""
        exchange = Exchange.objects.create(name='  nasdaq  ')
        
        # Verify it's stored as uppercase and trimmed
        self.assertEqual(exchange.name, 'NASDAQ')

    def test_exchange_str_representation(self):
        """Test the string representation of an exchange."""
        exchange = Exchange.objects.create(name='NYSE')
        self.assertEqual(str(exchange), 'NYSE')

    def test_exchange_repr(self):
        """Test the repr of an exchange."""
        exchange = Exchange.objects.create(name='NASDAQ')
        self.assertIn('NASDAQ', repr(exchange))
        self.assertIn('Exchange', repr(exchange))

    def test_exchange_get_or_create_creates_new(self):
        """Test that get_or_create creates a new exchange when it doesn't exist."""
        exchange, created = Exchange.objects.get_or_create(name='NASDAQ')
        
        self.assertTrue(created)
        self.assertEqual(exchange.name, 'NASDAQ')

    def test_exchange_get_or_create_retrieves_existing(self):
        """Test that get_or_create retrieves existing exchange."""
        # Create exchange first
        existing_exchange = Exchange.objects.create(name='NASDAQ')
        
        # Try to get_or_create with same name
        exchange, created = Exchange.objects.get_or_create(name='NASDAQ')
        
        self.assertFalse(created)
        self.assertEqual(exchange.id, existing_exchange.id)
        self.assertEqual(exchange.name, 'NASDAQ')

    def test_exchange_get_or_create_with_normalization(self):
        """Test that get_or_create works when name is normalized before calling it."""
        # Create exchange with uppercase
        existing_exchange = Exchange.objects.create(name='NASDAQ')
        
        # Normalize the name before calling get_or_create (this is the recommended pattern)
        normalized_name = 'nasdaq'.strip().upper()
        exchange, created = Exchange.objects.get_or_create(name=normalized_name)
        
        self.assertFalse(created)
        self.assertEqual(exchange.id, existing_exchange.id)
        self.assertEqual(exchange.name, 'NASDAQ')

    def test_exchange_ordering(self):
        """Test that exchanges are ordered by name."""
        Exchange.objects.create(name='NYSE')
        Exchange.objects.create(name='NASDAQ')
        Exchange.objects.create(name='AMEX')
        
        exchanges = list(Exchange.objects.all())
        
        self.assertEqual(exchanges[0].name, 'AMEX')
        self.assertEqual(exchanges[1].name, 'NASDAQ')
        self.assertEqual(exchanges[2].name, 'NYSE')


class SectorModelTest(TestCase):
    """Tests for the Sector model."""

    def test_create_sector(self):
        """Test creating a sector with valid data."""
        sector = Sector.objects.create(name='Information Technology')
        
        self.assertIsNotNone(sector.id)
        self.assertEqual(sector.name, 'Information Technology')
        self.assertIsNotNone(sector.created_at)
        self.assertIsNotNone(sector.updated_at)

    def test_sector_name_uniqueness(self):
        """Test that sector names must be unique."""
        Sector.objects.create(name='Information Technology')
        
        with self.assertRaises(IntegrityError):
            Sector.objects.create(name='Information Technology')

    def test_sector_name_preserves_case(self):
        """Test that sector names preserve case as-is (no normalization, unlike Exchange)."""
        # Create sector with mixed case name
        sector = Sector.objects.create(name='Information Technology')
        
        # Verify it's stored exactly as provided (case preserved)
        self.assertEqual(sector.name, 'Information Technology')
        
        # Verify case-sensitive uniqueness - different case should be allowed
        # (unlike Exchange which normalizes to uppercase)
        sector2 = Sector.objects.create(name='information technology')
        
        # Both should exist with different cases
        self.assertEqual(sector.name, 'Information Technology')
        self.assertEqual(sector2.name, 'information technology')
        self.assertNotEqual(sector.id, sector2.id)

    def test_sector_name_case_sensitive_uniqueness(self):
        """Test that sector names are case-sensitive (unlike Exchange which is case-insensitive)."""
        # Create sector with specific case
        Sector.objects.create(name='Information Technology')
        
        # Try to create with different case - should succeed (unlike Exchange)
        # This is intentional - sector names preserve case from external APIs
        sector2 = Sector.objects.create(name='INFORMATION TECHNOLOGY')
        
        # Both should exist
        self.assertEqual(Sector.objects.filter(name='Information Technology').count(), 1)
        self.assertEqual(Sector.objects.filter(name='INFORMATION TECHNOLOGY').count(), 1)

    def test_sector_str_representation(self):
        """Test the string representation of a sector."""
        sector = Sector.objects.create(name='Financials')
        self.assertEqual(str(sector), 'Financials')

    def test_sector_repr(self):
        """Test the repr of a sector."""
        sector = Sector.objects.create(name='Information Technology')
        self.assertIn('Information Technology', repr(sector))
        self.assertIn('Sector', repr(sector))

    def test_sector_get_or_create_creates_new(self):
        """Test that get_or_create creates a new sector when it doesn't exist."""
        sector, created = Sector.objects.get_or_create(name='Information Technology')
        
        self.assertTrue(created)
        self.assertEqual(sector.name, 'Information Technology')

    def test_sector_get_or_create_retrieves_existing(self):
        """Test that get_or_create retrieves existing sector."""
        # Create sector first
        existing_sector = Sector.objects.create(name='Information Technology')
        
        # Try to get_or_create with same name
        sector, created = Sector.objects.get_or_create(name='Information Technology')
        
        self.assertFalse(created)
        self.assertEqual(sector.id, existing_sector.id)
        self.assertEqual(sector.name, 'Information Technology')

    def test_sector_get_or_create_case_sensitive(self):
        """Test that get_or_create is case-sensitive (unlike Exchange)."""
        # Create sector with specific case
        existing_sector = Sector.objects.create(name='Information Technology')
        
        # Try get_or_create with different case - should create new (case-sensitive)
        sector, created = Sector.objects.get_or_create(name='information technology')
        
        self.assertTrue(created)  # Creates new because case differs
        self.assertNotEqual(sector.id, existing_sector.id)
        self.assertEqual(sector.name, 'information technology')

    def test_sector_ordering(self):
        """Test that sectors are ordered by name."""
        Sector.objects.create(name='Financials')
        Sector.objects.create(name='Information Technology')
        Sector.objects.create(name='Healthcare')
        
        sectors = list(Sector.objects.all())
        
        self.assertEqual(sectors[0].name, 'Financials')
        self.assertEqual(sectors[1].name, 'Healthcare')
        self.assertEqual(sectors[2].name, 'Information Technology')


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


class StockSectorForeignKeyTest(TestCase):
    """Tests for the Stock.sector ForeignKey relationship."""

    def test_stock_sector_can_be_null(self):
        """Test that stock.sector can be null."""
        stock = Stock.objects.create(ticker='AAPL')
        
        self.assertIsNone(stock.sector)

    def test_stock_sector_can_be_set(self):
        """Test that stock.sector ForeignKey can be set to a Sector."""
        sector = Sector.objects.create(name='Information Technology')
        stock = Stock.objects.create(ticker='AAPL', sector=sector)
        
        self.assertEqual(stock.sector, sector)
        self.assertEqual(stock.sector.name, 'Information Technology')

    def test_stock_sector_can_be_retrieved(self):
        """Test that stock.sector ForeignKey can be retrieved."""
        sector = Sector.objects.create(name='Financials')
        stock = Stock.objects.create(ticker='JPM', sector=sector)
        
        # Retrieve stock from database
        retrieved_stock = Stock.objects.get(ticker='JPM')
        
        self.assertEqual(retrieved_stock.sector, sector)
        self.assertEqual(retrieved_stock.sector.name, 'Financials')

    def test_sector_deletion_sets_stock_sector_to_none(self):
        """Test that deleting a Sector sets Stock.sector to None (SET_NULL behavior)."""
        sector = Sector.objects.create(name='Information Technology')
        stock = Stock.objects.create(ticker='AAPL', sector=sector)
        
        # Verify sector is set
        self.assertEqual(stock.sector, sector)
        
        # Delete the sector
        sector.delete()
        
        # Reload stock from database
        stock.refresh_from_db()
        
        # Verify sector is now None
        self.assertIsNone(stock.sector)

    def test_filter_stocks_by_sector(self):
        """Test filtering stocks by sector using ForeignKey."""
        tech_sector = Sector.objects.create(name='Information Technology')
        finance_sector = Sector.objects.create(name='Financials')
        
        # Create stocks with different sectors
        aapl = Stock.objects.create(ticker='AAPL', sector=tech_sector)
        msft = Stock.objects.create(ticker='MSFT', sector=tech_sector)
        jpm = Stock.objects.create(ticker='JPM', sector=finance_sector)
        tsla = Stock.objects.create(ticker='TSLA')  # No sector
        
        # Filter stocks by Information Technology sector
        tech_stocks = Stock.objects.filter(sector=tech_sector)
        
        self.assertEqual(tech_stocks.count(), 2)
        self.assertIn(aapl, tech_stocks)
        self.assertIn(msft, tech_stocks)
        self.assertNotIn(jpm, tech_stocks)
        self.assertNotIn(tsla, tech_stocks)

    def test_filter_stocks_by_sector_name(self):
        """Test filtering stocks by sector name using ForeignKey relationship."""
        tech_sector = Sector.objects.create(name='Information Technology')
        finance_sector = Sector.objects.create(name='Financials')
        
        Stock.objects.create(ticker='AAPL', sector=tech_sector)
        Stock.objects.create(ticker='MSFT', sector=tech_sector)
        Stock.objects.create(ticker='JPM', sector=finance_sector)
        
        # Filter stocks by sector name
        tech_stocks = Stock.objects.filter(sector__name='Information Technology')
        
        self.assertEqual(tech_stocks.count(), 2)
        tech_tickers = [stock.ticker for stock in tech_stocks]
        self.assertIn('AAPL', tech_tickers)
        self.assertIn('MSFT', tech_tickers)
        self.assertNotIn('JPM', tech_tickers)

    def test_sector_reverse_relationship(self):
        """Test accessing stocks from a sector using reverse relationship."""
        tech_sector = Sector.objects.create(name='Information Technology')
        
        aapl = Stock.objects.create(ticker='AAPL', sector=tech_sector)
        msft = Stock.objects.create(ticker='MSFT', sector=tech_sector)
        
        # Access stocks through reverse relationship
        tech_stocks = tech_sector.stocks.all()
        
        self.assertEqual(tech_stocks.count(), 2)
        self.assertIn(aapl, tech_stocks)
        self.assertIn(msft, tech_stocks)

    def test_stock_sector_update(self):
        """Test updating stock.sector ForeignKey."""
        tech_sector = Sector.objects.create(name='Information Technology')
        finance_sector = Sector.objects.create(name='Financials')
        
        stock = Stock.objects.create(ticker='AAPL', sector=tech_sector)
        
        # Verify initial sector
        self.assertEqual(stock.sector, tech_sector)
        
        # Update sector
        stock.sector = finance_sector
        stock.save()
        
        # Reload and verify
        stock.refresh_from_db()
        self.assertEqual(stock.sector, finance_sector)
        self.assertEqual(stock.sector.name, 'Financials')

    def test_sector_names_preserve_case(self):
        """Test that sector names preserve case (unlike Exchange which normalizes to uppercase)."""
        # Create sectors with different cases - both should be allowed
        tech1 = Sector.objects.create(name='Information Technology')
        tech2 = Sector.objects.create(name='information technology')
        tech3 = Sector.objects.create(name='INFORMATION TECHNOLOGY')
        
        # All should exist with their respective cases
        self.assertEqual(tech1.name, 'Information Technology')
        self.assertEqual(tech2.name, 'information technology')
        self.assertEqual(tech3.name, 'INFORMATION TECHNOLOGY')
        
        # All should be different records
        self.assertNotEqual(tech1.id, tech2.id)
        self.assertNotEqual(tech2.id, tech3.id)
        self.assertNotEqual(tech1.id, tech3.id)


class StockExchangeForeignKeyTest(TestCase):
    """Tests for the Stock.exchange ForeignKey relationship."""

    def test_stock_exchange_can_be_null(self):
        """Test that stock.exchange can be null."""
        stock = Stock.objects.create(ticker='AAPL')
        
        self.assertIsNone(stock.exchange)

    def test_stock_exchange_can_be_set(self):
        """Test that stock.exchange ForeignKey can be set to an Exchange."""
        exchange = Exchange.objects.create(name='NASDAQ')
        stock = Stock.objects.create(ticker='AAPL', exchange=exchange)
        
        self.assertEqual(stock.exchange, exchange)
        self.assertEqual(stock.exchange.name, 'NASDAQ')

    def test_stock_exchange_can_be_retrieved(self):
        """Test that stock.exchange ForeignKey can be retrieved."""
        exchange = Exchange.objects.create(name='NYSE')
        stock = Stock.objects.create(ticker='IBM', exchange=exchange)
        
        # Retrieve stock from database
        retrieved_stock = Stock.objects.get(ticker='IBM')
        
        self.assertEqual(retrieved_stock.exchange, exchange)
        self.assertEqual(retrieved_stock.exchange.name, 'NYSE')

    def test_exchange_deletion_sets_stock_exchange_to_none(self):
        """Test that deleting an Exchange sets Stock.exchange to None (SET_NULL behavior)."""
        exchange = Exchange.objects.create(name='NASDAQ')
        stock = Stock.objects.create(ticker='AAPL', exchange=exchange)
        
        # Verify exchange is set
        self.assertEqual(stock.exchange, exchange)
        
        # Delete the exchange
        exchange.delete()
        
        # Reload stock from database
        stock.refresh_from_db()
        
        # Verify exchange is now None
        self.assertIsNone(stock.exchange)

    def test_filter_stocks_by_exchange(self):
        """Test filtering stocks by exchange using ForeignKey."""
        nasdaq = Exchange.objects.create(name='NASDAQ')
        nyse = Exchange.objects.create(name='NYSE')
        
        # Create stocks with different exchanges
        aapl = Stock.objects.create(ticker='AAPL', exchange=nasdaq)
        googl = Stock.objects.create(ticker='GOOGL', exchange=nasdaq)
        ibm = Stock.objects.create(ticker='IBM', exchange=nyse)
        tsla = Stock.objects.create(ticker='TSLA')  # No exchange
        
        # Filter stocks by NASDAQ
        nasdaq_stocks = Stock.objects.filter(exchange=nasdaq)
        
        self.assertEqual(nasdaq_stocks.count(), 2)
        self.assertIn(aapl, nasdaq_stocks)
        self.assertIn(googl, nasdaq_stocks)
        self.assertNotIn(ibm, nasdaq_stocks)
        self.assertNotIn(tsla, nasdaq_stocks)

    def test_filter_stocks_by_exchange_name(self):
        """Test filtering stocks by exchange name using ForeignKey relationship."""
        nasdaq = Exchange.objects.create(name='NASDAQ')
        nyse = Exchange.objects.create(name='NYSE')
        
        Stock.objects.create(ticker='AAPL', exchange=nasdaq)
        Stock.objects.create(ticker='GOOGL', exchange=nasdaq)
        Stock.objects.create(ticker='IBM', exchange=nyse)
        
        # Filter stocks by exchange name
        nasdaq_stocks = Stock.objects.filter(exchange__name='NASDAQ')
        
        self.assertEqual(nasdaq_stocks.count(), 2)
        nasdaq_tickers = [stock.ticker for stock in nasdaq_stocks]
        self.assertIn('AAPL', nasdaq_tickers)
        self.assertIn('GOOGL', nasdaq_tickers)
        self.assertNotIn('IBM', nasdaq_tickers)

    def test_exchange_reverse_relationship(self):
        """Test accessing stocks from an exchange using reverse relationship."""
        nasdaq = Exchange.objects.create(name='NASDAQ')
        
        aapl = Stock.objects.create(ticker='AAPL', exchange=nasdaq)
        googl = Stock.objects.create(ticker='GOOGL', exchange=nasdaq)
        
        # Access stocks through reverse relationship
        nasdaq_stocks = nasdaq.stocks.all()
        
        self.assertEqual(nasdaq_stocks.count(), 2)
        self.assertIn(aapl, nasdaq_stocks)
        self.assertIn(googl, nasdaq_stocks)

    def test_stock_exchange_update(self):
        """Test updating stock.exchange ForeignKey."""
        nasdaq = Exchange.objects.create(name='NASDAQ')
        nyse = Exchange.objects.create(name='NYSE')
        
        stock = Stock.objects.create(ticker='AAPL', exchange=nasdaq)
        
        # Verify initial exchange
        self.assertEqual(stock.exchange, nasdaq)
        
        # Update exchange
        stock.exchange = nyse
        stock.save()
        
        # Reload and verify
        stock.refresh_from_db()
        self.assertEqual(stock.exchange, nyse)
        self.assertEqual(stock.exchange.name, 'NYSE')


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


class BulkQueueRunModelTest(TestCase):
    """Tests for the BulkQueueRun model."""

    def test_create_bulk_queue_run_with_defaults(self):
        """Test creating a BulkQueueRun with default values."""
        bulk_run = BulkQueueRun.objects.create(
            total_stocks=100
        )
        
        self.assertIsNotNone(bulk_run.id)
        self.assertEqual(bulk_run.total_stocks, 100)
        self.assertEqual(bulk_run.queued_count, 0)
        self.assertEqual(bulk_run.skipped_count, 0)
        self.assertEqual(bulk_run.error_count, 0)
        self.assertIsNone(bulk_run.requested_by)
        self.assertIsNotNone(bulk_run.created_at)
        self.assertIsNone(bulk_run.started_at)
        self.assertIsNone(bulk_run.completed_at)

    def test_create_bulk_queue_run_with_requested_by(self):
        """Test creating a BulkQueueRun with requested_by."""
        bulk_run = BulkQueueRun.objects.create(
            total_stocks=50,
            requested_by='admin@example.com'
        )
        
        self.assertEqual(bulk_run.requested_by, 'admin@example.com')
        self.assertEqual(bulk_run.total_stocks, 50)

    def test_bulk_queue_run_str_representation(self):
        """Test the string representation of a BulkQueueRun."""
        bulk_run = BulkQueueRun.objects.create(
            total_stocks=100,
            queued_count=75
        )
        str_repr = str(bulk_run)
        
        self.assertIn('75', str_repr)
        self.assertIn('100', str_repr)
        self.assertIn('queued', str_repr.lower())

    def test_bulk_queue_run_repr(self):
        """Test the repr of a BulkQueueRun."""
        bulk_run = BulkQueueRun.objects.create(
            total_stocks=100,
            queued_count=75,
            skipped_count=20,
            error_count=5
        )
        repr_str = repr(bulk_run)
        
        self.assertIn('BulkQueueRun', repr_str)
        self.assertIn('total=100', repr_str)
        self.assertIn('queued=75', repr_str)
        self.assertIn('skipped=20', repr_str)
        self.assertIn('errors=5', repr_str)

    def test_bulk_queue_run_update_statistics(self):
        """Test updating BulkQueueRun statistics."""
        bulk_run = BulkQueueRun.objects.create(
            total_stocks=100
        )
        
        # Update statistics
        bulk_run.queued_count = 80
        bulk_run.skipped_count = 15
        bulk_run.error_count = 5
        bulk_run.save()
        
        # Reload from database
        bulk_run.refresh_from_db()
        
        self.assertEqual(bulk_run.queued_count, 80)
        self.assertEqual(bulk_run.skipped_count, 15)
        self.assertEqual(bulk_run.error_count, 5)


class BulkQueueRunRelationshipTest(TestCase):
    """Tests for the BulkQueueRun foreign key relationship with StockIngestionRun."""

    def setUp(self):
        """Set up test fixtures."""
        self.stock = Stock.objects.create(ticker='AAPL')
        self.bulk_run = BulkQueueRun.objects.create(
            total_stocks=10,
            requested_by='test@example.com'
        )

    def test_ingestion_run_without_bulk_queue_run(self):
        """Test creating an ingestion run without linking to a bulk queue run."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        self.assertIsNone(run.bulk_queue_run)

    def test_ingestion_run_with_bulk_queue_run(self):
        """Test creating an ingestion run linked to a bulk queue run."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            bulk_queue_run=self.bulk_run,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        self.assertEqual(run.bulk_queue_run, self.bulk_run)
        self.assertEqual(run.bulk_queue_run.id, self.bulk_run.id)

    def test_bulk_queue_run_reverse_relationship(self):
        """Test accessing ingestion runs from a bulk queue run."""
        # Create multiple ingestion runs linked to the bulk queue run
        stock1 = self.stock  # Use the stock created in setUp
        stock2 = Stock.objects.create(ticker='GOOGL')
        stock3 = Stock.objects.create(ticker='MSFT')
        
        run1 = StockIngestionRun.objects.create(
            stock=stock1,
            bulk_queue_run=self.bulk_run,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        run2 = StockIngestionRun.objects.create(
            stock=stock2,
            bulk_queue_run=self.bulk_run,
            state=IngestionState.FETCHING
        )
        run3 = StockIngestionRun.objects.create(
            stock=stock3,
            bulk_queue_run=self.bulk_run,
            state=IngestionState.FAILED
        )
        
        # Access runs through reverse relationship
        related_runs = self.bulk_run.ingestion_runs.all()
        
        self.assertEqual(related_runs.count(), 3)
        self.assertIn(run1, related_runs)
        self.assertIn(run2, related_runs)
        self.assertIn(run3, related_runs)

    def test_bulk_queue_run_set_null_on_delete(self):
        """Test that deleting a BulkQueueRun sets the FK to NULL (SET_NULL)."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            bulk_queue_run=self.bulk_run,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Delete the bulk queue run
        bulk_run_id = self.bulk_run.id
        self.bulk_run.delete()
        
        # Reload the ingestion run
        run.refresh_from_db()
        
        # The FK should be set to NULL
        self.assertIsNone(run.bulk_queue_run)
        self.assertEqual(run.stock, self.stock)

    def test_query_failed_stocks_in_bulk_run(self):
        """Test querying failed stocks using the bulk_queue_run foreign key."""
        # Create multiple stocks with different states
        stock1 = self.stock  # Use the stock created in setUp
        stock2 = Stock.objects.create(ticker='GOOGL')
        stock3 = Stock.objects.create(ticker='MSFT')
        stock4 = Stock.objects.create(ticker='TSLA')
        
        # Create runs with different states
        StockIngestionRun.objects.create(
            stock=stock1,
            bulk_queue_run=self.bulk_run,
            state=IngestionState.FAILED
        )
        StockIngestionRun.objects.create(
            stock=stock2,
            bulk_queue_run=self.bulk_run,
            state=IngestionState.DONE
        )
        StockIngestionRun.objects.create(
            stock=stock3,
            bulk_queue_run=self.bulk_run,
            state=IngestionState.FAILED
        )
        StockIngestionRun.objects.create(
            stock=stock4,
            bulk_queue_run=self.bulk_run,
            state=IngestionState.FETCHING
        )
        
        # Query failed stocks
        failed_runs = StockIngestionRun.objects.filter(
            bulk_queue_run=self.bulk_run,
            state=IngestionState.FAILED
        )
        
        self.assertEqual(failed_runs.count(), 2)
        failed_tickers = [run.stock.ticker for run in failed_runs]
        self.assertIn('AAPL', failed_tickers)
        self.assertIn('MSFT', failed_tickers)
        self.assertNotIn('GOOGL', failed_tickers)
        self.assertNotIn('TSLA', failed_tickers)
