"""
API Tests for Filtering Capabilities.

This test module covers:
- Filtering capabilities for stocks and ingestion runs
"""

from datetime import timedelta

from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from api.models import IngestionState, Stock, StockIngestionRun

class TickerListFilterAPITest(APITestCase):
    """Tests for filtering on the GET /api/tickers endpoint."""

    def setUp(self):
        """Set up test fixtures with diverse stock data."""
        Stock.objects.create(
            ticker='AAPL',
            name='Apple Inc.',
            sector='Technology',
            exchange='NASDAQ',
            country='US'
        )
        Stock.objects.create(
            ticker='GOOGL',
            name='Alphabet Inc.',
            sector='Technology',
            exchange='NASDAQ',
            country='US'
        )
        Stock.objects.create(
            ticker='JPM',
            name='JPMorgan Chase',
            sector='Financials',
            exchange='NYSE',
            country='US'
        )
        Stock.objects.create(
            ticker='HSBC',
            name='HSBC Holdings',
            sector='Financials',
            exchange='LSE',
            country='GB'
        )
        Stock.objects.create(
            ticker='TSM',
            name='Taiwan Semiconductor',
            sector='Technology',
            exchange='NYSE',
            country='TW'
        )

    def test_filter_by_ticker_exact(self):
        """Test filtering by exact ticker match."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'ticker': 'AAPL'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['ticker'], 'AAPL')

    def test_filter_by_ticker_contains(self):
        """Test filtering by ticker contains (case-insensitive)."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'ticker__icontains': 'apl'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should match AAPL (contains 'apl')
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['ticker'], 'AAPL')

    def test_filter_by_sector_exact(self):
        """Test filtering by exact sector match."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'sector': 'Technology'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)
        tickers = [item['ticker'] for item in response.data['results']]
        self.assertIn('AAPL', tickers)
        self.assertIn('GOOGL', tickers)
        self.assertIn('TSM', tickers)

    def test_filter_by_sector_contains(self):
        """Test filtering by sector contains (case-insensitive)."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'sector__icontains': 'tech'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)

    def test_filter_by_exchange(self):
        """Test filtering by exchange."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'exchange': 'NASDAQ'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        tickers = [item['ticker'] for item in response.data['results']]
        self.assertIn('AAPL', tickers)
        self.assertIn('GOOGL', tickers)

    def test_filter_by_country(self):
        """Test filtering by country."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'country': 'US'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)
        tickers = [item['ticker'] for item in response.data['results']]
        self.assertIn('AAPL', tickers)
        self.assertIn('GOOGL', tickers)
        self.assertIn('JPM', tickers)

    def test_filter_multiple_parameters(self):
        """Test filtering with multiple parameters combined."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {
            'sector': 'Technology',
            'country': 'US'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        tickers = [item['ticker'] for item in response.data['results']]
        self.assertIn('AAPL', tickers)
        self.assertIn('GOOGL', tickers)

    def test_filter_no_results(self):
        """Test filtering that returns no results."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'ticker': 'NONEXISTENT'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)

    def test_filter_case_insensitive_ticker(self):
        """Test that ticker filtering is case-insensitive."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'ticker': 'aapl'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['ticker'], 'AAPL')


class RunListFilterAPITest(APITestCase):
    """Tests for filtering on the GET /api/runs endpoint."""

    def setUp(self):
        """Set up test fixtures with diverse run data."""
        # Create stocks
        self.stock_aapl = Stock.objects.create(ticker='AAPL')
        self.stock_googl = Stock.objects.create(ticker='GOOGL')
        self.stock_msft = Stock.objects.create(ticker='MSFT')
        
        # Create runs with different states and timestamps
        now = timezone.now()
        
        # AAPL runs
        self.run1 = StockIngestionRun.objects.create(
            stock=self.stock_aapl,
            state=IngestionState.DONE,
            requested_by='user1@example.com',
        )
        self.run1.created_at = now - timedelta(days=10)
        self.run1.save()
        
        self.run2 = StockIngestionRun.objects.create(
            stock=self.stock_aapl,
            state=IngestionState.FAILED,
            requested_by='user2@example.com',
        )
        self.run2.created_at = now - timedelta(days=5)
        self.run2.save()
        
        # GOOGL runs
        self.run3 = StockIngestionRun.objects.create(
            stock=self.stock_googl,
            state=IngestionState.FETCHING,
            requested_by='user1@example.com',
        )
        self.run3.created_at = now - timedelta(days=2)
        self.run3.save()
        
        # MSFT runs
        self.run4 = StockIngestionRun.objects.create(
            stock=self.stock_msft,
            state=IngestionState.QUEUED_FOR_FETCH,
            requested_by='system',
        )
        self.run4.created_at = now - timedelta(hours=1)
        self.run4.save()

    def test_filter_by_ticker_exact(self):
        """Test filtering runs by exact ticker match."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'ticker': 'AAPL'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        for run in response.data['results']:
            self.assertEqual(run['ticker'], 'AAPL')

    def test_filter_by_ticker_contains(self):
        """Test filtering runs by ticker contains."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'ticker__icontains': 'goo'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['ticker'], 'GOOGL')

    def test_filter_by_state(self):
        """Test filtering runs by state."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'state': 'FAILED'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['state'], 'FAILED')

    def test_filter_by_requested_by_exact(self):
        """Test filtering runs by exact requested_by match."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'requested_by': 'user1@example.com'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        for run in response.data['results']:
            self.assertEqual(run['requested_by'], 'user1@example.com')

    def test_filter_by_requested_by_contains(self):
        """Test filtering runs by requested_by contains."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'requested_by__icontains': 'user'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should match user1 and user2
        self.assertEqual(len(response.data['results']), 3)

    def test_filter_by_created_after(self):
        """Test filtering runs created after a date."""
        url = reverse('api:run-list')
        cutoff_date = (timezone.now() - timedelta(days=3)).isoformat()
        response = self.client.get(url, {'created_after': cutoff_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return runs from last 3 days (run3 and run4)
        self.assertEqual(len(response.data['results']), 2)

    def test_filter_by_created_before(self):
        """Test filtering runs created before a date."""
        url = reverse('api:run-list')
        cutoff_date = (timezone.now() - timedelta(days=3)).isoformat()
        response = self.client.get(url, {'created_before': cutoff_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return runs older than 3 days (run1 and run2)
        self.assertEqual(len(response.data['results']), 2)

    def test_filter_by_date_range(self):
        """Test filtering runs with both created_after and created_before."""
        url = reverse('api:run-list')
        after_date = (timezone.now() - timedelta(days=7)).isoformat()
        before_date = (timezone.now() - timedelta(days=3)).isoformat()
        response = self.client.get(url, {
            'created_after': after_date,
            'created_before': before_date
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return run2 (5 days ago)
        self.assertEqual(len(response.data['results']), 1)

    def test_filter_is_terminal_true(self):
        """Test filtering for terminal runs (DONE/FAILED)."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'is_terminal': 'true'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return run1 (DONE) and run2 (FAILED)
        self.assertEqual(len(response.data['results']), 2)
        states = [run['state'] for run in response.data['results']]
        self.assertIn('DONE', states)
        self.assertIn('FAILED', states)

    def test_filter_is_terminal_false(self):
        """Test filtering for non-terminal runs."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'is_terminal': 'false'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return run3 (FETCHING) and run4 (QUEUED_FOR_FETCH)
        self.assertEqual(len(response.data['results']), 2)
        states = [run['state'] for run in response.data['results']]
        self.assertIn('FETCHING', states)
        self.assertIn('QUEUED_FOR_FETCH', states)

    def test_filter_is_in_progress_true(self):
        """Test filtering for in-progress runs."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'is_in_progress': 'true'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return run3 and run4 (not DONE/FAILED)
        self.assertEqual(len(response.data['results']), 2)

    def test_filter_is_in_progress_false(self):
        """Test filtering for completed runs (not in progress)."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'is_in_progress': 'false'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return run1 and run2 (DONE/FAILED)
        self.assertEqual(len(response.data['results']), 2)

    def test_filter_multiple_parameters(self):
        """Test filtering with multiple parameters combined."""
        url = reverse('api:run-list')
        response = self.client.get(url, {
            'state': 'FAILED',
            'ticker': 'AAPL'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['state'], 'FAILED')
        self.assertEqual(response.data['results'][0]['ticker'], 'AAPL')

    def test_filter_no_results(self):
        """Test filtering that returns no results."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'ticker': 'NONEXISTENT'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)

    def test_filter_invalid_state(self):
        """Test filtering with invalid state value."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'state': 'INVALID_STATE'})
        
        # django-filter ChoiceFilter returns 400 for invalid choice values
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_filter_case_insensitive_ticker(self):
        """Test that ticker filtering is case-insensitive."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'ticker': 'aapl'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)


class TickerRunsListFilterAPITest(APITestCase):
    """Tests for filtering on the GET /api/runs/ticker/<ticker> endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create stocks
        self.stock_aapl = Stock.objects.create(ticker='AAPL')
        self.stock_googl = Stock.objects.create(ticker='GOOGL')
        
        # Create runs for AAPL with different states
        now = timezone.now()
        
        self.run1 = StockIngestionRun.objects.create(
            stock=self.stock_aapl,
            state=IngestionState.DONE,
            requested_by='user1@example.com',
        )
        self.run1.created_at = now - timedelta(days=10)
        self.run1.save()
        
        self.run2 = StockIngestionRun.objects.create(
            stock=self.stock_aapl,
            state=IngestionState.FAILED,
            requested_by='user2@example.com',
        )
        self.run2.created_at = now - timedelta(days=5)
        self.run2.save()
        
        self.run3 = StockIngestionRun.objects.create(
            stock=self.stock_aapl,
            state=IngestionState.FETCHING,
            requested_by='user1@example.com',
        )
        self.run3.created_at = now - timedelta(days=1)
        self.run3.save()
        
        # Create a run for GOOGL (should not appear in AAPL results)
        StockIngestionRun.objects.create(
            stock=self.stock_googl,
            state=IngestionState.DONE,
            requested_by='user1@example.com',
        )

    def test_filter_ticker_runs_by_state(self):
        """Test filtering ticker runs by state."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url, {'state': 'FAILED'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['state'], 'FAILED')
        self.assertEqual(response.data['results'][0]['ticker'], 'AAPL')

    def test_filter_ticker_runs_by_requested_by(self):
        """Test filtering ticker runs by requested_by."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url, {'requested_by': 'user1@example.com'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        for run in response.data['results']:
            self.assertEqual(run['requested_by'], 'user1@example.com')
            self.assertEqual(run['ticker'], 'AAPL')

    def test_filter_ticker_runs_by_date(self):
        """Test filtering ticker runs by date range."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        cutoff_date = (timezone.now() - timedelta(days=3)).isoformat()
        response = self.client.get(url, {'created_after': cutoff_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return runs from last 3 days (only run3 at 1 day old)
        # run2 is 5 days old, so it's excluded
        self.assertEqual(len(response.data['results']), 1)

    def test_filter_ticker_runs_is_terminal(self):
        """Test filtering ticker runs by terminal status."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url, {'is_terminal': 'true'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return run1 (DONE) and run2 (FAILED)
        self.assertEqual(len(response.data['results']), 2)

    def test_filter_ticker_runs_is_in_progress(self):
        """Test filtering ticker runs by in-progress status."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url, {'is_in_progress': 'true'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return run3 (FETCHING)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['state'], 'FETCHING')

    def test_filter_ticker_runs_multiple_filters(self):
        """Test filtering ticker runs with multiple filters."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url, {
            'requested_by': 'user1@example.com',
            'is_terminal': 'true'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return only run1 (DONE, user1)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['state'], 'DONE')

    def test_filter_ticker_runs_no_results(self):
        """Test filtering ticker runs that returns no results."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url, {'state': 'DELTA_RUNNING'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)

    def test_filter_ticker_runs_maintains_ticker_filter(self):
        """Test that URL ticker filter is maintained with query filters."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url, {'requested_by': 'user1@example.com'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should only return AAPL runs, not GOOGL
        for run in response.data['results']:
            self.assertEqual(run['ticker'], 'AAPL')
