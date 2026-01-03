"""
API Tests for Filtering Capabilities.

This test module covers:
- Filtering capabilities for stocks and ingestion runs
"""

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from api.models import BulkQueueRun, Exchange, IngestionState, Sector, Stock, StockIngestionRun

User = get_user_model()

class TickerListFilterAPITest(APITestCase):
    """Tests for filtering on the GET /api/tickers endpoint."""

    def setUp(self):
        """Set up test fixtures with diverse stock data."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        # Create exchanges first
        nasdaq = Exchange.objects.create(name='NASDAQ')
        nyse = Exchange.objects.create(name='NYSE')
        lse = Exchange.objects.create(name='LSE')
        
        # Create sectors
        tech_sector = Sector.objects.create(name='Technology')
        financials_sector = Sector.objects.create(name='Financials')
        
        Stock.objects.create(
            ticker='AAPL',
            name='Apple Inc.',
            sector=tech_sector,
            exchange=nasdaq,
            country='US'
        )
        Stock.objects.create(
            ticker='GOOGL',
            name='Alphabet Inc.',
            sector=tech_sector,
            exchange=nasdaq,
            country='US'
        )
        Stock.objects.create(
            ticker='JPM',
            name='JPMorgan Chase',
            sector=financials_sector,
            exchange=nyse,
            country='US'
        )
        Stock.objects.create(
            ticker='HSBC',
            name='HSBC Holdings',
            sector=financials_sector,
            exchange=lse,
            country='GB'
        )
        Stock.objects.create(
            ticker='TSM',
            name='Taiwan Semiconductor',
            sector=tech_sector,
            exchange=nyse,
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

    def test_filter_by_sector_name_exact(self):
        """Test filtering by exact sector name match."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'sector__name': 'Technology'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)
        tickers = [item['ticker'] for item in response.data['results']]
        self.assertIn('AAPL', tickers)
        self.assertIn('GOOGL', tickers)
        self.assertIn('TSM', tickers)

    def test_filter_by_sector_name_contains(self):
        """Test filtering by sector name contains (case-insensitive)."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'sector__name__icontains': 'tech'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)

    def test_filter_by_exchange(self):
        """Test filtering by exchange."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'exchange__name': 'NASDAQ'})
        
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
            'sector__name': 'Technology',
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

    def test_filter_by_sector_name_case_insensitive(self):
        """Test that sector__name filtering is case-insensitive."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'sector__name': 'technology'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)
        
        # Test with mixed case
        response = self.client.get(url, {'sector__name': 'FiNaNcIaLs'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)

    def test_filter_by_sector_name_icontains_case_insensitive(self):
        """Test that sector__name__icontains filtering is case-insensitive."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {'sector__name__icontains': 'TECH'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)
        
        # Test with lowercase
        response = self.client.get(url, {'sector__name__icontains': 'fin'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)

    def test_filter_combining_sector_name_with_other_filters(self):
        """Test combining sector__name filter with other filters."""
        url = reverse('api:ticker-list')
        response = self.client.get(url, {
            'sector__name': 'Technology',
            'exchange__name': 'NASDAQ',
            'country': 'US'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        tickers = [item['ticker'] for item in response.data['results']]
        self.assertIn('AAPL', tickers)
        self.assertIn('GOOGL', tickers)


class SectorListFilterAPITest(APITestCase):
    """Tests for filtering on the GET /api/sectors endpoint."""

    def setUp(self):
        """Set up test fixtures with diverse sector data."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        # Create sectors
        self.sector1 = Sector.objects.create(name='Information Technology')
        self.sector2 = Sector.objects.create(name='Financials')
        self.sector3 = Sector.objects.create(name='Healthcare')
        self.sector4 = Sector.objects.create(name='Consumer Technology')

    def test_filter_sectors_by_name_exact(self):
        """Test filtering sectors by exact name match (case-insensitive)."""
        url = reverse('api:sector-list')
        response = self.client.get(url, {'name': 'Information Technology'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['name'], 'Information Technology')

    def test_filter_sectors_by_name_exact_case_insensitive(self):
        """Test filtering sectors by exact name match is case-insensitive."""
        url = reverse('api:sector-list')
        # Test lowercase
        response = self.client.get(url, {'name': 'information technology'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['name'], 'Information Technology')
        
        # Test mixed case
        response = self.client.get(url, {'name': 'FiNaNcIaLs'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['name'], 'Financials')

    def test_filter_sectors_by_name_icontains(self):
        """Test filtering sectors by name contains (case-insensitive)."""
        url = reverse('api:sector-list')
        response = self.client.get(url, {'name__icontains': 'Tech'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should match Information Technology and Consumer Technology
        self.assertEqual(len(response.data['results']), 2)
        sector_names = [item['name'] for item in response.data['results']]
        self.assertIn('Information Technology', sector_names)
        self.assertIn('Consumer Technology', sector_names)

    def test_filter_sectors_by_name_icontains_case_insensitive(self):
        """Test filtering sectors by name contains is case-insensitive."""
        url = reverse('api:sector-list')
        # Test lowercase
        response = self.client.get(url, {'name__icontains': 'tech'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        
        # Test mixed case
        response = self.client.get(url, {'name__icontains': 'FiN'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['name'], 'Financials')

    def test_filter_sectors_multiple_filters(self):
        """Test combining multiple filters for sectors."""
        url = reverse('api:sector-list')
        # Filter by name contains 'Tech' and exact name 'Information Technology'
        response = self.client.get(url, {'name__icontains': 'Tech', 'name': 'Information Technology'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['name'], 'Information Technology')

    def test_filter_sectors_empty_result_set(self):
        """Test filtering sectors with no matching results."""
        url = reverse('api:sector-list')
        response = self.client.get(url, {'name': 'NONEXISTENT'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)

    def test_filter_sectors_preserves_case(self):
        """Test that sector names preserve case (unlike Exchange which normalizes)."""
        # Create sector with mixed case (should preserve case)
        sector = Sector.objects.create(name='MixedCase Sector')
        # Verify it preserved case
        sector.refresh_from_db()
        self.assertEqual(sector.name, 'MixedCase Sector')
        
        url = reverse('api:sector-list')
        # Filter should work with any case
        response = self.client.get(url, {'name': 'mixedcase sector'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['name'], 'MixedCase Sector')
        
        # Also test with contains
        response = self.client.get(url, {'name__icontains': 'mixed'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['name'], 'MixedCase Sector')


class RunListFilterAPITest(APITestCase):
    """Tests for filtering on the GET /api/runs endpoint."""

    def setUp(self):
        """Set up test fixtures with diverse run data."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
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
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
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


class BulkQueueRunFilterAPITest(APITestCase):
    """Tests for filtering ingestion runs by bulk_queue_run."""

    def setUp(self):
        """Set up test fixtures with bulk queue runs."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        # Create stocks
        self.stock_aapl = Stock.objects.create(ticker='AAPL')
        self.stock_googl = Stock.objects.create(ticker='GOOGL')
        self.stock_msft = Stock.objects.create(ticker='MSFT')
        
        # Create bulk queue runs
        self.bulk_run1 = BulkQueueRun.objects.create(
            requested_by='admin@example.com',
            total_stocks=100,
            queued_count=95,
            skipped_count=5,
            error_count=0
        )
        
        self.bulk_run2 = BulkQueueRun.objects.create(
            requested_by='system',
            total_stocks=100,
            queued_count=90,
            skipped_count=8,
            error_count=2
        )
        
        # Create runs linked to bulk_run1
        self.run1 = StockIngestionRun.objects.create(
            stock=self.stock_aapl,
            state=IngestionState.DONE,
            requested_by='admin@example.com',
            bulk_queue_run=self.bulk_run1
        )
        
        self.run2 = StockIngestionRun.objects.create(
            stock=self.stock_googl,
            state=IngestionState.FAILED,
            requested_by='admin@example.com',
            bulk_queue_run=self.bulk_run1
        )
        
        # Create run linked to bulk_run2
        self.run3 = StockIngestionRun.objects.create(
            stock=self.stock_msft,
            state=IngestionState.FETCHING,
            requested_by='system',
            bulk_queue_run=self.bulk_run2
        )
        
        # Create run without bulk_queue_run (manual queue)
        self.run4 = StockIngestionRun.objects.create(
            stock=self.stock_aapl,
            state=IngestionState.QUEUED_FOR_FETCH,
            requested_by='user@example.com',
            bulk_queue_run=None
        )

    def test_filter_by_bulk_queue_run(self):
        """Test filtering runs by bulk_queue_run UUID."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'bulk_queue_run': str(self.bulk_run1.id)})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        
        # Verify all runs belong to bulk_run1
        run_ids = [run['id'] for run in response.data['results']]
        self.assertIn(str(self.run1.id), run_ids)
        self.assertIn(str(self.run2.id), run_ids)
        self.assertNotIn(str(self.run3.id), run_ids)
        self.assertNotIn(str(self.run4.id), run_ids)

    def test_filter_by_bulk_queue_run_different_bulk_run(self):
        """Test filtering runs by different bulk_queue_run UUID."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'bulk_queue_run': str(self.bulk_run2.id)})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['id'], str(self.run3.id))

    def test_filter_by_bulk_queue_run_combined_with_state(self):
        """Test combining bulk_queue_run filter with state filter."""
        url = reverse('api:run-list')
        response = self.client.get(url, {
            'bulk_queue_run': str(self.bulk_run1.id),
            'state': 'FAILED'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['id'], str(self.run2.id))
        self.assertEqual(response.data['results'][0]['state'], 'FAILED')

    def test_filter_by_bulk_queue_run_combined_with_ticker(self):
        """Test combining bulk_queue_run filter with ticker filter."""
        url = reverse('api:run-list')
        response = self.client.get(url, {
            'bulk_queue_run': str(self.bulk_run1.id),
            'ticker': 'AAPL'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['id'], str(self.run1.id))
        self.assertEqual(response.data['results'][0]['ticker'], 'AAPL')

    def test_filter_by_bulk_queue_run_nonexistent_uuid(self):
        """Test filtering with non-existent bulk_queue_run UUID returns empty results."""
        url = reverse('api:run-list')
        nonexistent_uuid = '00000000-0000-0000-0000-000000000000'
        response = self.client.get(url, {'bulk_queue_run': nonexistent_uuid})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)

    def test_filter_by_bulk_queue_run_excludes_null_runs(self):
        """Test that filtering by bulk_queue_run excludes runs without bulk_queue_run."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'bulk_queue_run': str(self.bulk_run1.id)})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should not include run4 which has bulk_queue_run=None
        run_ids = [run['id'] for run in response.data['results']]
        self.assertNotIn(str(self.run4.id), run_ids)

    def test_filter_by_bulk_queue_run_invalid_uuid_format(self):
        """Test filtering with invalid UUID format is handled gracefully."""
        url = reverse('api:run-list')
        response = self.client.get(url, {'bulk_queue_run': 'not-a-uuid'})
        
        # django-filter UUIDFilter should return 400 for invalid UUID format
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_filter_by_bulk_queue_run_on_ticker_endpoint(self):
        """Test filtering by bulk_queue_run works on ticker-specific endpoint."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url, {'bulk_queue_run': str(self.bulk_run1.id)})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['id'], str(self.run1.id))
        self.assertEqual(response.data['results'][0]['ticker'], 'AAPL')

    def test_filter_by_bulk_queue_run_combined_with_multiple_filters(self):
        """Test combining bulk_queue_run with multiple other filters."""
        url = reverse('api:run-list')
        response = self.client.get(url, {
            'bulk_queue_run': str(self.bulk_run1.id),
            'state': 'DONE',
            'requested_by': 'admin@example.com'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['id'], str(self.run1.id))


class BulkQueueRunListFilterAPITest(APITestCase):
    """Tests for filtering on the GET /api/bulk-queue-runs endpoint."""

    def setUp(self):
        """Set up test fixtures with diverse bulk queue run data."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        now = timezone.now()
        yesterday = now - timedelta(days=1)
        two_days_ago = now - timedelta(days=2)
        
        # Create bulk queue runs with various states
        self.completed_run = BulkQueueRun.objects.create(
            requested_by='admin@example.com',
            total_stocks=100,
            queued_count=95,
            skipped_count=3,
            error_count=2,
            started_at=yesterday,
            completed_at=yesterday + timedelta(minutes=10)
        )
        
        self.incomplete_run = BulkQueueRun.objects.create(
            requested_by='user@example.com',
            total_stocks=50,
            queued_count=0,
            skipped_count=0,
            error_count=0,
            started_at=None,
            completed_at=None
        )
        
        self.run_with_errors = BulkQueueRun.objects.create(
            requested_by='admin@example.com',
            total_stocks=200,
            queued_count=190,
            skipped_count=5,
            error_count=5,
            started_at=yesterday,
            completed_at=yesterday + timedelta(minutes=15)
        )
        
        self.run_without_errors = BulkQueueRun.objects.create(
            requested_by='system@example.com',
            total_stocks=75,
            queued_count=75,
            skipped_count=0,
            error_count=0,
            started_at=yesterday,
            completed_at=yesterday + timedelta(minutes=5)
        )
        
        # Set created_at explicitly for date filtering tests
        self.completed_run.created_at = two_days_ago
        self.completed_run.save()
        
        self.incomplete_run.created_at = now - timedelta(hours=1)
        self.incomplete_run.save()

    def test_filter_by_requested_by_exact(self):
        """Test filtering by requested_by (exact match, case-insensitive)."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url, {'requested_by': 'admin@example.com'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        
        # Verify all results match
        for result in response.data['results']:
            self.assertEqual(result['requested_by'], 'admin@example.com')
        
        # Test case-insensitive
        response_lower = self.client.get(url, {'requested_by': 'ADMIN@EXAMPLE.COM'})
        self.assertEqual(response_lower.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response_lower.data['results']), 2)

    def test_filter_by_requested_by_icontains(self):
        """Test filtering by requested_by__icontains (partial match)."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url, {'requested_by__icontains': 'admin'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        
        # Verify all results contain 'admin'
        for result in response.data['results']:
            self.assertIn('admin', result['requested_by'].lower())

    def test_filter_by_created_after(self):
        """Test filtering by created_after date."""
        url = reverse('api:bulk-queue-run-list')
        # Filter for runs created after now (should return empty)
        future_date = (timezone.now() + timedelta(days=1)).isoformat()
        response = self.client.get(url, {'created_after': future_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)
        
        # Filter for runs created after 3 days ago (should return all)
        past_date = (timezone.now() - timedelta(days=3)).isoformat()
        response = self.client.get(url, {'created_after': past_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 4)

    def test_filter_by_created_before(self):
        """Test filtering by created_before date."""
        url = reverse('api:bulk-queue-run-list')
        # Filter for runs created before 3 days ago (should return empty)
        past_date = (timezone.now() - timedelta(days=3)).isoformat()
        response = self.client.get(url, {'created_before': past_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)
        
        # Filter for runs created before tomorrow (should return all)
        future_date = (timezone.now() + timedelta(days=1)).isoformat()
        response = self.client.get(url, {'created_before': future_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 4)

    def test_filter_by_started_at_after(self):
        """Test filtering by started_at_after date."""
        url = reverse('api:bulk-queue-run-list')
        # Filter for runs started after now (should return empty)
        future_date = timezone.now().isoformat()
        response = self.client.get(url, {'started_at_after': future_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)
        
        # Filter for runs started after 2 days ago (should return runs with started_at)
        past_date = (timezone.now() - timedelta(days=2)).isoformat()
        response = self.client.get(url, {'started_at_after': past_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return 3 runs (completed_run, run_with_errors, run_without_errors)
        self.assertEqual(len(response.data['results']), 3)

    def test_filter_by_started_at_before(self):
        """Test filtering by started_at_before date."""
        url = reverse('api:bulk-queue-run-list')
        # Filter for runs started before 2 days ago (should return empty)
        past_date = (timezone.now() - timedelta(days=2)).isoformat()
        response = self.client.get(url, {'started_at_before': past_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)
        
        # Filter for runs started before tomorrow (should return runs with started_at)
        future_date = (timezone.now() + timedelta(days=1)).isoformat()
        response = self.client.get(url, {'started_at_before': future_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return 3 runs (completed_run, run_with_errors, run_without_errors)
        self.assertEqual(len(response.data['results']), 3)

    def test_filter_by_completed_at_after(self):
        """Test filtering by completed_at_after date."""
        url = reverse('api:bulk-queue-run-list')
        # Filter for runs completed after now (should return empty)
        future_date = timezone.now().isoformat()
        response = self.client.get(url, {'completed_at_after': future_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)
        
        # Filter for runs completed after 2 days ago (should return completed runs)
        past_date = (timezone.now() - timedelta(days=2)).isoformat()
        response = self.client.get(url, {'completed_at_after': past_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return 3 runs (completed_run, run_with_errors, run_without_errors)
        self.assertEqual(len(response.data['results']), 3)

    def test_filter_by_completed_at_before(self):
        """Test filtering by completed_at_before date."""
        url = reverse('api:bulk-queue-run-list')
        # Filter for runs completed before 2 days ago (should return empty)
        past_date = (timezone.now() - timedelta(days=2)).isoformat()
        response = self.client.get(url, {'completed_at_before': past_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)
        
        # Filter for runs completed before tomorrow (should return completed runs)
        future_date = (timezone.now() + timedelta(days=1)).isoformat()
        response = self.client.get(url, {'completed_at_before': future_date})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return 3 runs (completed_run, run_with_errors, run_without_errors)
        self.assertEqual(len(response.data['results']), 3)

    def test_filter_by_is_completed_true(self):
        """Test filtering by is_completed=true returns only completed runs."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url, {'is_completed': 'true'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)
        
        # Verify all results have completed_at set
        for result in response.data['results']:
            self.assertIsNotNone(result['completed_at'])

    def test_filter_by_is_completed_false(self):
        """Test filtering by is_completed=false returns only incomplete runs."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url, {'is_completed': 'false'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        
        # Verify all results have completed_at=None
        for result in response.data['results']:
            self.assertIsNone(result['completed_at'])

    def test_filter_by_has_errors_true(self):
        """Test filtering by has_errors=true returns only runs with error_count > 0."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url, {'has_errors': 'true'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        
        # Verify all results have error_count > 0
        for result in response.data['results']:
            self.assertGreater(result['error_count'], 0)

    def test_filter_by_has_errors_false(self):
        """Test filtering by has_errors=false returns only runs with error_count = 0."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url, {'has_errors': 'false'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        
        # Verify all results have error_count = 0
        for result in response.data['results']:
            self.assertEqual(result['error_count'], 0)

    def test_filter_multiple_parameters(self):
        """Test filtering with multiple parameters combined."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url, {
            'requested_by': 'admin@example.com',
            'is_completed': 'true',
            'has_errors': 'true'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Both completed_run and run_with_errors match: admin@example.com, completed, and has errors
        self.assertEqual(len(response.data['results']), 2)
        
        # Verify all results match all filters
        for result in response.data['results']:
            self.assertEqual(result['requested_by'], 'admin@example.com')
            self.assertIsNotNone(result['completed_at'])
            self.assertGreater(result['error_count'], 0)

    def test_filter_no_results(self):
        """Test filtering that returns no results."""
        url = reverse('api:bulk-queue-run-list')
        # Filter by non-existent requester
        response = self.client.get(url, {'requested_by': 'nonexistent@example.com'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)

    def test_filter_case_insensitive_requested_by(self):
        """Test that requested_by filtering is case-insensitive."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url, {'requested_by': 'ADMIN@EXAMPLE.COM'})
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
