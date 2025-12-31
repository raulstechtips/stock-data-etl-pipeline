"""
Authentication Tests for Stock Ticker ETL Pipeline API.

This test module covers:
- Authentication requirements for all API endpoints
- Unauthenticated access should return 403 Forbidden
- Authenticated access should work normally
"""

from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from api.models import BulkQueueRun, Exchange, IngestionState, Stock, StockIngestionRun

User = get_user_model()


class AuthenticationRequiredAPITest(APITestCase):
    """Tests to verify that all API endpoints require authentication."""

    def setUp(self):
        """Set up test fixtures."""
        # Create test data
        self.exchange = Exchange.objects.create(name='NASDAQ')
        self.stock = Stock.objects.create(
            ticker='AAPL',
            name='Apple Inc.',
            exchange=self.exchange
        )
        self.run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        self.bulk_queue_run = BulkQueueRun.objects.create()

    def test_stock_status_requires_authentication(self):
        """Test that GET /api/ticker/<ticker>/status requires authentication."""
        url = reverse('api:stock-status', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_queue_for_fetch_requires_authentication(self):
        """Test that POST /api/ticker/queue requires authentication."""
        url = reverse('api:queue-for-fetch')
        response = self.client.post(
            url,
            {'ticker': 'AAPL'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_run_detail_requires_authentication(self):
        """Test that GET /api/run/<run_id>/detail requires authentication."""
        url = reverse('api:run-detail', kwargs={'run_id': str(self.run.id)})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_ticker_list_requires_authentication(self):
        """Test that GET /api/tickers requires authentication."""
        url = reverse('api:ticker-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_ticker_detail_requires_authentication(self):
        """Test that GET /api/ticker/<ticker>/detail requires authentication."""
        url = reverse('api:ticker-detail', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_run_list_requires_authentication(self):
        """Test that GET /api/runs requires authentication."""
        url = reverse('api:run-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_ticker_runs_list_requires_authentication(self):
        """Test that GET /api/runs/ticker/<ticker> requires authentication."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_queue_all_stocks_requires_authentication(self):
        """Test that POST /api/ticker/queue/all requires authentication."""
        url = reverse('api:queue-all-stocks-for-fetch')
        response = self.client.post(url, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_bulk_queue_run_list_requires_authentication(self):
        """Test that GET /api/bulk-queue-runs requires authentication."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_bulk_queue_run_stats_requires_authentication(self):
        """Test that GET /api/bulk-queue-runs/<id>/stats requires authentication."""
        url = reverse(
            'api:bulk-queue-run-stats-detail',
            kwargs={'bulk_queue_run_id': str(self.bulk_queue_run.id)}
        )
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


class AuthenticatedAccessAPITest(APITestCase):
    """Tests to verify that authenticated users can access endpoints."""

    def setUp(self):
        """Set up test fixtures and authenticate user."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        # Create test data
        self.exchange = Exchange.objects.create(name='NASDAQ')
        self.stock = Stock.objects.create(
            ticker='AAPL',
            name='Apple Inc.',
            exchange=self.exchange
        )
        self.run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        self.bulk_queue_run = BulkQueueRun.objects.create()

    def test_authenticated_user_can_access_stock_status(self):
        """Test that authenticated user can access stock status endpoint."""
        url = reverse('api:stock-status', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['ticker'], 'AAPL')

    def test_authenticated_user_can_access_ticker_list(self):
        """Test that authenticated user can access ticker list endpoint."""
        url = reverse('api:ticker-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)

    def test_authenticated_user_can_access_run_detail(self):
        """Test that authenticated user can access run detail endpoint."""
        url = reverse('api:run-detail', kwargs={'run_id': str(self.run.id)})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['id'], str(self.run.id))

    def test_authenticated_user_can_access_bulk_queue_run_list(self):
        """Test that authenticated user can access bulk queue run list endpoint."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)

