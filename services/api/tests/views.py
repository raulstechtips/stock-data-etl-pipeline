"""
API Tests for Stock Ingestion ETL Pipeline.

This test module covers:
- Retrieving stock status for existing and non-existent stocks via the API
- Handling stocks with and without ingestion runs
- Validating responses for stock and run detail endpoints
- Ensuring proper error handling for missing resources and invalid UUIDs
"""

import time
import uuid
from unittest.mock import Mock, patch

from celery.exceptions import OperationalError as CeleryOperationalError
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.urls import reverse
from django.db import IntegrityError
from rest_framework import status
from rest_framework.test import APITestCase

from api.models import BulkQueueRun, Exchange, IngestionState, Stock, StockIngestionRun

User = get_user_model()


class StockStatusAPITest(APITestCase):
    """Tests for the GET /api/ticker/<ticker>/status endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        self.stock = Stock.objects.create(ticker='AAPL')

    def test_get_status_existing_stock_with_run(self):
        """Test getting status for an existing stock with a run."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        
        url = reverse('api:stock-status', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['ticker'], 'AAPL')
        self.assertEqual(response.data['state'], 'FETCHING')
        self.assertEqual(response.data['run_id'], str(run.id))

    def test_get_status_existing_stock_no_runs(self):
        """Test getting status for a stock with no runs."""
        url = reverse('api:stock-status', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['ticker'], 'AAPL')
        self.assertIsNone(response.data['state'])
        self.assertIsNone(response.data['run_id'])

    def test_get_status_not_found(self):
        """Test getting status for a non-existent stock."""
        url = reverse('api:stock-status', kwargs={'ticker': 'NONEXISTENT'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'STOCK_NOT_FOUND')
        self.assertIn('details', response.data['error'])
        self.assertEqual(response.data['error']['details']['ticker'], 'NONEXISTENT')

    def test_get_status_case_insensitive(self):
        """Test that the endpoint is case-insensitive."""
        url = reverse('api:stock-status', kwargs={'ticker': 'aapl'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['ticker'], 'AAPL')


class QueueForFetchAPITest(APITestCase):
    """Tests for the POST /api/ticker/queue endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        self.url = reverse('api:queue-for-fetch')

    @patch('api.views.fetch_stock_data.delay')
    def test_queue_new_stock(self, mock_delay):
        """Test queuing a new stock creates stock and run and triggers Celery task."""
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'task-123'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {'ticker': 'AAPL', 'requested_by': 'test'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['ticker'], 'AAPL')
        self.assertEqual(response.data['state'], 'QUEUED_FOR_FETCH')
        
        # Verify stock was created
        self.assertTrue(Stock.objects.filter(ticker='AAPL').exists())
        
        # Verify Celery task was called
        mock_delay.assert_called_once()
        call_args = mock_delay.call_args
        self.assertEqual(call_args[1]['ticker'], 'AAPL')

    @patch('api.views.fetch_stock_data.delay')
    def test_queue_existing_stock_no_active_run(self, mock_delay):
        """Test queuing an existing stock with no active run."""
        mock_task_result = Mock()
        mock_task_result.id = 'task-123'
        mock_delay.return_value = mock_task_result
        
        stock = Stock.objects.create(ticker='AAPL')
        StockIngestionRun.objects.create(
            stock=stock,
            state=IngestionState.DONE
        )
        
        response = self.client.post(
            self.url,
            {'ticker': 'AAPL'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['state'], 'QUEUED_FOR_FETCH')
        
        # Verify Celery task was called
        mock_delay.assert_called_once()

    @patch('api.views.fetch_stock_data.delay')
    def test_queue_returns_existing_active_run(self, mock_delay):
        """Test that queuing returns existing active run and does not trigger task."""
        stock = Stock.objects.create(ticker='AAPL')
        existing_run = StockIngestionRun.objects.create(
            stock=stock,
            state=IngestionState.FETCHING
        )
        
        response = self.client.post(
            self.url,
            {'ticker': 'AAPL'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['id'], str(existing_run.id))
        self.assertEqual(response.data['state'], 'FETCHING')
        
        # Verify Celery task was NOT called (active run exists)
        mock_delay.assert_not_called()

    def test_queue_validates_ticker(self):
        """Test that ticker validation works."""
        response = self.client.post(
            self.url,
            {'ticker': ''},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'VALIDATION_ERROR')
        self.assertIn('details', response.data['error'])

    @patch('api.views.fetch_stock_data.delay')
    def test_queue_normalizes_ticker_to_uppercase(self, mock_delay):
        """Test that tickers are normalized to uppercase."""
        mock_task_result = Mock()
        mock_task_result.id = 'task-123'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {'ticker': 'aapl'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['ticker'], 'AAPL')

    @patch('api.views.fetch_stock_data.delay')
    def test_queue_with_all_optional_fields(self, mock_delay):
        """Test queuing with all optional fields provided."""
        mock_task_result = Mock()
        mock_task_result.id = 'task-123'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {
                'ticker': 'AAPL',
                'requested_by': 'data-pipeline',
                'request_id': 'req-2024-001'
            },
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['requested_by'], 'data-pipeline')
        self.assertEqual(response.data['request_id'], 'req-2024-001')
    
    @patch('api.views.fetch_stock_data.delay')
    def test_queue_broker_error_transitions_to_failed(self, mock_delay):
        """Test that broker errors transition run to FAILED."""
        # Mock Celery broker error
        mock_delay.side_effect = CeleryOperationalError("Connection to broker failed")
        
        # Use unique ticker to avoid conflicts with other tests (must be alphanumeric)
        unique_ticker = 'BRKRTST'
        
        response = self.client.post(
            self.url,
            {'ticker': unique_ticker},
            format='json'
        )
        
        # Should return 500 error
        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        self.assertIn('message', response.data)
        self.assertIn('code', response.data)
        self.assertIn('details', response.data)
        self.assertIn('run_id', response.data['details'])
        
        # Verify run was created but transitioned to FAILED
        run = StockIngestionRun.objects.get(id=response.data['details']['run_id'])
        self.assertEqual(run.state, IngestionState.FAILED)
        self.assertEqual(run.error_code, 'BROKER_ERROR')
        self.assertIn('broker', run.error_message.lower())
        # Verify the correct stock was used
        self.assertEqual(run.stock.ticker, unique_ticker)

    def test_queue_handles_integrity_error_race_condition(self):
        """Test that IntegrityError from race condition returns 409 Conflict."""
        
        # Mock the service to raise IntegrityError (simulating race condition)
        with patch('api.views.StockIngestionService') as MockService:
            mock_service = MockService.return_value
            mock_service.queue_for_fetch.side_effect = IntegrityError(
                'duplicate key value violates unique constraint'
            )
            
            response = self.client.post(
                self.url,
                {'ticker': 'AAPL'},
                format='json'
            )
            
            self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
            self.assertIn('error', response.data)
            self.assertIn('message', response.data['error'])
            self.assertIn('code', response.data['error'])
            self.assertEqual(response.data['error']['code'], 'RACE_CONDITION')
            self.assertIn('details', response.data['error'])
            self.assertEqual(response.data['error']['details']['ticker'], 'AAPL')
            self.assertIn('another request', response.data['error']['message'].lower())


class RunDetailAPITest(APITestCase):
    """Tests for the GET /api/run/<run_id>/detail endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        self.stock = Stock.objects.create(ticker='AAPL')
        self.run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING,
            requested_by='test-service'
        )

    def test_get_run_detail(self):
        """Test getting run details."""
        url = reverse('api:run-detail', kwargs={'run_id': str(self.run.id)})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['id'], str(self.run.id))
        self.assertEqual(response.data['ticker'], 'AAPL')
        self.assertEqual(response.data['state'], 'FETCHING')
        self.assertEqual(response.data['requested_by'], 'test-service')

    def test_get_run_not_found(self):
        """Test getting a non-existent run."""
        fake_id = uuid.uuid4()
        url = reverse('api:run-detail', kwargs={'run_id': str(fake_id)})
        
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'RUN_NOT_FOUND')
        self.assertIn('details', response.data['error'])
        self.assertEqual(response.data['error']['details']['run_id'], str(fake_id))

    def test_get_run_invalid_uuid(self):
        """Test getting with an invalid UUID format."""
        url = reverse('api:run-detail', kwargs={'run_id': 'invalid-uuid'})
        
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'INVALID_UUID')
        self.assertIn('details', response.data['error'])
        self.assertEqual(response.data['error']['details']['run_id'], 'invalid-uuid')


class TickerListAPITest(APITestCase):
    """Tests for the GET /api/tickers endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        # Create multiple stocks for pagination testing
        Stock.objects.create(ticker='AAPL', name='Apple Inc.')
        Stock.objects.create(ticker='GOOGL', name='Alphabet Inc.')
        Stock.objects.create(ticker='MSFT', name='Microsoft Corporation')

    def test_list_tickers(self):
        """Test listing all tickers."""
        url = reverse('api:ticker-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertEqual(len(response.data['results']), 3)
        
        # Verify ticker data
        tickers = [item['ticker'] for item in response.data['results']]
        self.assertIn('AAPL', tickers)
        self.assertIn('GOOGL', tickers)
        self.assertIn('MSFT', tickers)

    def test_list_tickers_empty(self):
        """Test listing tickers when none exist."""
        Stock.objects.all().delete()
        
        url = reverse('api:ticker-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertEqual(len(response.data['results']), 0)

    def test_list_tickers_pagination(self):
        """Test cursor pagination for tickers."""
        # Create more stocks to test pagination
        for i in range(55):
            Stock.objects.create(ticker=f'TEST{i:02d}')
        
        url = reverse('api:ticker-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertIn('next', response.data)
        # Default page size is 50
        self.assertEqual(len(response.data['results']), 50)
        
        # Test next page
        if response.data['next']:
            next_response = self.client.get(response.data['next'])
            self.assertEqual(next_response.status_code, status.HTTP_200_OK)
            self.assertGreater(len(next_response.data['results']), 0)


class TickerDetailAPITest(APITestCase):
    """Tests for the GET /api/ticker/<ticker>/detail endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        self.stock = Stock.objects.create(
            ticker='AAPL',
            name='Apple Inc.',
            sector='Technology'
        )

    def test_get_ticker_detail(self):
        """Test getting ticker details."""
        url = reverse('api:ticker-detail', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['ticker'], 'AAPL')
        self.assertEqual(response.data['name'], 'Apple Inc.')
        self.assertEqual(response.data['sector'], 'Technology')

    def test_get_ticker_detail_case_insensitive(self):
        """Test that ticker detail lookup is case-insensitive."""
        url = reverse('api:ticker-detail', kwargs={'ticker': 'aapl'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['ticker'], 'AAPL')

    def test_get_ticker_detail_not_found(self):
        """Test getting details for a non-existent ticker."""
        url = reverse('api:ticker-detail', kwargs={'ticker': 'NONEXISTENT'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'STOCK_NOT_FOUND')
        self.assertIn('details', response.data['error'])
        self.assertEqual(response.data['error']['details']['ticker'], 'NONEXISTENT')


class RunListAPITest(APITestCase):
    """Tests for the GET /api/runs endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        self.stock1 = Stock.objects.create(ticker='AAPL')
        self.stock2 = Stock.objects.create(ticker='GOOGL')
        
        # Create multiple runs
        StockIngestionRun.objects.create(stock=self.stock1, state=IngestionState.DONE)
        StockIngestionRun.objects.create(stock=self.stock1, state=IngestionState.FETCHING)
        StockIngestionRun.objects.create(stock=self.stock2, state=IngestionState.QUEUED_FOR_FETCH)

    def test_list_runs(self):
        """Test listing all runs."""
        url = reverse('api:run-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertEqual(len(response.data['results']), 3)

    def test_list_runs_empty(self):
        """Test listing runs when none exist."""
        StockIngestionRun.objects.all().delete()
        
        url = reverse('api:run-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertEqual(len(response.data['results']), 0)

    def test_list_runs_pagination(self):
        """Test cursor pagination for runs."""
        # Create more runs to test pagination
        for i in range(55):
            StockIngestionRun.objects.create(
                stock=self.stock1,
                state=IngestionState.DONE
            )
        
        url = reverse('api:run-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertIn('next', response.data)
        # Default page size is 50
        self.assertEqual(len(response.data['results']), 50)


class TickerRunsListAPITest(APITestCase):
    """Tests for the GET /api/ticker/<ticker>/runs endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        self.stock1 = Stock.objects.create(ticker='AAPL')
        self.stock2 = Stock.objects.create(ticker='GOOGL')
        
        # Create runs for AAPL
        StockIngestionRun.objects.create(stock=self.stock1, state=IngestionState.DONE)
        StockIngestionRun.objects.create(stock=self.stock1, state=IngestionState.FETCHING)
        StockIngestionRun.objects.create(stock=self.stock1, state=IngestionState.FAILED)
        
        # Create runs for GOOGL
        StockIngestionRun.objects.create(stock=self.stock2, state=IngestionState.QUEUED_FOR_FETCH)

    def test_list_ticker_runs(self):
        """Test listing runs for a specific ticker."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertEqual(len(response.data['results']), 3)
        
        # Verify all runs are for AAPL
        for run in response.data['results']:
            self.assertEqual(run['ticker'], 'AAPL')

    def test_list_ticker_runs_case_insensitive(self):
        """Test that ticker runs lookup is case-insensitive."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'aapl'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)

    def test_list_ticker_runs_no_runs(self):
        """Test listing runs for a ticker with no runs."""
        stock3 = Stock.objects.create(ticker='MSFT')
        
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'MSFT'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertEqual(len(response.data['results']), 0)

    def test_list_ticker_runs_not_found(self):
        """Test listing runs for a non-existent ticker."""
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'NONEXISTENT'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'STOCK_NOT_FOUND')
        self.assertIn('details', response.data['error'])
        self.assertEqual(response.data['error']['details']['ticker'], 'NONEXISTENT')

    def test_list_ticker_runs_pagination(self):
        """Test cursor pagination for ticker runs."""
        # Create more runs to test pagination
        for i in range(55):
            StockIngestionRun.objects.create(
                stock=self.stock1,
                state=IngestionState.DONE
            )
        
        url = reverse('api:ticker-runs-list', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertIn('next', response.data)
        # Default page size is 50
        self.assertEqual(len(response.data['results']), 50)


class QueueAllStocksForFetchAPITest(APITestCase):
    """Tests for the POST /api/ticker/queue/all endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        self.url = reverse('api:queue-all-stocks-for-fetch')

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_success(self, mock_delay):
        """Test successfully queuing all stocks creates BulkQueueRun and triggers task."""
        # Create some stocks
        Stock.objects.create(ticker='AAPL')
        Stock.objects.create(ticker='GOOGL')
        Stock.objects.create(ticker='MSFT')
        
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'bulk-task-123'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {'requested_by': 'admin@example.com'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        self.assertIn('bulk_queue_run', response.data)
        self.assertIn('task_id', response.data)
        self.assertIn('message', response.data)
        self.assertEqual(response.data['task_id'], 'bulk-task-123')
        
        # Verify BulkQueueRun was created
        bulk_run_id = response.data['bulk_queue_run']['id']
        bulk_run = BulkQueueRun.objects.get(id=bulk_run_id)
        self.assertEqual(bulk_run.total_stocks, 3)
        self.assertEqual(bulk_run.requested_by, 'admin@example.com')
        self.assertEqual(bulk_run.queued_count, 0)  # Not yet processed
        self.assertEqual(bulk_run.skipped_count, 0)
        self.assertEqual(bulk_run.error_count, 0)
        
        # Verify Celery task was called with bulk_queue_run_id
        mock_delay.assert_called_once()
        call_args = mock_delay.call_args
        self.assertEqual(call_args[1]['bulk_queue_run_id'], str(bulk_run.id))

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_empty_database(self, mock_delay):
        """Test queuing all stocks when database is empty."""
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'bulk-task-456'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        
        # Verify BulkQueueRun was created with total_stocks=0
        bulk_run_id = response.data['bulk_queue_run']['id']
        bulk_run = BulkQueueRun.objects.get(id=bulk_run_id)
        self.assertEqual(bulk_run.total_stocks, 0)
        self.assertIsNone(bulk_run.requested_by)
        
        # Verify task was still queued
        mock_delay.assert_called_once()

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_without_requested_by(self, mock_delay):
        """Test queuing all stocks without requested_by parameter."""
        Stock.objects.create(ticker='AAPL')
        
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'bulk-task-789'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        
        # Verify BulkQueueRun was created without requested_by
        bulk_run_id = response.data['bulk_queue_run']['id']
        bulk_run = BulkQueueRun.objects.get(id=bulk_run_id)
        self.assertIsNone(bulk_run.requested_by)

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_broker_error(self, mock_delay):
        """Test that broker errors return 500 Internal Server Error."""
        Stock.objects.create(ticker='AAPL')
        
        # Mock Celery broker error
        mock_delay.side_effect = CeleryOperationalError("Connection to broker failed")
        
        response = self.client.post(
            self.url,
            {'requested_by': 'admin@example.com'},
            format='json'
        )
        
        # Should return 500 error
        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'BROKER_ERROR')
        self.assertIn('details', response.data['error'])
        self.assertIn('bulk_queue_run_id', response.data['error']['details'])
        
        # Verify BulkQueueRun was still created
        bulk_run_id = response.data['error']['details']['bulk_queue_run_id']
        bulk_run = BulkQueueRun.objects.get(id=bulk_run_id)
        self.assertEqual(bulk_run.total_stocks, 1)

    def test_queue_all_stocks_invalid_request_body(self):
        """Test that invalid request body returns 400 Bad Request."""
        # Send invalid data (requested_by exceeds max_length would be caught by serializer)
        # For this test, we'll send a data type that's invalid
        response = self.client.post(
            self.url,
            {'requested_by': ['invalid', 'list']},  # Should be string, not list
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'VALIDATION_ERROR')
        self.assertIn('details', response.data['error'])

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_response_format(self, mock_delay):
        """Test that response format is correct."""
        Stock.objects.create(ticker='AAPL')
        Stock.objects.create(ticker='GOOGL')
        
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'task-abc-123'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {'requested_by': 'test-user'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        
        # Verify response structure
        self.assertIn('bulk_queue_run', response.data)
        self.assertIn('task_id', response.data)
        self.assertIn('message', response.data)
        
        # Verify bulk_queue_run structure
        bulk_run = response.data['bulk_queue_run']
        self.assertIn('id', bulk_run)
        self.assertIn('requested_by', bulk_run)
        self.assertIn('total_stocks', bulk_run)
        self.assertIn('queued_count', bulk_run)
        self.assertIn('skipped_count', bulk_run)
        self.assertIn('error_count', bulk_run)
        self.assertIn('created_at', bulk_run)
        self.assertIn('started_at', bulk_run)
        self.assertIn('completed_at', bulk_run)
        
        # Verify values
        self.assertEqual(bulk_run['requested_by'], 'test-user')
        self.assertEqual(bulk_run['total_stocks'], 2)
        self.assertEqual(response.data['task_id'], 'task-abc-123')
        self.assertIn('2 stocks', response.data['message'])

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_with_exchange_filter(self, mock_delay):
        """Test queuing all stocks with exchange parameter filters stocks correctly."""
        # Create exchanges
        nasdaq = Exchange.objects.create(name='NASDAQ')
        nyse = Exchange.objects.create(name='NYSE')
        
        # Create stocks with different exchanges
        Stock.objects.create(ticker='AAPL', exchange=nasdaq)
        Stock.objects.create(ticker='GOOGL', exchange=nasdaq)
        Stock.objects.create(ticker='MSFT', exchange=nasdaq)
        Stock.objects.create(ticker='IBM', exchange=nyse)
        Stock.objects.create(ticker='GE', exchange=nyse)
        
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'task-exchange-123'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {'requested_by': 'admin@example.com', 'exchange': 'NASDAQ'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        self.assertIn('bulk_queue_run', response.data)
        self.assertIn('task_id', response.data)
        self.assertIn('message', response.data)
        self.assertIn('exchange', response.data)
        self.assertEqual(response.data['exchange'], 'NASDAQ')
        
        # Verify BulkQueueRun was created with filtered count
        bulk_run_id = response.data['bulk_queue_run']['id']
        bulk_run = BulkQueueRun.objects.get(id=bulk_run_id)
        self.assertEqual(bulk_run.total_stocks, 3)  # Only NASDAQ stocks
        self.assertEqual(bulk_run.requested_by, 'admin@example.com')
        
        # Verify Celery task was called with exchange_name parameter
        mock_delay.assert_called_once()
        call_args = mock_delay.call_args
        self.assertEqual(call_args[1]['bulk_queue_run_id'], str(bulk_run.id))
        self.assertEqual(call_args[1]['exchange_name'], 'NASDAQ')

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_without_exchange_filter(self, mock_delay):
        """Test queuing all stocks without exchange parameter queues all stocks."""
        # Create exchanges
        nasdaq = Exchange.objects.create(name='NASDAQ')
        nyse = Exchange.objects.create(name='NYSE')
        
        # Create stocks with different exchanges
        Stock.objects.create(ticker='AAPL', exchange=nasdaq)
        Stock.objects.create(ticker='GOOGL', exchange=nasdaq)
        Stock.objects.create(ticker='IBM', exchange=nyse)
        Stock.objects.create(ticker='GE', exchange=nyse)
        Stock.objects.create(ticker='TSLA')  # No exchange
        
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'task-all-123'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {'requested_by': 'admin@example.com'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        
        # Verify BulkQueueRun was created with all stocks count
        bulk_run_id = response.data['bulk_queue_run']['id']
        bulk_run = BulkQueueRun.objects.get(id=bulk_run_id)
        self.assertEqual(bulk_run.total_stocks, 5)  # All stocks
        
        # Verify Celery task was called with exchange_name=None
        mock_delay.assert_called_once()
        call_args = mock_delay.call_args
        self.assertEqual(call_args[1]['bulk_queue_run_id'], str(bulk_run.id))
        self.assertEqual(call_args[1]['exchange_name'], None)

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_with_non_existent_exchange_creates_it(self, mock_delay):
        """Test queuing with non-existent exchange creates the Exchange."""
        # Create some stocks without exchange
        Stock.objects.create(ticker='AAPL')
        Stock.objects.create(ticker='GOOGL')
        
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'task-new-exchange-123'
        mock_delay.return_value = mock_task_result
        
        # Verify exchange doesn't exist yet
        self.assertFalse(Exchange.objects.filter(name='NEWEXCHANGE').exists())
        
        response = self.client.post(
            self.url,
            {'requested_by': 'admin@example.com', 'exchange': 'NEWEXCHANGE'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        
        # Verify Exchange was created
        self.assertTrue(Exchange.objects.filter(name='NEWEXCHANGE').exists())
        exchange = Exchange.objects.get(name='NEWEXCHANGE')
        
        # Verify BulkQueueRun was created with 0 stocks (no stocks have this exchange yet)
        bulk_run_id = response.data['bulk_queue_run']['id']
        bulk_run = BulkQueueRun.objects.get(id=bulk_run_id)
        self.assertEqual(bulk_run.total_stocks, 0)
        
        # Verify task was called with normalized exchange_name
        mock_delay.assert_called_once()
        call_args = mock_delay.call_args
        self.assertEqual(call_args[1]['exchange_name'], 'NEWEXCHANGE')

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_exchange_name_normalization(self, mock_delay):
        """Test that exchange name is normalized (uppercase, strip whitespace)."""
        # Create exchange
        nasdaq = Exchange.objects.create(name='NASDAQ')
        
        # Create stocks
        Stock.objects.create(ticker='AAPL', exchange=nasdaq)
        Stock.objects.create(ticker='GOOGL', exchange=nasdaq)
        
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'task-normalize-123'
        mock_delay.return_value = mock_task_result
        
        # Test with lowercase and whitespace
        response = self.client.post(
            self.url,
            {'requested_by': 'admin@example.com', 'exchange': '  nasdaq  '},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        
        # Verify normalized exchange_name was used
        mock_delay.assert_called_once()
        call_args = mock_delay.call_args
        self.assertEqual(call_args[1]['exchange_name'], 'NASDAQ')
        
        # Verify BulkQueueRun reflects correct filtered count
        bulk_run_id = response.data['bulk_queue_run']['id']
        bulk_run = BulkQueueRun.objects.get(id=bulk_run_id)
        self.assertEqual(bulk_run.total_stocks, 2)

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_exchange_name_case_variations(self, mock_delay):
        """Test exchange filtering with various case variations."""
        # Create exchange
        nasdaq = Exchange.objects.create(name='NASDAQ')
        
        # Create stocks
        Stock.objects.create(ticker='AAPL', exchange=nasdaq)
        
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'task-case-123'
        mock_delay.return_value = mock_task_result
        
        # Test with mixed case
        response = self.client.post(
            self.url,
            {'exchange': 'NasDaQ'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        
        # Verify normalized to uppercase
        call_args = mock_delay.call_args
        self.assertEqual(call_args[1]['exchange_name'], 'NASDAQ')
        
        # Verify correct count
        bulk_run_id = response.data['bulk_queue_run']['id']
        bulk_run = BulkQueueRun.objects.get(id=bulk_run_id)
        self.assertEqual(bulk_run.total_stocks, 1)

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_with_blank_exchange(self, mock_delay):
        """Test that blank exchange parameter is treated as no filter."""
        # Create stocks
        Stock.objects.create(ticker='AAPL')
        Stock.objects.create(ticker='GOOGL')
        
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'task-blank-123'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {'exchange': ''},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        
        # Verify all stocks are counted (blank exchange treated as no filter)
        bulk_run_id = response.data['bulk_queue_run']['id']
        bulk_run = BulkQueueRun.objects.get(id=bulk_run_id)
        self.assertEqual(bulk_run.total_stocks, 2)
        
        # Verify exchange_name is None
        call_args = mock_delay.call_args
        self.assertIsNone(call_args[1]['exchange_name'])

    @patch('workers.tasks.queue_all_stocks_for_fetch.queue_all_stocks_for_fetch.delay')
    def test_queue_all_stocks_response_includes_exchange(self, mock_delay):
        """Test that response includes exchange field when provided."""
        # Create exchange and stocks
        nasdaq = Exchange.objects.create(name='NASDAQ')
        Stock.objects.create(ticker='AAPL', exchange=nasdaq)
        
        # Mock Celery task
        mock_task_result = Mock()
        mock_task_result.id = 'task-response-123'
        mock_delay.return_value = mock_task_result
        
        response = self.client.post(
            self.url,
            {'exchange': 'NASDAQ'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        
        # Verify response includes exchange
        self.assertIn('exchange', response.data)
        self.assertEqual(response.data['exchange'], 'NASDAQ')
        
        # Verify message mentions exchange
        self.assertIn('NASDAQ', response.data['message'])


class BulkQueueRunListAPITest(APITestCase):
    """Tests for the GET /api/bulk-queue-runs endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        # Create bulk queue runs with various states
        BulkQueueRun.objects.create(
            requested_by='admin@example.com',
            total_stocks=100,
            queued_count=95,
            skipped_count=3,
            error_count=2
        )
        
        BulkQueueRun.objects.create(
            requested_by='user@example.com',
            total_stocks=50,
            queued_count=0,
            skipped_count=0,
            error_count=0
        )
        
        BulkQueueRun.objects.create(
            requested_by='system@example.com',
            total_stocks=200,
            queued_count=200,
            skipped_count=0,
            error_count=0
        )

    def test_list_bulk_queue_runs(self):
        """Test listing all bulk queue runs."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertEqual(len(response.data['results']), 3)

    def test_list_bulk_queue_runs_empty(self):
        """Test listing bulk queue runs when none exist."""
        BulkQueueRun.objects.all().delete()
        
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertEqual(len(response.data['results']), 0)

    def test_list_bulk_queue_runs_pagination(self):
        """Test cursor pagination for bulk queue runs."""
        # Create more runs to test pagination
        for i in range(55):
            BulkQueueRun.objects.create(
                requested_by=f'user{i}@example.com',
                total_stocks=10
            )
        
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        self.assertIn('next', response.data)
        # Default page size is 50
        self.assertEqual(len(response.data['results']), 50)
        
        # Test next page
        if response.data['next']:
            next_response = self.client.get(response.data['next'])
            self.assertEqual(next_response.status_code, status.HTTP_200_OK)
            self.assertGreater(len(next_response.data['results']), 0)

    def test_response_format_matches_serializer(self):
        """Test that response format matches BulkQueueRunSerializer output."""
        url = reverse('api:bulk-queue-run-list')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('results', response.data)
        
        if len(response.data['results']) > 0:
            result = response.data['results'][0]
            # Verify all expected fields are present
            self.assertIn('id', result)
            self.assertIn('requested_by', result)
            self.assertIn('total_stocks', result)
            self.assertIn('queued_count', result)
            self.assertIn('skipped_count', result)
            self.assertIn('error_count', result)
            self.assertIn('created_at', result)
            self.assertIn('started_at', result)
            self.assertIn('completed_at', result)


class BulkQueueRunStatsDetailAPITest(APITestCase):
    """Tests for the GET /api/bulk-queue-runs/<bulk_queue_run_id>/stats endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        # Clear cache before each test
        cache.clear()
        
        # Create a bulk queue run
        self.bulk_queue_run = BulkQueueRun.objects.create(
            requested_by='admin@example.com',
            total_stocks=20000,
            queued_count=19500,
            skipped_count=400,
            error_count=100
        )

    def tearDown(self):
        """Clean up after each test."""
        cache.clear()

    def test_get_stats_returns_200_with_correct_structure(self):
        """Test that GET returns 200 with correct data structure."""
        # Create some ingestion runs with various states
        stock1 = Stock.objects.create(ticker='AAPL')
        stock2 = Stock.objects.create(ticker='GOOGL')
        stock3 = Stock.objects.create(ticker='MSFT')
        
        StockIngestionRun.objects.create(
            stock=stock1,
            bulk_queue_run=self.bulk_queue_run,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        StockIngestionRun.objects.create(
            stock=stock2,
            bulk_queue_run=self.bulk_queue_run,
            state=IngestionState.FETCHING
        )
        StockIngestionRun.objects.create(
            stock=stock3,
            bulk_queue_run=self.bulk_queue_run,
            state=IngestionState.DONE
        )
        
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('id', response.data)
        self.assertIn('ingestion_run_stats', response.data)

    def test_response_includes_all_bulk_queue_run_fields(self):
        """Test that response includes all BulkQueueRun fields."""
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('id', response.data)
        self.assertIn('requested_by', response.data)
        self.assertIn('total_stocks', response.data)
        self.assertIn('queued_count', response.data)
        self.assertIn('skipped_count', response.data)
        self.assertIn('error_count', response.data)
        self.assertIn('created_at', response.data)
        self.assertIn('started_at', response.data)
        self.assertIn('completed_at', response.data)
        
        # Verify values match
        self.assertEqual(response.data['id'], str(self.bulk_queue_run.id))
        self.assertEqual(response.data['requested_by'], 'admin@example.com')
        self.assertEqual(response.data['total_stocks'], 20000)
        self.assertEqual(response.data['queued_count'], 19500)
        self.assertEqual(response.data['skipped_count'], 400)
        self.assertEqual(response.data['error_count'], 100)

    def test_ingestion_run_stats_includes_total_and_by_state(self):
        """Test that ingestion_run_stats includes total count and counts by state."""
        # Create ingestion runs with various states
        stocks = [Stock.objects.create(ticker=f'TICK{i}') for i in range(10)]
        
        # Create runs with different states
        states_to_create = [
            (IngestionState.QUEUED_FOR_FETCH, 3),
            (IngestionState.FETCHING, 2),
            (IngestionState.FETCHED, 2),
            (IngestionState.DONE, 2),
            (IngestionState.FAILED, 1),
        ]
        
        stock_idx = 0
        for state, count in states_to_create:
            for _ in range(count):
                StockIngestionRun.objects.create(
                    stock=stocks[stock_idx],
                    bulk_queue_run=self.bulk_queue_run,
                    state=state
                )
                stock_idx += 1
        
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('ingestion_run_stats', response.data)
        
        stats = response.data['ingestion_run_stats']
        self.assertIn('total', stats)
        self.assertIn('by_state', stats)
        
        # Verify total count
        self.assertEqual(stats['total'], 10)
        
        # Verify counts by state
        self.assertEqual(stats['by_state']['QUEUED_FOR_FETCH'], 3)
        self.assertEqual(stats['by_state']['FETCHING'], 2)
        self.assertEqual(stats['by_state']['FETCHED'], 2)
        self.assertEqual(stats['by_state']['DONE'], 2)
        self.assertEqual(stats['by_state']['FAILED'], 1)

    def test_counts_by_state_are_accurate(self):
        """Test that counts by state are accurate with various states."""
        # Create ingestion runs covering all states
        stocks = [Stock.objects.create(ticker=f'TICK{i}') for i in range(8)]
        all_states = [
            IngestionState.QUEUED_FOR_FETCH,
            IngestionState.FETCHING,
            IngestionState.FETCHED,
            IngestionState.QUEUED_FOR_DELTA,
            IngestionState.DELTA_RUNNING,
            IngestionState.DELTA_FINISHED,
            IngestionState.DONE,
            IngestionState.FAILED,
        ]
        
        for i, state in enumerate(all_states):
            StockIngestionRun.objects.create(
                stock=stocks[i],
                bulk_queue_run=self.bulk_queue_run,
                state=state
            )
        
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        stats = response.data['ingestion_run_stats']
        
        # Verify each state has count of 1
        for state in all_states:
            self.assertEqual(stats['by_state'][state], 1)
        
        # Verify total
        self.assertEqual(stats['total'], len(all_states))

    def test_caching_cache_miss_then_hit(self):
        """Test that first request misses cache and second request hits cache."""
        # Create some ingestion runs
        stock = Stock.objects.create(ticker='AAPL')
        StockIngestionRun.objects.create(
            stock=stock,
            bulk_queue_run=self.bulk_queue_run,
            state=IngestionState.DONE
        )
        
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        
        # First request - should miss cache
        cache_key = f'bulk_queue_run_stats:{self.bulk_queue_run.id}'
        self.assertIsNone(cache.get(cache_key))
        
        response1 = self.client.get(url)
        self.assertEqual(response1.status_code, status.HTTP_200_OK)
        
        # Verify data is now in cache
        cached_data = cache.get(cache_key)
        self.assertIsNotNone(cached_data)
        self.assertEqual(cached_data['id'], response1.data['id'])
        
        # Second request - should hit cache
        response2 = self.client.get(url)
        self.assertEqual(response2.status_code, status.HTTP_200_OK)
        self.assertEqual(response1.data, response2.data)

    def test_cache_key_format_is_correct(self):
        """Test that cache key format is correct."""
        stock = Stock.objects.create(ticker='AAPL')
        StockIngestionRun.objects.create(
            stock=stock,
            bulk_queue_run=self.bulk_queue_run,
            state=IngestionState.DONE
        )
        
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        
        # Make request to populate cache
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify cache key format
        expected_cache_key = f'bulk_queue_run_stats:{self.bulk_queue_run.id}'
        cached_data = cache.get(expected_cache_key)
        self.assertIsNotNone(cached_data)

    def test_cache_expiration_repopulates(self):
        """Test that cache expires and repopulates after TTL."""
        stock = Stock.objects.create(ticker='AAPL')
        run1 = StockIngestionRun.objects.create(
            stock=stock,
            bulk_queue_run=self.bulk_queue_run,
            state=IngestionState.DONE
        )
        
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        
        # First request - populate cache
        response1 = self.client.get(url)
        self.assertEqual(response1.status_code, status.HTTP_200_OK)
        
        cache_key = f'bulk_queue_run_stats:{self.bulk_queue_run.id}'
        self.assertIsNotNone(cache.get(cache_key))
        
        # Manually expire cache by deleting it
        cache.delete(cache_key)
        self.assertIsNone(cache.get(cache_key))
        
        # Create another ingestion run
        stock2 = Stock.objects.create(ticker='GOOGL')
        StockIngestionRun.objects.create(
            stock=stock2,
            bulk_queue_run=self.bulk_queue_run,
            state=IngestionState.FAILED
        )
        
        # Second request - should repopulate cache with new data
        response2 = self.client.get(url)
        self.assertEqual(response2.status_code, status.HTTP_200_OK)
        
        # Verify cache was repopulated
        cached_data = cache.get(cache_key)
        self.assertIsNotNone(cached_data)
        
        # Verify new data includes the new ingestion run
        # Total should be 2 now (was 1, now 2)
        self.assertEqual(response2.data['ingestion_run_stats']['total'], 2)

    def test_404_when_bulk_queue_run_not_found(self):
        """Test 404 response when BulkQueueRun doesn't exist."""
        non_existent_id = uuid.uuid4()
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(non_existent_id)
        })
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'BULK_QUEUE_RUN_NOT_FOUND')

    def test_400_when_invalid_uuid_format(self):
        """Test 400 response when bulk_queue_run_id is invalid UUID format."""
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': 'invalid-uuid'
        })
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'INVALID_UUID')

    def test_no_n_plus_1_queries(self):
        """Test that aggregation query is efficient (verify no N+1 queries)."""
        # Create multiple ingestion runs
        stocks = [Stock.objects.create(ticker=f'TICK{i}') for i in range(20)]
        for stock in stocks:
            StockIngestionRun.objects.create(
                stock=stock,
                bulk_queue_run=self.bulk_queue_run,
                state=IngestionState.DONE
            )
        
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        
        # Use assertNumQueries to verify query efficiency
        with self.assertNumQueries(2):  # One for BulkQueueRun, one for aggregation
            response = self.client.get(url)
            self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_empty_stats_when_no_ingestion_runs(self):
        """Test with BulkQueueRun that has no related IngestionRuns (empty stats)."""
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('ingestion_run_stats', response.data)
        
        stats = response.data['ingestion_run_stats']
        self.assertEqual(stats['total'], 0)
        
        # All states should be 0
        for state_value, _ in IngestionState.choices:
            self.assertEqual(stats['by_state'][state_value], 0)

    def test_performance_with_many_ingestion_runs(self):
        """Test with BulkQueueRun that has many IngestionRuns (performance test)."""
        # Create 100 ingestion runs (simulating large dataset)
        stocks = [Stock.objects.create(ticker=f'TICK{i}') for i in range(100)]
        for stock in stocks:
            StockIngestionRun.objects.create(
                stock=stock,
                bulk_queue_run=self.bulk_queue_run,
                state=IngestionState.DONE
            )
        
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        
        # Measure response time
        start_time = time.time()
        response = self.client.get(url)
        elapsed_time = time.time() - start_time
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['ingestion_run_stats']['total'], 100)
        
        # Verify it completes within reasonable time (should be < 1 second for 100 runs)
        self.assertLess(elapsed_time, 1.0)

    def test_all_ingestion_state_values_represented(self):
        """Test that all IngestionState values are represented in the counts."""
        # Create one ingestion run for each state
        stocks = [Stock.objects.create(ticker=f'TICK{i}') for i in range(len(IngestionState.choices))]
        
        for i, (state_value, _) in enumerate(IngestionState.choices):
            StockIngestionRun.objects.create(
                stock=stocks[i],
                bulk_queue_run=self.bulk_queue_run,
                state=state_value
            )
        
        url = reverse('api:bulk-queue-run-stats-detail', kwargs={
            'bulk_queue_run_id': str(self.bulk_queue_run.id)
        })
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        stats = response.data['ingestion_run_stats']
        
        # Verify all states are present in by_state
        for state_value, _ in IngestionState.choices:
            self.assertIn(state_value, stats['by_state'])
            self.assertEqual(stats['by_state'][state_value], 1)
        
        # Verify total matches number of states
        self.assertEqual(stats['total'], len(IngestionState.choices))


class StockDataAPITest(APITestCase):
    """Tests for the GET /api/data/all-data/<ticker> endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        # Create and authenticate user
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.client.force_authenticate(user=self.user)
        
        self.stock = Stock.objects.create(ticker='AAPL')
        self.test_json_data = b'{"ticker": "AAPL", "data": "test"}'
        self.test_json_data_str = '{"ticker": "AAPL", "data": "test"}'

    @patch('api.views.Minio')
    def test_get_stock_data_success(self, mock_minio_class):
        """Test successful retrieval of stock data."""
        # Create DONE run with raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        # Mock MinIO client
        mock_client = Mock()
        mock_response = Mock()
        mock_response.read.return_value = self.test_json_data
        mock_client.get_object.return_value = mock_response
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.content, self.test_json_data)
        self.assertEqual(response['content-type'], 'application/json')
        
        # Verify MinIO was called correctly
        mock_minio_class.assert_called_once()
        mock_client.get_object.assert_called_once_with('test-bucket', 'AAPL/123.json')
        mock_response.read.assert_called_once()
        # Response should be closed once in the finally block
        self.assertGreaterEqual(mock_response.close.call_count, 1)
        self.assertGreaterEqual(mock_response.release_conn.call_count, 1)

    def test_get_stock_data_stock_not_found(self):
        """Test stock not found (404)."""
        url = reverse('api:stock-data', kwargs={'ticker': 'NONEXISTENT'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'STOCK_NOT_FOUND')
        self.assertEqual(response.data['error']['details']['ticker'], 'NONEXISTENT')

    def test_get_stock_data_no_done_run(self):
        """Test no DONE run found (404)."""
        # Create stock but no DONE runs
        StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.FETCHING
        )
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'NO_DONE_RUN_FOUND')
        self.assertEqual(response.data['error']['details']['ticker'], 'AAPL')

    def test_get_stock_data_no_raw_data_uri(self):
        """Test DONE run exists but no raw_data_uri (404)."""
        # Create DONE run with raw_data_uri=None
        StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri=None
        )
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'NO_RAW_DATA_URI')
        self.assertEqual(response.data['error']['details']['ticker'], 'AAPL')

    def test_get_stock_data_empty_raw_data_uri(self):
        """Test DONE run exists but raw_data_uri is empty string (404)."""
        # Create DONE run with raw_data_uri=''
        StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri=''
        )
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'NO_RAW_DATA_URI')

    @patch('api.views.Minio')
    def test_get_stock_data_multiple_done_runs_returns_latest(self, mock_minio_class):
        """Test multiple DONE runs (returns latest)."""
        import time
        
        # Create multiple DONE runs with different created_at times
        run1 = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/old.json'
        )
        time.sleep(0.01)  # Ensure different timestamps
        
        run2 = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/latest.json'
        )
        
        # Mock MinIO client
        mock_client = Mock()
        mock_response = Mock()
        mock_response.read.return_value = self.test_json_data
        mock_client.get_object.return_value = mock_response
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Verify latest run's data is returned (by created_at)
        mock_client.get_object.assert_called_once_with('test-bucket', 'AAPL/latest.json')

    @patch('api.views.Minio')
    def test_get_stock_data_s3_file_not_found(self, mock_minio_class):
        """Test S3 file not found (404)."""
        from minio.error import S3Error
        
        # Create DONE run with valid raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        # Mock MinIO to raise S3Error with NoSuchKey
        # Create a real S3Error instance with a mock response
        mock_client = Mock()
        mock_response = Mock()
        s3_error = S3Error(
            response=mock_response,
            code='NoSuchKey',
            message='NoSuchKey',
            resource='resource',
            request_id='request_id',
            host_id='host_id',
            bucket_name='test-bucket',
            object_name='AAPL/123.json'
        )
        mock_client.get_object.side_effect = s3_error
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'DATA_FILE_NOT_FOUND')

    @patch('api.views.Minio')
    def test_get_stock_data_s3_authentication_error(self, mock_minio_class):
        """Test S3 authentication error (401)."""
        from minio.error import S3Error
        
        # Create DONE run with valid raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        # Mock MinIO to raise S3Error with InvalidAccessKeyId
        mock_client = Mock()
        mock_response = Mock()
        s3_error = S3Error(
            response=mock_response,
            code='InvalidAccessKeyId',
            message='InvalidAccessKeyId',
            resource='resource',
            request_id='request_id',
            host_id='host_id',
            bucket_name='test-bucket',
            object_name='AAPL/123.json'
        )
        mock_client.get_object.side_effect = s3_error
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'STORAGE_AUTHENTICATION_ERROR')

    @patch('api.views.Minio')
    def test_get_stock_data_s3_bucket_not_found(self, mock_minio_class):
        """Test S3 bucket not found (404)."""
        from minio.error import S3Error
        
        # Create DONE run with valid raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        # Mock MinIO to raise S3Error with NoSuchBucket
        mock_client = Mock()
        mock_response = Mock()
        s3_error = S3Error(
            response=mock_response,
            code='NoSuchBucket',
            message='NoSuchBucket',
            resource='resource',
            request_id='request_id',
            host_id='host_id',
            bucket_name='test-bucket',
            object_name='AAPL/123.json'
        )
        mock_client.get_object.side_effect = s3_error
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'STORAGE_BUCKET_NOT_FOUND')

    @patch('api.views.Minio')
    def test_get_stock_data_s3_connection_error(self, mock_minio_class):
        """Test S3 connection error (500)."""
        from minio.error import MinioException
        
        # Create DONE run with valid raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        # Mock MinIO to raise MinioException
        mock_client = Mock()
        mock_client.get_object.side_effect = MinioException('Connection failed')
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'STORAGE_CONNECTION_ERROR')

    @patch('api.views.Minio')
    def test_get_stock_data_invalid_json(self, mock_minio_class):
        """Test invalid JSON in file (500)."""
        # Create DONE run with valid raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        # Mock MinIO to return invalid JSON bytes
        mock_client = Mock()
        mock_response = Mock()
        mock_response.read.return_value = b'Invalid JSON {'
        mock_client.get_object.return_value = mock_response
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'INVALID_JSON_DATA')
        
        # Verify response was closed even on error
        self.assertGreaterEqual(mock_response.close.call_count, 1)
        self.assertGreaterEqual(mock_response.release_conn.call_count, 1)

    def test_get_stock_data_invalid_s3_uri_format(self):
        """Test invalid S3 URI format (500)."""
        # Create DONE run with invalid raw_data_uri (not s3:// format)
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='invalid-uri'
        )
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'INVALID_DATA_URI')

    def test_get_stock_data_invalid_s3_uri_malformed(self):
        """Test malformed S3 URI (500)."""
        # Create DONE run with malformed raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://bucket'  # Missing key part
        )
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'INVALID_DATA_URI')

    def test_get_stock_data_case_insensitive_ticker(self):
        """Test case-insensitive ticker lookup."""
        # Create stock with ticker 'AAPL'
        # Request data with 'aapl' (lowercase)
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        with patch('api.views.Minio') as mock_minio_class:
            mock_client = Mock()
            mock_response = Mock()
            mock_response.read.return_value = self.test_json_data
            mock_client.get_object.return_value = mock_response
            mock_minio_class.return_value = mock_client
            
            url = reverse('api:stock-data', kwargs={'ticker': 'aapl'})
            response = self.client.get(url)
            
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertEqual(response.content, self.test_json_data)

    @patch('api.views.Minio')
    def test_get_stock_data_s3_uri_parsing(self, mock_minio_class):
        """Test S3 URI parsing."""
        # Create DONE run with raw_data_uri='s3://bucket-name/path/to/file.json'
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://bucket-name/path/to/file.json'
        )
        
        # Mock MinIO client
        mock_client = Mock()
        mock_response = Mock()
        mock_response.read.return_value = self.test_json_data
        mock_client.get_object.return_value = mock_response
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Verify correct bucket and key are extracted and used
        mock_client.get_object.assert_called_once_with('bucket-name', 'path/to/file.json')

    @patch('api.views.Minio')
    def test_get_stock_data_response_content_matches_exactly(self, mock_minio_class):
        """Test response content matches file content exactly."""
        # Create DONE run with raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        # Mock MinIO to return specific JSON bytes
        specific_json = b'{"exact": "content", "no": "transformation"}'
        mock_client = Mock()
        mock_response = Mock()
        mock_response.read.return_value = specific_json
        mock_client.get_object.return_value = mock_response
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Verify response content is exactly the same bytes (no transformation)
        self.assertEqual(response.content, specific_json)

    @patch('api.views.Minio')
    def test_get_stock_data_minio_response_closed_on_success(self, mock_minio_class):
        """Test MinIO response is properly closed on success."""
        # Create DONE run with raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        # Mock MinIO client
        mock_client = Mock()
        mock_response = Mock()
        mock_response.read.return_value = self.test_json_data
        mock_client.get_object.return_value = mock_response
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Verify response.close() and response.release_conn() are called
        self.assertGreaterEqual(mock_response.close.call_count, 1)
        self.assertGreaterEqual(mock_response.release_conn.call_count, 1)

    @patch('api.views.Minio')
    def test_get_stock_data_minio_response_closed_on_error(self, mock_minio_class):
        """Test MinIO response is properly closed even on errors."""
        # Create DONE run with valid raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        # Mock MinIO client - get_object succeeds but read fails
        # Note: read() raising an exception is unusual, but we test cleanup happens
        mock_client = Mock()
        mock_response = Mock()
        # Create an exception that will be raised when read() is called
        read_exception = Exception("Read error")
        mock_response.read.side_effect = read_exception
        mock_client.get_object.return_value = mock_response
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        # Verify response was closed even on error
        self.assertGreaterEqual(mock_response.close.call_count, 1)
        self.assertGreaterEqual(mock_response.release_conn.call_count, 1)

    @patch('api.views.Minio')
    def test_get_stock_data_s3_error_other_codes(self, mock_minio_class):
        """Test S3Error with other error codes (500)."""
        from minio.error import S3Error
        
        # Create DONE run with valid raw_data_uri
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.DONE,
            raw_data_uri='s3://test-bucket/AAPL/123.json'
        )
        
        # Mock MinIO to raise S3Error with other code
        mock_client = Mock()
        mock_response = Mock()
        s3_error = S3Error(
            response=mock_response,
            code='InternalError',
            message='InternalError',
            resource='resource',
            request_id='request_id',
            host_id='host_id',
            bucket_name='test-bucket',
            object_name='AAPL/123.json'
        )
        mock_client.get_object.side_effect = s3_error
        mock_minio_class.return_value = mock_client
        
        url = reverse('api:stock-data', kwargs={'ticker': 'AAPL'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        self.assertIn('error', response.data)
        self.assertEqual(response.data['error']['code'], 'STORAGE_ERROR')
