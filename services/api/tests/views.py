"""
API Tests for Stock Ingestion ETL Pipeline.

This test module covers:
- Retrieving stock status for existing and non-existent stocks via the API
- Handling stocks with and without ingestion runs
- Validating responses for stock and run detail endpoints
- Ensuring proper error handling for missing resources and invalid UUIDs
"""

import uuid
from unittest.mock import Mock, patch

from celery.exceptions import OperationalError as CeleryOperationalError
from django.urls import reverse
from django.db import IntegrityError
from rest_framework import status
from rest_framework.test import APITestCase

from api.models import BulkQueueRun, IngestionState, Stock, StockIngestionRun


class StockStatusAPITest(APITestCase):
    """Tests for the GET /api/ticker/<ticker>/status endpoint."""

    def setUp(self):
        """Set up test fixtures."""
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
