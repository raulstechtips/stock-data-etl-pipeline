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

from api.models import IngestionState, Stock, StockIngestionRun


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


class UpdateRunStateAPITest(APITestCase):
    """Tests for the PATCH /api/runs/<run_id>/state endpoint."""

    def setUp(self):
        """Set up test fixtures."""
        self.stock = Stock.objects.create(ticker='AAPL')
        self.run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        self.url = reverse('api:update-run-state', kwargs={'run_id': str(self.run.id)})

    def test_update_valid_state_transition(self):
        """Test updating with a valid state transition."""
        response = self.client.patch(
            self.url,
            {'state': 'FETCHING'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['state'], 'FETCHING')

    def test_update_invalid_state_transition(self):
        """Test updating with an invalid state transition."""
        response = self.client.patch(
            self.url,
            {'state': 'DONE'},  # Invalid: QUEUED_FOR_FETCH -> DONE
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'INVALID_STATE_TRANSITION')
        self.assertIn('details', response.data['error'])

    def test_update_to_failed_requires_error_info(self):
        """Test that transitioning to FAILED requires error info."""
        response = self.client.patch(
            self.url,
            {'state': 'FAILED'},  # Missing error_code and error_message
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'VALIDATION_ERROR')
        self.assertIn('details', response.data['error'])

    def test_update_to_failed_with_error_info(self):
        """Test transitioning to FAILED with proper error info."""
        response = self.client.patch(
            self.url,
            {
                'state': 'FAILED',
                'error_code': 'NETWORK_ERROR',
                'error_message': 'Connection timed out'
            },
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['state'], 'FAILED')
        self.assertEqual(response.data['error_code'], 'NETWORK_ERROR')

    def test_update_run_not_found(self):
        """Test updating a non-existent run."""
        fake_id = uuid.uuid4()
        url = reverse('api:update-run-state', kwargs={'run_id': str(fake_id)})
        
        response = self.client.patch(
            url,
            {'state': 'FETCHING'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'RUN_NOT_FOUND')
        self.assertIn('details', response.data['error'])
        self.assertEqual(response.data['error']['details']['run_id'], str(fake_id))

    def test_update_invalid_uuid_format(self):
        """Test updating with an invalid UUID format."""
        url = reverse('api:update-run-state', kwargs={'run_id': 'invalid-uuid'})
        
        response = self.client.patch(
            url,
            {'state': 'FETCHING'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('error', response.data)
        self.assertIn('message', response.data['error'])
        self.assertIn('code', response.data['error'])
        self.assertEqual(response.data['error']['code'], 'INVALID_UUID')
        self.assertIn('details', response.data['error'])
        self.assertEqual(response.data['error']['details']['run_id'], 'invalid-uuid')

    def test_update_with_data_uris(self):
        """Test updating with data URIs."""
        response = self.client.patch(
            self.url,
            {
                'state': 'FETCHING',
                'raw_data_uri': 's3://bucket/raw/AAPL'
            },
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['raw_data_uri'], 's3://bucket/raw/AAPL')


class RunDetailAPITest(APITestCase):
    """Tests for the GET /api/runs/<run_id> endpoint."""

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
