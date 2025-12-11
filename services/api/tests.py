"""
Tests for Stock Ticker ETL Pipeline API.

This module contains comprehensive tests for:
- Models (Stock, StockIngestionRun)
- Service layer (StockIngestionService)
- API endpoints
"""

import threading
import uuid
from unittest.mock import patch

from django.db import transaction
from django.test import TestCase, TransactionTestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from api.models import IngestionState, Stock, StockIngestionRun
from api.services import StockIngestionService
from api.services.stock_ingestion_service import (
    IngestionRunNotFoundError,
    InvalidStateTransitionError,
    StockNotFoundError,
)


# =============================================================================
# Model Tests
# =============================================================================

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
        
        with self.assertRaises(Exception):
            Stock.objects.create(ticker='AAPL')

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
            IngestionState.QUEUED_FOR_SPARK,
            IngestionState.SPARK_RUNNING,
            IngestionState.SPARK_FINISHED,
        ]
        
        for state in active_states:
            run = StockIngestionRun.objects.create(
                stock=self.stock,
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


class StockIngestionRunManagerTest(TestCase):
    """Tests for the StockIngestionRunManager custom manager."""

    def setUp(self):
        """Set up test fixtures."""
        self.stock = Stock.objects.create(ticker='AAPL')

    def test_get_latest_for_stock(self):
        """Test getting the latest run for a stock."""
        # Create multiple runs
        older_run = StockIngestionRun.objects.create(
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


# =============================================================================
# Service Tests
# =============================================================================

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
        run, created = self.service.queue_for_fetch(ticker='NEWSTOCK')
        
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
            side_effect=Exception('Database error')
        ):
            with self.assertRaises(Exception):
                self.service.queue_for_fetch(ticker='NEWSTOCK')
        
        # Verify no run was created
        self.assertEqual(StockIngestionRun.objects.count(), initial_run_count)

    def test_update_run_state_concurrent_updates(self):
        """Test that concurrent state updates are handled correctly."""
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        results = []
        errors = []
        
        def update_state():
            try:
                service = StockIngestionService()
                updated = service.update_run_state(
                    run_id=run.id,
                    new_state=IngestionState.FETCHING
                )
                results.append(updated)
            except Exception as e:
                errors.append(e)
        
        # Run concurrent updates
        threads = [threading.Thread(target=update_state) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
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
            (IngestionState.QUEUED_FOR_SPARK, 'queued_for_spark_at'),
            (IngestionState.SPARK_RUNNING, 'spark_started_at'),
            (IngestionState.SPARK_FINISHED, 'spark_finished_at'),
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
            IngestionState.QUEUED_FOR_SPARK,
            IngestionState.SPARK_RUNNING,
            IngestionState.SPARK_FINISHED,
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


# =============================================================================
# API Tests
# =============================================================================

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

    def test_queue_new_stock(self):
        """Test queuing a new stock creates stock and run."""
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

    def test_queue_existing_stock_no_active_run(self):
        """Test queuing an existing stock with no active run."""
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

    def test_queue_returns_existing_active_run(self):
        """Test that queuing returns existing active run."""
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

    def test_queue_validates_ticker(self):
        """Test that ticker validation works."""
        response = self.client.post(
            self.url,
            {'ticker': ''},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_queue_normalizes_ticker_to_uppercase(self):
        """Test that tickers are normalized to uppercase."""
        response = self.client.post(
            self.url,
            {'ticker': 'aapl'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['ticker'], 'AAPL')

    def test_queue_with_all_optional_fields(self):
        """Test queuing with all optional fields provided."""
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

    def test_update_to_failed_requires_error_info(self):
        """Test that transitioning to FAILED requires error info."""
        response = self.client.patch(
            self.url,
            {'state': 'FAILED'},  # Missing error_code and error_message
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

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

    def test_update_invalid_uuid_format(self):
        """Test updating with an invalid UUID format."""
        url = reverse('api:update-run-state', kwargs={'run_id': 'invalid-uuid'})
        
        response = self.client.patch(
            url,
            {'state': 'FETCHING'},
            format='json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

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

    def test_get_run_invalid_uuid(self):
        """Test getting with an invalid UUID format."""
        url = reverse('api:run-detail', kwargs={'run_id': 'invalid-uuid'})
        
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
