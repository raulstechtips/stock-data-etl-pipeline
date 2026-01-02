"""
API Views for Stock Ticker ETL Pipeline.

This module contains the API views for:
- GET /tickers - List all stocks
- GET /ticker/<ticker>/detail - Get stock details for a specific stock
- GET /ticker/<ticker>/status - Get the current status of a stock
- POST /ticker/queue - Queue a stock for ingestion
- GET /runs - List all ingestion runs
- GET /runs/ticker/<ticker> - List runs for a specific ticker
- GET /run/<run_id>/detail - Get details of a specific run
- GET /bulk-queue-runs - List all bulk queue runs
- GET /data/all-data/<ticker> - Get latest raw stock data JSON for a ticker
"""

import json
import logging
import time
from urllib.parse import urlparse
from uuid import UUID

from django.conf import settings
from django.core.cache import cache
from django.db import IntegrityError
from django.http import HttpResponse
from celery.exceptions import CeleryError, OperationalError as CeleryOperationalError
from django_filters.rest_framework import DjangoFilterBackend
from minio import Minio
from minio.error import MinioException, S3Error
from rest_framework import status
from rest_framework.generics import ListAPIView
from rest_framework.pagination import CursorPagination
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from api.filters import BulkQueueRunFilter, StockFilter, StockIngestionRunFilter
from api.models import BulkQueueRun, Exchange, IngestionState, Stock, StockIngestionRun
from api.serializers import (
    BulkQueueRunSerializer,
    BulkQueueRunStatsSerializer,
    QueueAllStocksRequestSerializer,
    QueueForFetchRequestSerializer,
    StockIngestionRunSerializer,
    StockSerializer,
    StockStatusResponseSerializer,
)
from api.services import StockIngestionService
from api.services.stock_ingestion_service import (
    IngestionRunNotFoundError,
    StockNotFoundError,
)
from workers.tasks.queue_for_fetch import fetch_stock_data


logger = logging.getLogger(__name__)


class StandardCursorPagination(CursorPagination):
    """Standard cursor pagination configuration for list views."""
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 100
    ordering = '-created_at'


class TickerListView(ListAPIView):
    """
    API endpoint for listing all stocks.
    
    GET /tickers
    
    Returns a paginated list of all stocks with cursor-based pagination.
    Supports filtering by ticker, sector, exchange, and country.
    """
    serializer_class = StockSerializer
    pagination_class = StandardCursorPagination
    filter_backends = [DjangoFilterBackend]
    filterset_class = StockFilter
    queryset = Stock.objects.all().order_by('-created_at')


class TickerDetailView(APIView):
    """
    API endpoint for getting details of a specific stock.
    
    GET /ticker/<ticker>/detail
    
    Returns detailed information about a stock.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = StockIngestionService()

    def get(self, request: Request, ticker: str) -> Response:
        """
        Get details of a specific stock.
        
        Args:
            request: DRF Request object
            ticker: Stock ticker symbol from URL path
            
        Returns:
            Response with stock details or 404 if not found
        """
        try:
            stock = Stock.objects.get(ticker=ticker.upper())
            serializer = StockSerializer(stock)
            logger.info(
                "Stock details retrieved successfully",
                extra={'ticker': ticker.upper(), 'stock_id': str(stock.id)}
            )
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except Stock.DoesNotExist:
            logger.warning(
                "Stock not found",
                extra={'ticker': ticker.upper()}
            )
            return Response(
                {
                    'error': {
                        'message': f"Stock with ticker '{ticker.upper()}' not found",
                        'code': 'STOCK_NOT_FOUND',
                        'details': {'ticker': ticker.upper()}
                    }
                },
                status=status.HTTP_404_NOT_FOUND
            )


class StockStatusView(APIView):
    """
    API endpoint for getting the current status of a stock.
    
    GET /ticker/<ticker>/status
    
    Returns the stock's latest ingestion run status or 404 if not found.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = StockIngestionService()

    def get(self, request: Request, ticker: str) -> Response:
        """
        Get the current status of a stock's latest ingestion run.
        
        Args:
            request: DRF Request object
            ticker: Stock ticker symbol from URL path
            
        Returns:
            Response with stock status or 404 if not found
        """
        try:
            result = self.service.get_stock_status(ticker)
            serializer = StockStatusResponseSerializer(result)
            logger.info(
                "Stock status retrieved successfully",
                extra={'ticker': ticker.upper(), 'state': result.state, 'run_id': str(result.run_id) if result.run_id else None}
            )
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except StockNotFoundError as e:
            logger.warning(
                "Stock not found",
                extra={'ticker': ticker.upper(), 'error': str(e)}
            )
            return Response(
                {
                    'error': {
                        'message': str(e),
                        'code': 'STOCK_NOT_FOUND',
                        'details': {'ticker': ticker.upper()}
                    }
                },
                status=status.HTTP_404_NOT_FOUND
            )


class QueueForFetchView(APIView):
    """
    API endpoint for queuing a stock for ingestion.
    
    POST /ticker/queue
    
    Creates the stock if it doesn't exist, then either:
    - Returns the existing active run if one exists
    - Creates a new run in QUEUED_FOR_FETCH state
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = StockIngestionService()

    def post(self, request: Request) -> Response:
        """
        Queue a stock for fetching.
        
        Request body:
            {
                "ticker": "AAPL",
                "requested_by": "user@example.com",  // optional
                "request_id": "unique-request-123"   // optional
            }
        
        Returns:
            - 200: If an active run already exists (returns existing run)
            - 201: If a new run was created and queued successfully
            - 400: If the request is invalid
            - 500: If failed to queue the task to message broker
        """
        serializer = QueueForFetchRequestSerializer(data=request.data)
        
        if not serializer.is_valid():
            logger.warning(
                "Queue for fetch validation failed",
                extra={'errors': serializer.errors}
            )
            return Response(
                {
                    'error': {
                        'message': 'Validation failed',
                        'code': 'VALIDATION_ERROR',
                        'details': serializer.errors
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        validated_data = serializer.validated_data
        ticker = validated_data['ticker']
        requested_by = validated_data.get('requested_by')
        request_id = validated_data.get('request_id')
        
        try:
            run, created = self.service.queue_for_fetch(
                ticker=ticker,
                requested_by=requested_by,
                request_id=request_id,
            )
        except IntegrityError:
            # Race condition: Another request created a run between our check and create
            logger.warning(
                "Race condition detected while queuing stock for fetch",
                extra={
                    'ticker': ticker.upper(),
                    'requested_by': requested_by,
                    'request_id': request_id,
                }
            )
            return Response(
                {
                    'error': {
                        'message': 'An ingestion run for this stock was created by another request. Please try again.',
                        'code': 'RACE_CONDITION',
                        'details': {'ticker': ticker.upper()}
                    }
                },
                status=status.HTTP_409_CONFLICT
            )
        
        # If a new run was created, trigger the Celery task
        if created:
            try:
                # Send task to message broker
                # Note: We call this AFTER the transaction commits to ensure
                # the run exists in the database before the worker processes it
                task_result = fetch_stock_data.delay(
                    run_id=str(run.id),
                    ticker=ticker
                )

                logger.info(
                    "Stock queued for fetch successfully - new run created",
                    extra={
                        'ticker': ticker.upper(),
                        'run_id': str(run.id),
                        'task_id': task_result.id,
                        'requested_by': requested_by,
                        'request_id': request_id
                    }
                )
                
                response_serializer = StockIngestionRunSerializer(run)
                return Response(
                    response_serializer.data,
                    status=status.HTTP_201_CREATED
                )
                
            except (CeleryError, CeleryOperationalError) as e:
                # Failed to send task to message broker
                logger.exception(
                    "Failed to queue fetch task for run",
                    extra={
                        'run_id': str(run.id),
                        'ticker': ticker.upper(),
                        'requested_by': requested_by,
                        'request_id': request_id
                    }
                )
                
                # Transition the run to FAILED state since we can't process it
                try:
                    self.service.update_run_state(
                        run_id=run.id,
                        new_state=IngestionState.FAILED,
                        error_code='BROKER_ERROR',
                        error_message=f'Failed to queue task to message broker: {str(e)}'
                    )
                except Exception as state_error:
                    logger.exception(
                        "Failed to transition run to FAILED after broker error",
                        extra={
                            'run_id': str(run.id),
                            'ticker': ticker.upper(),
                            'requested_by': requested_by,
                            'request_id': request_id,
                        }
                    )
                
                return Response(
                    {
                        'message': 'Failed to queue task to message broker',
                        'code': 'BROKER_ERROR',
                        'details': {'run_id': str(run.id)}
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        else:
            # Active run already exists, don't trigger a new task
            logger.info(
                "Stock already queued - returning existing run",
                extra={
                    'ticker': ticker.upper(),
                    'run_id': str(run.id),
                    'run_state': run.state,
                    'requested_by': requested_by
                }
            )
            
            response_serializer = StockIngestionRunSerializer(run)

            return Response(
                response_serializer.data,
                status=status.HTTP_200_OK
            )


class RunListView(ListAPIView):
    """
    API endpoint for listing all ingestion runs.
    
    GET /runs
    
    Returns a paginated list of all ingestion runs with cursor-based pagination.
    Supports filtering by run_id, ticker, state, requested_by, date ranges, and terminal/in-progress status.
    """
    serializer_class = StockIngestionRunSerializer
    pagination_class = StandardCursorPagination
    filter_backends = [DjangoFilterBackend]
    filterset_class = StockIngestionRunFilter
    queryset = StockIngestionRun.objects.select_related('stock').all().order_by('-created_at')


class TickerRunsListView(ListAPIView):
    """
    API endpoint for listing ingestion runs for a specific ticker.
    
    GET /runs/ticker/<ticker>
    
    Returns a paginated list of runs for the specified ticker.
    Supports additional filtering by run_id, state, requested_by, date ranges, and terminal/in-progress status.
    """
    serializer_class = StockIngestionRunSerializer
    pagination_class = StandardCursorPagination
    filter_backends = [DjangoFilterBackend]
    filterset_class = StockIngestionRunFilter

    def get_queryset(self):
        """Get runs filtered by ticker from URL parameter."""
        ticker = self.kwargs['ticker'].upper()
        return (
            StockIngestionRun.objects
            .select_related('stock')
            .filter(stock__ticker=ticker)
            .order_by('-created_at')
        )
    
    def list(self, request: Request, *args, **kwargs) -> Response:
        """
        List runs for a ticker, returning 404 if ticker doesn't exist.
        
        Args:
            request: DRF Request object
            
        Returns:
            Response with paginated list of runs or 404 if ticker not found
        """
        ticker = self.kwargs['ticker'].upper()
        
        # Check if stock exists
        if not Stock.objects.filter(ticker=ticker).exists():
            logger.warning(
                "Stock not found for runs list",
                extra={'ticker': ticker}
            )
            return Response(
                {
                    'error': {
                        'message': f"Stock with ticker '{ticker}' not found",
                        'code': 'STOCK_NOT_FOUND',
                        'details': {'ticker': ticker}
                    }
                },
                status=status.HTTP_404_NOT_FOUND
            )
        
        logger.info(
            "Listing runs for ticker",
            extra={'ticker': ticker}
        )
        return super().list(request, *args, **kwargs)


class RunDetailView(APIView):
    """
    API endpoint for getting details of a specific ingestion run.
    
    GET /run/<run_id>/detail
    
    Returns detailed information about an ingestion run.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.service = StockIngestionService()

    def get(self, request: Request, run_id: str) -> Response:
        """
        Get details of a specific ingestion run.
        
        Args:
            request: DRF Request object
            run_id: UUID of the ingestion run
            
        Returns:
            Response with run details or 404 if not found
        """
        # Validate UUID format
        try:
            run_uuid = UUID(run_id)
        except ValueError as e:
            logger.warning(
                "Invalid UUID format in run detail request",
                extra={'run_id': run_id, 'error': str(e)}
            )
            return Response(
                {
                    'error': {
                        'message': f"Invalid run ID format: '{run_id}'",
                        'code': 'INVALID_UUID',
                        'details': {'run_id': run_id}
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            run = self.service.get_run_by_id(run_uuid)
            serializer = StockIngestionRunSerializer(run)
            logger.info(
                "Run details retrieved successfully",
                extra={
                    'run_id': str(run_uuid),
                    'ticker': run.stock.ticker,
                    'state': run.state
                }
            )
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except IngestionRunNotFoundError as e:
            logger.warning(
                "Ingestion run not found",
                extra={'run_id': str(run_uuid), 'error': str(e)}
            )
            return Response(
                {
                    'error': {
                        'message': str(e),
                        'code': 'RUN_NOT_FOUND',
                        'details': {'run_id': str(run_uuid)}
                    }
                },
                status=status.HTTP_404_NOT_FOUND
            )


class BulkQueueRunStatsDetailView(APIView):
    """
    API endpoint for getting aggregated statistics for a specific bulk queue run.
    
    GET /bulk-queue-runs/<bulk_queue_run_id>/stats
    
    Returns detailed information about a bulk queue run including aggregated
    statistics from all related StockIngestionRun objects, grouped by state.
    
    This is an expensive operation as it processes ~20k IngestionRuns per call.
    To optimize performance, responses are cached for 5 minutes (300 seconds).
    The cache key format is 'bulk_queue_run_stats:{bulk_queue_run_id}'.
    
    Caching Strategy:
    - First request: Cache miss - performs expensive aggregation query, stores
      result in cache with 5-minute TTL, returns data
    - Subsequent requests (within 5 minutes): Cache hit - returns cached data
      immediately without database queries
    - After 5 minutes: Cache expires, next request repopulates cache
    
    The view uses efficient database aggregation queries to minimize
    database load. Aggregation is performed at the database level rather than
    loading all objects into memory.
    
    Performance Considerations:
    - Cached responses: <100ms
    - Uncached responses: Completes within reasonable time for 20k+ IngestionRuns
    - Uses database aggregations to minimize query overhead
    """
    cache_ttl = 300  # 5 minutes in seconds

    def get(self, request: Request, bulk_queue_run_id: str) -> Response:
        """
        Get aggregated statistics for a specific bulk queue run.
        
        Args:
            request: DRF Request object
            bulk_queue_run_id: UUID string of the bulk queue run from URL path
            
        Returns:
            Response with bulk queue run details and aggregated ingestion run
            statistics, or 404 if not found, or 400 if UUID format is invalid
        """
        # Validate UUID format
        try:
            bulk_queue_run_uuid = UUID(bulk_queue_run_id)
        except ValueError as e:
            logger.warning(
                "Invalid UUID format in bulk queue run stats request",
                extra={'bulk_queue_run_id': bulk_queue_run_id, 'error': str(e)}
            )
            return Response(
                {
                    'error': {
                        'message': f"Invalid bulk queue run ID format: '{bulk_queue_run_id}'",
                        'code': 'INVALID_UUID',
                        'details': {'bulk_queue_run_id': bulk_queue_run_id}
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        start_time = time.time()
        cache_key = f'bulk_queue_run_stats:{bulk_queue_run_uuid}'
        
        # Check cache first
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            elapsed_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            logger.info(
                "Bulk queue run stats retrieved from cache",
                extra={
                    'bulk_queue_run_id': str(bulk_queue_run_uuid),
                    'cache_key': cache_key,
                    'elapsed_ms': round(elapsed_time, 2)
                }
            )
            return Response(cached_data, status=status.HTTP_200_OK)
        
        # Cache miss - perform expensive aggregation
        logger.info(
            "Bulk queue run stats cache miss - performing aggregation",
            extra={'bulk_queue_run_id': str(bulk_queue_run_uuid), 'cache_key': cache_key}
        )
        
        try:
            # Fetch BulkQueueRun
            # Note: We don't use prefetch_related here because the serializer
            # performs database aggregation which is more efficient than loading
            # all objects into memory
            bulk_queue_run = BulkQueueRun.objects.get(id=bulk_queue_run_uuid)
        except BulkQueueRun.DoesNotExist:
            logger.warning(
                "Bulk queue run not found",
                extra={'bulk_queue_run_id': str(bulk_queue_run_uuid)}
            )
            return Response(
                {
                    'error': {
                        'message': f"Bulk queue run with ID '{bulk_queue_run_uuid}' not found",
                        'code': 'BULK_QUEUE_RUN_NOT_FOUND',
                        'details': {'bulk_queue_run_id': str(bulk_queue_run_uuid)}
                    }
                },
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Serialize the data (this triggers the aggregation in the serializer)
        serializer = BulkQueueRunStatsSerializer(bulk_queue_run)
        serialized_data = serializer.data
        
        # Store in cache with 5-minute TTL
        cache.set(cache_key, serialized_data, self.cache_ttl)
        
        elapsed_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        logger.info(
            "Bulk queue run stats aggregated and cached",
            extra={
                'bulk_queue_run_id': str(bulk_queue_run_uuid),
                'cache_key': cache_key,
                'cache_ttl_seconds': self.cache_ttl,
                'elapsed_ms': round(elapsed_time, 2),
                'total_ingestion_runs': serialized_data.get('ingestion_run_stats', {}).get('total', 0)
            }
        )
        
        return Response(serialized_data, status=status.HTTP_200_OK)


class QueueAllStocksForFetchView(APIView):
    """
    API endpoint for queuing all stocks for ingestion via background worker.
    
    POST /ticker/queue/all
    
    Creates a BulkQueueRun to track statistics and queues a background worker
    task to process all stocks asynchronously. Supports optional exchange
    filtering to queue only stocks from a specific exchange. Returns immediately
    with task information (202 Accepted).
    """

    def post(self, request: Request) -> Response:
        """
        Queue all stocks for fetching via background worker.
        
        Request body:
            {
                "requested_by": "admin@example.com",  // optional
                "exchange": "NASDAQ"  // optional - filter by exchange
            }
        
        Returns:
            - 202: Task queued successfully (asynchronous processing)
            - 400: If the request is invalid
            - 404: If the specified exchange does not exist
            - 500: If failed to queue the task to message broker
        """
        serializer = QueueAllStocksRequestSerializer(data=request.data)
        
        if not serializer.is_valid():
            logger.warning(
                "Queue all stocks validation failed",
                extra={'errors': serializer.errors}
            )
            return Response(
                {
                    'error': {
                        'message': 'Validation failed',
                        'code': 'VALIDATION_ERROR',
                        'details': serializer.errors
                    }
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        validated_data = serializer.validated_data
        requested_by = validated_data.get('requested_by')
        exchange_param = validated_data.get('exchange')
        
        # Handle exchange filtering if provided
        exchange_instance = None
        exchange_name = None
        stocks_queryset = Stock.objects.all()
        
        if exchange_param:
            # Normalize exchange name (strip and uppercase)
            exchange_name = exchange_param.strip().upper()
            
            # Get or create the Exchange instance
            try:
                exchange_instance, created = Exchange.objects.get_or_create(name=exchange_name)
                if created:
                    logger.info(
                        "Created new Exchange during queue all stocks operation",
                        extra={'exchange_name': exchange_name}
                    )
            except Exception as e:
                logger.exception(
                    "Failed to get or create Exchange",
                    extra={'exchange_name': exchange_name, 'error': str(e)}
                )
                return Response(
                    {
                        'error': {
                            'message': f'Failed to process exchange: {str(e)}',
                            'code': 'EXCHANGE_ERROR',
                            'details': {'exchange': exchange_name}
                        }
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
            # Filter stocks by exchange
            stocks_queryset = stocks_queryset.filter(exchange=exchange_instance)
            
            logger.info(
                "Filtering stocks by exchange for queue all stocks operation",
                extra={
                    'exchange_name': exchange_name,
                    'exchange_id': str(exchange_instance.id)
                }
            )
        
        # Get total stocks count (filtered or all)
        total_stocks = stocks_queryset.count()
        
        # Create BulkQueueRun to track statistics
        bulk_run = BulkQueueRun.objects.create(
            requested_by=requested_by,
            total_stocks=total_stocks,
        )
        
        logger.info(
            "Created BulkQueueRun for queue all stocks operation",
            extra={
                'bulk_queue_run_id': str(bulk_run.id),
                'total_stocks': total_stocks,
                'requested_by': requested_by,
                'exchange_name': exchange_name
            }
        )
        
        # Queue the background worker task
        try:
            # Import here to avoid circular dependency
            from workers.tasks import queue_all_stocks_for_fetch
            
            task_result = queue_all_stocks_for_fetch.delay(
                bulk_queue_run_id=str(bulk_run.id),
                exchange_name=exchange_name
            )
            
            logger.info(
                "Background worker task queued successfully for bulk queue operation",
                extra={
                    'bulk_queue_run_id': str(bulk_run.id),
                    'task_id': task_result.id,
                    'total_stocks': total_stocks,
                    'requested_by': requested_by,
                    'exchange_name': exchange_name
                }
            )
            
            response_serializer = BulkQueueRunSerializer(bulk_run)
            message = f'Bulk queue operation started. Processing {total_stocks} stocks asynchronously.'
            if exchange_name:
                message = f'Bulk queue operation started for exchange {exchange_name}. Processing {total_stocks} stocks asynchronously.'
            
            return Response(
                {
                    'bulk_queue_run': response_serializer.data,
                    'task_id': task_result.id,
                    'message': message,
                    'exchange': exchange_name
                },
                status=status.HTTP_202_ACCEPTED
            )
            
        except (CeleryError, CeleryOperationalError) as e:
            logger.exception(
                "Failed to queue background worker task for bulk queue operation",
                extra={
                    'bulk_queue_run_id': str(bulk_run.id),
                    'total_stocks': total_stocks,
                    'requested_by': requested_by,
                    'exchange_name': exchange_name
                }
            )
            
            return Response(
                {
                    'error': {
                        'message': 'Failed to queue background worker task',
                        'code': 'BROKER_ERROR',
                        'details': {
                            'bulk_queue_run_id': str(bulk_run.id),
                            'error': str(e)
                        }
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class BulkQueueRunListView(ListAPIView):
    """
    API endpoint for listing all bulk queue runs.
    
    GET /bulk-queue-runs
    
    Returns a paginated list of all bulk queue runs with cursor-based pagination.
    Supports filtering by requested_by, date ranges (created_at, started_at, completed_at),
    completion status, and error presence.
    
    Filtering capabilities:
    - requested_by: Exact requester match (case-insensitive)
    - requested_by__icontains: Requester contains (case-insensitive)
    - created_after/created_before: Filter by creation date range
    - started_at_after/started_at_before: Filter by start date range
    - completed_at_after/completed_at_before: Filter by completion date range
    - is_completed: Filter by completion status (true = completed, false = incomplete)
    - has_errors: Filter by error presence (true = has errors, false = no errors)
    
    Filters can be combined for precise queries. All filters work seamlessly with
    cursor-based pagination.
    
    Example requests:
        GET /api/bulk-queue-runs
        GET /api/bulk-queue-runs?requested_by=admin@example.com
        GET /api/bulk-queue-runs?is_completed=true&has_errors=false
        GET /api/bulk-queue-runs?created_after=2025-01-01T00:00:00Z&is_completed=true
        GET /api/bulk-queue-runs?requested_by__icontains=admin&has_errors=true
    """
    serializer_class = BulkQueueRunSerializer
    pagination_class = StandardCursorPagination
    filter_backends = [DjangoFilterBackend]
    filterset_class = BulkQueueRunFilter
    queryset = BulkQueueRun.objects.all().order_by('-created_at')


class StockDataView(APIView):
    """
    API endpoint for getting the latest raw stock data JSON for a specific ticker.
    
    GET /data/all-data/<ticker>
    
    Returns the latest DONE state ingestion run's raw data JSON exactly as stored in S3/MinIO.
    The response is the raw JSON bytes with no transformation or parsing.
    """
    
    permission_classes = [AllowAny]

    def get(self, request: Request, ticker: str) -> HttpResponse:
        """
        Get the latest raw stock data JSON for a specific ticker.
        
        Args:
            request: DRF Request object
            ticker: Stock ticker symbol from URL path (case-insensitive)
            
        Returns:
            HttpResponse with raw JSON bytes (content_type='application/json') or error response
            
        Error Codes:
            - STOCK_NOT_FOUND: Stock with ticker does not exist
            - NO_DONE_RUN_FOUND: No DONE state run exists for the stock
            - NO_RAW_DATA_URI: DONE run exists but raw_data_uri is None or empty
            - INVALID_DATA_URI: S3 URI format is invalid (not s3:// format)
            - DATA_FILE_NOT_FOUND: JSON file not found in S3 (NoSuchKey)
            - STORAGE_AUTHENTICATION_ERROR: S3/MinIO authentication failed
            - STORAGE_BUCKET_NOT_FOUND: S3 bucket not found
            - STORAGE_ERROR: Other S3/MinIO errors
            - STORAGE_CONNECTION_ERROR: MinIO connection error
            - INVALID_JSON_DATA: File contains invalid JSON
            - INTERNAL_SERVER_ERROR: Unexpected errors
        """
        normalized_ticker = ticker.upper()
        minio_response = None
        
        try:
            # Look up stock (case-insensitive)
            try:
                stock = Stock.objects.get(ticker=normalized_ticker)
            except Stock.DoesNotExist:
                logger.warning(
                    "Stock not found for data request",
                    extra={'ticker': normalized_ticker}
                )
                return Response(
                    {
                        'error': {
                            'code': 'STOCK_NOT_FOUND',
                            'message': f"Stock with ticker '{normalized_ticker}' not found",
                            'details': {'ticker': normalized_ticker}
                        }
                    },
                    status=status.HTTP_404_NOT_FOUND
                )
            
            # Query for latest DONE run
            done_run = (
                StockIngestionRun.objects
                .filter(stock=stock, state=IngestionState.DONE)
                .order_by('-created_at')
                .first()
            )
            
            if not done_run:
                logger.warning(
                    "No DONE run found for stock",
                    extra={'ticker': normalized_ticker, 'stock_id': str(stock.id)}
                )
                return Response(
                    {
                        'error': {
                            'code': 'NO_DONE_RUN_FOUND',
                            'message': f"No DONE state ingestion run found for ticker '{normalized_ticker}'",
                            'details': {'ticker': normalized_ticker}
                        }
                    },
                    status=status.HTTP_404_NOT_FOUND
                )
            
            # Check raw_data_uri exists
            if not done_run.raw_data_uri:
                logger.warning(
                    "DONE run exists but raw_data_uri is missing",
                    extra={
                        'ticker': normalized_ticker,
                        'run_id': str(done_run.id),
                        'stock_id': str(stock.id)
                    }
                )
                return Response(
                    {
                        'error': {
                            'code': 'NO_RAW_DATA_URI',
                            'message': f"DONE run exists but raw_data_uri is not set for ticker '{normalized_ticker}'",
                            'details': {
                                'ticker': normalized_ticker,
                                'run_id': str(done_run.id)
                            }
                        }
                    },
                    status=status.HTTP_404_NOT_FOUND
                )
            
            raw_data_uri = done_run.raw_data_uri
            
            # Parse S3 URI
            if not raw_data_uri.startswith('s3://'):
                logger.error(
                    "Invalid S3 URI format",
                    extra={
                        'ticker': normalized_ticker,
                        'run_id': str(done_run.id),
                        'raw_data_uri': raw_data_uri
                    }
                )
                return Response(
                    {
                        'error': {
                            'code': 'INVALID_DATA_URI',
                            'message': f"Invalid S3 URI format: {raw_data_uri}",
                            'details': {
                                'ticker': normalized_ticker,
                                'run_id': str(done_run.id),
                                'raw_data_uri': raw_data_uri
                            }
                        }
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
            # Extract bucket and key from URI
            uri_parts = raw_data_uri[5:].split('/', 1)
            if len(uri_parts) != 2:
                logger.error(
                    "Invalid S3 URI format - cannot parse bucket/key",
                    extra={
                        'ticker': normalized_ticker,
                        'run_id': str(done_run.id),
                        'raw_data_uri': raw_data_uri
                    }
                )
                return Response(
                    {
                        'error': {
                            'code': 'INVALID_DATA_URI',
                            'message': f"Invalid S3 URI format: {raw_data_uri}",
                            'details': {
                                'ticker': normalized_ticker,
                                'run_id': str(done_run.id),
                                'raw_data_uri': raw_data_uri
                            }
                        }
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
            bucket_name, object_key = uri_parts
            
            # Initialize MinIO client using base S3 credentials
            parsed = urlparse(settings.AWS_S3_ENDPOINT_URL)
            endpoint = parsed.netloc or parsed.path
            secure = parsed.scheme == 'https'
            
            client = Minio(
                endpoint=endpoint,
                access_key=settings.AWS_ACCESS_KEY_ID,
                secret_key=settings.AWS_SECRET_ACCESS_KEY,
                secure=secure
            )
            
            # Fetch JSON file from S3/MinIO
            minio_response = None
            try:
                minio_response = client.get_object(bucket_name, object_key)
                
                # Read raw JSON bytes
                json_bytes = minio_response.read()
                
                # Validate it's valid JSON by attempting to parse
                try:
                    json.loads(json_bytes)
                except json.JSONDecodeError as e:
                    logger.exception(
                        "Invalid JSON data in file",
                        extra={
                            'ticker': normalized_ticker,
                            'run_id': str(done_run.id),
                            'bucket': bucket_name,
                            'key': object_key
                        }
                    )
                    return Response(
                        {
                            'error': {
                                'code': 'INVALID_JSON_DATA',
                                'message': f"File contains invalid JSON data: {str(e)}",
                                'details': {
                                    'ticker': normalized_ticker,
                                    'run_id': str(done_run.id),
                                    'bucket': bucket_name,
                                    'key': object_key
                                }
                            }
                        },
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
                
                # Return raw JSON bytes
                logger.info(
                    "Stock data retrieved successfully",
                    extra={
                        'ticker': normalized_ticker,
                        'run_id': str(done_run.id),
                        'bucket': bucket_name,
                        'key': object_key
                    }
                )
                return HttpResponse(
                    content=json_bytes,
                    content_type='application/json',
                    status=200
                )
            
            except S3Error as e:
                error_code = e.code
                
                if error_code == 'NoSuchKey':
                    logger.warning(
                        "Data file not found in S3",
                        extra={
                            'ticker': normalized_ticker,
                            'run_id': str(done_run.id),
                            'bucket': bucket_name,
                            'key': object_key,
                            's3_error_code': error_code
                        }
                    )
                    return Response(
                        {
                            'error': {
                                'code': 'DATA_FILE_NOT_FOUND',
                                'message': f"Data file not found in storage: {object_key}",
                                'details': {
                                    'ticker': normalized_ticker,
                                    'run_id': str(done_run.id),
                                    'bucket': bucket_name,
                                    'key': object_key
                                }
                            }
                        },
                        status=status.HTTP_404_NOT_FOUND
                    )
                elif error_code in ['InvalidAccessKeyId', 'SignatureDoesNotMatch', 'AccessDenied']:
                    logger.error(
                        "S3/MinIO authentication error",
                        extra={
                            'ticker': normalized_ticker,
                            'run_id': str(done_run.id),
                            's3_error_code': error_code
                        }
                    )
                    return Response(
                        {
                            'error': {
                                'code': 'STORAGE_AUTHENTICATION_ERROR',
                                'message': f"S3/MinIO authentication failed: {error_code}",
                                'details': {
                                    'ticker': normalized_ticker,
                                    'run_id': str(done_run.id),
                                    's3_error_code': error_code
                                }
                            }
                        },
                        status=status.HTTP_401_UNAUTHORIZED
                    )
                elif error_code == 'NoSuchBucket':
                    logger.error(
                        "S3 bucket not found",
                        extra={
                            'ticker': normalized_ticker,
                            'run_id': str(done_run.id),
                            'bucket': bucket_name,
                            's3_error_code': error_code
                        }
                    )
                    return Response(
                        {
                            'error': {
                                'code': 'STORAGE_BUCKET_NOT_FOUND',
                                'message': f"S3 bucket not found: {bucket_name}",
                                'details': {
                                    'ticker': normalized_ticker,
                                    'run_id': str(done_run.id),
                                    'bucket': bucket_name,
                                    's3_error_code': error_code
                                }
                            }
                        },
                        status=status.HTTP_404_NOT_FOUND
                    )
                else:
                    logger.exception(
                        "S3/MinIO error",
                        extra={
                            'ticker': normalized_ticker,
                            'run_id': str(done_run.id),
                            's3_error_code': error_code
                        }
                    )
                    return Response(
                        {
                            'error': {
                                'code': 'STORAGE_ERROR',
                                'message': f"S3/MinIO error: {error_code}",
                                'details': {
                                    'ticker': normalized_ticker,
                                    'run_id': str(done_run.id),
                                    's3_error_code': error_code
                                }
                            }
                        },
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
            
            except MinioException as e:
                logger.exception(
                    "MinIO connection error",
                    extra={
                        'ticker': normalized_ticker,
                        'run_id': str(done_run.id)
                    },
                )
                return Response(
                    {
                        'error': {
                            'code': 'STORAGE_CONNECTION_ERROR',
                            'message': f"MinIO connection error: {str(e)}",
                            'details': {
                                'ticker': normalized_ticker,
                                'run_id': str(done_run.id)
                            }
                        }
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
            finally:
                # Always close the MinIO response if it was opened
                if minio_response:
                    try:
                        minio_response.close()
                        minio_response.release_conn()
                    except Exception as cleanup_error:
                        logger.debug(
                            "Error during MinIO response cleanup",
                            extra={
                                'error': str(cleanup_error),
                                'ticker': normalized_ticker,
                                'run_id': str(done_run.id)
                            }
                        )
        
        except Exception as e:
            logger.exception(
                "Unexpected error retrieving stock data",
                extra={
                    'ticker': normalized_ticker,
                },
                exc_info=True
            )
            return Response(
                {
                    'error': {
                        'code': 'INTERNAL_SERVER_ERROR',
                        'message': f"An unexpected error occurred: {str(e)}",
                        'details': {
                            'ticker': normalized_ticker
                        }
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

