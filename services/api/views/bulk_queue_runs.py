"""
API Bulk Queue Run Views for Stock Ticker ETL Pipeline.

This module contains the API views for:
- GET /bulk-queue-runs/<bulk_queue_run_id>/stats - Get stats for a bulk queue run
- POST /ticker/queue/all - Queue all stocks for ingestion (bulk)
"""

import logging
import time
from uuid import UUID

from django.core.cache import cache
from celery.exceptions import CeleryError, OperationalError as CeleryOperationalError
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from api.models import BulkQueueRun, Exchange, Stock
from api.serializers import (
    BulkQueueRunSerializer,
    BulkQueueRunStatsSerializer,
    QueueAllStocksRequestSerializer,
)


logger = logging.getLogger(__name__)



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

