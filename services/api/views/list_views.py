"""
API List Views for Stock Ticker ETL Pipeline.

This module contains the API views for:
- GET /tickers - List all stocks
- GET /runs - List all ingestion runs
- GET /bulk-queue-runs - List all bulk queue runs
- GET /exchanges - List all exchanges
- GET /runs/ticker/<ticker> - List runs for a specific ticker
"""

import logging

from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import status
from rest_framework.generics import ListAPIView
from rest_framework.request import Request
from rest_framework.response import Response

from api.filters import BulkQueueRunFilter, ExchangeFilter, StockFilter, StockIngestionRunFilter
from api.models import BulkQueueRun, Exchange, Stock, StockIngestionRun
from api.serializers import (
    BulkQueueRunSerializer,
    ExchangeSerializer,
    StockIngestionRunSerializer,
    StockSerializer,
)
from .paginator import StandardCursorPagination


logger = logging.getLogger(__name__)


class ExchangeListView(ListAPIView):
    """
    API endpoint for listing all exchanges.
    
    GET /exchanges
    
    Returns a paginated list of all exchanges with cursor-based pagination.
    Supports filtering by name (exact match and contains, both case-insensitive).
    
    Filtering capabilities:
    - name: Exact exchange name match (case-insensitive)
    - name__icontains: Exchange name contains (case-insensitive)
    
    Note: Exchange names are stored in uppercase (normalized on save),
    but filters work with any case input and will match correctly.
    
    Filters can be combined for precise queries. All filters work seamlessly with
    cursor-based pagination.
    
    Example requests:
        GET /api/exchanges
        GET /api/exchanges?name=NASDAQ
        GET /api/exchanges?name__icontains=nas
        GET /api/exchanges?name=NYSE&name__icontains=Y
    """
    serializer_class = ExchangeSerializer
    pagination_class = StandardCursorPagination
    filter_backends = [DjangoFilterBackend]
    filterset_class = ExchangeFilter
    queryset = Exchange.objects.all().order_by('-created_at')


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

