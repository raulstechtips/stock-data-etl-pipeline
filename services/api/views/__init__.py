"""
API Views package.

This module initializes and exposes all primary API views for the Stock Ticker ETL Pipeline project.
It aggregates view classes for:
- Stock and ticker detail endpoints
- Ingestion runs (queuing, status, detail, and bulk operations)
- List endpoints for tickers, exchanges, sectors, and bulk queue runs
- Stock data retrieval

Usage:
    from api.views import QueueForFetchView, RunDetailView, TickerListView, ExchangeListView, SectorListView, ...

Views:
    - BulkQueueRunStatsDetailView
    - QueueAllStocksForFetchView
    - RunDetailView
    - QueueForFetchView
    - TickerListView
    - ExchangeListView
    - SectorListView
    - RunListView
    - BulkQueueRunListView
    - TickerRunsListView
    - TickerDetailView
    - StockStatusView
    - StockDataView
"""

from .bulk_queue_runs import BulkQueueRunStatsDetailView, QueueAllStocksForFetchView
from .ingestion_runs import QueueForFetchView, RunDetailView
from .list_views import ExchangeListView, SectorListView, TickerListView, RunListView, BulkQueueRunListView, TickerRunsListView
from .stocks import TickerDetailView, StockStatusView, StockDataView
__all__ = [
    'BulkQueueRunStatsDetailView',
    'QueueAllStocksForFetchView',
    'RunDetailView',
    'QueueForFetchView',
    'TickerListView',
    'ExchangeListView',
    'SectorListView',
    'RunListView',
    'BulkQueueRunListView',
    'TickerRunsListView',
    'TickerDetailView',
    'StockStatusView',
    'StockDataView',
]


