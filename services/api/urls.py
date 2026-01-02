"""
URL configuration for Stock Ticker ETL Pipeline API.

This module defines the URL patterns for the stock ingestion
status tracking endpoints.

Endpoints:
    GET  /tickers                                   - List all stocks
    GET  /ticker/<ticker>/detail                    - Get stock details
    GET  /ticker/<ticker>/status                    - Get current status of a stock
    POST /ticker/queue                              - Queue a stock for ingestion
    POST /ticker/queue/all                          - Queue all stocks for ingestion (bulk)
    GET  /runs                                      - List all ingestion runs
    GET  /runs/ticker/<ticker>                      - List runs for a specific ticker
    GET  /run/<run_id>/detail                       - Get details of a specific run
    GET  /bulk-queue-runs                           - List all bulk queue runs
    GET  /bulk-queue-runs/<bulk_queue_run_id>/stats - Get stats for a bulk queue run
    GET  /data/all-data/<ticker>                   - Get latest raw stock data JSON
"""

from django.urls import path

from api.views import (
    BulkQueueRunListView,
    BulkQueueRunStatsDetailView,
    QueueAllStocksForFetchView,
    QueueForFetchView,
    RunDetailView,
    RunListView,
    StockDataView,
    StockStatusView,
    TickerDetailView,
    TickerListView,
    TickerRunsListView,
)


app_name = 'api'

urlpatterns = [
    # Stock list and detail endpoints
    path(
        'tickers',
        TickerListView.as_view(),
        name='ticker-list'
    ),
    path(
        'ticker/<str:ticker>/detail',
        TickerDetailView.as_view(),
        name='ticker-detail'
    ),
    path(
        'ticker/<str:ticker>/status',
        StockStatusView.as_view(),
        name='stock-status'
    ),
    path(
        'runs/ticker/<str:ticker>',
        TickerRunsListView.as_view(),
        name='ticker-runs-list'
    ),
    path(
        'ticker/queue',
        QueueForFetchView.as_view(),
        name='queue-for-fetch'
    ),
    path(
        'ticker/queue/all',
        QueueAllStocksForFetchView.as_view(),
        name='queue-all-stocks-for-fetch'
    ),
    
    # Run list and detail endpoints
    path(
        'runs',
        RunListView.as_view(),
        name='run-list'
    ),
    path(
        'run/<str:run_id>/detail',
        RunDetailView.as_view(),
        name='run-detail'
    ),
    
    # Bulk queue run endpoints
    path(
        'bulk-queue-runs',
        BulkQueueRunListView.as_view(),
        name='bulk-queue-run-list'
    ),
    path(
        'bulk-queue-runs/<str:bulk_queue_run_id>/stats',
        BulkQueueRunStatsDetailView.as_view(),
        name='bulk-queue-run-stats-detail'
    ),
    
    # Stock data endpoints
    path(
        'data/all-data/<str:ticker>',
        StockDataView.as_view(),
        name='stock-data'
    ),
]

