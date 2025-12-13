"""
URL configuration for Stock Ticker ETL Pipeline API.

This module defines the URL patterns for the stock ingestion
status tracking endpoints.

Endpoints:
    GET  /ticker/<ticker>/status - Get current status of a stock
    POST /ticker/queue           - Queue a stock for ingestion
    GET  /runs/<run_id>          - Get details of a specific run
    PATCH /runs/<run_id>/state   - Update state of a run (internal)
"""

from django.urls import path

from api.views import (
    QueueForFetchView,
    RunDetailView,
    StockStatusView,
    UpdateRunStateView,
)


app_name = 'api'

urlpatterns = [
    # Stock status endpoints
    path(
        'ticker/<str:ticker>/status',
        StockStatusView.as_view(),
        name='stock-status'
    ),
    path(
        'ticker/queue',
        QueueForFetchView.as_view(),
        name='queue-for-fetch'
    ),
    
    # Run management endpoints
    path(
        'runs/<str:run_id>',
        RunDetailView.as_view(),
        name='run-detail'
    ),
    path(
        'runs/<str:run_id>/state',
        UpdateRunStateView.as_view(),
        name='update-run-state'
    ),
]

