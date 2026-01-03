"""
Tests for Stock Ticker ETL Pipeline API.

This module contains comprehensive tests for:
- Models (Stock, StockIngestionRun)
- Service layer (StockIngestionService)
- API endpoints
"""

from .models import (
    ExchangeModelTest,
    StockExchangeForeignKeyTest,
    StockModelTest, 
    StockIngestionRunModelTest, 
    StockIngestionRunManagerTest,
    BulkQueueRunModelTest,
    BulkQueueRunRelationshipTest,
)
from .services import StockIngestionServiceTest, StockIngestionServiceTransactionTest, StateTransitionTest
from .views import (
    StockStatusAPITest,
    QueueForFetchAPITest,
    RunDetailAPITest,
    TickerListAPITest,
    TickerDetailAPITest,
    RunListAPITest,
    TickerRunsListAPITest,
    QueueAllStocksForFetchAPITest,
    BulkQueueRunListAPITest,
    BulkQueueRunStatsDetailAPITest,
    StockDataAPITest,
    ExchangeListAPITest,
    SectorListAPITest
)
from .filters import (
    TickerListFilterAPITest, 
    RunListFilterAPITest, 
    TickerRunsListFilterAPITest, 
    BulkQueueRunFilterAPITest,
    BulkQueueRunListFilterAPITest
)
from .auth import (
    AuthenticationRequiredAPITest,
    AuthenticatedAccessAPITest,
)
from .cache_invalidation import (
    ExchangeListViewCacheTest, 
    TickerListViewCacheTest, 
    CacheInvalidationUtilityTest, 
    CacheInvalidationSignalsTest, 
    SectorListViewCacheTest
)

__all__ = [
    'ExchangeModelTest',
    'StockExchangeForeignKeyTest',
    'StockModelTest',
    'StockIngestionRunModelTest',
    'StockIngestionRunManagerTest',
    'BulkQueueRunModelTest',
    'BulkQueueRunRelationshipTest',
    'StockIngestionServiceTest',
    'StockIngestionServiceTransactionTest',
    'StateTransitionTest',
    'StockStatusAPITest',
    'QueueForFetchAPITest',
    'RunDetailAPITest',
    'TickerListAPITest',
    'TickerDetailAPITest',
    'RunListAPITest',
    'TickerRunsListAPITest',
    'TickerListFilterAPITest',
    'RunListFilterAPITest',
    'TickerRunsListFilterAPITest',
    'QueueAllStocksForFetchAPITest',
    'BulkQueueRunFilterAPITest',
    'BulkQueueRunListAPITest',
    'BulkQueueRunListFilterAPITest',
    'BulkQueueRunStatsDetailAPITest',
    'AuthenticationRequiredAPITest',
    'AuthenticatedAccessAPITest',
    'StockDataAPITest',
    'ExchangeListAPITest',
    'ExchangeListViewCacheTest', 
    'TickerListViewCacheTest', 
    'CacheInvalidationUtilityTest', 
    'CacheInvalidationSignalsTest',
    'SectorListAPITest',
    'SectorListViewCacheTest'
]
