"""
Tests for the workers module.

This module contains comprehensive tests for:
- fetch_stock_data Celery task
- send_discord_notification Celery task
- queue_all_stocks_for_fetch Celery task
- Error handling and retry logic
- State transitions
- Idempotency checks
"""

from .queue_for_fetch import FetchStockDataTaskTest, FetchStockDataInvalidInputTest
from .send_discord_notification import SendDiscordNotificationTaskTest, DiscordNotificationIntegrationTest
from .queue_for_delta import ProcessDeltaLakeTaskTest, ProcessDeltaLakeInvalidInputTest, TTMDataProcessingTest
from .update_stock_metadata import UpdateStockMetadataTaskTests, ReadMetadataFromDeltaLakeTests, UpdateStockWithMetadataTests, MetadataWorkerIntegrationTests
from .queue_all_stocks_for_fetch import QueueAllStocksForFetchTaskTest

__all__ = [
    'FetchStockDataTaskTest',
    'FetchStockDataInvalidInputTest',
    'SendDiscordNotificationTaskTest',
    'DiscordNotificationIntegrationTest',
    'ProcessDeltaLakeTaskTest',
    'ProcessDeltaLakeInvalidInputTest',
    'TTMDataProcessingTest',
    'UpdateStockMetadataTaskTests',
    'ReadMetadataFromDeltaLakeTests',
    'UpdateStockWithMetadataTests',
    'MetadataWorkerIntegrationTests',
    'QueueAllStocksForFetchTaskTest'
]