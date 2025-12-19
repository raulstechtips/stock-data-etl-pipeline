"""
Tests for the workers module.

This module contains comprehensive tests for:
- fetch_stock_data Celery task
- send_discord_notification Celery task
- Error handling and retry logic
- State transitions
- Idempotency checks
"""

from .queue_for_fetch import FetchStockDataTaskTest, FetchStockDataInvalidInputTest
from .send_discord_notification import SendDiscordNotificationTaskTest, DiscordNotificationIntegrationTest
from .queue_for_delta import ProcessDeltaLakeTaskTest, ProcessDeltaLakeInvalidInputTest, TTMDataProcessingTest

__all__ = [
    'FetchStockDataTaskTest',
    'FetchStockDataInvalidInputTest',
    'SendDiscordNotificationTaskTest',
    'DiscordNotificationIntegrationTest',
    'ProcessDeltaLakeTaskTest',
    'ProcessDeltaLakeInvalidInputTest',
    'TTMDataProcessingTest'
]