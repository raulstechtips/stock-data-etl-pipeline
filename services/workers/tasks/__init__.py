"""
Tasks package for the workers app.

This package contains Celery tasks for background processing.
"""

from .base import BaseTask
from .queue_for_fetch import fetch_stock_data
from .queue_for_delta import process_delta_lake
from .send_discord_notification import send_discord_notification
from .update_stock_metadata import update_stock_metadata

__all__ = [
    'BaseTask',
    'fetch_stock_data',
    'process_delta_lake',
    'send_discord_notification',
    'update_stock_metadata'
]
