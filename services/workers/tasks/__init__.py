"""
Tasks package for the workers app.

This package contains Celery tasks for background processing.
"""

from .queue_for_fetch import fetch_stock_data
from .send_discord_notification import send_discord_notification

__all__ = ['fetch_stock_data', 'send_discord_notification']
