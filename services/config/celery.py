"""
Celery configuration for the Stock Ticker ETL Pipeline.

This module configures Celery with:
- RabbitMQ as the message broker
- Redis as the results backend
- Auto-discovery of tasks from installed apps
- Separate queues for different task types
"""

import os
import logging
from celery import Celery

logger = logging.getLogger(__name__)

# Set default Django settings module for Celery
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

# Create Celery app instance
app = Celery('stock_etl_pipeline')

# Load configuration from Django settings with CELERY_ prefix
app.config_from_object('django.conf:settings', namespace='CELERY')

# Configure task routing to separate queues
# Each task type will be routed to its own dedicated queue
app.conf.task_routes = {
    'workers.tasks.fetch_stock_data': {'queue': 'queue_for_fetch'},
    'workers.tasks.process_delta_lake': {'queue': 'queue_for_delta'},
    'workers.tasks.send_discord_notification': {'queue': 'send_discord_notifications'},
}

# Auto-discover tasks from all registered Django apps
# This will look for tasks.py in each app
app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Debug task for testing Celery configuration."""
    logger.debug('Request: %r', self.request)
