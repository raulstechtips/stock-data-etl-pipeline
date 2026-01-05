"""
Celery configuration for the Stock Ticker ETL Pipeline.

This module configures Celery with:
- RabbitMQ as the message broker (using Quorum Queues for HA in prod/stage)
- Redis as the results backend
- Auto-discovery of tasks from installed apps
- Separate queues for different task types
"""

import os
import logging
from celery import Celery
from kombu import Queue

logger = logging.getLogger(__name__)

# Set default Django settings module for Celery
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

# Get environment to determine queue configuration
APP_ENV = os.environ.get('APP_ENV', 'dev')

# Create Celery app instance
app = Celery('stock_etl_pipeline')

# Load configuration from Django settings with CELERY_ prefix
app.config_from_object('django.conf:settings', namespace='CELERY')

# Configure queues based on environment
# Use quorum queues in production/staging for HA, regular queues in development
if APP_ENV in ['prod', 'stage']:
    # Configure quorum queues for high availability
    # Each queue is explicitly defined as a quorum queue for production HA deployment
    # Quorum queues provide better data safety and fault tolerance in RabbitMQ clusters
    app.conf.task_queues = (
        Queue('queue_for_fetch', queue_arguments={'x-queue-type': 'quorum'}),
        Queue('queue_for_delta', queue_arguments={'x-queue-type': 'quorum'}),
        Queue('send_discord_notifications', queue_arguments={'x-queue-type': 'quorum'}),
    )
    
    # Enable publisher confirms (required for quorum queues)
    # This ensures messages are replicated to a quorum of nodes before acknowledgment
    app.conf.broker_transport_options = {
        'confirm_publish': True,
    }
    logger.info(f"Configured quorum queues for {APP_ENV} environment")
else:
    # Use regular queues in development
    # No explicit queue configuration needed - Celery will use default classic queues
    app.conf.task_queues = (
        Queue('queue_for_fetch'),
        Queue('queue_for_delta'),
        Queue('send_discord_notifications'),
    )
    logger.info(f"Configured classic queues for {APP_ENV} environment")

# Configure task routing to separate queues
# Each task type will be routed to its own dedicated queue
app.conf.task_routes = {
    'workers.tasks.fetch_stock_data': {'queue': 'queue_for_fetch'},
    'workers.tasks.process_delta_lake': {'queue': 'queue_for_delta'},
    'workers.tasks.send_discord_notification': {'queue': 'send_discord_notifications'},
    'workers.tasks.update_stock_metadata': {'queue': 'queue_for_fetch'},  # Low priority, non-critical
    'workers.tasks.queue_all_stocks_for_fetch': {'queue': 'queue_for_fetch'},
}

# Auto-discover tasks from all registered Django apps
# This will look for tasks.py in each app
app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    """Debug task for testing Celery configuration."""
    logger.debug('Request: %r', self.request)
