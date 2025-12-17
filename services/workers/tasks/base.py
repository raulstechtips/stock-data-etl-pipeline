"""
Base class for all Celery tasks.
"""

from celery import Task
from workers.exceptions import RetryableError


class BaseTask(Task):
    """
    Custom Celery task class with retry configuration.
    
    This provides a base class for fetch tasks with automatic retry
    logic and proper error handling.
    """
    
    # Retry configuration
    autoretry_for = (RetryableError,)
    retry_kwargs = {'max_retries': 3}
    retry_backoff = True  # Exponential backoff
    retry_backoff_max = 600  # Max 10 minutes between retries
    retry_jitter = True  # Add randomness to prevent thundering herd
