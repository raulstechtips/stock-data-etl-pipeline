"""
Custom exceptions for worker tasks.

This module defines exceptions used by Celery tasks for proper
error handling and retry logic.
"""


class RetryableError(Exception):
    """
    Base exception for errors that should trigger a task retry.
    
    These are transient errors that might succeed on retry, such as
    network timeouts, temporary service unavailability, etc.
    """
    pass


class NonRetryableError(Exception):
    """
    Base exception for errors that should NOT trigger a retry.
    
    These are permanent errors that won't be fixed by retrying, such as
    authentication failures, invalid data, resource not found, etc.
    """
    pass


# API Fetch Errors
class APIFetchError(RetryableError):
    """Error fetching data from the external API (retryable)."""
    pass


class APITimeoutError(RetryableError):
    """API request timed out (retryable)."""
    pass


class APIAuthenticationError(NonRetryableError):
    """API authentication failed (non-retryable)."""
    pass


class APINotFoundError(NonRetryableError):
    """Requested resource not found in API (non-retryable)."""
    pass


class APIClientError(NonRetryableError):
    """API client error (4xx non-retryable, except rate limits)."""
    pass


class APIRateLimitError(RetryableError):
    """API rate limit exceeded (retryable with backoff)."""
    pass


# Storage Errors
class StorageUploadError(RetryableError):
    """Error uploading file to S3/MinIO (retryable)."""
    pass


class StorageConnectionError(RetryableError):
    """Connection to S3/MinIO failed (retryable)."""
    pass


class StorageAuthenticationError(NonRetryableError):
    """S3/MinIO authentication failed (non-retryable)."""
    pass

class StorageBucketNotFoundError(NonRetryableError):
    """S3/MinIO bucket not found (non-retryable)."""
    pass


# Data Processing Errors
class InvalidDataFormatError(NonRetryableError):
    """Downloaded data is in an invalid format (non-retryable)."""
    pass


class InvalidStateError(NonRetryableError):
    """Task cannot proceed due to invalid state (non-retryable)."""
    pass

