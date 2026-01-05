"""
Cache utility functions for API list view cache invalidation.

This module provides utilities for invalidating cached responses from
cache_page decorator, particularly for paginated list views.

Cache Key Format:
    The cache_page decorator generates cache keys in the format:
    'views.decorators.cache.cache_page.{key_prefix}.GET.{hash1}.{hash2}.en-us.UTC'
    
    Where:
    - key_prefix: The key_prefix parameter passed to cache_page decorator (e.g., 'api:ticker-list')
    - hash1: Hash of the request path/parameters
    - hash2: Hash of the query string (includes cursor, filters, etc.)
    
    Each unique combination of path and query parameters (including pagination cursors
    and filters) results in a unique cache key. This means:
    - Different paginated pages (different cursor values) have different cache keys
    - Different filter combinations have different cache keys
    - The same page with the same filters will reuse the same cache key
    
Cache Invalidation Strategy:
    When a model is created, updated, or deleted, we need to invalidate all cached
    pages for the affected list views. Since query parameters vary per page, we
    use pattern matching to find and delete all cache keys matching the path prefix.
    
    For Redis backend:
    - Use SCAN command (via scan_iter) to find all keys matching pattern
    - Use DELETE command to remove all matching keys
    - SCAN is preferred over KEYS for production environments (non-blocking)
    
Performance Considerations:
    - SCAN is non-blocking and safe for production use
    - KEYS command blocks Redis and should be avoided in production
    - Pattern matching may need to scan many keys, but this is acceptable for
      infrequent cache invalidation operations (only on model changes)
"""

import logging

from django.core.cache import cache
from django.urls import reverse

logger = logging.getLogger(__name__)


def invalidate_list_view_cache(view_name: str) -> None:
    """
    Invalidate all cached responses for a list view.
    
    This function invalidates all cache keys for a specific list view endpoint,
    including all paginated pages and filter combinations. It works by:
    1. Getting the URL path for the view using reverse()
    2. Constructing a cache key prefix pattern
    3. Finding all cache keys matching the pattern
    4. Deleting all matching keys
    
    The cache_page decorator generates cache keys in the format:
    'views.decorators.cache.cache_page.{key_prefix}.GET.{hash1}.{hash2}.en-us.UTC'
    
    Where key_prefix is the key_prefix parameter passed to cache_page decorator.
    Since query parameters (cursor, filters) vary per page, we need to delete
    all keys matching the key_prefix pattern.
    
    Args:
        view_name: The name of the view to invalidate (e.g., 'api:exchange-list')
        
    Example:
        invalidate_list_view_cache('api:exchange-list')
        invalidate_list_view_cache('api:ticker-list')
        
    Note:
        This function handles Redis backend specifically. For other backends,
        it logs a warning and skips invalidation. Cache invalidation is logged
        at INFO level with the view name and number of keys deleted.
    """
    try:
        
        # Construct cache key prefix pattern
        # Django's cache_page creates two types of cache keys:
        # 1. cache_page format: 'views.decorators.cache.cache_page.{key_prefix}.GET.{hash1}.{hash2}.en-us.UTC'
        # 2. cache_header format: 'views.decorators.cache.cache_header.{key_prefix}.{hash}.en-us.UTC'
        # Where key_prefix is the key_prefix parameter passed to cache_page decorator (e.g., 'api:ticker-list')
        # Redis pattern uses '*' to match any characters
        
        # Get cache backend
        cache_backend = cache._cache
        
        # Check if we're using Redis backend
        if not hasattr(cache_backend, 'get_client'):
            logger.warning(
                f"Cache backend does not support pattern-based invalidation for view '{view_name}'. "
                f"Backend type: {type(cache_backend).__name__}"
            )
            return
        
        # Get Redis client
        redis_client = cache_backend.get_client()
        
        # Use SCAN to find all keys matching the pattern (non-blocking)
        # Django's cache_page creates two types of cache keys:
        # 1. cache_page keys: views.decorators.cache.cache_page.{key_prefix}.GET.*
        # 2. cache_header keys: views.decorators.cache.cache_header.{key_prefix}.*
        keys_to_delete = []
        
        # Pattern for cache_page keys (use * prefix to match version prefix like :1:)
        cache_page_pattern = f'*views.decorators.cache.cache_page.{view_name}.GET.*'
        # Pattern for cache_header keys (use * prefix to match version prefix like :1:)
        cache_header_pattern = f'*views.decorators.cache.cache_header.{view_name}.*'
        
        # SCAN iterates through all keys matching both patterns
        for pattern_to_use in [cache_page_pattern, cache_header_pattern]:
            for key in redis_client.scan_iter(match=pattern_to_use):
                # Decode bytes to string if needed
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                keys_to_delete.append(key)
        
        # Delete all matching keys
        if keys_to_delete:
            deleted_count = redis_client.delete(*keys_to_delete)
            logger.debug(
                f"Invalidated cache for view '{view_name}': deleted {deleted_count} cache key(s)",
                extra={
                    'view_name': view_name,
                    'keys_deleted': deleted_count,
                    'patterns': [cache_page_pattern, cache_header_pattern]
                }
            )
        else:
            logger.debug(
                f"No cache keys found to invalidate for view '{view_name}'",
                extra={
                    'view_name': view_name,
                    'patterns': [cache_page_pattern, cache_header_pattern]
                }
            )
            
    except Exception:
        logger.exception(
            f"Failed to invalidate cache for view '{view_name}'",
            extra={
                'view_name': view_name,
            }
        )

