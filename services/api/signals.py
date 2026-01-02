"""
Django signals for cache invalidation.

This module contains signal handlers that automatically invalidate cached
list view responses when models are created, updated, or deleted.

Signal Handlers:
    - invalidate_exchange_list_cache: Invalidates ExchangeListView and TickerListView
      caches when Exchange model is saved or deleted
    - invalidate_stock_list_cache: Invalidates TickerListView cache when Stock
      model is saved or deleted

When Signals Fire:
    - post_save: Fires after a model instance is saved (both created and updated)
    - post_delete: Fires after a model instance is deleted

Cache Invalidation Strategy:
    When an Exchange is created/updated/deleted:
    - ExchangeListView cache is invalidated (direct impact)
    - TickerListView cache is invalidated (stocks reference exchanges)
    
    When a Stock is created/updated/deleted:
    - TickerListView cache is invalidated (direct impact)

Note:
    Both created and updated cases are handled by post_save signal (it fires
    for both operations). The signal handler doesn't need to distinguish
    between create and update - both require cache invalidation.
"""

import logging

from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from api.cache_utils import invalidate_list_view_cache
from api.models import Exchange, Stock

logger = logging.getLogger(__name__)


@receiver([post_save, post_delete], sender=Exchange)
def invalidate_exchange_list_cache(sender, **kwargs):
    """
    Invalidate ExchangeListView and TickerListView caches when Exchange changes.
    
    This signal handler is connected to post_save and post_delete signals
    for the Exchange model. When an exchange is created, updated, or deleted,
    it invalidates the cache for both ExchangeListView and TickerListView.
    
    TickerListView is also invalidated because stocks reference exchanges,
    and the exchange information is included in the stock list response.
    
    Args:
        sender: The Exchange model class
        **kwargs: Signal arguments including 'instance' and 'created' (for post_save)
        
    Example:
        When an Exchange is saved or deleted, this handler automatically
        invalidates cached responses for:
        - GET /api/exchanges (ExchangeListView)
        - GET /api/tickers (TickerListView)
    """
    instance = kwargs.get('instance')
    signal_type = 'post_save' if 'created' in kwargs else 'post_delete'
    was_created = kwargs.get('created', False) if signal_type == 'post_save' else None
    
    logger.info(
        f"Exchange {signal_type} signal received, invalidating list view caches",
        extra={
            'exchange_id': str(instance.id) if instance else None,
            'exchange_name': instance.name if instance else None,
            'signal_type': signal_type,
            'was_created': was_created
        }
    )
    
    # Invalidate ExchangeListView cache (direct impact)
    invalidate_list_view_cache('api:exchange-list')
    
    # Invalidate TickerListView cache (stocks reference exchanges)
    invalidate_list_view_cache('api:ticker-list')


@receiver([post_save, post_delete], sender=Stock)
def invalidate_stock_list_cache(sender, **kwargs):
    """
    Invalidate TickerListView cache when Stock changes.
    
    This signal handler is connected to post_save and post_delete signals
    for the Stock model. When a stock is created, updated, or deleted,
    it invalidates the cache for TickerListView.
    
    Args:
        sender: The Stock model class
        **kwargs: Signal arguments including 'instance' and 'created' (for post_save)
        
    Example:
        When a Stock is saved or deleted, this handler automatically
        invalidates cached responses for:
        - GET /api/tickers (TickerListView)
    """
    instance = kwargs.get('instance')
    signal_type = 'post_save' if 'created' in kwargs else 'post_delete'
    was_created = kwargs.get('created', False) if signal_type == 'post_save' else None
    
    logger.info(
        f"Stock {signal_type} signal received, invalidating list view cache",
        extra={
            'stock_id': str(instance.id) if instance else None,
            'ticker': instance.ticker if instance else None,
            'signal_type': signal_type,
            'was_created': was_created
        }
    )
    
    # Invalidate TickerListView cache (direct impact)
    invalidate_list_view_cache('api:ticker-list')

