"""
Django filters for Stock and StockIngestionRun models.

This module provides FilterSet classes for filtering API list views
using django-filter. Filters support various lookup expressions including
exact matches, contains searches, date ranges, and boolean filters.
"""

from django_filters import rest_framework as filters

from api.models import Stock, StockIngestionRun, IngestionState


class StockFilter(filters.FilterSet):
    """
    FilterSet for Stock model.
    
    Provides filtering capabilities for stock ticker list views:
    - ticker: Exact match or contains (case-insensitive)
    - sector: Exact match or contains (case-insensitive)
    - exchange: Exact match
    - country: Exact match
    
    Example usage:
        ?ticker=AAPL                    # Exact match (case-insensitive)
        ?ticker__icontains=app          # Contains 'app' (case-insensitive)
        ?sector=Technology              # Exact sector match (case-insensitive)
        ?sector__icontains=tech         # Sector contains 'tech'
        ?exchange=NASDAQ                # Exact exchange match (case-insensitive)
        ?country=US                     # Exact country match (case-insensitive)
        ?sector=Technology&country=US   # Multiple filters combined
    """
    ticker = filters.CharFilter(field_name='ticker', lookup_expr='iexact')
    sector = filters.CharFilter(field_name='sector', lookup_expr='iexact')
    exchange = filters.CharFilter(field_name='exchange', lookup_expr='iexact')
    country = filters.CharFilter(field_name='country', lookup_expr='iexact')
    
    class Meta:
        model = Stock
        fields = {
            'ticker': ['icontains'],
            'sector': ['icontains'],
        }


class StockIngestionRunFilter(filters.FilterSet):
    """
    FilterSet for StockIngestionRun model.
    
    Provides comprehensive filtering for ingestion run list views:
    - ticker: Filter by stock ticker (exact or contains)
    - state: Filter by ingestion state
    - requested_by: Filter by requester identifier
    - created_after: Filter runs created after a date
    - created_before: Filter runs created before a date
    - is_terminal: Filter terminal states (DONE/FAILED)
    - is_in_progress: Filter in-progress runs
    
    Example usage:
        ?ticker=AAPL                           # Runs for AAPL
        ?ticker__icontains=app                 # Runs for tickers containing 'app'
        ?state=FAILED                          # Failed runs only
        ?requested_by=user@example.com         # Runs requested by specific user
        ?created_after=2025-01-01              # Runs created after Jan 1, 2025
        ?created_before=2025-12-31             # Runs created before Dec 31, 2025
        ?is_terminal=true                      # Only terminal runs (DONE/FAILED)
        ?is_in_progress=true                   # Only in-progress runs
        ?state=FAILED&created_after=2025-01-01 # Multiple filters combined
    """
    ticker = filters.CharFilter(field_name='stock__ticker', lookup_expr='iexact')
    ticker__icontains = filters.CharFilter(field_name='stock__ticker', lookup_expr='icontains')
    state = filters.ChoiceFilter(field_name='state', choices=IngestionState.choices)
    requested_by = filters.CharFilter(field_name='requested_by', lookup_expr='iexact')
    requested_by__icontains = filters.CharFilter(field_name='requested_by', lookup_expr='icontains')
    created_after = filters.DateTimeFilter(field_name='created_at', lookup_expr='gte')
    created_before = filters.DateTimeFilter(field_name='created_at', lookup_expr='lte')
    is_terminal = filters.BooleanFilter(method='filter_is_terminal')
    is_in_progress = filters.BooleanFilter(method='filter_is_in_progress')
    
    class Meta:
        model = StockIngestionRun
        fields = []
    
    def filter_is_terminal(self, queryset, name, value):
        """
        Filter runs by terminal state.
        
        Args:
            queryset: The base queryset
            name: The filter field name (unused)
            value: Boolean - True for terminal, False for non-terminal
            
        Returns:
            Filtered queryset
        """
        terminal_states = [IngestionState.DONE, IngestionState.FAILED]
        if value:
            return queryset.filter(state__in=terminal_states)
        else:
            return queryset.exclude(state__in=terminal_states)
    
    def filter_is_in_progress(self, queryset, name, value):
        """
        Filter runs by in-progress state.
        
        Args:
            queryset: The base queryset
            name: The filter field name (unused)
            value: Boolean - True for in-progress, False for terminal
            
        Returns:
            Filtered queryset
        """
        terminal_states = [IngestionState.DONE, IngestionState.FAILED]
        if value:
            return queryset.exclude(state__in=terminal_states)
        else:
            return queryset.filter(state__in=terminal_states)

