"""
Django filters for Stock and StockIngestionRun models.

This module provides FilterSet classes for filtering API list views
using django-filter. Filters support various lookup expressions including
exact matches, contains searches, date ranges, and boolean filters.
"""

from django_filters import rest_framework as filters

from api.models import BulkQueueRun, Stock, StockIngestionRun, IngestionState


class StockFilter(filters.FilterSet):
    """
    FilterSet for Stock model.
    
    Provides filtering capabilities for stock ticker list views:
    - ticker: Exact match or contains (case-insensitive)
    - sector: Exact match or contains (case-insensitive)
    - exchange__name: Exact match by exchange name (case-insensitive)
    - country: Exact match
    
    Example usage:
        ?ticker=AAPL                    # Exact match (case-insensitive)
        ?ticker__icontains=app          # Contains 'app' (case-insensitive)
        ?sector=Technology              # Exact sector match (case-insensitive)
        ?sector__icontains=tech         # Sector contains 'tech'
        ?exchange__name=NASDAQ          # Exact exchange name match (case-insensitive)
        ?country=US                     # Exact country match (case-insensitive)
        ?sector=Technology&country=US   # Multiple filters combined
    """
    ticker = filters.CharFilter(field_name='ticker', lookup_expr='iexact')
    sector = filters.CharFilter(field_name='sector', lookup_expr='iexact')
    exchange__name = filters.CharFilter(field_name='exchange__name', lookup_expr='iexact')
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
    - run_id: Filter by ingestion run UUID (exact match)
    - ticker: Filter by stock ticker (exact or contains)
    - state: Filter by ingestion state
    - requested_by: Filter by requester identifier
    - created_after: Filter runs created after a date
    - created_before: Filter runs created before a date
    - is_terminal: Filter terminal states (DONE/FAILED)
    - is_in_progress: Filter in-progress runs
    - bulk_queue_run: Filter by BulkQueueRun UUID
    
    Example usage:
        ?run_id=550e8400-e29b-41d4-a716-446655440000  # Specific run by UUID
        ?ticker=AAPL                           # Runs for AAPL
        ?ticker__icontains=app                 # Runs for tickers containing 'app'
        ?state=FAILED                          # Failed runs only
        ?requested_by=user@example.com         # Runs requested by specific user
        ?created_after=2025-01-01              # Runs created after Jan 1, 2025
        ?created_before=2025-12-31             # Runs created before Dec 31, 2025
        ?is_terminal=true                      # Only terminal runs (DONE/FAILED)
        ?is_in_progress=true                   # Only in-progress runs
        ?bulk_queue_run=550e8400-e29b-41d4-a716-446655440000  # Runs from specific bulk operation
        ?state=FAILED&created_after=2025-01-01 # Multiple filters combined
        ?bulk_queue_run=<uuid>&state=FAILED    # Failed runs from a bulk operation
    """
    run_id = filters.UUIDFilter(field_name='id', lookup_expr='exact')
    ticker = filters.CharFilter(field_name='stock__ticker', lookup_expr='iexact')
    ticker__icontains = filters.CharFilter(field_name='stock__ticker', lookup_expr='icontains')
    state = filters.ChoiceFilter(field_name='state', choices=IngestionState.choices)
    requested_by = filters.CharFilter(field_name='requested_by', lookup_expr='iexact')
    requested_by__icontains = filters.CharFilter(field_name='requested_by', lookup_expr='icontains')
    created_after = filters.DateTimeFilter(field_name='created_at', lookup_expr='gte')
    created_before = filters.DateTimeFilter(field_name='created_at', lookup_expr='lte')
    is_terminal = filters.BooleanFilter(method='filter_is_terminal')
    is_in_progress = filters.BooleanFilter(method='filter_is_in_progress')
    bulk_queue_run = filters.UUIDFilter(field_name='bulk_queue_run', lookup_expr='exact')
    
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


class BulkQueueRunFilter(filters.FilterSet):
    """
    FilterSet for BulkQueueRun model.
    
    Provides comprehensive filtering for bulk queue run list views:
    - requested_by: Filter by requester identifier (exact match, case-insensitive)
    - requested_by__icontains: Filter by requester contains (case-insensitive)
    - created_after: Filter runs created after a date
    - created_before: Filter runs created before a date
    - started_at_after: Filter runs started after a date
    - started_at_before: Filter runs started before a date
    - completed_at_after: Filter runs completed after a date
    - completed_at_before: Filter runs completed before a date
    - is_completed: Filter by completion status (true = completed, false = not completed)
    - has_errors: Filter by error presence (true = has errors, false = no errors)
    
    Example usage:
        ?requested_by=admin@example.com                    # Exact requester match (case-insensitive)
        ?requested_by__icontains=admin                     # Requester contains 'admin'
        ?created_after=2025-01-01T00:00:00Z                 # Runs created after Jan 1, 2025
        ?created_before=2025-12-31T23:59:59Z               # Runs created before Dec 31, 2025
        ?started_at_after=2025-01-01T00:00:00Z             # Runs started after Jan 1, 2025
        ?started_at_before=2025-12-31T23:59:59Z            # Runs started before Dec 31, 2025
        ?completed_at_after=2025-01-01T00:00:00Z           # Runs completed after Jan 1, 2025
        ?completed_at_before=2025-12-31T23:59:59Z          # Runs completed before Dec 31, 2025
        ?is_completed=true                                  # Only completed runs
        ?is_completed=false                                 # Only incomplete runs
        ?has_errors=true                                    # Only runs with errors (error_count > 0)
        ?has_errors=false                                   # Only runs without errors (error_count = 0)
        ?requested_by=admin@example.com&is_completed=true  # Multiple filters combined
        ?created_after=2025-01-01T00:00:00Z&has_errors=true # Date range + error filter
    """
    requested_by = filters.CharFilter(field_name='requested_by', lookup_expr='iexact')
    requested_by__icontains = filters.CharFilter(field_name='requested_by', lookup_expr='icontains')
    created_after = filters.DateTimeFilter(field_name='created_at', lookup_expr='gte')
    created_before = filters.DateTimeFilter(field_name='created_at', lookup_expr='lte')
    started_at_after = filters.DateTimeFilter(field_name='started_at', lookup_expr='gte')
    started_at_before = filters.DateTimeFilter(field_name='started_at', lookup_expr='lte')
    completed_at_after = filters.DateTimeFilter(field_name='completed_at', lookup_expr='gte')
    completed_at_before = filters.DateTimeFilter(field_name='completed_at', lookup_expr='lte')
    is_completed = filters.BooleanFilter(field_name='completed_at', lookup_expr='isnull', exclude=True)
    has_errors = filters.BooleanFilter(method='filter_has_errors')
    
    class Meta:
        model = BulkQueueRun
        fields = []
    
    def filter_has_errors(self, queryset, name, value):
        """
        Filter runs by error presence.
        
        Args:
            queryset: The base queryset
            name: The filter field name (unused)
            value: Boolean - True for runs with errors, False for runs without errors
            
        Returns:
            Filtered queryset
        """
        if value:
            return queryset.filter(error_count__gt=0)
        else:
            return queryset.filter(error_count=0)

