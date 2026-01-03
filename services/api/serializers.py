"""
Serializers for Stock Ticker ETL Pipeline API.

This module contains DRF serializers for validating input and
serializing output for the stock ingestion API endpoints.
"""

from rest_framework import serializers

from api.models import BulkQueueRun, Exchange, IngestionState, Sector, Stock, StockIngestionRun


class ExchangeSerializer(serializers.ModelSerializer):
    """
    Serializer for the Exchange model.
    
    Used for listing and retrieving exchange information. All fields
    except 'name' are read-only as they are managed by the system.
    """

    class Meta:
        model = Exchange
        fields = [
            'id',
            'name',
            'created_at',
            'updated_at',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class SectorSerializer(serializers.ModelSerializer):
    """
    Serializer for the Sector model.
    
    Used for listing and retrieving sector information. All fields
    except 'name' are read-only as they are managed by the system.
    """

    class Meta:
        model = Sector
        fields = [
            'id',
            'name',
            'created_at',
            'updated_at',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class StockSerializer(serializers.ModelSerializer):
    """
    Serializer for the Stock model.
    
    Used for listing and retrieving stock information including metadata
    fields populated from Delta Lake. The exchange field is a ForeignKey
    to the Exchange model, and the sector field is a ForeignKey to the Sector
    model. For backward compatibility, we provide both the foreign key IDs
    and human-readable names (exchange_name and sector_name) as separate fields.
    """
    exchange_name = serializers.SerializerMethodField()
    sector_name = serializers.SerializerMethodField()

    class Meta:
        model = Stock
        fields = [
            'id',
            'ticker',
            'created_at',
            'updated_at',
            'sector',
            'sector_name',
            'name',
            'exchange',
            'exchange_name',
            'country',
            'subindustry',
            'morningstar_sector',
            'morningstar_industry',
            'industry',
            'description',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at', 'exchange', 'exchange_name', 'sector', 'sector_name']

    def get_exchange_name(self, obj: Stock) -> str | None:
        """
        Get the exchange name from the related Exchange model.
        
        Returns None if no exchange is associated with the stock.
        This maintains backward compatibility with the previous CharField implementation.
        """
        return obj.exchange.name if obj.exchange else None

    def get_sector_name(self, obj: Stock) -> str | None:
        """
        Get the sector name from the related Sector model.
        
        Returns None if no sector is associated with the stock.
        This maintains backward compatibility with the previous CharField implementation.
        """
        return obj.sector.name if obj.sector else None


class StockIngestionRunSerializer(serializers.ModelSerializer):
    """
    Serializer for the StockIngestionRun model.
    
    Provides detailed information about an ingestion run including
    all timestamps and metadata.
    """
    ticker = serializers.CharField(source='stock.ticker', read_only=True)
    is_terminal = serializers.BooleanField(read_only=True)
    is_in_progress = serializers.BooleanField(read_only=True)

    class Meta:
        model = StockIngestionRun
        fields = [
            'id',
            'stock_id',
            'ticker',
            'requested_by',
            'request_id',
            'state',
            'is_terminal',
            'is_in_progress',
            'created_at',
            'updated_at',
            'queued_for_fetch_at',
            'fetching_started_at',
            'fetching_finished_at',
            'queued_for_delta_at',
            'delta_started_at',
            'delta_finished_at',
            'done_at',
            'failed_at',
            'error_code',
            'error_message',
            'raw_data_uri',
            'processed_data_uri',
        ]
        read_only_fields = [
            'id',
            'stock_id',
            'ticker',
            'created_at',
            'updated_at',
            'is_terminal',
            'is_in_progress',
        ]


class StockStatusResponseSerializer(serializers.Serializer):
    """
    Serializer for the stock status response.
    
    Returns a simplified view of the stock's current status
    in the ETL pipeline.
    """
    ticker = serializers.CharField()
    stock_id = serializers.UUIDField()
    run_id = serializers.UUIDField(allow_null=True)
    state = serializers.ChoiceField(
        choices=IngestionState.choices,
        allow_null=True
    )
    created_at = serializers.DateTimeField(allow_null=True)
    updated_at = serializers.DateTimeField(allow_null=True)


class BulkQueueRunSerializer(serializers.ModelSerializer):
    """
    Serializer for the BulkQueueRun model.
    
    Provides information about bulk queue operations including statistics
    on how many stocks were queued, skipped, or encountered errors.
    """

    class Meta:
        model = BulkQueueRun
        fields = [
            'id',
            'requested_by',
            'total_stocks',
            'queued_count',
            'skipped_count',
            'error_count',
            'created_at',
            'started_at',
            'completed_at',
        ]
        read_only_fields = [
            'id',
            'created_at',
            'started_at',
            'completed_at',
        ]


class QueueForFetchRequestSerializer(serializers.Serializer):
    """
    Serializer for validating queue-for-fetch requests.
    
    Used to validate the POST request body when queuing a
    stock for ingestion.
    """
    ticker = serializers.CharField(
        max_length=10,
        help_text="Stock ticker symbol (e.g., 'AAPL')"
    )
    requested_by = serializers.CharField(
        max_length=255,
        required=False,
        allow_blank=True,
        help_text="Identifier for the requesting entity"
    )
    request_id = serializers.CharField(
        max_length=255,
        required=False,
        allow_blank=True,
        help_text="Unique request identifier"
    )

    def validate_ticker(self, value: str) -> str:
        """Validate and normalize the ticker symbol."""
        if not value:
            raise serializers.ValidationError("Ticker symbol is required")
        
        # Normalize to uppercase
        normalized = value.upper().strip()
        
        # Basic validation for ticker format
        if not normalized.isalnum():
            raise serializers.ValidationError(
                "Ticker symbol must contain only alphanumeric characters"
            )
        
        if len(normalized) > 10:
            raise serializers.ValidationError(
                "Ticker symbol must be 10 characters or fewer"
            )
        
        return normalized


class QueueAllStocksRequestSerializer(serializers.Serializer):
    """
    Serializer for validating queue-all-stocks requests.
    
    Used to validate the POST request body when queuing all
    stocks for ingestion via bulk operation. Supports optional
    exchange filtering to queue only stocks from a specific exchange.
    """
    requested_by = serializers.CharField(
        max_length=255,
        required=False,
        allow_blank=True,
        help_text="Identifier for the requesting entity"
    )
    exchange = serializers.CharField(
        max_length=50,
        required=False,
        allow_blank=True,
        help_text="Exchange name to filter stocks by (e.g., 'NASDAQ', 'NYSE')"
    )


class BulkQueueRunStatsSerializer(serializers.ModelSerializer):
    """
    Serializer for BulkQueueRun with aggregated statistics from related ingestion runs.
    
    Extends BulkQueueRunSerializer to include base BulkQueueRun fields and adds
    an ingestion_run_stats field that aggregates statistics from all related
    StockIngestionRun objects linked to the BulkQueueRun.
    
    The ingestion_run_stats field provides:
    - total: Total count of related StockIngestionRun objects
    - by_state: Dictionary mapping each IngestionState to its count
    
    This serializer is used for the bulk queue run stats detail endpoint which
    provides comprehensive statistics about the ingestion runs created by a
    bulk queue operation. The aggregation is performed efficiently using
    database annotations to minimize query overhead.
    
    Fields:
        All fields from BulkQueueRun (id, requested_by, total_stocks, queued_count,
        skipped_count, error_count, created_at, started_at, completed_at)
        ingestion_run_stats: Dictionary containing total count and counts by state
    """
    ingestion_run_stats = serializers.SerializerMethodField()

    class Meta:
        model = BulkQueueRun
        fields = [
            'id',
            'requested_by',
            'total_stocks',
            'queued_count',
            'skipped_count',
            'error_count',
            'created_at',
            'started_at',
            'completed_at',
            'ingestion_run_stats',
        ]
        read_only_fields = [
            'id',
            'created_at',
            'started_at',
            'completed_at',
            'ingestion_run_stats',
        ]

    def get_ingestion_run_stats(self, obj: BulkQueueRun) -> dict:
        """
        Aggregate statistics from all related StockIngestionRun objects.
        
        Groups ingestion runs by state and provides total count and counts
        for each IngestionState. Uses efficient database aggregation to
        minimize query overhead.
        
        Args:
            obj: BulkQueueRun instance
            
        Returns:
            Dictionary with 'total' count and 'by_state' dictionary mapping
            each IngestionState value to its count. Example:
            {
                'total': 19500,
                'by_state': {
                    'QUEUED_FOR_FETCH': 5000,
                    'FETCHING': 2000,
                    'FETCHED': 3000,
                    'QUEUED_FOR_DELTA': 4000,
                    'DELTA_RUNNING': 1500,
                    'DELTA_FINISHED': 2000,
                    'DONE': 1500,
                    'FAILED': 500
                }
            }
        """
        from django.db.models import Count
        
        # Get all related ingestion runs and aggregate by state
        ingestion_runs = obj.ingestion_runs.all()
        
        # Aggregate counts by state using database aggregation
        state_counts = (
            ingestion_runs
            .values('state')
            .annotate(count=Count('id'))
            .order_by('state')
        )
        
        # Build the by_state dictionary, initializing all states to 0
        by_state = {state[0]: 0 for state in IngestionState.choices}
        
        # Populate with actual counts
        total = 0
        for item in state_counts:
            state = item['state']
            count = item['count']
            by_state[state] = count
            total += count
        
        return {
            'total': total,
            'by_state': by_state,
        }


