"""
Serializers for Stock Ticker ETL Pipeline API.

This module contains DRF serializers for validating input and
serializing output for the stock ingestion API endpoints.
"""

from rest_framework import serializers

from api.models import BulkQueueRun, IngestionState, Stock, StockIngestionRun


class StockSerializer(serializers.ModelSerializer):
    """
    Serializer for the Stock model.
    
    Used for listing and retrieving stock information including metadata
    fields populated from Delta Lake.
    """

    class Meta:
        model = Stock
        fields = [
            'id',
            'ticker',
            'created_at',
            'updated_at',
            'sector',
            'name',
            'exchange',
            'country',
            'subindustry',
            'morningstar_sector',
            'morningstar_industry',
            'industry',
            'description',
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


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
    stocks for ingestion via bulk operation.
    """
    requested_by = serializers.CharField(
        max_length=255,
        required=False,
        allow_blank=True,
        help_text="Identifier for the requesting entity"
    )


