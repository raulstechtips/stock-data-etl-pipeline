"""
Serializers for Stock Ticker ETL Pipeline API.

This module contains DRF serializers for validating input and
serializing output for the stock ingestion API endpoints.
"""

from rest_framework import serializers

from api.models import IngestionState, Stock, StockIngestionRun


class StockSerializer(serializers.ModelSerializer):
    """
    Serializer for the Stock model.
    
    Used for listing and retrieving stock information.
    """

    class Meta:
        model = Stock
        fields = ['id', 'ticker', 'created_at', 'updated_at']
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


class UpdateRunStateRequestSerializer(serializers.Serializer):
    """
    Serializer for validating state update requests.
    
    Used by internal services to update the state of an ingestion run.
    """
    state = serializers.ChoiceField(
        choices=IngestionState.choices,
        help_text="New state for the ingestion run"
    )
    error_code = serializers.CharField(
        max_length=50,
        required=False,
        allow_blank=True,
        help_text="Error code (required if state is FAILED)"
    )
    error_message = serializers.CharField(
        required=False,
        allow_blank=True,
        help_text="Error message (required if state is FAILED)"
    )
    raw_data_uri = serializers.CharField(
        max_length=500,
        required=False,
        allow_blank=True,
        help_text="URI to raw data location"
    )
    processed_data_uri = serializers.CharField(
        max_length=500,
        required=False,
        allow_blank=True,
        help_text="URI to processed data location"
    )

    def validate(self, data: dict) -> dict:
        """
        Validate that error information is provided when transitioning to FAILED.
        """
        state = data.get('state')
        
        if state == IngestionState.FAILED:
            if not data.get('error_code'):
                raise serializers.ValidationError({
                    'error_code': 'Error code is required when state is FAILED'
                })
            if not data.get('error_message'):
                raise serializers.ValidationError({
                    'error_message': 'Error message is required when state is FAILED'
                })
        
        return data
