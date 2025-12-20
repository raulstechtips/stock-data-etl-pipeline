"""
Stock Ticker ETL Pipeline models.

This module contains the database models for tracking stock ticker
data through an ETL pipeline with multiple processing states.
"""

import uuid
from django.db import models


class IngestionState(models.TextChoices):
    """
    Enumeration of possible states for a stock ingestion run.
    
    States follow the ETL pipeline flow:
    1. QUEUED_FOR_FETCH - Initial state when ingestion is requested
    2. FETCHING - Data is being fetched from source
    3. FETCHED - Data has been successfully fetched
    4. QUEUED_FOR_DELTA - Ready for Delta Lake processing
    5. DELTA_RUNNING - Delta Lake job is processing the data
    6. DELTA_FINISHED - Delta Lake processing completed
    7. DONE - Pipeline completed successfully
    8. FAILED - Pipeline encountered an error
    """
    QUEUED_FOR_FETCH = 'QUEUED_FOR_FETCH', 'Queued for Fetch'
    FETCHING = 'FETCHING', 'Fetching'
    FETCHED = 'FETCHED', 'Fetched'
    QUEUED_FOR_DELTA = 'QUEUED_FOR_DELTA', 'Queued for Delta Lake'
    DELTA_RUNNING = 'DELTA_RUNNING', 'Delta Lake Running'
    DELTA_FINISHED = 'DELTA_FINISHED', 'Delta Lake Finished'
    DONE = 'DONE', 'Done'
    FAILED = 'FAILED', 'Failed'


class Stock(models.Model):
    """
    Represents a stock ticker symbol.
    
    This model stores basic stock information and serves as the
    parent entity for ingestion runs.
    
    The ticker field is automatically normalized to uppercase and trimmed
    of whitespace on save, ensuring case-insensitive uniqueness. This
    prevents duplicate entries like 'aapl' and 'AAPL' from being created.
    
    Attributes:
        id: UUID primary key
        ticker: Unique stock ticker symbol (e.g., 'AAPL', 'GOOGL')
            Automatically normalized to uppercase on save.
        created_at: Timestamp when the stock was first added
        updated_at: Timestamp when the stock was last modified
        sector: Industry sector classification (e.g., 'Information Technology')
        name: Company name
        exchange: Stock exchange where traded (e.g., 'NASDAQ')
        country: Country code where company is based (e.g., 'US')
        subindustry: Sub-industry classification
        morningstar_sector: Morningstar sector classification
        morningstar_industry: Morningstar industry classification
        industry: Industry classification
        description: Company description
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    ticker = models.CharField(max_length=20, unique=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Metadata fields from Delta Lake
    sector = models.CharField(max_length=255, null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    exchange = models.CharField(max_length=50, null=True, blank=True)
    country = models.CharField(max_length=10, null=True, blank=True)
    subindustry = models.CharField(max_length=255, null=True, blank=True)
    morningstar_sector = models.CharField(max_length=255, null=True, blank=True)
    morningstar_industry = models.CharField(max_length=255, null=True, blank=True)
    industry = models.CharField(max_length=255, null=True, blank=True)
    description = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'stocks'
        ordering = ['ticker']

    def save(self, *args, **kwargs):
        """
        Override save to normalize ticker to uppercase before saving.
        
        This ensures consistent storage regardless of input case,
        preventing duplicate entries like 'aapl' and 'AAPL'.
        """
        if self.ticker:
            self.ticker = self.ticker.strip().upper()
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.ticker

    def __repr__(self) -> str:
        return f"<Stock(id={self.id}, ticker='{self.ticker}')>"


class StockIngestionRunManager(models.Manager):
    """Custom manager for StockIngestionRun with common query patterns."""

    def get_latest_for_stock(self, stock_id: uuid.UUID) -> 'StockIngestionRun | None':
        """
        Get the most recent ingestion run for a stock.
        
        Args:
            stock_id: UUID of the stock
            
        Returns:
            The latest StockIngestionRun or None if no runs exist
        """
        return (
            self.select_related('stock')
            .filter(stock_id=stock_id)
            .order_by('-created_at')
            .first()
        )

    def get_latest_by_ticker(self, ticker: str) -> 'StockIngestionRun | None':
        """
        Get the most recent ingestion run for a stock by ticker symbol.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            The latest StockIngestionRun or None if no runs exist
        """
        return (
            self.select_related('stock')
            .filter(stock__ticker=ticker.strip().upper())
            .order_by('-created_at')
            .first()
        )

    def get_active_runs(self) -> models.QuerySet:
        """
        Get all runs that are currently in-progress (not DONE or FAILED).
        
        Returns:
            QuerySet of active StockIngestionRun objects
        """
        terminal_states = [IngestionState.DONE, IngestionState.FAILED]
        return self.exclude(state__in=terminal_states)


class StockIngestionRun(models.Model):
    """
    Represents a single ingestion run for a stock ticker.
    
    Tracks the state and progress of a stock through the ETL pipeline,
    including timestamps for each phase and error information if applicable.
    
    Attributes:
        id: UUID primary key
        stock: Foreign key to the Stock model
        requested_by: Identifier of the entity that requested the run
        request_id: Unique identifier for the request (usually timestamp-based)
        state: Current state in the ETL pipeline
        Various timestamp fields for tracking phase transitions
        error_code: Error code if the run failed
        error_message: Detailed error message if the run failed
        raw_data_uri: URI to raw data location (e.g., S3)
        processed_data_uri: URI to processed data location (e.g., lakehouse)
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Relationship to stock
    stock = models.ForeignKey(
        Stock,
        on_delete=models.CASCADE,
        related_name='ingestion_runs',
        db_index=True
    )
    
    # Request metadata
    requested_by = models.CharField(max_length=255, null=True, blank=True)
    request_id = models.CharField(max_length=255, null=True, blank=True)
    
    # Current state
    state = models.CharField(
        max_length=20,
        choices=IngestionState.choices,
        default=IngestionState.QUEUED_FOR_FETCH,
        db_index=True
    )
    
    # Lifecycle timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Phase-specific timestamps (optional, helpful for debugging/SLAs)
    queued_for_fetch_at = models.DateTimeField(null=True, blank=True)
    fetching_started_at = models.DateTimeField(null=True, blank=True)
    fetching_finished_at = models.DateTimeField(null=True, blank=True)
    queued_for_delta_at = models.DateTimeField(null=True, blank=True)
    delta_started_at = models.DateTimeField(null=True, blank=True)
    delta_finished_at = models.DateTimeField(null=True, blank=True)
    done_at = models.DateTimeField(null=True, blank=True)
    failed_at = models.DateTimeField(null=True, blank=True)
    
    # Error information
    error_code = models.CharField(max_length=50, null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    
    # Data location references
    raw_data_uri = models.CharField(max_length=500, null=True, blank=True)
    processed_data_uri = models.CharField(max_length=500, null=True, blank=True)

    objects = StockIngestionRunManager()

    class Meta:
        db_table = 'stock_ingestion_runs'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['stock', '-created_at'], name='idx_run_stock_created_at'),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['stock'],
                condition=models.Q(state__in=[
                    IngestionState.QUEUED_FOR_FETCH,
                    IngestionState.FETCHING,
                    IngestionState.FETCHED,
                    IngestionState.QUEUED_FOR_DELTA,
                    IngestionState.DELTA_RUNNING,
                    IngestionState.DELTA_FINISHED,
                ]),
                name='unique_active_run_per_stock'
            )
        ]

    def __str__(self) -> str:
        return f"{self.stock.ticker} - {self.state} ({self.id})"

    def __repr__(self) -> str:
        return f"<StockIngestionRun(id={self.id}, stock='{self.stock.ticker}', state='{self.state}')>"

    @property
    def is_terminal(self) -> bool:
        """Check if the run is in a terminal state (DONE or FAILED)."""
        return self.state in [IngestionState.DONE, IngestionState.FAILED]

    @property
    def is_in_progress(self) -> bool:
        """Check if the run is currently in progress."""
        return not self.is_terminal
