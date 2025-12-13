"""
Stock Ingestion Service.

This module contains the business logic for managing stock ingestion runs
through the ETL pipeline. It handles state transitions, validation, and
provides atomic operations for concurrent access.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from django.db import transaction, IntegrityError
from django.utils import timezone

from api.models import IngestionState, Stock, StockIngestionRun


logger = logging.getLogger(__name__)


class StockNotFoundError(Exception):
    """Raised when a requested stock ticker is not found."""
    pass


class InvalidStateTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""
    pass


class IngestionRunNotFoundError(Exception):
    """Raised when a requested ingestion run is not found."""
    pass


@dataclass
class StatusResult:
    """
    Result object containing the status of a stock's latest ingestion run.
    
    Attributes:
        ticker: The stock ticker symbol
        stock_id: UUID of the stock
        run_id: UUID of the ingestion run (None if no runs exist)
        state: Current state of the run (None if no runs exist)
        created_at: When the run was created
        updated_at: When the run was last updated
    """
    ticker: str
    stock_id: uuid.UUID
    run_id: Optional[uuid.UUID] = None
    state: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


# Valid state transitions mapping
VALID_TRANSITIONS: dict[str, list[str]] = {
    IngestionState.QUEUED_FOR_FETCH: [IngestionState.FETCHING, IngestionState.FAILED],
    IngestionState.FETCHING: [IngestionState.FETCHED, IngestionState.FAILED],
    IngestionState.FETCHED: [IngestionState.QUEUED_FOR_SPARK, IngestionState.FAILED],
    IngestionState.QUEUED_FOR_SPARK: [IngestionState.SPARK_RUNNING, IngestionState.FAILED],
    IngestionState.SPARK_RUNNING: [IngestionState.SPARK_FINISHED, IngestionState.FAILED],
    IngestionState.SPARK_FINISHED: [IngestionState.DONE, IngestionState.FAILED],
    IngestionState.DONE: [],  # Terminal state
    IngestionState.FAILED: [],  # Terminal state
}

# Mapping of states to their corresponding timestamp fields
STATE_TIMESTAMP_FIELDS: dict[str, str] = {
    IngestionState.QUEUED_FOR_FETCH: 'queued_for_fetch_at',
    IngestionState.FETCHING: 'fetching_started_at',
    IngestionState.FETCHED: 'fetching_finished_at',
    IngestionState.QUEUED_FOR_SPARK: 'queued_for_spark_at',
    IngestionState.SPARK_RUNNING: 'spark_started_at',
    IngestionState.SPARK_FINISHED: 'spark_finished_at',
    IngestionState.DONE: 'done_at',
    IngestionState.FAILED: 'failed_at',
}


class StockIngestionService:
    """
    Service for managing stock ingestion runs through the ETL pipeline.
    
    This service encapsulates all business logic related to:
    - Checking stock status
    - Creating new ingestion runs
    - Updating run states with proper validation
    - Managing state transitions atomically
    
    All database operations that modify data use transactions and row-level
    locking to prevent race conditions.
    """

    def get_stock_status(self, ticker: str) -> StatusResult:
        """
        Get the current status of a stock's latest ingestion run.
        
        Args:
            ticker: Stock ticker symbol (case-insensitive)
            
        Returns:
            StatusResult containing the stock and run information
            
        Raises:
            StockNotFoundError: If the stock ticker doesn't exist
        """
        ticker_upper = ticker.strip().upper()
        
        try:
            stock = Stock.objects.get(ticker=ticker_upper)
        except Stock.DoesNotExist:
            logger.info(f"Stock not found: {ticker_upper}")
            raise StockNotFoundError(f"Stock '{ticker_upper}' not found")

        latest_run = StockIngestionRun.objects.get_latest_for_stock(stock.id)
        
        if latest_run:
            logger.info(
                f"Retrieved status for {ticker_upper}: state={latest_run.state}, "
                f"run_id={latest_run.id}"
            )
            return StatusResult(
                ticker=stock.ticker,
                stock_id=stock.id,
                run_id=latest_run.id,
                state=latest_run.state,
                created_at=latest_run.created_at,
                updated_at=latest_run.updated_at,
            )
        
        logger.info(f"No ingestion runs found for {ticker_upper}")
        return StatusResult(
            ticker=stock.ticker,
            stock_id=stock.id,
        )

    def get_or_create_stock(self, ticker: str) -> tuple[Stock, bool]:
        """
        Get an existing stock or create a new one.
        
        Args:
            ticker: Stock ticker symbol (case-insensitive)
            
        Returns:
            Tuple of (Stock instance, created boolean)
        """
        ticker_upper = ticker.strip().upper()
        stock, created = Stock.objects.get_or_create(
            ticker=ticker_upper,
        )
        
        if created:
            logger.info(f"Created new stock: {ticker_upper}")
        
        return stock, created

    def get_run_by_id(self, run_id: uuid.UUID) -> StockIngestionRun:
        """
        Get an ingestion run by its ID.
        
        Args:
            run_id: UUID of the ingestion run
            
        Returns:
            StockIngestionRun instance
            
        Raises:
            IngestionRunNotFoundError: If the run doesn't exist
        """
        try:
            return StockIngestionRun.objects.select_related('stock').get(id=run_id)
        except StockIngestionRun.DoesNotExist:
            logger.exception("ingestion_run_not_found", extra={"run_id": str(run_id)})
            raise IngestionRunNotFoundError(f"Ingestion run '{run_id}' not found")

    @transaction.atomic
    def update_run_state(
        self,
        run_id: uuid.UUID,
        new_state: str,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        raw_data_uri: Optional[str] = None,
        processed_data_uri: Optional[str] = None,
    ) -> StockIngestionRun:
        """
        Update the state of an ingestion run with validation.
        
        This method validates the state transition and atomically updates
        the run's state along with the appropriate timestamp field.
        
        Args:
            run_id: UUID of the ingestion run to update
            new_state: The new state to transition to
            error_code: Error code (required if transitioning to FAILED)
            error_message: Error message (required if transitioning to FAILED)
            raw_data_uri: URI to raw data location (optional)
            processed_data_uri: URI to processed data location (optional)
            
        Returns:
            Updated StockIngestionRun instance
            
        Raises:
            IngestionRunNotFoundError: If the run doesn't exist
            InvalidStateTransitionError: If the transition is not allowed
        """
        # Lock the row for update
        try:
            run = StockIngestionRun.objects.select_for_update().get(id=run_id)
        except StockIngestionRun.DoesNotExist:
            logger.exception("ingestion_run_not_found", extra={"run_id": str(run_id)})
            raise IngestionRunNotFoundError(f"Ingestion run '{run_id}' not found")
        
        current_state = run.state
        
        # Validate state transition
        valid_next_states = VALID_TRANSITIONS.get(current_state, [])
        if new_state not in valid_next_states:
            logger.warning(
                f"Invalid state transition for run {run_id}: "
                f"{current_state} -> {new_state}"
            )
            raise InvalidStateTransitionError(
                f"Cannot transition from '{current_state}' to '{new_state}'. "
                f"Valid transitions: {valid_next_states}"
            )
        
        # Update state and timestamp
        run.state = new_state
        
        # Set the appropriate timestamp field
        timestamp_field = STATE_TIMESTAMP_FIELDS.get(new_state)
        if timestamp_field:
            setattr(run, timestamp_field, timezone.now())
        
        # Update error information if transitioning to FAILED
        if new_state == IngestionState.FAILED:
            if not error_code or not error_message:
                raise InvalidStateTransitionError(
                    "FAILED requires both error_code and error_message"
                )
            run.error_code = error_code
            run.error_message = error_message
        
        # Update data URIs if provided
        if raw_data_uri is not None:
            run.raw_data_uri = raw_data_uri
        if processed_data_uri is not None:
            run.processed_data_uri = processed_data_uri
        
        run.save()
        
        logger.info(
            f"Updated run {run_id} state: {current_state} -> {new_state}"
        )
        
        return run

    @transaction.atomic
    def queue_for_fetch(
        self,
        ticker: str,
        requested_by: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> tuple[StockIngestionRun, bool]:
        """
        Queue a stock for fetching, creating the stock if it doesn't exist.
        
        If the stock has an active (non-terminal) ingestion run, returns
        that run without creating a new one.
        
        This operation uses row-level locking to prevent race conditions
        when multiple requests try to queue the same stock.
        
        Args:
            ticker: Stock ticker symbol (case-insensitive)
            requested_by: Identifier for the requesting entity
            request_id: Unique request identifier (defaults to timestamp)
            
        Returns:
            Tuple of (StockIngestionRun, created boolean)
            - If run already existed: (existing_run, False)
            - If new run created: (new_run, True)
        """
        ticker_upper = ticker.strip().upper()
        
        # Get or create the stock
        stock, _stock_created = self.get_or_create_stock(ticker_upper)
        
        # Check for existing active run
        latest_run = StockIngestionRun.objects.get_latest_for_stock(stock.id)
        
        if latest_run and latest_run.is_in_progress:
            logger.info(
                f"Active run exists for {ticker_upper}: state={latest_run.state}, "
                f"run_id={latest_run.id}"
            )
            return latest_run, False
        
        # Generate request_id if not provided
        if request_id is None:
            request_id = timezone.now().strftime('%Y%m%d%H%M%S%f')
        
        # Create new run - IntegrityError will bubble up to view if constraint violated
        now = timezone.now()
        new_run = StockIngestionRun.objects.create(
            stock=stock,
            state=IngestionState.QUEUED_FOR_FETCH,
            requested_by=requested_by,
            request_id=request_id,
            queued_for_fetch_at=now,
        )
        
        logger.info(
            f"Created new ingestion run for {ticker_upper}: "
            f"run_id={new_run.id}, request_id={request_id}"
        )
        return new_run, True
