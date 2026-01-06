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
from typing import Any, Dict, List, Optional

from django.db import transaction
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
    IngestionState.FETCHED: [IngestionState.QUEUED_FOR_DELTA, IngestionState.FAILED],
    IngestionState.QUEUED_FOR_DELTA: [IngestionState.DELTA_RUNNING, IngestionState.FAILED],
    IngestionState.DELTA_RUNNING: [IngestionState.DELTA_FINISHED, IngestionState.FAILED],
    IngestionState.DELTA_FINISHED: [IngestionState.DONE, IngestionState.FAILED],
    IngestionState.DONE: [],  # Terminal state
    IngestionState.FAILED: [],  # Terminal state
}

# Mapping of states to their corresponding timestamp fields
STATE_TIMESTAMP_FIELDS: dict[str, str] = {
    IngestionState.QUEUED_FOR_FETCH: 'queued_for_fetch_at',
    IngestionState.FETCHING: 'fetching_started_at',
    IngestionState.FETCHED: 'fetching_finished_at',
    IngestionState.QUEUED_FOR_DELTA: 'queued_for_delta_at',
    IngestionState.DELTA_RUNNING: 'delta_started_at',
    IngestionState.DELTA_FINISHED: 'delta_finished_at',
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
        except Stock.DoesNotExist as err:
            logger.warning("stock_not_found", extra={"ticker": ticker_upper})
            raise StockNotFoundError(f"Stock '{ticker_upper}' not found") from err

        latest_run = StockIngestionRun.objects.get_latest_for_stock(stock.id)
        
        if latest_run:
            logger.debug(
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
        
        logger.debug(f"No ingestion runs found for {ticker_upper}")
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
        except StockIngestionRun.DoesNotExist as err:
            logger.exception("ingestion_run_not_found", extra={"run_id": str(run_id)})
            raise IngestionRunNotFoundError(f"Ingestion run '{run_id}' not found") from err

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
        except StockIngestionRun.DoesNotExist as err:
            logger.exception("ingestion_run_not_found", extra={"run_id": str(run_id)})
            raise IngestionRunNotFoundError(f"Ingestion run '{run_id}' not found") from err
        
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

            # Schedule Discord notification to be sent after transaction commits
            # This ensures notification is only sent if the state update succeeds
            transaction.on_commit(lambda: self._send_discord_notification(run_id, run.stock.ticker, new_state))
        
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
        
        This operation relies on a unique database constraint combined with
        transaction.atomic to detect and handle concurrent requests. When
        multiple requests try to queue the same stock simultaneously, conflicts
        are detected via IntegrityError raised by the unique constraint violation.
        The IntegrityError bubbles up to the view layer, which handles it by
        returning a 409 Conflict response (or fetching the existing run, depending
        on the view implementation).
        
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
            logger.debug(
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
    
    @transaction.atomic
    def batch_update_run_states(
        self,
        updates: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Batch update multiple ingestion run states efficiently.
        
        This method updates multiple run states in a single operation, using
        bulk_update() for efficiency. It validates state transitions for each
        run and handles partial failures gracefully.
        
        Args:
            updates: List of update dictionaries, each containing:
                - run_id (UUID): UUID of the ingestion run to update
                - new_state (str): The new state to transition to
                - error_code (str, optional): Error code (required if new_state is FAILED)
                - error_message (str, optional): Error message (required if new_state is FAILED)
                - processed_data_uri (str, optional): URI to processed data location
                - raw_data_uri (str, optional): URI to raw data location
        
        Returns:
            Dictionary with two keys:
                - 'successful': List of successful updates, each containing:
                    - run_id: UUID of the updated run
                    - new_state: The new state that was set
                - 'failed': List of failed updates, each containing:
                    - run_id: UUID of the run that failed
                    - reason: String describing why the update failed
        
        Note:
            This method does NOT send Discord notifications. Discord notifications
            for batch failures should be handled separately.
        """
        if not updates:
            logger.info("batch_update_run_states called with empty updates list")
            return {'successful': [], 'failed': []}
        
        logger.info(
            "Starting batch state update",
            extra={"update_count": len(updates)}
        )
        
        # Validate input structure and collect run IDs
        run_ids = []
        update_map = {}  # Maps run_id to update dict
        validation_errors = []
        
        for idx, update in enumerate(updates):
            # Validate required fields
            if 'run_id' not in update:
                validation_errors.append({
                    'index': idx,
                    'reason': 'Missing required field: run_id'
                })
                continue
            
            if 'new_state' not in update:
                validation_errors.append({
                    'index': idx,
                    'run_id': update.get('run_id'),
                    'reason': 'Missing required field: new_state'
                })
                continue
            
            # Validate run_id is a valid UUID
            try:
                run_id = uuid.UUID(str(update['run_id']))
            except (ValueError, TypeError) as err:
                validation_errors.append({
                    'index': idx,
                    'run_id': update.get('run_id'),
                    'reason': f'Invalid run_id format: {err}'
                })
                continue
            
            # Validate new_state is a valid IngestionState
            new_state = update['new_state']
            valid_states = [choice[0] for choice in IngestionState.choices]
            if new_state not in valid_states:
                validation_errors.append({
                    'index': idx,
                    'run_id': str(run_id),
                    'reason': f'Invalid new_state: {new_state}. Valid states: {valid_states}'
                })
                continue
            
            # Validate error fields for FAILED state
            if new_state == IngestionState.FAILED:
                if 'error_code' not in update or 'error_message' not in update:
                    validation_errors.append({
                        'index': idx,
                        'run_id': str(run_id),
                        'reason': 'FAILED state requires both error_code and error_message'
                    })
                    continue
                if not update['error_code'] or not update['error_message']:
                    validation_errors.append({
                        'index': idx,
                        'run_id': str(run_id),
                        'reason': 'error_code and error_message cannot be empty for FAILED state'
                    })
                    continue
            
            run_ids.append(run_id)
            update_map[run_id] = update
        
        # Fetch all runs with row-level locking
        try:
            runs = list(
                StockIngestionRun.objects
                .select_for_update()
                .select_related('stock')
                .filter(id__in=run_ids)
            )
        except Exception as err:
            logger.exception(
                "Error fetching runs for batch update",
                extra={"run_ids": [str(rid) for rid in run_ids]}
            )
            # Mark all as failed
            failed = [
                {
                    'run_id': str(rid),
                    'reason': f'Database error while fetching runs: {err}'
                }
                for rid in run_ids
            ]
            return {'successful': [], 'failed': failed + validation_errors}
        
        # Create a map of run_id to run object
        run_map = {run.id: run for run in runs}
        
        # Track missing runs
        missing_run_ids = set(run_ids) - set(run_map.keys())
        failed_updates = []
        for missing_id in missing_run_ids:
            failed_updates.append({
                'run_id': str(missing_id),
                'reason': 'Run not found'
            })
        
        # Process valid runs
        runs_to_update = []
        successful_updates = []
        now = timezone.now()
        
        for run_id, update in update_map.items():
            if run_id not in run_map:
                continue  # Already handled as missing
            
            run = run_map[run_id]
            current_state = run.state
            new_state = update['new_state']
            
            # Validate state transition
            valid_next_states = VALID_TRANSITIONS.get(current_state, [])
            if new_state not in valid_next_states:
                reason = (
                    f"Invalid state transition: {current_state} -> {new_state}. "
                    f"Valid transitions: {valid_next_states}"
                )
                logger.warning(
                    f"Invalid state transition for run {run_id}: {current_state} -> {new_state}",
                    extra={"run_id": str(run_id), "current_state": current_state, "new_state": new_state}
                )
                failed_updates.append({
                    'run_id': str(run_id),
                    'reason': reason
                })
                continue
            
            # Update state
            run.state = new_state
            
            # Set the appropriate timestamp field
            timestamp_field = STATE_TIMESTAMP_FIELDS.get(new_state)
            if timestamp_field:
                setattr(run, timestamp_field, now)
            
            # Update error information if transitioning to FAILED
            if new_state == IngestionState.FAILED:
                run.error_code = update['error_code']
                run.error_message = update['error_message']
            
            # Update data URIs if provided
            if 'raw_data_uri' in update and update['raw_data_uri'] is not None:
                run.raw_data_uri = update['raw_data_uri']
            if 'processed_data_uri' in update and update['processed_data_uri'] is not None:
                run.processed_data_uri = update['processed_data_uri']
            
            runs_to_update.append(run)
            successful_updates.append({
                'run_id': str(run_id),
                'new_state': new_state
            })
        
        # Perform bulk update
        if runs_to_update:
            try:
                # Collect all fields that might be updated
                # Include all possible timestamp fields since different runs may set different ones
                fields_to_update = [
                    'state',
                    'error_code',
                    'error_message',
                    'raw_data_uri',
                    'processed_data_uri',
                ] + list(STATE_TIMESTAMP_FIELDS.values())
                
                StockIngestionRun.objects.bulk_update(
                    runs_to_update,
                    fields=fields_to_update,
                    batch_size=100
                )
                logger.info(
                    "Batch state update completed",
                    extra={
                        "successful_count": len(successful_updates),
                        "failed_count": len(failed_updates) + len(validation_errors),
                        "total_count": len(updates)
                    }
                )
            except Exception as err:
                logger.exception(
                    "Error during bulk_update",
                    extra={"runs_count": len(runs_to_update)}
                )
                # Mark all attempted updates as failed
                for run in runs_to_update:
                    failed_updates.append({
                        'run_id': str(run.id),
                        'reason': f'Database error during bulk update: {err}'
                    })
                successful_updates = []
        
        # Combine validation errors with other failures
        all_failed = failed_updates + [
            {
                'run_id': err.get('run_id', f'index_{err.get("index", "unknown")}'),
                'reason': err['reason']
            }
            for err in validation_errors
        ]
        
        return {
            'successful': successful_updates,
            'failed': all_failed
        }
    
    def _send_discord_notification(self, run_id: uuid.UUID, ticker: str, state: str) -> None:
        """
        Send a Discord notification for a state change.
        
        This method is called via transaction.on_commit() to ensure the notification
        is only sent after the database transaction commits successfully.
        
        The notification is sent asynchronously via Celery task.
        
        Args:
            run_id: UUID of the ingestion run
            ticker: Stock ticker symbol
            state: Current state of the run
        """
        try:
            # Import here to avoid circular imports
            from workers.tasks import send_discord_notification
            
            # Queue the notification task asynchronously
            send_discord_notification.delay(
                run_id=str(run_id),
                ticker=ticker,
                state=state
            )
            
            logger.debug(
                "Queued Discord notification",
                extra={"run_id": str(run_id), "ticker": ticker, "state": state}
            )
        except Exception:
            # Log error but don't fail the transaction
            logger.exception(
                "Failed to queue Discord notification",
                extra={"run_id": str(run_id), "ticker": ticker, "state": state}
            )
