"""
Celery task for queuing all stocks for fetch in a bulk operation.

This task is triggered by the Bulk Queue All Stocks API endpoint and performs
the following steps:
1. Retrieves the BulkQueueRun instance to track statistics
2. Updates BulkQueueRun.started_at when processing begins
3. Queries all stocks from the database
4. For each stock:
   a. Calls StockIngestionService.queue_for_fetch() to create/get a run
   b. Links the run to the BulkQueueRun via foreign key
   c. Increments queued_count if new run created
   d. Increments skipped_count if run already exists
   e. Increments error_count if an error occurs
   f. Queues fetch_stock_data task for new runs
5. Updates BulkQueueRun.completed_at when processing finishes
6. Logs progress at appropriate intervals
"""

import logging
import uuid
from typing import TypedDict
from django.db.models import F
from django.utils import timezone

from celery import shared_task

from api.models import BulkQueueRun, Exchange, Stock, StockIngestionRun
from api.services.stock_ingestion_service import StockIngestionService
from workers.exceptions import NonRetryableError
from workers.tasks.base import BaseTask

logger = logging.getLogger(__name__)

# Batch size for bulk_update operations
# This balances memory usage against database roundtrips.
# With 1000 stocks, this would result in ~10 bulk_update calls instead of 1000 individual saves.
BULK_UPDATE_BATCH_SIZE = 100


class QueueAllStocksForFetchResult(TypedDict):
    """
    Result object returned by the queue_all_stocks_for_fetch task.
    
    Attributes:
        bulk_queue_run_id: UUID of the BulkQueueRun
        total_stocks: Total number of stocks processed (filtered by exchange if provided)
        queued_count: Number of stocks successfully queued
        skipped_count: Number of stocks skipped (existing active runs)
        error_count: Number of stocks that failed to queue
        success: Whether the bulk operation completed successfully
    """
    bulk_queue_run_id: str
    total_stocks: int
    queued_count: int
    skipped_count: int
    error_count: int
    success: bool


@shared_task(bind=True, base=BaseTask, name='workers.tasks.queue_all_stocks_for_fetch')
def queue_all_stocks_for_fetch(
    self,
    bulk_queue_run_id: str,
    exchange_name: str | None = None
) -> QueueAllStocksForFetchResult:
    """
    Queue all stocks for fetching in a bulk operation.
    
    This task processes all stocks in the database (or filtered by exchange)
    and queues them for ingestion. It updates the BulkQueueRun statistics
    throughout processing and handles individual stock failures gracefully.
    
    Args:
        bulk_queue_run_id: UUID string of the BulkQueueRun to track statistics
        exchange_name: Optional exchange name to filter stocks by (e.g., 'NASDAQ', 'NYSE')
                      If provided, only stocks belonging to this exchange will be queued.
        
    Returns:
        QueueAllStocksForFetchResult: Result object with statistics about the operation
        
    Raises:
        NonRetryableError: If the BulkQueueRun is not found or other critical errors occur
    """
    logger.info(
        "Starting queue_all_stocks_for_fetch task",
        extra={
            "bulk_queue_run_id": bulk_queue_run_id,
            "exchange_name": exchange_name
        }
    )
    
    # Step 1: Retrieve the BulkQueueRun instance
    try:
        bulk_queue_run_uuid = uuid.UUID(bulk_queue_run_id)
        bulk_queue_run = BulkQueueRun.objects.get(id=bulk_queue_run_uuid)
    except (ValueError, BulkQueueRun.DoesNotExist) as e:
        logger.exception(
            "BulkQueueRun not found",
            extra={"bulk_queue_run_id": bulk_queue_run_id}
        )
        raise NonRetryableError(f"BulkQueueRun not found: {bulk_queue_run_id}") from e
    
    # Step 2: Update BulkQueueRun.started_at when processing begins
    bulk_queue_run.started_at = timezone.now()
    bulk_queue_run.save(update_fields=['started_at'])
    logger.debug(
        "Updated BulkQueueRun started_at",
        extra={
            "bulk_queue_run_id": bulk_queue_run_id,
            "started_at": bulk_queue_run.started_at,
            "exchange_name": exchange_name
        }
    )
    
    # Step 3: Query all stocks efficiently (with optional exchange filtering)
    # Using values_list to get just the tickers (more memory efficient)
    stocks_queryset = Stock.objects.all()
    
    # Apply exchange filtering if provided
    if exchange_name:
        # Normalize exchange name (strip and uppercase)
        normalized_exchange_name = exchange_name.strip().upper()
        
        try:
            # Get the Exchange instance
            exchange_instance = Exchange.objects.get(name=normalized_exchange_name)
            
            # Filter stocks by exchange
            stocks_queryset = stocks_queryset.filter(exchange=exchange_instance)
            
            logger.debug(
                "Filtering stocks by exchange in queue_all_stocks_for_fetch task",
                extra={
                    "bulk_queue_run_id": bulk_queue_run_id,
                    "exchange_name": normalized_exchange_name,
                    "exchange_id": str(exchange_instance.id)
                }
            )
        except Exchange.DoesNotExist:
            # Exchange not found - log warning and return early
            logger.warning(
                "Exchange not found in queue_all_stocks_for_fetch task",
                extra={
                    "bulk_queue_run_id": bulk_queue_run_id,
                    "exchange_name": normalized_exchange_name
                }
            )
            
            # Update completed_at to mark task as finished
            bulk_queue_run.completed_at = timezone.now()
            bulk_queue_run.save(update_fields=['completed_at'])
            
            # Return early with 0 stocks processed
            return QueueAllStocksForFetchResult(
                bulk_queue_run_id=bulk_queue_run_id,
                total_stocks=0,
                queued_count=0,
                skipped_count=0,
                error_count=0,
                success=False
            )
    
    stock_tickers = list(stocks_queryset.values_list('ticker', flat=True).order_by('ticker'))
    total_stocks = len(stock_tickers)
    
    # Update total_stocks count in BulkQueueRun
    bulk_queue_run.total_stocks = total_stocks
    bulk_queue_run.save(update_fields=['total_stocks'])
    
    logger.debug(
        "Retrieved stocks for processing",
        extra={
            "bulk_queue_run_id": bulk_queue_run_id,
            "total_stocks": total_stocks,
            "exchange_name": exchange_name
        }
    )
    
    # Initialize service for queueing stocks
    service = StockIngestionService()
    
    # Import fetch_stock_data task here to avoid circular imports
    from workers.tasks.queue_for_fetch import fetch_stock_data
    
    # Initialize batch list for bulk_update operations
    bulk_queue_run_updates_batch = []
    
    def flush_bulk_queue_run_updates():
        """Flush pending bulk_queue_run updates using bulk_update."""
        nonlocal bulk_queue_run_updates_batch
        if bulk_queue_run_updates_batch:
            try:
                StockIngestionRun.objects.bulk_update(
                    bulk_queue_run_updates_batch,
                    fields=['bulk_queue_run'],
                    batch_size=BULK_UPDATE_BATCH_SIZE
                )
                logger.debug(
                    "Flushed bulk_queue_run updates",
                    extra={
                        "bulk_queue_run_id": bulk_queue_run_id,
                        "batch_size": len(bulk_queue_run_updates_batch)
                    }
                )
            except Exception:
                # Log the error but don't fail the entire operation
                logger.exception(
                    "Failed to flush bulk_queue_run updates",
                    extra={
                        "bulk_queue_run_id": bulk_queue_run_id,
                        "batch_size": len(bulk_queue_run_updates_batch),
                    }
                )
            finally:
                # Clear the batch regardless of success/failure to avoid reprocessing
                bulk_queue_run_updates_batch = []
    
    # Step 4: Process each stock
    for index, ticker in enumerate(stock_tickers, start=1):
        try:
            # Step 4a: Call StockIngestionService.queue_for_fetch()
            run, created = service.queue_for_fetch(
                ticker=ticker,
                requested_by=bulk_queue_run.requested_by,
                request_id=f"bulk-queue-{bulk_queue_run_id}"
            )
            
            # Step 4b: Link the run to the BulkQueueRun (batched for efficiency)
            # Only link if not already assigned to any BulkQueueRun
            if run.bulk_queue_run_id is None:
                run.bulk_queue_run = bulk_queue_run
                bulk_queue_run_updates_batch.append(run)
                
                # Flush batch if it reaches the batch size
                if len(bulk_queue_run_updates_batch) >= BULK_UPDATE_BATCH_SIZE:
                    flush_bulk_queue_run_updates()
            
            # Step 4c & 4d: Update counters and queue task if new run created
            if created:
                # Atomically increment queued_count in database
                BulkQueueRun.objects.filter(id=bulk_queue_run.id).update(
                    queued_count=F('queued_count') + 1
                )
                
                # Queue the fetch_stock_data task
                try:
                    fetch_stock_data.delay(run_id=str(run.id), ticker=ticker)
                    logger.debug(
                        "Queued stock for fetch",
                        extra={
                            "ticker": ticker,
                            "run_id": str(run.id),
                            "bulk_queue_run_id": bulk_queue_run_id
                        }
                    )
                except Exception:
                    # If we fail to queue the task, atomically decrement queued_count and increment error_count
                    logger.exception(
                        "Failed to queue fetch_stock_data task",
                        extra={
                            "ticker": ticker,
                            "run_id": str(run.id),
                            "bulk_queue_run_id": bulk_queue_run_id
                        }
                    )
                    BulkQueueRun.objects.filter(id=bulk_queue_run.id).update(
                        queued_count=F('queued_count') - 1,
                        error_count=F('error_count') + 1
                    )
            else:
                # Atomically increment skipped_count in database
                BulkQueueRun.objects.filter(id=bulk_queue_run.id).update(
                    skipped_count=F('skipped_count') + 1
                )
                logger.debug(
                    "Skipped stock (active run exists)",
                    extra={
                        "ticker": ticker,
                        "run_id": str(run.id),
                        "state": run.state,
                        "bulk_queue_run_id": bulk_queue_run_id
                    }
                )
        
        except Exception as e:
            # Step 4e: Handle errors for individual stocks
            # Atomically increment error_count in database
            BulkQueueRun.objects.filter(id=bulk_queue_run.id).update(
                error_count=F('error_count') + 1
            )
            logger.error(
                "Error processing stock in bulk queue",
                extra={
                    "ticker": ticker,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "bulk_queue_run_id": bulk_queue_run_id
                },
                exc_info=True
            )
            # Continue processing other stocks
        
        # Step 5: Log progress at appropriate intervals (every 100 stocks)
        if index % 100 == 0:
            # Refresh from database to get current counter values for logging
            bulk_queue_run.refresh_from_db()
            logger.info(
                "Bulk queue progress",
                extra={
                    "bulk_queue_run_id": bulk_queue_run_id,
                    "processed": index,
                    "total_stocks": total_stocks,
                    "queued": bulk_queue_run.queued_count,
                    "skipped": bulk_queue_run.skipped_count,
                    "errors": bulk_queue_run.error_count
                }
            )
    
    # Flush any remaining bulk_queue_run updates
    flush_bulk_queue_run_updates()
    
    # Step 6: Update completed_at and read final statistics from database
    bulk_queue_run.completed_at = timezone.now()
    bulk_queue_run.save(update_fields=['completed_at'])
    
    # Refresh from database to get final counter values
    bulk_queue_run.refresh_from_db()
    
    logger.info(
        "Completed queue_all_stocks_for_fetch task",
        extra={
            "bulk_queue_run_id": bulk_queue_run_id,
            "total_stocks": total_stocks,
            "queued": bulk_queue_run.queued_count,
            "skipped": bulk_queue_run.skipped_count,
            "errors": bulk_queue_run.error_count,
            "completed_at": bulk_queue_run.completed_at,
            "exchange_name": exchange_name
        }
    )
    
    # Step 7: Return statistics
    return QueueAllStocksForFetchResult(
        bulk_queue_run_id=bulk_queue_run_id,
        total_stocks=total_stocks,
        queued_count=bulk_queue_run.queued_count,
        skipped_count=bulk_queue_run.skipped_count,
        error_count=bulk_queue_run.error_count,
        success=True
    )

