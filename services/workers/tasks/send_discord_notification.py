"""
Celery task for sending Discord notifications about stock ingestion run state changes.

This task is triggered by the stock ingestion service after state transitions,
using transaction.on_commit() to ensure notifications are only sent when the
transaction successfully commits.

The task formats and sends embedded Discord messages with different colors
based on the ingestion state:
- Done state: Green (success)
- Failed state: Red (error)
- In-progress states: Yellow (in progress)
"""

import logging
import uuid
from typing import TypedDict, NotRequired

import requests
from celery import shared_task
from django.conf import settings
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout

from api.models import IngestionState, StockIngestionRun
from workers.exceptions import (
    NonRetryableError,
)
from workers.tasks.base import BaseTask


logger = logging.getLogger(__name__)


class DiscordNotificationResult(TypedDict):
    """
    Result object returned by the send_discord_notification task.
    
    Attributes:
        run_id: UUID of the StockIngestionRun
        ticker: Stock ticker symbol
        state: Current state of the run
        notification_sent: Whether the notification was successfully sent
        skipped: Whether the task was skipped (e.g., webhook not configured)
        reason: Reason for skipping (optional)
    """
    run_id: str
    ticker: str
    state: str
    notification_sent: bool
    skipped: bool
    reason: NotRequired[str]


@shared_task(bind=True, base=BaseTask, name='workers.tasks.send_discord_notification')
def send_discord_notification(
    self,
    run_id: str,
    ticker: str,
    state: str
) -> dict:
    """
    Send a Discord notification for a stock ingestion run state change.
    
    This task formats an embedded message based on the state and sends it to
    the configured Discord webhook URL with the thread ID attached.
    
    States and their colors:
    - DONE: Green (#00FF00) - Success
    - FAILED: Red (#FF0000) - Error
    - All other states: Yellow (#FFFF00) - In progress
    
    Args:
        run_id: UUID of the StockIngestionRun
        ticker: Stock ticker symbol
        state: Current state of the run
        
    Returns:
        DiscordNotificationResult: Result object with notification status
        
    Raises:
        NonRetryableError: For all errors
    """
    logger.info(
        "Starting Discord notification task",
        extra={"run_id": run_id, "ticker": ticker, "state": state}
    )
    
    # Check if Discord webhook is configured
    if not settings.DISCORD_WEBHOOK_URL:
        logger.warning(
            "Discord webhook not configured, skipping notification",
            extra={"run_id": run_id}
        )
        return DiscordNotificationResult(
            run_id=run_id,
            ticker=ticker,
            state=state,
            notification_sent=False,
            skipped=True,
            reason='webhook_not_configured'
        )
    
    try:
        # Build Discord webhook URL with thread ID if configured
        webhook_url = settings.DISCORD_WEBHOOK_URL
        if settings.DISCORD_THREAD_ID:
            webhook_url = f"{webhook_url}?thread_id={settings.DISCORD_THREAD_ID}"
        
        # For failed notifications, fetch full run details for comprehensive reporting
        if state == IngestionState.FAILED:
            try:
                run = StockIngestionRun.objects.select_related('stock').get(id=uuid.UUID(run_id))
                embed = _create_failed_embed(run)
            except StockIngestionRun.DoesNotExist:
                logger.warning(
                    "Run not found for failed notification, using basic embed",
                    extra={"run_id": run_id}
                )
                embed = _create_embed(run_id, ticker, state)
        else:
            # Create embedded message for non-failed states
            embed = _create_embed(run_id, ticker, state)
        
        # Send notification to Discord
        _send_to_discord(webhook_url, embed)
        
        logger.info(
            "Successfully sent Discord notification",
            extra={"run_id": run_id, "ticker": ticker, "state": state}
        )
        
        return DiscordNotificationResult(
            run_id=run_id,
            ticker=ticker,
            state=state,
            notification_sent=True,
            skipped=False
        )
    
    except NonRetryableError as e:
        # Log non-retryable error but don't fail the task
        logger.exception(
            "Non-retryable error sending Discord notification",
            extra={"run_id": str(run_id), "ticker": ticker, "state": state}
        )
        # Return a result indicating failure but don't raise
        return DiscordNotificationResult(
            run_id=run_id,
            ticker=ticker,
            state=state,
            notification_sent=False,
            skipped=False,
            reason='non_retryable_error'
        )
    
    except Exception:
        # Catch any unexpected errors
        logger.exception(
            "Unexpected error sending Discord notification",
            extra={"run_id": run_id, "ticker": ticker, "state": state}
        )
        # Return a result indicating failure
        return DiscordNotificationResult(
            run_id=run_id,
            ticker=ticker,
            state=state,
            notification_sent=False,
            skipped=False,
            reason='unexpected_error'
        )


def _create_embed(run_id: str, ticker: str, state: str) -> dict:
    """
    Create a Discord embed object based on the ingestion run state.
    
    States are color-coded:
    - DONE: Green (#00FF00)
    - FAILED: Red (#FF0000)
    - All others: Yellow (#FFFF00)
    
    Args:
        run_id: UUID of the StockIngestionRun
        ticker: Stock ticker symbol
        state: Current state of the run
        
    Returns:
        dict: Discord embed object
    """
    # Determine color and title based on state
    if state == IngestionState.DONE:
        color = 0x00FF00  # Green
        title = f"{ticker} - Ingestion Complete"
        description = f"Stock ingestion for {ticker} has completed successfully."
    elif state == IngestionState.FAILED:
        color = 0xFF0000  # Red
        title = f"{ticker} - Ingestion Failed"
        description = f"Stock ingestion for {ticker} has failed."
    else:
        color = 0xFFFF00  # Yellow
        title = f"{ticker} - {state.replace('_', ' ').title()}"
        description = f"Stock ingestion for {ticker} is in progress."
    
    # Create embed structure
    embed = {
        "title": title,
        "description": description,
        "color": color,
        "fields": [
            {
                "name": "Ticker",
                "value": ticker,
                "inline": True
            },
            {
                "name": "State",
                "value": state,
                "inline": True
            },
            {
                "name": "Run ID",
                "value": run_id,
                "inline": False
            }
        ],
        "footer": {
            "text": "Stock Ingestion Pipeline"
        }
    }
    
    return embed


def _create_failed_embed(run: StockIngestionRun) -> dict:
    """
    Create a detailed Discord embed object for failed ingestion runs.
    
    This function formats comprehensive information about the failed run,
    including error details, timestamps, request metadata, and data locations.
    
    Args:
        run: StockIngestionRun instance with full details
        
    Returns:
        dict: Discord embed object with detailed failure information
    """
    ticker = run.stock.ticker
    color = 0xFF0000  # Red
    title = f"{ticker} - Ingestion Failed"
    description = f"Stock ingestion for {ticker} has failed."
    
    # Build fields with comprehensive run details
    fields = []
    
    # Basic information
    fields.append({
        "name": "Ticker",
        "value": ticker,
        "inline": True
    })
    fields.append({
        "name": "State",
        "value": run.state,
        "inline": True
    })
    fields.append({
        "name": "Run ID",
        "value": str(run.id),
        "inline": False
    })
    
    # Error details
    if run.error_code or run.error_message:
        error_details = []
        if run.error_code:
            error_details.append(f"**Code:** {run.error_code}")
        if run.error_message:
            # Truncate long error messages to fit Discord's field value limit (1024 chars)
            error_msg = run.error_message
            if len(error_msg) > 1000:
                error_msg = error_msg[:997] + "..."
            error_details.append(f"**Message:** {error_msg}")
        if error_details:
            fields.append({
                "name": "Error Details",
                "value": "\n".join(error_details),
                "inline": False
            })
    
    # Request metadata
    if run.requested_by or run.request_id:
        request_info = []
        if run.requested_by:
            request_info.append(f"**Requested By:** {run.requested_by}")
        if run.request_id:
            request_info.append(f"**Request ID:** {run.request_id}")
        if request_info:
            fields.append({
                "name": "Request Information",
                "value": "\n".join(request_info),
                "inline": False
            })
    
    # Timestamps - lifecycle
    timestamp_info = []
    if run.created_at:
        timestamp_info.append(f"**Created:** {run.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if run.updated_at:
        timestamp_info.append(f"**Last Updated:** {run.updated_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if run.failed_at:
        timestamp_info.append(f"**Failed At:** {run.failed_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    if timestamp_info:
        fields.append({
            "name": "Timestamps",
            "value": "\n".join(timestamp_info),
            "inline": False
        })
    
    # Phase-specific timestamps
    phase_timestamps = []
    if run.queued_for_fetch_at:
        phase_timestamps.append(f"**Queued for Fetch:** {run.queued_for_fetch_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if run.fetching_started_at:
        phase_timestamps.append(f"**Fetching Started:** {run.fetching_started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if run.fetching_finished_at:
        phase_timestamps.append(f"**Fetching Finished:** {run.fetching_finished_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if run.queued_for_spark_at:
        phase_timestamps.append(f"**Queued for Spark:** {run.queued_for_spark_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if run.spark_started_at:
        phase_timestamps.append(f"**Spark Started:** {run.spark_started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if run.spark_finished_at:
        phase_timestamps.append(f"**Spark Finished:** {run.spark_finished_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    if phase_timestamps:
        fields.append({
            "name": "Pipeline Phases",
            "value": "\n".join(phase_timestamps),
            "inline": False
        })
    
    # Data locations
    data_locations = []
    if run.raw_data_uri:
        data_locations.append(f"**Raw Data:** {run.raw_data_uri}")
    if run.processed_data_uri:
        data_locations.append(f"**Processed Data:** {run.processed_data_uri}")
    
    if data_locations:
        fields.append({
            "name": "Data Locations",
            "value": "\n".join(data_locations),
            "inline": False
        })
    
    # Create embed structure
    embed = {
        "title": title,
        "description": description,
        "color": color,
        "fields": fields,
        "footer": {
            "text": "Stock Ingestion Pipeline"
        },
        "timestamp": run.failed_at.isoformat() if run.failed_at else run.updated_at.isoformat()
    }
    
    return embed


def _send_to_discord(webhook_url: str, embed: dict) -> None:
    """
    Send an embedded message to Discord via webhook.
    
    Args:
        webhook_url: Discord webhook URL (with thread_id if applicable)
        embed: Discord embed object
        
    Raises:
        NonRetryableError: For all errors
    """
    try:
        # Prepare payload
        payload = {
            "embeds": [embed]
        }
        
        # Send POST request to Discord webhook
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10  # 10 second timeout
        )
        
        # Check for specific error status codes
        if response.status_code == 401 or response.status_code == 403:
            logger.error("Discord webhook authentication failed", extra={"status_code": response.status_code})
            raise NonRetryableError(
                f"Discord webhook authentication failed: {response.status_code}"
            )
        
        if response.status_code == 404:
            logger.error("Discord webhook not found", extra={"webhook_url": webhook_url})
            raise NonRetryableError("Discord webhook not found (404)")
        
        if response.status_code == 429:
            # Rate limited - this is retryable
            logger.warning("Discord rate limit exceeded")
            raise NonRetryableError("Discord rate limit exceeded")
        
        # Raise for other HTTP errors
        response.raise_for_status()
        
        logger.debug("Discord notification sent successfully", extra={"status_code": response.status_code})
    
    except Timeout as e:
        logger.warning("Discord webhook request timed out")
        raise NonRetryableError("Discord webhook request timed out") from e
    
    except ConnectionError as e:
        logger.warning("Discord webhook connection error")
        raise NonRetryableError("Discord webhook connection error") from e
    
    except HTTPError as e:
        if e.response.status_code >= 500:
            # Server errors are non-retryable
            logger.warning("Discord server error", extra={"status_code": e.response.status_code})
            raise NonRetryableError(f"Discord server error: {e.response.status_code}") from e
        else:
            # Client errors (except specific ones handled above) are not retryable
            logger.exception("Discord client error", extra={"status_code": e.response.status_code})
            raise NonRetryableError(f"Discord client error: {e.response.status_code}") from e
    
    except RequestException as e:
        logger.exception("Discord webhook request error")
        raise NonRetryableError(f"Discord webhook request error: {str(e)}") from e
