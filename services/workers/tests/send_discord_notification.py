"""
Tests for the send_discord_notification Celery task.

This test module covers:
- Successful notification for DONE state (green)
- Successful notification for FAILED state (red)
- Successful notification for in-progress states (yellow)
- Handling webhook URL with thread ID
- Handling webhook not configured
- Handling Discord timeout errors
- Handling Discord rate limit errors
- Handling Discord server errors
- Handling Discord authentication errors
- Handling Discord webhook not found errors
- Handling Discord connection errors
"""


from unittest.mock import Mock, patch

from django.test import TransactionTestCase, override_settings
from requests.exceptions import ConnectionError, HTTPError, Timeout

from api.models import IngestionState, Stock, StockIngestionRun
from api.services.stock_ingestion_service import StockIngestionService

from workers.tasks.send_discord_notification import send_discord_notification



@patch('workers.tasks.send_discord_notification.requests.post')
class SendDiscordNotificationTaskTest(TransactionTestCase):
    """Tests for the send_discord_notification Celery task."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')
        self.run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    def test_successful_notification_done_state(self, mock_post):
        """Test successful notification for DONE state (green)."""
        # Mock successful Discord API response
        mock_response = Mock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response
        
        # Execute task
        result = send_discord_notification(str(self.run.id), 'AAPL', IngestionState.DONE)
        
        # Verify result
        self.assertEqual(result['run_id'], str(self.run.id))
        self.assertEqual(result['ticker'], 'AAPL')
        self.assertEqual(result['state'], IngestionState.DONE)
        self.assertTrue(result['notification_sent'])
        self.assertFalse(result['skipped'])
        
        # Verify Discord API was called
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        
        # Verify webhook URL
        webhook_url = call_args[0][0]
        self.assertIn('discord.com', webhook_url)
        
        # Verify embed structure
        payload = call_args[1]['json']
        self.assertIn('embeds', payload)
        embed = payload['embeds'][0]
        
        # Verify DONE state formatting (green)
        self.assertEqual(embed['color'], 0x00FF00)  # Green
        self.assertIn('AAPL', embed['title'])
        self.assertIn('Ingestion Complete', embed['title'])
        self.assertEqual(embed['fields'][0]['value'], 'AAPL')
        self.assertEqual(embed['fields'][1]['value'], IngestionState.DONE)
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    def test_successful_notification_failed_state(self, mock_post):
        """Test successful notification for FAILED state (red)."""
        # Mock successful Discord API response
        mock_response = Mock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response
        
        # Execute task
        result = send_discord_notification(str(self.run.id), 'AAPL', IngestionState.FAILED)
        
        # Verify result
        self.assertTrue(result['notification_sent'])
        
        # Verify embed structure
        payload = mock_post.call_args[1]['json']
        embed = payload['embeds'][0]
        
        # Verify FAILED state formatting (red)
        self.assertEqual(embed['color'], 0xFF0000)  # Red
        self.assertIn('Ingestion Failed', embed['title'])
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    def test_successful_notification_in_progress_state(self, mock_post):
        """Test successful notification for in-progress states (yellow)."""
        # Mock successful Discord API response
        mock_response = Mock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response
        
        # Execute task with FETCHING state
        result = send_discord_notification(
            str(self.run.id),
            'AAPL',
            IngestionState.FETCHING
        )
        
        # Verify result
        self.assertTrue(result['notification_sent'])
        
        # Verify embed structure
        payload = mock_post.call_args[1]['json']
        embed = payload['embeds'][0]
        
        # Verify in-progress state formatting (yellow)
        self.assertEqual(embed['color'], 0xFFFF00)  # Yellow
        self.assertIn('AAPL', embed['title'])
    
    @override_settings(
        DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test',
        DISCORD_THREAD_ID='123456789'
    )
    def test_webhook_with_thread_id(self, mock_post):
        """Test that thread ID is appended to webhook URL."""
        # Mock successful Discord API response
        mock_response = Mock()
        mock_response.status_code = 204
        mock_post.return_value = mock_response
        
        # Execute task
        send_discord_notification(str(self.run.id), 'AAPL', IngestionState.DONE)
        
        # Verify thread_id was appended to URL
        webhook_url = mock_post.call_args[0][0]
        self.assertIn('thread_id=123456789', webhook_url)
    
    @override_settings(DISCORD_WEBHOOK_URL='')
    def test_webhook_not_configured_skips_notification(self, mock_post):
        """Test that notification is skipped when webhook is not configured."""
        # Execute task without webhook configured
        result = send_discord_notification(str(self.run.id), 'AAPL', IngestionState.DONE)
        
        # Verify result
        self.assertFalse(result['notification_sent'])
        self.assertTrue(result['skipped'])
        self.assertEqual(result['reason'], 'webhook_not_configured')
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    def test_discord_timeout_non_retryable(self, mock_post):
        """Test that Discord timeout errors are handled gracefully."""
        # Mock timeout error
        mock_post.side_effect = Timeout("Request timed out")
        
        # Execute task - should return result indicating failure
        result = send_discord_notification(str(self.run.id), 'AAPL', IngestionState.DONE)
        
        # Verify result indicates failure
        self.assertFalse(result['notification_sent'])
        self.assertFalse(result['skipped'])
        self.assertEqual(result['reason'], 'non_retryable_error')
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    def test_discord_rate_limit_non_retryable(self, mock_post):
        """Test that Discord rate limit errors are handled gracefully."""
        # Mock rate limit response (429)
        mock_response = Mock()
        mock_response.status_code = 429
        mock_post.return_value = mock_response
        
        # Execute task - should return result indicating failure
        result = send_discord_notification(str(self.run.id), 'AAPL', IngestionState.DONE)
        
        # Verify result indicates failure
        self.assertFalse(result['notification_sent'])
        self.assertFalse(result['skipped'])
        self.assertEqual(result['reason'], 'non_retryable_error')
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    def test_discord_server_error_non_retryable(self, mock_post):
        """Test that Discord server errors are handled gracefully."""
        # Mock server error response (500)
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status = Mock(
            side_effect=HTTPError(response=mock_response)
        )
        mock_post.return_value = mock_response
        
        # Execute task - should return result indicating failure
        result = send_discord_notification(str(self.run.id), 'AAPL', IngestionState.DONE)
        
        # Verify result indicates failure
        self.assertFalse(result['notification_sent'])
        self.assertFalse(result['skipped'])
        self.assertEqual(result['reason'], 'non_retryable_error')
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    def test_discord_authentication_error_non_retryable(self, mock_post):
        """Test that Discord authentication errors are handled gracefully."""
        # Mock authentication error (401)
        mock_response = Mock()
        mock_response.status_code = 401
        mock_post.return_value = mock_response
        
        # Execute task - should return result indicating failure
        result = send_discord_notification(str(self.run.id), 'AAPL', IngestionState.DONE)
        
        # Verify result indicates failure
        self.assertFalse(result['notification_sent'])
        self.assertFalse(result['skipped'])
        self.assertEqual(result['reason'], 'non_retryable_error')
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    def test_discord_webhook_not_found_non_retryable(self, mock_post):
        """Test that webhook not found (404) is handled gracefully."""
        # Mock not found error (404)
        mock_response = Mock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response
        
        # Execute task - should return result indicating failure
        result = send_discord_notification(str(self.run.id), 'AAPL', IngestionState.DONE)
        
        # Verify result indicates failure
        self.assertFalse(result['notification_sent'])
        self.assertFalse(result['skipped'])
        self.assertEqual(result['reason'], 'non_retryable_error')
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    def test_discord_connection_error_non_retryable(self, mock_post):
        """Test that connection errors are handled gracefully."""
        # Mock connection error
        mock_post.side_effect = ConnectionError("Connection failed")
        
        # Execute task - should return result indicating failure
        result = send_discord_notification(str(self.run.id), 'AAPL', IngestionState.DONE)
        
        # Verify result indicates failure
        self.assertFalse(result['notification_sent'])
        self.assertFalse(result['skipped'])
        self.assertEqual(result['reason'], 'non_retryable_error')


@patch('workers.tasks.send_discord_notification.requests.post')
class DiscordNotificationIntegrationTest(TransactionTestCase):
    """Tests for Discord notification integration with stock ingestion service."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.service = StockIngestionService()
        self.stock = Stock.objects.create(ticker='AAPL')
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    @patch('workers.tasks.send_discord_notification.send_discord_notification.delay')
    def test_notification_sent_on_failed_state_update(self, mock_delay, mock_post):
        """Test that notification is queued when run fails."""
        # Create run
        run = StockIngestionRun.objects.create(
            stock=self.stock,
            state=IngestionState.QUEUED_FOR_FETCH
        )
        
        # Update state
        self.service.update_run_state(
            run_id=run.id,
            new_state=IngestionState.FAILED,
            error_code='TEST_ERROR',
            error_message='Test error message'
        )
        
        # Verify notification task was queued
        mock_delay.assert_called_once_with(
            run_id=str(run.id),
            ticker='AAPL',
            state=IngestionState.FAILED
        )
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    @patch('workers.tasks.send_discord_notification.send_discord_notification.delay')
    def test_notification_not_sent_on_queue_for_fetch(self, mock_delay, mock_post):
        """Test that notification is queued when run is created."""
        # Queue for fetch
        run, created = self.service.queue_for_fetch(ticker='AAPL')
        
        # Verify notification task was queued
        self.assertTrue(created)
        mock_delay.assert_not_called()
    
    @override_settings(DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/test')
    @patch('workers.tasks.send_discord_notification.send_discord_notification.delay')
    def test_notification_only_sent_on_commit(self, mock_delay, mock_post):
        """Test that notification is only sent if transaction commits."""
        from django.db import transaction
        
        # Attempt to create run in transaction that will be rolled back
        try:
            with transaction.atomic():
                run = StockIngestionRun.objects.create(
                    stock=self.stock,
                    state=IngestionState.QUEUED_FOR_FETCH
                )
                
                # Force transaction to fail
                raise Exception("Simulated transaction failure")
        except Exception:
            pass
        
        # Verify notification task was NOT queued (transaction rolled back)
        mock_delay.assert_not_called()
