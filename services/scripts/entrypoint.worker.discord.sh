#!/bin/sh

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Wait for all dependencies (PostgreSQL, RabbitMQ, Redis) to be ready
if [ -f "$SCRIPT_DIR/wait_for_dependencies.sh" ]; then
    "$SCRIPT_DIR/wait_for_dependencies.sh"
else
    echo "Error: wait_for_dependencies.sh not found in $SCRIPT_DIR"
    exit 1
fi

# Production setup
if [ "$APP_ENV" = "prod" ] || [ "$APP_ENV" = "stage" ]
then
    # Start Celery worker for send_discord_notifications
    echo "Starting Celery worker for send_discord_notifications... in production mode"

    # Production configuration - concurrency 2
    exec celery -A config worker \
        --hostname=discord-worker@%h \
        --loglevel=info \
        --concurrency=2 \
        --queues=send_discord_notifications \
        --max-tasks-per-child=50 \
        --time-limit=1800 \
        --soft-time-limit=1500 \
        --prefetch-multiplier=1
else
    # Development mode
    echo "Running worker_discord in development mode..."
    exec "$@"
fi