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
if [ "$APP_ENV" = "dev" ]
then
    # This entrypoint is for development only
    # It listens to both queue_for_fetch and send_discord_notifications queues
    echo "Starting Celery worker for development (fetch + discord queues)..."

    # Development configuration - listens to fetch and discord queues with concurrency 4
    exec celery -A config worker \
        --hostname=dev-worker@%h \
        --loglevel=info \
        --concurrency=4 \
        --queues=queue_for_fetch,send_discord_notifications \
        --max-tasks-per-child=50 \
        --time-limit=1800 \
        --soft-time-limit=1500 \
        --prefetch-multiplier=1
else
    # Development mode
    echo "This should not be used in production"
    exit 1
fi