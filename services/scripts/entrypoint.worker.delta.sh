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
if [ "$APP_ENV" = "prod" ] || [ "$APP_ENV" = "stage" ] || [ "$APP_ENV" = "dev" ]
then
    # Start Celery worker for queue_for_delta
    echo "Starting Celery worker for queue_for_delta (concurrency=1)... in $APP_ENV mode"

    # Celery configuration - concurrency 1 (strict requirement)
    exec celery -A config worker \
        --hostname=delta-worker@%h \
        --loglevel=info \
        --concurrency=1 \
        --queues=queue_for_delta \
        --max-tasks-per-child=50 \
        --time-limit=1800 \
        --soft-time-limit=1500 \
        --prefetch-multiplier=1
else
    echo "Error: Invalid APP_ENV='$APP_ENV'. Expected: prod, stage, or dev"
    exit 1
fi