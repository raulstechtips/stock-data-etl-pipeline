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

# Start Celery worker
echo "Starting Celery worker..."

if [ "$APP_ENV" = "prod" ] || [ "$APP_ENV" = "stage" ]; then
    # Production configuration
    exec celery -A config worker \
        --loglevel=info \
        --concurrency=4 \
        --max-tasks-per-child=50 \
        --time-limit=1800 \
        --soft-time-limit=1500 \
        --prefetch-multiplier=1
else
    # Development configuration
    exec celery -A config worker \
        --loglevel=info \
        --concurrency=2 \
        --max-tasks-per-child=10 \
        --time-limit=1800 \
        --soft-time-limit=1500 \
        --prefetch-multiplier=1
fi
