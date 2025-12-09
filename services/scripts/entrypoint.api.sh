#!/bin/sh

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -f "$SCRIPT_DIR/wait_for_db.sh" ]; then
    "$SCRIPT_DIR/wait_for_db.sh"
else
    echo "Error: wait_for_db.sh not found in $SCRIPT_DIR"
    exit 1
fi

# Production setup
if [ "$APP_ENV" = "prod" ] || [ "$APP_ENV" = "stage" ]
then
    echo "Running api in production mode..."
    
    
    # Apply database migrations
    echo "Applying database migrations..."
    python manage.py migrate
    
    # Collect static files
    echo "Collecting static files..."
    python manage.py collectstatic --noinput
    
    # Start production server
    echo "Starting production server..."
    exec gunicorn config.asgi:application \
        --workers 4 \
        --worker-class uvicorn.workers.UvicornWorker \
        --bind 0.0.0.0:8000 \
        --timeout 120 \
        --keep-alive 2 \
        --max-requests 1000 \
        --max-requests-jitter 100 \
        --log-level info \
        --access-logfile - \
        --error-logfile -

else
    # Development mode
    echo "Running api in development mode..."
    exec "$@"
fi