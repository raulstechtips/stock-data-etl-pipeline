#!/bin/sh

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -f "$SCRIPT_DIR/wait_for_db.sh" ]; then
    "$SCRIPT_DIR/wait_for_db.sh"
else
    echo "Error: wait_for_db.sh not found in $SCRIPT_DIR"
    exit 1
fi

exec "$@"
