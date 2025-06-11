#!/bin/bash

# Toast ETL Pipeline - Production Server Startup Script
# Uses Gunicorn for production-ready Flask deployment

set -e

# Configuration
PORT=${PORT:-8080}
WORKERS=${WORKERS:-2}
TIMEOUT=${TIMEOUT:-3600}
LOG_LEVEL=${LOG_LEVEL:-info}

# Start Gunicorn server
exec gunicorn \
    --bind "0.0.0.0:$PORT" \
    --workers $WORKERS \
    --worker-class sync \
    --timeout $TIMEOUT \
    --keep-alive 2 \
    --max-requests 1000 \
    --max-requests-jitter 50 \
    --preload \
    --log-level $LOG_LEVEL \
    --access-logfile - \
    --error-logfile - \
    "src.server.app:app" 