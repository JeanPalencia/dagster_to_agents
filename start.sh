#!/bin/bash
set -e

# Map Railway PostgreSQL env vars to Dagster's expected vars (if not already set)
if [ -n "$DATABASE_URL" ] && [ -z "$DAGSTER_PG_HOST" ]; then
  echo "Mapping DATABASE_URL to DAGSTER_PG_* variables..."
  eval "$(python3 -c "
from urllib.parse import urlparse
url = urlparse('$DATABASE_URL')
print(f'export DAGSTER_PG_HOST={url.hostname}')
print(f'export DAGSTER_PG_USERNAME={url.username}')
print(f'export DAGSTER_PG_PASSWORD={url.password}')
print(f'export DAGSTER_PG_DB={url.path.lstrip(\"/\")}')
")"
fi

# Start daemon in background
echo "Starting Dagster daemon..."
dagster-daemon run &
DAEMON_PID=$!

# Start webserver (Railway injects $PORT)
echo "Starting Dagster webserver on port ${PORT:-3000}..."
exec dagster-webserver -h 0.0.0.0 -p "${PORT:-3000}"
