#!/bin/bash

# ValidateService script for Dagster deployment
set -e

echo "Starting ValidateService phase..."

# Check if services are running
echo "Validating service status..."

if ! systemctl is-active --quiet dagster-webserver; then
    echo "✗ Dagster webserver is not running"
    exit 1
fi

if ! systemctl is-active --quiet dagster-daemon; then
    echo "✗ Dagster daemon is not running"
    exit 1
fi

# Check if webserver is responding
echo "Testing webserver connectivity..."
max_attempts=30
attempt=1

while [ $attempt -le $max_attempts ]; do
    if curl -f -s http://localhost:3001 > /dev/null 2>&1; then
        echo "✓ Dagster webserver is responding on port 3001"
        break
    fi
    
    if [ $attempt -eq $max_attempts ]; then
        echo "✗ Dagster webserver failed to respond after $max_attempts attempts"
        exit 1
    fi
    
    echo "Attempt $attempt/$max_attempts: Webserver not responding yet, waiting..."
    sleep 10
    attempt=$((attempt + 1))
done

# Check service logs for any errors
echo "Checking service logs for errors..."
webserver_errors=$(journalctl -u dagster-webserver --since "5 minutes ago" | grep -i error | wc -l)
daemon_errors=$(journalctl -u dagster-daemon --since "5 minutes ago" | grep -i error | wc -l)

if [ $webserver_errors -gt 0 ]; then
    echo "⚠ Warning: Found $webserver_errors errors in webserver logs"
fi

if [ $daemon_errors -gt 0 ]; then
    echo "⚠ Warning: Found $daemon_errors errors in daemon logs"
fi

echo "✓ All services are running and responding correctly"
echo "ValidateService phase completed successfully"
