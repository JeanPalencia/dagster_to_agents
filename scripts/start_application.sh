#!/bin/bash

# ApplicationStart script for Dagster deployment
set -e

echo "Starting ApplicationStart phase..."

# Start Dagster services
echo "Starting Dagster webserver..."
systemctl start dagster-webserver

echo "Starting Dagster daemon..."
systemctl start dagster-daemon

echo "Starting oauth2-proxy..."
systemctl start oauth2-proxy

# Wait a moment for services to start
sleep 5

# Check service status
echo "Checking service status..."
if systemctl is-active --quiet dagster-webserver; then
    echo "✓ Dagster webserver is running"
else
    echo "✗ Dagster webserver failed to start"
    systemctl status dagster-webserver
    exit 1
fi

if systemctl is-active --quiet dagster-daemon; then
    echo "✓ Dagster daemon is running"
else
    echo "✗ Dagster daemon failed to start"
    systemctl status dagster-daemon
    exit 1
fi

echo "Checking oauth2-proxy status..."
if systemctl is-active --quiet oauth2-proxy; then
    echo "✓ oauth2-proxy is running"
else
    echo "✗ oauth2-proxy failed to start"
    systemctl status oauth2-proxy
    exit 1
fi


echo "ApplicationStart phase completed successfully"
