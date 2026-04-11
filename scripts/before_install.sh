#!/bin/bash

# BeforeInstall script for Dagster deployment
set -e

echo "Starting BeforeInstall phase..."

# Stop existing Dagster services if running
if systemctl is-active --quiet dagster-webserver; then
    echo "Stopping existing Dagster webserver..."
    systemctl stop dagster-webserver
fi

if systemctl is-active --quiet dagster-daemon; then
    echo "Stopping existing Dagster daemon..."
    systemctl stop dagster-daemon
fi

# Create application directory
mkdir -p /home/ubuntu/dagster
chown ubuntu:ubuntu /home/ubuntu/dagster

echo "BeforeInstall phase completed successfully"
