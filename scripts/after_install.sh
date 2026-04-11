#!/bin/bash

# AfterInstall script for Dagster deployment
set -e

echo "Starting AfterInstall phase..."

cd /home/ubuntu/dagster/dagster-pipeline

# Install UV if not already installed
if ! command -v uv &> /dev/null; then
    echo "Installing UV package manager..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    mv /root/.local/bin/uv /usr/bin/uv
fi

if ! command -v oauth2-proxy &> /dev/null; then
    echo "Installing oauthproxy..."
    wget https://github.com/oauth2-proxy/oauth2-proxy/releases/download/v7.12.0/oauth2-proxy-v7.12.0.linux-arm64.tar.gz
    tar -xvf oauth2-proxy-v7.12.0.linux-arm64.tar.gz
    mv oauth2-proxy-v7.12.0.linux-arm64/oauth2-proxy /usr/bin/oauth2-proxy
fi


# Set up UV environment
echo "Setting up UV environment..."
su - ubuntu -c "cd /home/ubuntu/dagster/dagster-pipeline && uv sync"

# Create systemd service files
echo "Creating systemd service files..."

# Dagster webserver service
cat > /etc/systemd/system/dagster-webserver.service << 'EOF'
[Unit]
Description=Dagster Webserver
After=network.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/dagster/dagster-pipeline
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/ubuntu/dagster/dagster-pipeline/.venv/bin
Environment=DAGSTER_HOME=/home/ubuntu/dagster/dagster-pipeline/
EnvironmentFile=-/home/ubuntu/dagster/dagster-pipeline/.env.dagster
ExecStart=/home/ubuntu/dagster/dagster-pipeline/.venv/bin/dagster-webserver -h 0.0.0.0 -p 3001
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Dagster daemon service
cat > /etc/systemd/system/dagster-daemon.service << 'EOF'
[Unit]
Description=Dagster Daemon
After=network.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/dagster/dagster-pipeline
Environment=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/ubuntu/dagster/dagster-pipeline/.venv/bin
Environment=DAGSTER_HOME=/home/ubuntu/dagster/dagster-pipeline/
EnvironmentFile=-/home/ubuntu/dagster/dagster-pipeline/.env.dagster
ExecStart=/home/ubuntu/dagster/dagster-pipeline/.venv/bin/dagster-daemon run
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# oauth2-proxy service
cat > /etc/systemd/system/oauth2-proxy.service << 'EOF' 
[Unit]
Description=OAuth2 Proxy
After=network.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
ExecStart=/usr/bin/oauth2-proxy --config=/home/ubuntu/oauth2-proxy.cfg
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=oauth2-proxy

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and enable services
systemctl daemon-reload
systemctl enable dagster-webserver
systemctl enable dagster-daemon
systemctl enable oauth2-proxy

# Get from parameter store the configuration for oauth2-proxy
rm -fr /home/ubuntu/oauth2-proxy.cfg
aws ssm get-parameter --name /dagster/oauth_proxy.cfg --with-decryption --query Parameter.Value --output text > /home/ubuntu/oauth2-proxy.cfg
chown ubuntu:ubuntu /home/ubuntu/oauth2-proxy.cfg

# Dagster Postgres (dagster.yaml storage.postgres): variables desde SSM -> .env.dagster
# Parámetros dedicados (crear los cuatro en Parameter Store bajo /dagster_ops/).
ENV_DAGSTER=/home/ubuntu/dagster/dagster-pipeline/.env.dagster
{
  printf 'DAGSTER_PG_HOST=%s\n' "$(aws ssm get-parameter --name /dagster_ops/pg_host --query Parameter.Value --output text)"
  printf 'DAGSTER_PG_USERNAME=%s\n' "$(aws ssm get-parameter --name /dagster_ops/pg_username --with-decryption --query Parameter.Value --output text)"
  printf 'DAGSTER_PG_PASSWORD=%s\n' "$(aws ssm get-parameter --name /dagster_ops/pg_password --with-decryption --query Parameter.Value --output text)"
  printf 'DAGSTER_PG_DB=%s\n' "$(aws ssm get-parameter --name /dagster_ops/pg_database --query Parameter.Value --output text)"
} > "$ENV_DAGSTER"
chmod 600 "$ENV_DAGSTER"
chown ubuntu:ubuntu "$ENV_DAGSTER"

# Set proper permissions
chown -R ubuntu:ubuntu /home/ubuntu/dagster
chmod +x /home/ubuntu/dagster/scripts/*.sh

echo "AfterInstall phase completed successfully"
