#!/bin/bash
set -x

export CLAUDE_CODE_USE_BEDROCK=1
export AWS_REGION=us-east-1
export ANTHROPIC_DEFAULT_SONNET_MODEL=us.anthropic.claude-sonnet-4-6

echo "=== Environment ==="
echo "CLAUDE_CODE_USE_BEDROCK=$CLAUDE_CODE_USE_BEDROCK"
echo "AWS_REGION=$AWS_REGION"
echo "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:0:10}..."
echo "HOME=$HOME"
echo "USER=$USER"
echo "PWD=$PWD"

echo ""
echo "=== Testing claude CLI ==="
claude --version

echo ""
echo "=== Running claude -p with --bare ==="
timeout 30 claude -p "list jobs" \
  --output-format json \
  --bare \
  --verbose \
  --mcp-config /app/mcp.json \
  --system-prompt "You are Dagster Agent." \
  --allowedTools "mcp__dagster__list_jobs" \
  --dangerously-skip-permissions \
  --max-turns 3 \
  2>&1

EXIT_CODE=$?
echo ""
echo "Exit code: $EXIT_CODE"
