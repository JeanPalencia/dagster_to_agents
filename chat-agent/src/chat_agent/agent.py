"""
Claude Code CLI orchestration - replaces the manual Bedrock loop.

Receives a user message, runs `claude -p` with MCP config,
and returns the final text response plus session ID.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

from chat_agent.config import DAGSTER_SYSTEM_PROMPT

logger = logging.getLogger(__name__)

_MCP_CONFIG_PATH = "/app/mcp.json"
_MAX_TURNS = 10
_TIMEOUT_SECONDS = 30  # Reduced from 120 - Google Chat webhooks timeout at 30s anyway


async def run_agent(user_message: str, session_id: str | None = None) -> tuple[str, str | None]:
    """
    Run Claude Code CLI for a single user message.

    Returns:
        (response_text, new_session_id)
    """
    # Build the command
    cmd = [
        "claude",
        "-p", user_message,
        "--output-format", "json",
        "--bare",  # Skip auto-discovery for faster startup
        "--mcp-config", _MCP_CONFIG_PATH,
        "--system-prompt", DAGSTER_SYSTEM_PROMPT,
        "--allowedTools", "mcp__dagster__list_jobs,mcp__dagster__launch_job,mcp__dagster__get_run_status,mcp__dagster__get_recent_runs",
        "--dangerously-skip-permissions",
        "--max-turns", str(_MAX_TURNS),
    ]

    # Add session resume if we have a previous session
    if session_id:
        cmd.extend(["--resume", session_id])

    logger.info("Running claude CLI: %s", " ".join(cmd))

    # Build environment for subprocess - inherit current env + Bedrock config
    env = os.environ.copy()
    env.update({
        "CLAUDE_CODE_USE_BEDROCK": "1",
        "AWS_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        # These should already be in os.environ from Railway, but be explicit
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN", ""),
        # Pin model version for Bedrock
        "ANTHROPIC_DEFAULT_SONNET_MODEL": "us.anthropic.claude-sonnet-4-6",
    })

    logger.info("Subprocess env: CLAUDE_CODE_USE_BEDROCK=%s, AWS_REGION=%s, AWS_ACCESS_KEY_ID=%s",
                env.get("CLAUDE_CODE_USE_BEDROCK"),
                env.get("AWS_REGION"),
                "***" if env.get("AWS_ACCESS_KEY_ID") else "NOT SET")

    try:
        # Run the subprocess with explicit env
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

        # Wait with timeout
        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=_TIMEOUT_SECONDS,
        )

        # Always log stderr for debugging
        stderr_text = stderr.decode()
        if stderr_text:
            logger.info("claude CLI stderr: %s", stderr_text[:500])  # First 500 chars

        if process.returncode != 0:
            logger.error("claude CLI failed (exit %d): %s", process.returncode, stderr_text)
            return f"Error: Claude CLI failed with exit code {process.returncode}", None

        # Parse JSON output
        output = stdout.decode()
        logger.info("claude CLI stdout (%d bytes): %s", len(output), output[:200] if len(output) > 200 else output)

        try:
            result = json.loads(output)
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse claude CLI output as JSON: %s", exc)
            return f"Error: Failed to parse Claude response: {exc}", None

        # Extract result text and session_id
        response_text = result.get("result", "(no response)")
        new_session_id = result.get("session_id")

        return response_text, new_session_id

    except asyncio.TimeoutError:
        logger.error("claude CLI timed out after %d seconds", _TIMEOUT_SECONDS)
        # Kill the process
        try:
            process.kill()
            await process.wait()
        except Exception as kill_exc:
            logger.warning("Failed to kill timed-out process: %s", kill_exc)
        return f"Error: Request timed out after {_TIMEOUT_SECONDS} seconds", None
    except Exception as exc:
        logger.exception("Unexpected error running claude CLI: %s", exc)
        return f"Error: {exc}", None
