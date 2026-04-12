"""
Claude Code CLI orchestration - replaces the manual Bedrock loop.

Receives a user message, runs `claude -p` with MCP config,
and returns the final text response plus session ID.
"""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from chat_agent.config import DAGSTER_SYSTEM_PROMPT

logger = logging.getLogger(__name__)

_MCP_CONFIG_PATH = "/app/mcp.json"
_MAX_TURNS = 10
_TIMEOUT_SECONDS = 120


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
        "--mcp-config", _MCP_CONFIG_PATH,
        "--system-prompt", DAGSTER_SYSTEM_PROMPT,
        "--allowedTools", "mcp__dagster__list_jobs,mcp__dagster__launch_job,mcp__dagster__get_run_status,mcp__dagster__get_recent_runs",
        "--dangerously-skip-permissions",
        "--max-turns", str(_MAX_TURNS),
        "--no-update-check",
    ]

    # Add session resume if we have a previous session
    if session_id:
        cmd.extend(["--resume", session_id])

    logger.info("Running claude CLI: %s", " ".join(cmd))

    try:
        # Run the subprocess
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Wait with timeout
        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=_TIMEOUT_SECONDS,
        )

        if process.returncode != 0:
            logger.error("claude CLI failed (exit %d): %s", process.returncode, stderr.decode())
            return f"Error: Claude CLI failed with exit code {process.returncode}", None

        # Parse JSON output
        output = stdout.decode()
        logger.debug("claude CLI output: %s", output)

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
        return f"Error: Request timed out after {_TIMEOUT_SECONDS} seconds", None
    except Exception as exc:
        logger.exception("Unexpected error running claude CLI: %s", exc)
        return f"Error: {exc}", None
