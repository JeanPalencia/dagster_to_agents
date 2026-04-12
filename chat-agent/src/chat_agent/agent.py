"""
Claude Agent SDK orchestration.

Receives a user message, runs Claude via the Agent SDK with Dagster tools,
and returns the final text response plus session ID.
"""
from __future__ import annotations

import logging
import os

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query
from claude_agent_sdk.types import McpStdioServerConfig

from chat_agent.config import DAGSTER_SYSTEM_PROMPT
from dagster_mcp.server import DAGSTER_TOOLS, dagster_server

logger = logging.getLogger(__name__)

_MAX_TURNS = 10

# Engram MCP server (persistent memory via stdio)
_engram_server = McpStdioServerConfig(
    command="engram",
    args=["mcp"],
    env={"ENGRAM_DATA_DIR": os.environ.get("ENGRAM_DATA_DIR", "/data/.engram")},
)

# Allowed tools: all dagster tools + key engram memory tools
_DAGSTER_TOOL_NAMES = [f"mcp__dagster__{t.name}" for t in DAGSTER_TOOLS]
_ENGRAM_TOOL_NAMES = [
    "mcp__engram__mem_save",
    "mcp__engram__mem_search",
    "mcp__engram__mem_context",
]


async def run_agent(user_message: str, session_id: str | None = None) -> tuple[str, str | None]:
    """
    Run Claude Agent SDK for a single user message.

    Returns:
        (response_text, new_session_id)
    """
    options = ClaudeAgentOptions(
        system_prompt=DAGSTER_SYSTEM_PROMPT,
        mcp_servers={"dagster": dagster_server, "engram": _engram_server},
        allowed_tools=_DAGSTER_TOOL_NAMES + _ENGRAM_TOOL_NAMES,
        permission_mode="bypassPermissions",
        max_turns=_MAX_TURNS,
        resume=session_id,
        env={
            "CLAUDE_CODE_USE_BEDROCK": "1",
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN", ""),
            "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            "ENGRAM_DATA_DIR": os.environ.get("ENGRAM_DATA_DIR", "/data/.engram"),
        },
    )

    logger.info("Running Claude Agent SDK for: %s", user_message[:100])

    response_text = ""
    new_session_id = None

    try:
        async for message in query(prompt=user_message, options=options):
            # Extract session ID from any message
            if hasattr(message, 'session_id') and message.session_id:
                new_session_id = message.session_id

            # Handle result messages using isinstance (not .type — SystemMessage has no .type)
            if isinstance(message, ResultMessage):
                if message.subtype == "success":
                    response_text = message.result or "(no response)"
                    logger.info("Agent completed successfully")
                elif message.subtype == "error_during_execution":
                    response_text = f"Error: {getattr(message, 'error_message', str(message))}"
                    logger.error("Agent error: %s", message.error_message)
                else:
                    response_text = f"Agent finished with: {message.subtype}"
                    logger.warning("Unexpected result subtype: %s", message.subtype)

        return response_text, new_session_id

    except Exception as exc:
        logger.exception("Unexpected error running Agent SDK: %s", exc)
        return f"Error: {exc}", None
