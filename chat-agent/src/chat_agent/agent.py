"""
Claude Agent SDK orchestration.

## Adding a new capability

Add an entry to _MCP_REGISTRY:

    "my_server": {
        "server": McpStdioServerConfig(command="...", args=["..."]),
        "tools": ["mcp__my_server__tool_a", "mcp__my_server__tool_b"],
        "internal": False,  # True = agent uses it but doesn't expose it to users
    },

That's it — allowed_tools, mcp_servers, and system prompt update automatically.
"""
from __future__ import annotations

import logging
import os

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query
from claude_agent_sdk.types import McpStdioServerConfig

from chat_agent.config import build_system_prompt
from dagster_mcp.server import DAGSTER_TOOLS, dagster_server

logger = logging.getLogger(__name__)

_MAX_TURNS = 10

# Central registry of all MCP capabilities.
# internal=False → user-facing (shown when asked "what can you do?")
# internal=True  → used by the agent silently, not exposed to users
_MCP_REGISTRY: dict[str, dict] = {
    "dagster": {
        "server": dagster_server,
        "tools": [f"mcp__dagster__{t.name}" for t in DAGSTER_TOOLS],
        "internal": False,
    },
    "engram": {
        "server": McpStdioServerConfig(
            command="engram",
            args=["mcp"],
            env={"ENGRAM_DATA_DIR": os.environ.get("ENGRAM_DATA_DIR", "/data/.engram")},
        ),
        "tools": [
            "mcp__engram__mem_save",
            "mcp__engram__mem_search",
            "mcp__engram__mem_context",
        ],
        "internal": True,
    },
}

# Derived from registry
_MCP_SERVERS = {name: entry["server"] for name, entry in _MCP_REGISTRY.items()}
_ALLOWED_TOOLS = [tool for entry in _MCP_REGISTRY.values() for tool in entry["tools"]]

# User-facing tool names for the system prompt (strip mcp__<server>__ prefix)
_USER_TOOL_NAMES = [
    tool.split("__", 2)[-1]
    for entry in _MCP_REGISTRY.values()
    if not entry["internal"]
    for tool in entry["tools"]
]

_SYSTEM_PROMPT = build_system_prompt(_USER_TOOL_NAMES)


async def run_agent(user_message: str, session_id: str | None = None) -> tuple[str, str | None]:
    """
    Run Claude Agent SDK for a single user message.

    Returns:
        (response_text, new_session_id)
    """
    options = ClaudeAgentOptions(
        system_prompt=_SYSTEM_PROMPT,
        mcp_servers=_MCP_SERVERS,
        allowed_tools=_ALLOWED_TOOLS,
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
            if hasattr(message, "session_id") and message.session_id:
                new_session_id = message.session_id

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
