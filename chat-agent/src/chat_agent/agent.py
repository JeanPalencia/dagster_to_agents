"""
Claude Agent SDK orchestration.

## Adding a new capability

MCP tool (external server):
    "my_server": {
        "kind": "mcp",
        "server": McpStdioServerConfig(command="...", args=["..."]),
        "tools": ["mcp__my_server__tool_a"],
        "internal": False,
    },

Claude Code sub-agent (defined in .claude/agents/<name>.md):
    "my_agent": {
        "kind": "subagent",
        "description": "What this sub-agent does (shown to users)",
        "internal": False,
    },

That's it — mcp_servers, allowed_tools, and system prompt update automatically.
"""
from __future__ import annotations

import logging
import os

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query
from claude_agent_sdk.types import McpStdioServerConfig

from chat_agent.config import build_system_prompt
from dagster_mcp.server import DAGSTER_TOOLS, dagster_server

logger = logging.getLogger(__name__)

_MAX_TURNS = 50

# Central registry of all capabilities.
# kind="mcp"      → MCP server with tools
# kind="subagent" → Claude Code sub-agent defined in .claude/agents/<name>.md
# internal=False  → user-facing (shown when asked "what can you do?")
# internal=True   → used by the agent silently
_REGISTRY: dict[str, dict] = {
    "dagster": {
        "kind": "mcp",
        "server": dagster_server,
        "tools": [f"mcp__dagster__{t.name}" for t in DAGSTER_TOOLS],
        "internal": False,
    },
    "engram": {
        "kind": "mcp",
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
    "spot2": {
        "kind": "mcp",
        "server": McpStdioServerConfig(
            command="npx",
            args=[
                "mcp-remote",
                "https://mcp.ai.spot2.mx/mcp",
                "--header",
                f"Authorization: Bearer {os.environ.get('SPOT2_API_KEY', '')}",
            ],
        ),
        "tools": [],  # dynamic tools allowed via mcp__spot2__* wildcard
        "description": (
            "Consultas a bases de datos (MySQL y PostgreSQL). "
            "Úsalo para verificar datos en tablas, validar resultados de jobs, "
            "consultar fuentes de datos o explorar schemas."
        ),
        "internal": False,
    },
    "logic_modifier": {
        "kind": "subagent",
        "description": (
            "Modifica la lógica de cálculo de columnas existentes en tablas del lakehouse. "
            "Preserva el schema, sigue ARCHITECTURE.md, valida con backfill antes de mergear."
        ),
        "internal": False,
    },
    "test_specialist": {
        "kind": "subagent",
        "description": (
            "Valida cambios de PRs ejecutando flujos contra datos reales (dg.materialize()). "
            "Diseña assertions específicas al cambio y reporta resultados."
        ),
        "internal": True,  # Triggered by GitHub Actions, not user-facing
    },
}

# Derived from registry — don't edit manually
_MCP_SERVERS = {
    name: entry["server"]
    for name, entry in _REGISTRY.items()
    if entry["kind"] == "mcp" and entry.get("server") is not None
}
_ALLOWED_TOOLS = [
    tool
    for entry in _REGISTRY.values()
    if entry["kind"] == "mcp"
    for tool in entry["tools"]
] + ["mcp__spot2__*"]  # spot2 tools are dynamic — wildcard allows all of them

# User-facing capabilities for system prompt
_USER_CAPABILITIES = [
    # MCP tools with description: show "name: description"
    *(
        f"{name}: {entry['description']}"
        for name, entry in _REGISTRY.items()
        if entry["kind"] == "mcp" and not entry["internal"] and "description" in entry
    ),
    # MCP tools without description: show individual tool names
    *(
        tool.split("__", 2)[-1]
        for entry in _REGISTRY.values()
        if entry["kind"] == "mcp" and not entry["internal"] and "description" not in entry
        for tool in entry["tools"]
    ),
    # Sub-agents: show as "name: description"
    *(
        f"{name}: {entry['description']}"
        for name, entry in _REGISTRY.items()
        if entry["kind"] == "subagent" and not entry["internal"]
    ),
]

_SYSTEM_PROMPT = build_system_prompt(_USER_CAPABILITIES)


async def run_agent(
    user_message: str,
    session_id: str | None = None,
    progress_callback: callable | None = None,
) -> tuple[str, str | None]:
    """
    Run Claude Agent SDK for a single user message.

    Args:
        user_message: User's input text
        session_id: Optional session ID to resume
        progress_callback: Optional async callback(event) called for each SDK event

    Returns:
        (response_text, new_session_id)
    """
    options = ClaudeAgentOptions(
        system_prompt=_SYSTEM_PROMPT,
        mcp_servers=_MCP_SERVERS,
        # bypassPermissions approves all tools automatically.
        # allowed_tools is NOT passed — it acts as exclusive whitelist and blocks dynamic tools.
        permission_mode="bypassPermissions",
        max_turns=_MAX_TURNS,
        resume=session_id,
        cwd="/app/repo",  # full repo clone — CLI reads .claude/agents/ and can git push
        env={
            "CLAUDE_CODE_USE_BEDROCK": "1",
            "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            # Bedrock API key (permanent, no expiry) — takes priority over IAM credentials
            "AWS_BEARER_TOKEN_BEDROCK": os.environ.get("AWS_BEARER_TOKEN_BEDROCK", ""),
            # IAM credentials fallback (STS temporary, expire every few hours)
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN", ""),
            "ENGRAM_DATA_DIR": os.environ.get("ENGRAM_DATA_DIR", "/data/.engram"),
            "SPOT2_API_KEY": os.environ.get("SPOT2_API_KEY", ""),
            "GITHUB_TOKEN": os.environ.get("GITHUB_TOKEN", ""),  # For gh CLI (PR creation)
            # Ensure engram binary (/usr/local/bin) and venv are on PATH for the CLI subprocess
            "PATH": "/usr/local/bin:/app/.venv/bin:" + os.environ.get("PATH", ""),
        },
    )

    logger.info("Running Claude Agent SDK for: %s", user_message[:100])

    response_text = ""
    new_session_id = None

    try:
        async for message in query(prompt=user_message, options=options):
            # Call progress callback for every event (if provided)
            if progress_callback:
                try:
                    await progress_callback(message)
                except Exception as cb_exc:
                    logger.warning("Progress callback error: %s", cb_exc)

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
