"""
Progress tracker for Google Chat message updates.

Maps Claude Agent SDK events to concise Spanish status messages and
updates the Google Chat message in real-time.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# Map of SDK events/tool names to concise Spanish messages
_STATUS_MESSAGES = {
    # General phases
    "start": "⏳ Analizando solicitud...",
    "thinking": "🤔 Pensando...",
    "planning": "📋 Planificando cambios...",
    "reading": "📖 Leyendo código...",
    "analyzing": "🔍 Analizando arquitectura...",
    "modifying": "⚙️ Modificando lógica...",
    "testing": "🧪 Probando cambios...",
    "validating": "✅ Validando resultados...",
    "creating_pr": "📝 Creando pull request...",
    "finishing": "🎯 Finalizando...",
    "error": "❌ Error procesando...",

    # Tool-specific (based on tool names that appear in SDK events)
    "Read": "📖 Leyendo archivos...",
    "Edit": "✏️ Editando código...",
    "Write": "📝 Escribiendo archivos...",
    "Bash": "⚙️ Ejecutando comandos...",
    "Grep": "🔍 Buscando en código...",
    "Glob": "📂 Explorando archivos...",
    "Agent": "🤖 Delegando a sub-agente...",

    # MCP tools
    "mcp__dagster": "📊 Consultando Dagster...",
    "mcp__spot2": "🗄️ Consultando base de datos...",
    "mcp__engram": "💾 Guardando en memoria...",

    # Git operations (detected from bash commands)
    "git_commit": "💾 Guardando cambios...",
    "git_push": "🚀 Subiendo código...",
    "git_branch": "🌿 Creando branch...",
    "gh_pr": "📝 Creando PR...",

    # Test harness
    "test_harness": "🧪 Ejecutando tests con datos reales...",
    "materialize": "⚡ Materializando flujo...",
}


def infer_status_from_event(event: Any) -> str | None:
    """
    Infer a status message from an SDK event.

    Args:
        event: SDK event object (TaskStartedMessage, TaskProgressMessage, etc.)

    Returns:
        Spanish status message or None if no match
    """
    # Log event type for debugging (first time we see it)
    event_type = type(event).__name__
    logger.debug(f"Processing event: {event_type}")

    # Check event type directly (SDK message types)
    if "TaskStarted" in event_type:
        return _STATUS_MESSAGES["start"]
    if "TaskProgress" in event_type:
        return _STATUS_MESSAGES["planning"]
    if "Thinking" in event_type or hasattr(event, "thinking"):
        return _STATUS_MESSAGES["thinking"]

    # Check for tool use in content blocks
    if hasattr(event, "content") and isinstance(event.content, list):
        for block in event.content:
            # ToolUseBlock
            if hasattr(block, "name"):
                tool_name = block.name
                if tool_name in _STATUS_MESSAGES:
                    return _STATUS_MESSAGES[tool_name]

                # MCP tools
                if tool_name.startswith("mcp__"):
                    parts = tool_name.split("__")
                    if len(parts) >= 2:
                        prefix = parts[1]
                        key = f"mcp__{prefix}"
                        if key in _STATUS_MESSAGES:
                            return _STATUS_MESSAGES[key]

                # Bash tool - try to detect command
                if tool_name == "Bash" and hasattr(block, "input"):
                    cmd_input = block.input
                    if isinstance(cmd_input, dict):
                        cmd = cmd_input.get("command", "").lower()
                    else:
                        cmd = str(cmd_input).lower()

                    if "git commit" in cmd:
                        return _STATUS_MESSAGES["git_commit"]
                    if "git push" in cmd:
                        return _STATUS_MESSAGES["git_push"]
                    if "git checkout" in cmd or "git branch" in cmd:
                        return _STATUS_MESSAGES["git_branch"]
                    if "gh pr create" in cmd:
                        return _STATUS_MESSAGES["gh_pr"]
                    if "test_harness" in cmd:
                        return _STATUS_MESSAGES["test_harness"]
                    if "materialize" in cmd or "dg.materialize" in cmd:
                        return _STATUS_MESSAGES["materialize"]

                    # Fallback for Bash
                    return _STATUS_MESSAGES["Bash"]

    # Check for text content (assistant messages)
    if hasattr(event, "content") and isinstance(event.content, str):
        text = event.content.lower()
        if "test" in text or "validat" in text:
            return _STATUS_MESSAGES["testing"]
        if "analy" in text or "understand" in text:
            return _STATUS_MESSAGES["analyzing"]
        if "modif" in text or "edit" in text or "chang" in text:
            return _STATUS_MESSAGES["modifying"]
        if "creat" in text and ("pr" in text or "pull request" in text):
            return _STATUS_MESSAGES["creating_pr"]

    # Legacy: check for direct attributes (backwards compat)
    if hasattr(event, "tool_name"):
        tool_name = event.tool_name
        if tool_name in _STATUS_MESSAGES:
            return _STATUS_MESSAGES[tool_name]

    return None


def infer_status_from_tool(tool_name: str) -> str:
    """
    Get status message for a specific tool.

    Args:
        tool_name: Name of the tool being used

    Returns:
        Spanish status message
    """
    if tool_name in _STATUS_MESSAGES:
        return _STATUS_MESSAGES[tool_name]

    # Fallback for unknown tools
    return f"⚙️ Usando {tool_name}..."


def get_initial_message() -> str:
    """Get the initial status message shown to user."""
    return _STATUS_MESSAGES["start"]
