"""
Claude tool_use orchestration loop.

Receives a user message, runs the agent loop (calling Dagster tools as needed),
and returns the final text response.
"""
from __future__ import annotations

import json

import anthropic

from chat_agent import tools
from chat_agent.config import ANTHROPIC_API_KEY

_SYSTEM_PROMPT = """\
You are Dagster Agent, an AI assistant that helps users manage Dagster data pipelines
deployed on Railway. You can launch jobs, check run status, and list available jobs.

Respond concisely in the same language the user used (Spanish or English).
When you launch a job, always include the run URL so the user can track it.
When reporting a run status, summarize: status, succeeded/failed steps, and the URL.
If asked about a topic outside of Dagster jobs/runs, politely say you only handle pipeline operations.
"""

_TOOL_DEFS = [
    {
        "name": "list_jobs",
        "description": "List all available Dagster jobs with their descriptions.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "launch_job",
        "description": "Launch a Dagster job by name. Returns run_id and a URL to track the run.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_name": {
                    "type": "string",
                    "description": "The exact job name (e.g. amenity_desc_consistency_job)",
                }
            },
            "required": ["job_name"],
        },
    },
    {
        "name": "get_run_status",
        "description": "Get the current status of a Dagster run by run_id.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {
                    "type": "string",
                    "description": "The Dagster run ID (UUID)",
                }
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "get_recent_runs",
        "description": "Get recent Dagster runs with their status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Number of recent runs to return (default: 5)",
                    "default": 5,
                }
            },
            "required": [],
        },
    },
]

_MAX_ITERATIONS = 10  # safety cap to avoid infinite loops


async def _execute_tool(name: str, tool_input: dict) -> str:
    """Dispatch a tool call and return a JSON string result."""
    if name == "list_jobs":
        result = tools.list_jobs()
    elif name == "launch_job":
        result = await tools.launch_job(tool_input["job_name"])
    elif name == "get_run_status":
        result = await tools.get_run_status(tool_input["run_id"])
    elif name == "get_recent_runs":
        limit = tool_input.get("limit", 5)
        result = await tools.get_recent_runs(limit)
    else:
        result = {"error": f"Unknown tool: {name}"}
    return json.dumps(result)


async def run_agent(user_message: str) -> str:
    """
    Run the Claude tool_use loop for a single user message.
    Returns the final text response.
    """
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    messages = [{"role": "user", "content": user_message}]

    for _ in range(_MAX_ITERATIONS):
        response = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=_SYSTEM_PROMPT,
            tools=_TOOL_DEFS,
            messages=messages,
        )

        # Collect text content so far (may be empty on tool_use turns)
        text_parts = [b.text for b in response.content if b.type == "text"]

        if response.stop_reason == "end_turn":
            return "\n".join(text_parts) or "(no response)"

        if response.stop_reason != "tool_use":
            return "\n".join(text_parts) or f"Unexpected stop_reason: {response.stop_reason}"

        # --- Process all tool_use blocks ---
        messages.append({"role": "assistant", "content": response.content})

        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            tool_output = await _execute_tool(block.name, block.input)
            tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": tool_output,
                }
            )

        messages.append({"role": "user", "content": tool_results})

    return "Agent reached maximum iterations without a final response."
