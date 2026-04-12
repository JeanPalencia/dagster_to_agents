"""
Dagster tools for Claude Agent SDK.

Exposes 4 Dagster tools using the @tool decorator pattern.
These run in-process (no separate MCP server needed).
"""
from __future__ import annotations

import json

from claude_agent_sdk import create_sdk_mcp_server, tool

from chat_agent import tools


@tool(
    "list_jobs",
    "List all available Dagster jobs with their descriptions.",
    {},  # No input parameters
)
async def list_jobs_tool(args: dict) -> dict:
    """List available Dagster jobs."""
    result = tools.list_jobs()
    return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}


@tool(
    "launch_job",
    "Launch a Dagster job by name. Returns run_id and a URL to track the run.",
    {
        "type": "object",
        "properties": {
            "job_name": {
                "type": "string",
                "description": "The exact job name (e.g. amenity_desc_consistency_job)",
            }
        },
        "required": ["job_name"],
    },
)
async def launch_job_tool(args: dict) -> dict:
    """Launch a Dagster job."""
    result = await tools.launch_job(args["job_name"])
    return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}


@tool(
    "get_run_status",
    "Get the current status of a Dagster run by run_id.",
    {
        "type": "object",
        "properties": {
            "run_id": {
                "type": "string",
                "description": "The Dagster run ID (UUID)",
            }
        },
        "required": ["run_id"],
    },
)
async def get_run_status_tool(args: dict) -> dict:
    """Get Dagster run status."""
    result = await tools.get_run_status(args["run_id"])
    return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}


@tool(
    "get_recent_runs",
    "Get recent Dagster runs with their status.",
    {
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
)
async def get_recent_runs_tool(args: dict) -> dict:
    """Get recent Dagster runs."""
    limit = args.get("limit", 5)
    result = await tools.get_recent_runs(limit)
    return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}


# Create the MCP server with all 4 tools
dagster_server = create_sdk_mcp_server(
    name="dagster",
    version="1.0.0",
    tools=[list_jobs_tool, launch_job_tool, get_run_status_tool, get_recent_runs_tool],
)
