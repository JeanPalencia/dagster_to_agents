"""
Dagster MCP Server - exposes 4 Dagster tools via MCP stdio protocol.

Tools:
- list_jobs: List available Dagster jobs
- launch_job: Launch a job by name
- get_run_status: Get status of a run by ID
- get_recent_runs: Get recent runs with status

This server reuses the existing tool implementations from chat_agent.tools.
"""
from __future__ import annotations

import json
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from chat_agent import tools

# Create the MCP server instance
server = Server("dagster")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """Return the list of available tools."""
    return [
        Tool(
            name="list_jobs",
            description="List all available Dagster jobs with their descriptions.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="launch_job",
            description="Launch a Dagster job by name. Returns run_id and a URL to track the run.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_name": {
                        "type": "string",
                        "description": "The exact job name (e.g. amenity_desc_consistency_job)",
                    }
                },
                "required": ["job_name"],
            },
        ),
        Tool(
            name="get_run_status",
            description="Get the current status of a Dagster run by run_id.",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The Dagster run ID (UUID)",
                    }
                },
                "required": ["run_id"],
            },
        ),
        Tool(
            name="get_recent_runs",
            description="Get recent Dagster runs with their status.",
            inputSchema={
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
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool calls by dispatching to the appropriate function."""
    if name == "list_jobs":
        result = tools.list_jobs()
    elif name == "launch_job":
        result = await tools.launch_job(arguments["job_name"])
    elif name == "get_run_status":
        result = await tools.get_run_status(arguments["run_id"])
    elif name == "get_recent_runs":
        limit = arguments.get("limit", 5)
        result = await tools.get_recent_runs(limit)
    else:
        result = {"error": f"Unknown tool: {name}"}

    # Return result as formatted JSON text
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def main():
    """Run the MCP server via stdio."""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
