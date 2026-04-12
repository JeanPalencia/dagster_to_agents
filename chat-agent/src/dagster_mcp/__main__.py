"""Entry point for running the Dagster MCP server via python -m dagster_mcp."""
import asyncio

from dagster_mcp.server import main

if __name__ == "__main__":
    asyncio.run(main())
