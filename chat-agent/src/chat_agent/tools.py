"""
Dagster GraphQL tool implementations.

GraphQL queries/mutations are taken directly from the run-and-verify skill.
All functions are async and use httpx.AsyncClient.
"""
from __future__ import annotations

import httpx

from chat_agent.config import (
    DAGSTER_GRAPHQL_URL,
    DAGSTER_UI_BASE,
    JOB_REGISTRY,
    REPO_LOCATION,
    REPO_NAME,
)

_TIMEOUT = 30.0


async def _gql(query: str, variables: dict | None = None) -> dict:
    payload: dict = {"query": query}
    if variables:
        payload["variables"] = variables
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.post(
            DAGSTER_GRAPHQL_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()


async def check_location_loaded() -> dict:
    """Returns {loaded: bool, status: str}."""
    data = await _gql(
        "{ locationStatusesOrError { "
        "... on WorkspaceLocationStatusEntries { entries { loadStatus } } "
        "} }"
    )
    entries = (
        data.get("data", {})
        .get("locationStatusesOrError", {})
        .get("entries", [])
    )
    if not entries:
        return {"loaded": False, "status": "NO_ENTRIES"}
    status = entries[0].get("loadStatus", "UNKNOWN")
    return {"loaded": status == "LOADED", "status": status}


async def launch_job(job_name: str) -> dict:
    """
    Launches a Dagster job by name.
    Returns {success: bool, run_id: str|None, error: str|None, url: str|None}.
    """
    if job_name not in JOB_REGISTRY:
        return {
            "success": False,
            "run_id": None,
            "error": f"Unknown job '{job_name}'. Available: {list(JOB_REGISTRY)}",
            "url": None,
        }

    mutation = """
    mutation LaunchRun($params: ExecutionParams!) {
      launchRun(executionParams: $params) {
        __typename
        ... on LaunchRunSuccess { run { runId status } }
        ... on PythonError { message }
      }
    }
    """
    variables = {
        "params": {
            "selector": {
                "repositoryLocationName": REPO_LOCATION,
                "repositoryName": REPO_NAME,
                "jobName": job_name,
            },
            "executionMetadata": {"tags": []},
        }
    }
    data = await _gql(mutation, variables)
    result = data.get("data", {}).get("launchRun", {})
    typename = result.get("__typename")

    if typename == "LaunchRunSuccess":
        run_id = result["run"]["runId"]
        return {
            "success": True,
            "run_id": run_id,
            "error": None,
            "url": f"{DAGSTER_UI_BASE}/runs/{run_id}",
        }
    else:
        return {
            "success": False,
            "run_id": None,
            "error": result.get("message", f"Unexpected response: {typename}"),
            "url": None,
        }


async def get_run_status(run_id: str) -> dict:
    """
    Returns current status of a run.
    {status: str, steps_succeeded: int, steps_failed: int, failed_steps: list[str]}
    """
    query = """
    {
      runOrError(runId: "%s") {
        ... on Run {
          status
          stats { ... on RunStatsSnapshot { stepsSucceeded stepsFailed } }
          stepStats { stepKey status }
        }
        ... on RunNotFoundError { message }
        ... on PythonError { message }
      }
    }
    """ % run_id

    data = await _gql(query)
    run = data.get("data", {}).get("runOrError", {})

    if "status" not in run:
        return {
            "status": "NOT_FOUND",
            "steps_succeeded": 0,
            "steps_failed": 0,
            "failed_steps": [],
            "error": run.get("message", "Run not found"),
        }

    stats = run.get("stats", {})
    step_stats = run.get("stepStats", [])
    failed_steps = [s["stepKey"] for s in step_stats if s.get("status") == "FAILURE"]

    return {
        "status": run["status"],
        "steps_succeeded": stats.get("stepsSucceeded", 0),
        "steps_failed": stats.get("stepsFailed", 0),
        "failed_steps": failed_steps,
        "url": f"{DAGSTER_UI_BASE}/runs/{run_id}",
    }


async def get_recent_runs(limit: int = 5) -> dict:
    """Returns a list of recent runs with status."""
    query = """
    {
      runsOrError(limit: %d) {
        ... on Runs {
          results {
            runId
            jobName
            status
            startTime
          }
        }
        ... on PythonError { message }
      }
    }
    """ % limit

    data = await _gql(query)
    runs_data = data.get("data", {}).get("runsOrError", {})
    results = runs_data.get("results", [])

    runs = [
        {
            "run_id": r["runId"],
            "job_name": r["jobName"],
            "status": r["status"],
            "url": f"{DAGSTER_UI_BASE}/runs/{r['runId']}",
        }
        for r in results
    ]
    return {"runs": runs}


def list_jobs() -> dict:
    """Returns the list of registered jobs with descriptions."""
    jobs = [
        {"job_name": name, "description": meta["description"]}
        for name, meta in JOB_REGISTRY.items()
    ]
    return {"jobs": jobs}
