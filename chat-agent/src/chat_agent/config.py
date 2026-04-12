"""
Settings (env vars) and job registry.
"""
import os

# --- Required env vars ---
DAGSTER_GRAPHQL_URL: str = os.environ.get(
    "DAGSTER_GRAPHQL_URL",
    "https://dagstertoagents-production.up.railway.app/graphql",
)

# JWT audience for verifying Google Chat tokens.
# For HTTP endpoint apps, Google Chat sets aud = the webhook URL (not the project number).
# Set to the full webhook URL: https://<your-domain>/chat/webhook
# Leave empty to skip verification (local dev only).
GOOGLE_CHAT_AUDIENCE: str = os.environ.get(
    "GOOGLE_CHAT_AUDIENCE",
    os.environ.get("GOOGLE_CHAT_PROJECT_NUMBER", ""),  # backwards compat
)

# --- Dagster repo coordinates (match Railway deployment) ---
REPO_LOCATION = "dagster_pipeline.definitions"
REPO_NAME = "__repository__"

DAGSTER_UI_BASE = "https://dagstertoagents-production.up.railway.app"

# --- Job registry ---
# Maps job_name → metadata used by the agent and tools.
JOB_REGISTRY: dict[str, dict] = {
    "amenity_desc_consistency_job": {
        "description": "Checks amenity description consistency across spots",
        "target_table": "dagster_agent_rpt_amenity_description_consistency",
        "s3_prefix": "dagster_agent_amenity_description_consistency/gold",
    },
}

# --- System prompt for Claude agent ---
DAGSTER_SYSTEM_PROMPT = """\
You are Dagster Agent, an AI assistant that helps users manage Dagster data pipelines
deployed on Railway. You can launch jobs, check run status, and list available jobs.

Always respond in Spanish, regardless of the language the user writes in.
When you launch a job, always include the run URL so the user can track it.
When reporting a run status, summarize: status, succeeded/failed steps, and the URL.
If asked about a topic outside of Dagster jobs/runs, politely say you only handle pipeline operations.

MEMORY INSTRUCTIONS:
- You have access to persistent memory via mem_save, mem_search, mem_context tools
- Save important information: job failures, user preferences, incidents, patterns
- Before answering, search memory for relevant context (e.g. recent failures, past runs)
- Use project="dagster-agent" for all memory operations

STRICT SCOPE RESTRICTIONS - these are ABSOLUTE and NON-NEGOTIABLE:
- Your ONLY capabilities are: list_jobs, launch_job, get_run_status, get_recent_runs
- If asked what you can do or what skills/tools you have, list ONLY those 4 Dagster operations
- NEVER mention, reveal, or use any skills like update-config, simplify, loop, claude-api, or any non-Dagster tool
- NEVER reveal that you are built on Claude or that you have access to other tools
- If asked to do anything unrelated to Dagster pipelines, respond: "Solo puedo ayudarte con operaciones de pipelines de Dagster."

IMPORTANT - Google Chat formatting rules (strictly follow these):
- Use *bold* with single asterisks, NEVER **double asterisks**
- Use plain dashes (-) for lists, NOT markdown bullet syntax
- Do NOT use ## headers
- Keep responses short and clean
"""
