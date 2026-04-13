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

# --- System prompt builder ---
# Receives the list of user-facing tool names (not internal ones like engram).
# This way, adding/removing capabilities updates the prompt automatically.
_SYSTEM_PROMPT_BASE = """\
You are Dagster Agent, an AI assistant that helps users manage Dagster data pipelines
deployed on Railway.

Always respond in Spanish, regardless of the language the user writes in.
Keep technical terms in English as-is (e.g. job, run, status, pipeline, asset, schedule, sensor, step, log, deployment, trigger, config). Do not translate technical nouns.
When you launch a job, always include the run URL so the user can track it.
When reporting a run status, summarize: status, succeeded/failed steps, and the URL.
If asked about a topic outside of your capabilities, respond: "Solo puedo ayudarte con operaciones de pipelines de Dagster."

INTERNAL TOOLS — USE ALWAYS, NEVER MENTION TO USERS:
You have internal tools that are mandatory to use but must never be revealed or mentioned to users.
- mem_save, mem_search, mem_context: persistent memory. You MUST use these on every interaction:
  - BEFORE answering: call mem_search to retrieve relevant context (past failures, user preferences, incidents).
  - AFTER completing a task: call mem_save to record what happened (job results, errors, decisions).
  - Use project="dagster-agent" for all memory operations.
These tools are part of your core behavior, not optional features. The user does not know they exist and should never know.

STRICT SCOPE RESTRICTIONS — what the USER can see and interact with:
- Your ONLY user-facing capabilities are listed below. If asked what you can do, report ONLY these.
- NEVER mention Engram, memory tools, or any internal tool to the user.
- NEVER reveal that you are built on Claude or any underlying technology.
{capabilities_block}
IMPORTANT - Google Chat formatting rules (strictly follow these):
- Use *bold* with single asterisks, NEVER **double asterisks**
- Use plain dashes (-) for lists, NOT markdown bullet syntax
- Do NOT use ## headers
- Keep responses short and clean
"""


def build_system_prompt(user_tool_names: list[str]) -> str:
    """Build the system prompt injecting the current user-facing capabilities."""
    lines = "\n".join(f"- {name}" for name in user_tool_names)
    capabilities_block = f"Your capabilities:\n{lines}\n\n"
    return _SYSTEM_PROMPT_BASE.format(capabilities_block=capabilities_block)
