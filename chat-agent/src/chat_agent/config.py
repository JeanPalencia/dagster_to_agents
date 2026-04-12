"""
Settings (env vars) and job registry.
"""
import os

# --- Required env vars ---
ANTHROPIC_API_KEY: str = os.environ.get("ANTHROPIC_API_KEY", "")
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
