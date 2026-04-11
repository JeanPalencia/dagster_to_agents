# Conversation Analysis Pipeline
"""
This module exports all Conversation Analysis pipeline components for Dagster's load_from_defs_folder.
"""
# Step that sends traces to LangSmith/LangChain — commented out for both legacy and governance to avoid 429 (usage limit).
# from dagster_pipeline.defs.conversation_analysis.utils.database import ensure_langsmith_env_from_ssm
# ensure_langsmith_env_from_ssm()

from dagster_pipeline.defs.conversation_analysis.assets import (
    conv_raw_conversations,
    conv_raw_clients,
    conv_raw_project_requirements,
    conv_raw_lk_projects,
    conv_grouped,
    conv_merged,
    conv_with_projects,
    conv_seniority,
    conv_with_variables,
    conv_upsert,
    conv_final_output,
)
from dagster_pipeline.defs.conversation_analysis.jobs import (
    conv_job,
    conv_schedule_hourly,
    conv_schedule_4am,
)

# Export all components for load_from_defs_folder
__all__ = [
    # Assets
    "conv_raw_conversations",
    "conv_raw_clients",
    "conv_raw_project_requirements",
    "conv_raw_lk_projects",
    "conv_grouped",
    "conv_merged",
    "conv_with_projects",
    "conv_seniority",
    "conv_with_variables",
    "conv_upsert",
    "conv_final_output",
    # Jobs
    "conv_job",
    # Schedules
    "conv_schedule_hourly",
    "conv_schedule_4am",
]

