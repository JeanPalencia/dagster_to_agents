"""Jobs and Schedules for the Conversation Analysis pipeline."""

import dagster as dg


# ============ Asset Selection ============

# Cleanup asset selection (runs at the end to free disk space)
cleanup_selection = dg.AssetSelection.assets("cleanup_storage")

# Select final output and all its upstream dependencies
# This automatically includes: conv_raw_conversations, conv_raw_clients,
# conv_grouped, conv_merged, conv_with_variables, conv_final_output
# Plus cleanup_storage at the end
conv_selection = dg.AssetSelection.assets("conv_final_output").upstream() | cleanup_selection


# ============ Jobs ============

conv_job = dg.define_asset_job(
    name="conv_job",
    description="Conversation Analysis pipeline: extracts conversations, merges with clients, and uploads to S3.",
    selection=conv_selection,
    # Para que DSPy e IA actualicen variables (score, ai_tag), pasa config al lanzar el run:
    # config={"ops": {"conv_with_variables": {"config": {"enable_openai_analysis": True, "enable_dspy_evaluator": True}}}}
    # O en la UI: Launch run → Add config → pegar el JSON anterior.
)

# Run config de ejemplo para ejecutar CON DSPy y AI tags (actualiza score y ai_tag)
# Copiar al "Launch run" → Config en la UI, o usar como default_run_config en el Schedule.
CONV_JOB_CONFIG_WITH_AI = {
    "ops": {
        "conv_with_variables": {
            "config": {
                "enable_openai_analysis": True,
                "enable_dspy_evaluator": True,
            }
        }
    }
}


# ============ Schedules ============


# Schedule: Every hour during business hours (9 AM - 6 PM) Mexico City
conv_schedule_hourly = dg.ScheduleDefinition(
    name="conv_schedule_hourly",
    job=conv_job,
    cron_schedule="0 9-18 * * *",  # Every hour from 9:00 to 18:00
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

# Schedule: Daily at 4:00 AM Mexico City
conv_schedule_4am = dg.ScheduleDefinition(
    name="conv_schedule_4am",
    job=conv_job,
    cron_schedule="0 4 * * *",  # 4:00 AM every day
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

