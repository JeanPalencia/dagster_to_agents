"""Jobs and Schedules for the LK Visitors pipeline."""

import dagster as dg

# Cleanup asset selection (runs at the end to free disk space)
cleanup_selection = dg.AssetSelection.assets("cleanup_storage")

# Select lk_output and all its upstream dependencies, plus cleanup at the end
lk_visitors_selection = dg.AssetSelection.assets("lk_output").upstream() | cleanup_selection


# Job definition
lk_visitors_job = dg.define_asset_job(
    name="lk_visitors_job",
    description="LK Visitors pipeline: funnel_with_channel from BigQuery, deduplicated and formatted.",
    selection=lk_visitors_selection,
)


# Schedule: Daily at 4:00 AM Mexico City time
lk_visitors_schedule = dg.ScheduleDefinition(
    name="lk_visitors_schedule",
    job=lk_visitors_job,
    cron_schedule="0 4 * * *",
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,  
)
