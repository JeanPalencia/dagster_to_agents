"""Jobs and Schedules for the Visitors to Leads (VTL) pipeline."""
import dagster as dg


# ============ Asset Selection ============

# Cleanup asset selection (runs at the end to free disk space)
cleanup_selection = dg.AssetSelection.assets("cleanup_storage")

# Select VTL final output and all its upstream dependencies
# This automatically includes: vtl_clients, vtl_lead_events, vtl_conversations,
# vtl_processed_lead_events, vtl_matched, vtl_final_output
# Plus cleanup_storage at the end
vtl_selection = dg.AssetSelection.assets("vtl_final_output").upstream() | cleanup_selection


# ============ Jobs ============

vtl_job = dg.define_asset_job(
    name="vtl_job",
    description="Visitors to Leads pipeline: matches website visitors to lead events and uploads results to S3.",
    selection=vtl_selection,
)


# ============ Schedules ============

# Schedule: Daily at 10:00 AM Mexico City time
vtl_schedule = dg.ScheduleDefinition(
    name="vtl_schedule",
    job=vtl_job,
    cron_schedule="0 8 * * *",  # 10:00 AM local time
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,  # Starts enabled automatically
)
