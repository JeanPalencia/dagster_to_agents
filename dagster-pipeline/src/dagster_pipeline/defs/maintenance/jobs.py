import dagster as dg

# Daily maintenance job
maintenance_job = dg.define_asset_job(
    name="maintenance_job",
    description="Daily maintenance: clears local storage/ only; Postgres run history is not deleted.",
    selection=[dg.AssetKey("cleanup_old_partitions")],
)

# Schedule: Run daily at 11 PM Mexico City time (after all other jobs complete)
maintenance_schedule = dg.ScheduleDefinition(
    name="maintenance_schedule",
    job=maintenance_job,
    cron_schedule="0 23 * * *",  # 11 PM daily
    default_status=dg.DefaultScheduleStatus.RUNNING,
    execution_timezone="America/Mexico_City",
)