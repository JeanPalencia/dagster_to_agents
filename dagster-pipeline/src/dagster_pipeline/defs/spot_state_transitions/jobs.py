# defs/spot_state_transitions/jobs.py
"""
Jobs and Schedules for the Spot State Transitions pipeline.

- spot_state_transitions_daily_job: Materializes all assets from Bronze to Publish.
- spot_state_transitions_daily_schedule: Runs daily at 9:00 AM Mexico City time.
"""
import dagster as dg


# ============ Cleanup Selection ============
cleanup_selection = dg.AssetSelection.assets("cleanup_storage")


# ============ Asset Selection ============

lk_spot_status_history_selection = (
    dg.AssetSelection.assets("lk_spot_status_history_to_s3")
    .upstream()
    | cleanup_selection
)


# ============ Jobs ============

spot_state_transitions_daily_job = dg.define_asset_job(
    name="spot_state_transitions_daily_job",
    selection=lk_spot_status_history_selection,
    description=(
        "Runs spot state transitions pipeline daily: "
        "Bronze (watermark, new transitions, affected history, snapshots, active spots) -> "
        "STG (type normalization) -> Core (dedup, prev/next, sstd IDs) -> "
        "Gold (hard-delete flag + audit) -> Publish (S3 CSV)"
    ),
)


# ============ Schedules ============

@dg.schedule(
    cron_schedule="0 9 * * *",  # 9:00 AM daily
    job=spot_state_transitions_daily_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def spot_state_transitions_daily_schedule(context: dg.ScheduleEvaluationContext):
    """
    Runs spot state transitions pipeline daily at 9:00 AM Mexico City time.

    Incremental pipeline:
    1. Reads watermark from GeoSpot
    2. Extracts new/edited transitions from MySQL
    3. Rebuilds affected spots (dedup, prev/next, sstd IDs)
    4. Flags hard-deleted spots
    5. Saves to S3 as CSV
    """
    yield dg.RunRequest(run_key=None)
