"""
Jobs and Schedules for the Browse Visitors pipeline.

- browse_visitors_job: Materializes all assets from Bronze to Publish.
- browse_visitors_schedule: Runs daily at 10:00 AM Mexico City time
  (after the VTL job at 8:45 AM that feeds the MAT table).
"""
import dagster as dg


cleanup_selection = dg.AssetSelection.assets("cleanup_storage")

browse_visitors_selection = (
    dg.AssetSelection.assets("rpt_browse_visitors_to_geospot")
    .upstream()
    | cleanup_selection
)

browse_visitors_job = dg.define_asset_job(
    name="browse_visitors_job",
    selection=browse_visitors_selection,
    description=(
        "Runs browse visitors pipeline: "
        "Bronze (BigQuery GA4 sessions, GeoSpot MAT) -> "
        "STG (normalization, sector extraction) -> "
        "Core (JOIN, identity resolution, aggregation per visitor) -> "
        "Gold (audit fields) -> "
        "Publish (S3 CSV + GeoSpot)"
    ),
)


@dg.schedule(
    cron_schedule="0 10 * * *",
    job=browse_visitors_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def browse_visitors_schedule(context: dg.ScheduleEvaluationContext):
    """Runs browse visitors pipeline daily at 10:00 AM Mexico City time."""
    yield dg.RunRequest(run_key=None)
