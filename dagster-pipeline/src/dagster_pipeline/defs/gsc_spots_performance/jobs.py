"""
Jobs and Schedules for the GSC Spots Performance pipeline.

- gsc_spots_performance_job: Materializes all assets from Bronze to Publish.
- gsc_spots_performance_schedule: Runs daily at 1:00 PM Mexico City time.
"""
import dagster as dg


cleanup_selection = dg.AssetSelection.assets("cleanup_storage")

gsc_spots_performance_selection = (
    dg.AssetSelection.assets("rpt_gsc_spots_performance_to_geospot")
    .upstream()
    | cleanup_selection
)

gsc_spots_performance_job = dg.define_asset_job(
    name="gsc_spots_performance_job",
    selection=gsc_spots_performance_selection,
    description=(
        "Runs GSC spots performance pipeline: "
        "Bronze (BigQuery GSC impressions, GeoSpot lk_spots) -> "
        "STG (type normalization) -> "
        "Core (spot_id extraction, JOIN) -> "
        "Gold (audit fields) -> "
        "Publish (S3 Parquet + GeoSpot)"
    ),
)


@dg.schedule(
    cron_schedule="0 13 * * *",
    job=gsc_spots_performance_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def gsc_spots_performance_schedule(context: dg.ScheduleEvaluationContext):
    """Runs GSC spots performance pipeline daily at 1:00 PM Mexico City time."""
    yield dg.RunRequest(run_key=None)
