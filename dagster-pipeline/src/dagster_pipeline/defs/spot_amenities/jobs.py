"""
Jobs and Schedules for the Spot Amenities pipeline.

- spot_amenities_job: Materializes all assets from Bronze to Publish.
- spot_amenities_schedule: Runs daily at 5:00 AM Mexico City time.
"""
import dagster as dg


cleanup_selection = dg.AssetSelection.assets("cleanup_storage")

spot_amenities_selection = (
    dg.AssetSelection.assets("bt_spot_amenities_to_geospot")
    .upstream()
    | cleanup_selection
)

spot_amenities_job = dg.define_asset_job(
    name="spot_amenities_job",
    selection=spot_amenities_selection,
    description=(
        "Runs spot amenities pipeline: "
        "Bronze (amenities, spot_amenities from MySQL) -> "
        "STG (type normalization, category classification) -> "
        "Core (LEFT JOIN) -> Gold (audit fields) -> "
        "Publish (S3 CSV + GeoSpot)"
    ),
)


@dg.schedule(
    cron_schedule="0 5 * * *",
    job=spot_amenities_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def spot_amenities_schedule(context: dg.ScheduleEvaluationContext):
    """Runs spot amenities pipeline daily at 5:00 AM Mexico City time."""
    yield dg.RunRequest(run_key=None)
