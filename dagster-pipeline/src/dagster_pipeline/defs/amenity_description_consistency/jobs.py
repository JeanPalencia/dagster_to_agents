"""
Jobs and Sensor for the Amenity Description Consistency pipeline.

Terminal asset: rpt_amenity_desc_consistency_to_geospot (Publish layer).
Trigger: sensor that waits for spot_amenities_job to complete,
then delays 10 min to allow GeoSpot async load to finish.
"""
import json
from datetime import datetime, timedelta

import dagster as dg


cleanup_selection = dg.AssetSelection.assets("cleanup_storage")

adc_selection = (
    dg.AssetSelection.assets("rpt_amenity_desc_consistency_to_geospot")
    .upstream()
    | cleanup_selection
)

amenity_desc_consistency_job = dg.define_asset_job(
    name="amenity_desc_consistency_job",
    selection=adc_selection,
    executor_def=dg.in_process_executor,
    description=(
        "Amenity-description consistency analysis: "
        "Bronze (lk_spots + bt_spot_amenities from GeoSpot) -> "
        "STG (normalization) -> "
        "Core (regex matching + classification) -> "
        "Gold (audit fields) -> "
        "Publish (S3 CSV + GeoSpot rpt_amenity_description_consistency)"
    ),
)

_UPSTREAM_JOB = "spot_amenities_job"
_GEOSPOT_DELAY_SECONDS = 600
_SENSOR_INTERVAL_SECONDS = 300


@dg.sensor(
    job=amenity_desc_consistency_job,
    minimum_interval_seconds=_SENSOR_INTERVAL_SECONDS,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description=(
        "Launches amenity_desc_consistency_job after spot_amenities_job "
        "succeeds, with a 10-minute delay for GeoSpot async load."
    ),
)
def adc_after_spot_amenities_sensor(context: dg.SensorEvaluationContext):
    instance = context.instance
    now = datetime.now()
    one_day_ago = now - timedelta(days=1)

    records = instance.get_run_records(
        filters=dg.RunsFilter(
            job_name=_UPSTREAM_JOB,
            statuses=[dg.DagsterRunStatus.SUCCESS],
        ),
        limit=1,
        order_by="update_timestamp",
        ascending=False,
    )
    if not records:
        return dg.SkipReason(f"No successful run found for {_UPSTREAM_JOB}.")

    record = records[0]
    end_time = record.end_time
    if end_time is None:
        return dg.SkipReason(f"{_UPSTREAM_JOB} has no end_time recorded.")

    completion_dt = datetime.fromtimestamp(end_time)
    if completion_dt < one_day_ago:
        return dg.SkipReason(
            f"{_UPSTREAM_JOB} last succeeded at {completion_dt}, more than 24h ago."
        )

    previous_cursor = json.loads(context.cursor) if context.cursor else {}
    prev_ts = previous_cursor.get("last_ts")

    current_ts = completion_dt.timestamp()
    if prev_ts is not None and current_ts <= prev_ts:
        return dg.SkipReason(
            "No new spot_amenities_job completion since last ADC launch."
        )

    elapsed = (now - completion_dt).total_seconds()
    if elapsed < _GEOSPOT_DELAY_SECONDS:
        remaining = _GEOSPOT_DELAY_SECONDS - elapsed
        return dg.SkipReason(
            f"spot_amenities_job succeeded, waiting {remaining:.0f}s more "
            f"for GeoSpot async load ({_GEOSPOT_DELAY_SECONDS}s delay)."
        )

    context.update_cursor(json.dumps({"last_ts": current_ts}))

    run_key = f"adc_{completion_dt.strftime('%Y%m%d_%H%M%S')}"
    context.log.info(
        f"Launching amenity_desc_consistency_job: spot_amenities_job completed, "
        f"delay of {_GEOSPOT_DELAY_SECONDS}s elapsed. run_key={run_key}"
    )
    return dg.RunRequest(run_key=run_key)
