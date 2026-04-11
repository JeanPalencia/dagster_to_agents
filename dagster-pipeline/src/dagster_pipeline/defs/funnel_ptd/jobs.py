# defs/funnel_ptd/jobs.py
"""
Jobs and Sensor for the Funnel Period-to-Date pipeline.

Terminal asset: rpt_funnel_ptd_to_geospot (Publish layer).
Trigger: sensor that waits for both gold_lk_leads_new and gold_lk_projects_new
to complete, then delays to allow Geospot async load to finish.
"""
import json
from datetime import datetime, timedelta

import dagster as dg


cleanup_selection = dg.AssetSelection.assets("cleanup_storage")

funnel_ptd_selection = (
    dg.AssetSelection.assets("rpt_funnel_ptd_to_geospot")
    .upstream()
    | cleanup_selection
)

funnel_ptd_job = dg.define_asset_job(
    name="funnel_ptd_job",
    selection=funnel_ptd_selection,
    description=(
        "Runs Funnel Period-to-Date pipeline: "
        "Bronze (lk_leads, lk_projects, lk_okrs, bt_lds_lead_spots) -> "
        "STG -> Core (current counts, monitor, KM survival, projections) -> "
        "Gold -> Publish (S3 + GeoSpot rpt_funnel_ptd)"
    ),
)

_UPSTREAM_GOLD_JOBS = ("lk_leads_new", "lk_projects_new")
_GEOSPOT_DELAY_SECONDS = 600
_SENSOR_INTERVAL_SECONDS = 300


@dg.sensor(
    job=funnel_ptd_job,
    minimum_interval_seconds=_SENSOR_INTERVAL_SECONDS,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description=(
        "Launches funnel_ptd_job after both gold_lk_leads_new and "
        "gold_lk_projects_new succeed, with a delay for Geospot async load."
    ),
)
def funnel_ptd_after_gold_sensor(context: dg.SensorEvaluationContext):
    instance = context.instance
    now = datetime.now()
    one_day_ago = now - timedelta(days=1)

    run_records = {}
    for job_name in _UPSTREAM_GOLD_JOBS:
        records = instance.get_run_records(
            filters=dg.RunsFilter(
                job_name=job_name,
                statuses=[dg.DagsterRunStatus.SUCCESS],
            ),
            limit=1,
            order_by="update_timestamp",
            ascending=False,
        )
        if not records:
            return dg.SkipReason(f"No successful run found for {job_name}.")
        run_records[job_name] = records[0]

    completion_times = {}
    for job_name, record in run_records.items():
        end_time = record.end_time
        if end_time is None:
            return dg.SkipReason(f"{job_name} has no end_time recorded.")
        dt = datetime.fromtimestamp(end_time)
        if dt < one_day_ago:
            return dg.SkipReason(
                f"{job_name} last succeeded at {dt}, more than 24h ago."
            )
        completion_times[job_name] = dt

    previous_cursor = json.loads(context.cursor) if context.cursor else {}
    prev_times = {
        k: previous_cursor.get(k) for k in _UPSTREAM_GOLD_JOBS
    }

    has_new_run = False
    for job_name in _UPSTREAM_GOLD_JOBS:
        current_ts = completion_times[job_name].timestamp()
        prev_ts = prev_times.get(job_name)
        if prev_ts is None or current_ts > prev_ts:
            has_new_run = True

    if not has_new_run:
        return dg.SkipReason("No new gold completions since last funnel_ptd launch.")

    latest_completion = max(completion_times.values())
    elapsed = (now - latest_completion).total_seconds()

    if elapsed < _GEOSPOT_DELAY_SECONDS:
        remaining = _GEOSPOT_DELAY_SECONDS - elapsed
        return dg.SkipReason(
            f"Both gold jobs succeeded, waiting {remaining:.0f}s more "
            f"for Geospot async load ({_GEOSPOT_DELAY_SECONDS}s delay)."
        )

    context.update_cursor(json.dumps({
        job_name: completion_times[job_name].timestamp()
        for job_name in _UPSTREAM_GOLD_JOBS
    }))

    run_key = f"funnel_ptd_{latest_completion.strftime('%Y%m%d_%H%M%S')}"
    context.log.info(
        f"Launching funnel_ptd_job: both gold jobs completed, "
        f"delay of {_GEOSPOT_DELAY_SECONDS}s elapsed. run_key={run_key}"
    )
    return dg.RunRequest(run_key=run_key)
