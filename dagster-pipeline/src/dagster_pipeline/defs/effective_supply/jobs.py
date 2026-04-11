# defs/effective_supply/jobs.py
"""
Jobs and Schedules for the Effective Supply pipeline.

This module defines:
- effective_supply_monthly_job: Materializes all assets from STG to publish,
  including ML propensity scoring, drivers, category effects, and rules.
- effective_supply_monthly_schedule: Runs on the 1st day of each month at 3:00 AM
"""
import dagster as dg


# ============ Cleanup Selection ============
# Added to free disk space after pipeline completes (same pattern as data_lakehouse)
cleanup_selection = dg.AssetSelection.assets("cleanup_storage")


# ============ Asset Selection ============

# Select ALL terminal publish assets and their upstream dependencies.
# This ensures the entire pipeline runs:
# STG -> Core -> Gold -> ML -> Gold ML -> Publish (all five publish chains)
# cleanup_storage runs at the end to free pickled DataFrames from storage/
lk_effective_supply_selection = (
    dg.AssetSelection.assets(
        "lk_effective_supply_to_geospot",
        "lk_effective_supply_propensity_scores_to_geospot",
        "lk_effective_supply_drivers_to_geospot",
        "lk_effective_supply_category_effects_to_geospot",
        "lk_effective_supply_rules_to_geospot",
        "lk_effective_supply_model_metrics_to_geospot",
    )
    .upstream()
    | cleanup_selection
)


# ============ Jobs ============

effective_supply_monthly_job = dg.define_asset_job(
    name="effective_supply_monthly_job",
    selection=lk_effective_supply_selection,
    description=(
        "Runs effective supply pipeline monthly: "
        "STG -> Core -> Gold -> ML (CatBoost Rent+Sale) -> "
        "Gold ML (scores, drivers, category effects, rules, model metrics) -> "
        "Publish (S3 + GeoSpot)"
    ),
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "tag_concurrency_limits": [
                        {
                            "key": "geospot_api",
                            "value": "write",
                            "limit": 1,
                        }
                    ],
                }
            }
        }
    },
)


# ============ Schedules ============

@dg.schedule(
    cron_schedule="0 12 1 * *",  # 12:00 PM on the 1st day of each month
    job=effective_supply_monthly_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def effective_supply_monthly_schedule(context: dg.ScheduleEvaluationContext):
    """
    Runs effective supply pipeline on the 1st of each month at 3:00 AM Mexico City time.

    Includes:
    - Event aggregation (BigQuery + GeoSpot)
    - ML propensity scoring (CatBoost Rent + Sale)
    - Feature importance (drivers)
    - Category effects (lift per feature value)
    - Interpretable rules (decision tree)
    - Publish to S3 + GeoSpot PostgreSQL
    """
    yield dg.RunRequest(run_key=None)
