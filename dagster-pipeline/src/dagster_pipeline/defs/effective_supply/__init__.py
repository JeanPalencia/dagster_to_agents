# defs/effective_supply/__init__.py
"""
Effective Supply Pipeline (Oferta Efectiva).

Pipeline Structure (Medallion Architecture):
- Bronze: raw_bq_spot_contact_view_event_counts, raw_gs_bt_lds_spot_added,
          raw_gs_lk_spots, raw_gs_effective_supply_run_id
- Silver STG: stg_bq_*, stg_gs_* (type normalization)
- Silver Core: core_effective_supply_events (UNION), core_effective_supply (pivot + JOIN + flags)
- Core ML: core_ml_build_universes -> core_ml_feature_engineering -> core_ml_train
           -> core_ml_score / core_ml_drivers / core_ml_category_effects / core_ml_rules
- Core Final: core_lk_* (run_id + model metadata)
- Gold: gold_lk_* (aud_run_id + audit fields)
- Publish: S3 + GeoSpot for all gold tables

Schedule: Monthly (1st day of each month at 12:00 PM Mexico City)
"""

# Bronze assets
from dagster_pipeline.defs.effective_supply.bronze.raw_bq_spot_contact_view_event_counts import (
    raw_bq_spot_contact_view_event_counts,
)
from dagster_pipeline.defs.effective_supply.bronze.raw_gs_bt_lds_spot_added import (
    raw_gs_bt_lds_spot_added,
)
from dagster_pipeline.defs.effective_supply.bronze.raw_gs_lk_spots import (
    raw_gs_lk_spots,
)
from dagster_pipeline.defs.effective_supply.bronze.raw_gs_effective_supply_run_id import (
    raw_gs_effective_supply_run_id,
)

# Silver STG assets
from dagster_pipeline.defs.effective_supply.silver.stg.stg_bq_spot_contact_view_event_counts import (
    stg_bq_spot_contact_view_event_counts,
)
from dagster_pipeline.defs.effective_supply.silver.stg.stg_gs_bt_lds_spot_added import (
    stg_gs_bt_lds_spot_added,
)
from dagster_pipeline.defs.effective_supply.silver.stg.stg_gs_lk_spots import (
    stg_gs_lk_spots,
)
from dagster_pipeline.defs.effective_supply.silver.stg.stg_gs_effective_supply_run_id import (
    stg_gs_effective_supply_run_id,
)

# Silver Core assets
from dagster_pipeline.defs.effective_supply.silver.core.core_effective_supply_events import (
    core_effective_supply_events,
)
from dagster_pipeline.defs.effective_supply.silver.core.core_effective_supply import (
    core_effective_supply,
)
from dagster_pipeline.defs.effective_supply.silver.core.core_lk_effective_supply import (
    core_lk_effective_supply,
)
from dagster_pipeline.defs.effective_supply.silver.core.core_lk_effective_supply_propensity_scores import (
    core_lk_effective_supply_propensity_scores,
)
from dagster_pipeline.defs.effective_supply.silver.core.core_lk_effective_supply_drivers import (
    core_lk_effective_supply_drivers,
)
from dagster_pipeline.defs.effective_supply.silver.core.core_lk_effective_supply_category_effects import (
    core_lk_effective_supply_category_effects,
)
from dagster_pipeline.defs.effective_supply.silver.core.core_lk_effective_supply_rules import (
    core_lk_effective_supply_rules,
)
from dagster_pipeline.defs.effective_supply.silver.core.core_lk_effective_supply_model_metrics import (
    core_lk_effective_supply_model_metrics,
)

# Core ML assets
from dagster_pipeline.defs.effective_supply.silver.core.ml import (
    core_ml_build_universes,
    core_ml_feature_engineering,
    core_ml_train,
    core_ml_score,
    core_ml_drivers,
    core_ml_category_effects,
    core_ml_rules,
)

# Gold assets
from dagster_pipeline.defs.effective_supply.gold.gold_lk_effective_supply import (
    gold_lk_effective_supply,
)
from dagster_pipeline.defs.effective_supply.gold.gold_lk_effective_supply_propensity_scores import (
    gold_lk_effective_supply_propensity_scores,
)
from dagster_pipeline.defs.effective_supply.gold.gold_lk_effective_supply_drivers import (
    gold_lk_effective_supply_drivers,
)
from dagster_pipeline.defs.effective_supply.gold.gold_lk_effective_supply_category_effects import (
    gold_lk_effective_supply_category_effects,
)
from dagster_pipeline.defs.effective_supply.gold.gold_lk_effective_supply_rules import (
    gold_lk_effective_supply_rules,
)
from dagster_pipeline.defs.effective_supply.gold.gold_lk_effective_supply_model_metrics import (
    gold_lk_effective_supply_model_metrics,
)

# Publish assets
from dagster_pipeline.defs.effective_supply.publish.lk_effective_supply import (
    lk_effective_supply_to_s3,
    lk_effective_supply_to_geospot,
)
from dagster_pipeline.defs.effective_supply.publish.lk_effective_supply_propensity_scores import (
    lk_effective_supply_propensity_scores_to_s3,
    lk_effective_supply_propensity_scores_to_geospot,
)
from dagster_pipeline.defs.effective_supply.publish.lk_effective_supply_drivers import (
    lk_effective_supply_drivers_to_s3,
    lk_effective_supply_drivers_to_geospot,
)
from dagster_pipeline.defs.effective_supply.publish.lk_effective_supply_category_effects import (
    lk_effective_supply_category_effects_to_s3,
    lk_effective_supply_category_effects_to_geospot,
)
from dagster_pipeline.defs.effective_supply.publish.lk_effective_supply_rules import (
    lk_effective_supply_rules_to_s3,
    lk_effective_supply_rules_to_geospot,
)
from dagster_pipeline.defs.effective_supply.publish.lk_effective_supply_model_metrics import (
    lk_effective_supply_model_metrics_to_s3,
    lk_effective_supply_model_metrics_to_geospot,
)

# Jobs and schedules
from dagster_pipeline.defs.effective_supply.jobs import (
    effective_supply_monthly_job,
    effective_supply_monthly_schedule,
)

__all__ = [
    # Bronze
    "raw_bq_spot_contact_view_event_counts",
    "raw_gs_bt_lds_spot_added",
    "raw_gs_lk_spots",
    "raw_gs_effective_supply_run_id",
    # Silver STG
    "stg_bq_spot_contact_view_event_counts",
    "stg_gs_bt_lds_spot_added",
    "stg_gs_lk_spots",
    "stg_gs_effective_supply_run_id",
    # Silver Core
    "core_effective_supply_events",
    "core_effective_supply",
    "core_lk_effective_supply",
    "core_lk_effective_supply_propensity_scores",
    "core_lk_effective_supply_drivers",
    "core_lk_effective_supply_category_effects",
    "core_lk_effective_supply_rules",
    "core_lk_effective_supply_model_metrics",
    # Core ML
    "core_ml_build_universes",
    "core_ml_feature_engineering",
    "core_ml_train",
    "core_ml_score",
    "core_ml_drivers",
    "core_ml_category_effects",
    "core_ml_rules",
    # Gold
    "gold_lk_effective_supply",
    "gold_lk_effective_supply_propensity_scores",
    "gold_lk_effective_supply_drivers",
    "gold_lk_effective_supply_category_effects",
    "gold_lk_effective_supply_rules",
    "gold_lk_effective_supply_model_metrics",
    # Publish
    "lk_effective_supply_to_s3",
    "lk_effective_supply_to_geospot",
    "lk_effective_supply_propensity_scores_to_s3",
    "lk_effective_supply_propensity_scores_to_geospot",
    "lk_effective_supply_drivers_to_s3",
    "lk_effective_supply_drivers_to_geospot",
    "lk_effective_supply_category_effects_to_s3",
    "lk_effective_supply_category_effects_to_geospot",
    "lk_effective_supply_rules_to_s3",
    "lk_effective_supply_rules_to_geospot",
    "lk_effective_supply_model_metrics_to_s3",
    "lk_effective_supply_model_metrics_to_geospot",
    # Jobs
    "effective_supply_monthly_job",
    "effective_supply_monthly_schedule",
]
