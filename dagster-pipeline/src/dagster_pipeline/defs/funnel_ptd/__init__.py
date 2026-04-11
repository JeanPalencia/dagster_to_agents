# defs/funnel_ptd/__init__.py
"""
Funnel Period-to-Date Pipeline (PTD Monitor).

Replicates the period_to_date.sql query as a Dagster pipeline:
- Bronze: raw extraction from GeoSpot (lk_leads, lk_projects, lk_okrs, bt_lds_lead_spots)
- Silver STG: type normalization and calculated fields
- Silver Core: current counts (Cohort + Rolling), project monitor,
  Kaplan-Meier survival curves, KM-based projections, final output
- Gold: audit fields
- Publish: S3 (CSV) → GeoSpot (rpt_funnel_ptd)
"""

# Bronze assets
from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_lk_leads import raw_gs_lk_leads
from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_lk_projects_v2 import raw_gs_lk_projects_v2
from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_lk_okrs import raw_gs_lk_okrs
from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_bt_lds_lead_spots import raw_gs_bt_lds_lead_spots

# Silver STG assets
from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_leads import stg_gs_fptd_leads
from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_projects import stg_gs_fptd_projects
from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_okrs import stg_gs_fptd_okrs
from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_lead_spots import stg_gs_fptd_lead_spots

# Silver Core assets
from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_current_counts import core_fptd_current_counts
from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_monitor import core_fptd_monitor
from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_km_survival import core_fptd_km_survival
from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_projections import core_fptd_projections
from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_final import core_fptd_final

# Gold assets
from dagster_pipeline.defs.funnel_ptd.gold.gold_fptd_period_to_date import gold_fptd_period_to_date

# Publish assets
from dagster_pipeline.defs.funnel_ptd.publish.rpt_funnel_ptd import (
    rpt_funnel_ptd_to_s3,
    rpt_funnel_ptd_to_geospot,
)

# Jobs & Sensors
from dagster_pipeline.defs.funnel_ptd.jobs import funnel_ptd_job, funnel_ptd_after_gold_sensor

__all__ = [
    # Bronze
    "raw_gs_lk_leads",
    "raw_gs_lk_projects_v2",
    "raw_gs_lk_okrs",
    "raw_gs_bt_lds_lead_spots",
    # Silver STG
    "stg_gs_fptd_leads",
    "stg_gs_fptd_projects",
    "stg_gs_fptd_okrs",
    "stg_gs_fptd_lead_spots",
    # Silver Core
    "core_fptd_current_counts",
    "core_fptd_monitor",
    "core_fptd_km_survival",
    "core_fptd_projections",
    "core_fptd_final",
    # Gold
    "gold_fptd_period_to_date",
    # Publish
    "rpt_funnel_ptd_to_s3",
    "rpt_funnel_ptd_to_geospot",
    # Jobs & Sensors
    "funnel_ptd_job",
    "funnel_ptd_after_gold_sensor",
]
