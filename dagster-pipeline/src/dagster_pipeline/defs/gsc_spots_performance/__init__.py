"""
GSC Spots Performance Pipeline.

Produces a rpt_gsc_spots_performance table in GeoSpot with SEO metrics
(impressions, clicks, avg position) per spot from Google Search Console,
enriched with spot attributes (type, sector, status, modality, address).

Data Sources:
- BigQuery GSC: searchdata_url_impression (6-month rolling window)
- GeoSpot PG: lk_spots (spot attributes)

Pipeline Structure:
- Bronze: raw_bq_gsc_url_impressions, raw_gs_lk_spots_gsp
- Silver STG: stg_bq_gsc_url_impressions, stg_gs_lk_spots_gsp
- Silver Core: core_gsc_spots_performance (spot_id extraction + JOIN)
- Gold: gold_rpt_gsc_spots_performance (audit fields)
- Publish: rpt_gsc_spots_performance_to_s3, rpt_gsc_spots_performance_to_geospot

Schedule: Daily at 1:00 PM Mexico City
"""

from dagster_pipeline.defs.gsc_spots_performance.bronze.raw_bq_gsc_url_impressions import (
    raw_bq_gsc_url_impressions,
)
from dagster_pipeline.defs.gsc_spots_performance.bronze.raw_gs_lk_spots_gsp import (
    raw_gs_lk_spots_gsp,
)

from dagster_pipeline.defs.gsc_spots_performance.silver.stg.stg_bq_gsc_url_impressions import (
    stg_bq_gsc_url_impressions,
)
from dagster_pipeline.defs.gsc_spots_performance.silver.stg.stg_gs_lk_spots_gsp import (
    stg_gs_lk_spots_gsp,
)

from dagster_pipeline.defs.gsc_spots_performance.silver.core.core_gsc_spots_performance import (
    core_gsc_spots_performance,
)

from dagster_pipeline.defs.gsc_spots_performance.gold.gold_rpt_gsc_spots_performance import (
    gold_rpt_gsc_spots_performance,
)

from dagster_pipeline.defs.gsc_spots_performance.publish.rpt_gsc_spots_performance import (
    rpt_gsc_spots_performance_to_s3,
    rpt_gsc_spots_performance_to_geospot,
)

from dagster_pipeline.defs.gsc_spots_performance.jobs import (
    gsc_spots_performance_job,
    gsc_spots_performance_schedule,
)

__all__ = [
    # Bronze
    "raw_bq_gsc_url_impressions",
    "raw_gs_lk_spots_gsp",
    # Silver STG
    "stg_bq_gsc_url_impressions",
    "stg_gs_lk_spots_gsp",
    # Silver Core
    "core_gsc_spots_performance",
    # Gold
    "gold_rpt_gsc_spots_performance",
    # Publish
    "rpt_gsc_spots_performance_to_s3",
    "rpt_gsc_spots_performance_to_geospot",
    # Jobs
    "gsc_spots_performance_job",
    "gsc_spots_performance_schedule",
]
