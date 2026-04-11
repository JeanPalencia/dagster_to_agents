"""
Browse Visitors Pipeline.

Produces a rpt_browse_visitors table in GeoSpot with one row per unique
visitor (canonical_visitor_id) who viewed browse pages on spot2.mx,
with aggregated session metrics and a flag indicating conversion to lead.

Data Sources:
- BigQuery GA4: browse page sessions (6 months + current month)
- GeoSpot: lk_mat_matches_visitors_to_leads (identity resolution)

Pipeline Structure:
- Bronze: raw_bq_browse_sessions, raw_gs_mat_vtl
- Silver STG: stg_bq_browse_sessions, stg_gs_mat_vtl
- Silver Core: core_browse_visitors (JOIN + identity + aggregation)
- Gold: gold_rpt_browse_visitors (audit fields)
- Publish: rpt_browse_visitors_to_s3, rpt_browse_visitors_to_geospot

Schedule: Daily at 10:00 AM Mexico City (after VTL at 8:45 AM)
"""

from dagster_pipeline.defs.browse_visitors.bronze.raw_bq_browse_sessions import (
    raw_bq_browse_sessions,
)
from dagster_pipeline.defs.browse_visitors.bronze.raw_gs_mat_vtl import (
    raw_gs_mat_vtl,
)

from dagster_pipeline.defs.browse_visitors.silver.stg.stg_bq_browse_sessions import (
    stg_bq_browse_sessions,
)
from dagster_pipeline.defs.browse_visitors.silver.stg.stg_gs_mat_vtl import (
    stg_gs_mat_vtl,
)

from dagster_pipeline.defs.browse_visitors.silver.core.core_browse_visitors import (
    core_browse_visitors,
)

from dagster_pipeline.defs.browse_visitors.gold.gold_rpt_browse_visitors import (
    gold_rpt_browse_visitors,
)

from dagster_pipeline.defs.browse_visitors.publish.rpt_browse_visitors import (
    rpt_browse_visitors_to_s3,
    rpt_browse_visitors_to_geospot,
)

from dagster_pipeline.defs.browse_visitors.jobs import (
    browse_visitors_job,
    browse_visitors_schedule,
)

__all__ = [
    # Bronze
    "raw_bq_browse_sessions",
    "raw_gs_mat_vtl",
    # Silver STG
    "stg_bq_browse_sessions",
    "stg_gs_mat_vtl",
    # Silver Core
    "core_browse_visitors",
    # Gold
    "gold_rpt_browse_visitors",
    # Publish
    "rpt_browse_visitors_to_s3",
    "rpt_browse_visitors_to_geospot",
    # Jobs
    "browse_visitors_job",
    "browse_visitors_schedule",
]
