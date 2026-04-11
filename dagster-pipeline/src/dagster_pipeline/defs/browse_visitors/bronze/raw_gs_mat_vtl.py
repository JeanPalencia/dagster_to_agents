"""
Bronze External: MAT visitor-to-lead matches from GeoSpot PostgreSQL.

Provides identity resolution data (canonical_visitor_id) and lead metadata
for joining with browse sessions.

Has a corresponding STG asset: stg_gs_mat_vtl.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.browse_visitors.shared import query_bronze_source


_MAT_VTL_SQL = """
SELECT
    vis_user_pseudo_id,
    mat_canonical_visitor_id,
    lead_id,
    mat_channel,
    lead_email,
    lead_phone_number,
    lead_sector,
    lead_cohort_type,
    mat_with_match
FROM lk_mat_matches_visitors_to_leads
"""


@dg.asset(
    group_name="bv_bronze",
    description="Bronze: MAT visitor-to-lead matches from GeoSpot.",
)
def raw_gs_mat_vtl(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Extracts visitor-to-lead match data from GeoSpot MAT table."""
    df = query_bronze_source(_MAT_VTL_SQL, source_type="geospot_postgres", context=context)
    context.log.info(f"raw_gs_mat_vtl: {df.height:,} rows, {df.width} columns")
    return df
