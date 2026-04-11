# defs/funnel_ptd/bronze/raw_gs_lk_projects_v2.py
"""
Bronze: Raw extraction of project data from GeoSpot lk_projects.

Source: GeoSpot PostgreSQL — lk_projects
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.funnel_ptd.shared import query_bronze_source


@dg.asset(
    group_name="funnel_ptd_bronze",
    description="Bronze: raw projects from GeoSpot lk_projects for funnel PTD.",
)
def raw_gs_lk_projects_v2(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    query = """
    SELECT
        lead_id,
        project_id,
        project_enable_id,
        project_created_at,
        project_funnel_visit_created_date,
        project_funnel_visit_confirmed_at,
        project_funnel_visit_realized_at,
        project_funnel_loi_date,
        project_won_date
    FROM lk_projects
    WHERE project_id IS NOT NULL
    """

    df = query_bronze_source(query, source_type="geospot_postgres", context=context)
    context.log.info(f"raw_gs_lk_projects_v2: {df.height:,} rows, {df.width} columns")
    return df
