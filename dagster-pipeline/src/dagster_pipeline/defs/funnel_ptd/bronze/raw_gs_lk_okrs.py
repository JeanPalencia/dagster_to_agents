# defs/funnel_ptd/bronze/raw_gs_lk_okrs.py
"""
Bronze: Raw extraction of OKR targets from GeoSpot lk_okrs.

Source: GeoSpot PostgreSQL — lk_okrs (externally loaded, not managed by Dagster).
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.funnel_ptd.shared import query_bronze_source


@dg.asset(
    group_name="funnel_ptd_bronze",
    description="Bronze: raw OKR monthly targets from GeoSpot lk_okrs.",
)
def raw_gs_lk_okrs(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    query = """
    SELECT
        okr_month_start_ts,
        okr_leads,
        okr_projects,
        okr_scheduled_visits,
        okr_confirmed_visits,
        okr_completed_visits,
        okr_lois
    FROM lk_okrs
    """

    df = query_bronze_source(query, source_type="geospot_postgres", context=context)
    context.log.info(f"raw_gs_lk_okrs: {df.height:,} rows, {df.width} columns")
    return df
