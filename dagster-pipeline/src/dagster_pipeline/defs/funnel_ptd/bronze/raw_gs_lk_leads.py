# defs/funnel_ptd/bronze/raw_gs_lk_leads.py
"""
Bronze: Raw extraction of lead data from GeoSpot lk_leads.

Source: GeoSpot PostgreSQL — lk_leads
Filters: at least one lead flag active, domain != spot2.mx, not deleted.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.funnel_ptd.shared import query_bronze_source


@dg.asset(
    group_name="funnel_ptd_bronze",
    description="Bronze: raw leads from GeoSpot lk_leads for funnel PTD.",
)
def raw_gs_lk_leads(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    query = """
    SELECT
        lead_id,
        lead_lead0_at,
        lead_lead1_at,
        lead_lead2_at,
        lead_lead3_at,
        lead_lead4_at,
        lead_l0,
        lead_l1,
        lead_l2,
        lead_l3,
        lead_l4,
        lead_domain,
        lead_deleted_at
    FROM lk_leads
    WHERE (lead_l0 OR lead_l1 OR lead_l2 OR lead_l3 OR lead_l4)
      AND (lead_domain NOT IN ('spot2.mx') OR lead_domain IS NULL)
      AND lead_deleted_at IS NULL
    """

    df = query_bronze_source(query, source_type="geospot_postgres", context=context)
    context.log.info(f"raw_gs_lk_leads: {df.height:,} rows, {df.width} columns")
    return df
