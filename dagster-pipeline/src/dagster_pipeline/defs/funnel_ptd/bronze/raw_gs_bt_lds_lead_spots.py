# defs/funnel_ptd/bronze/raw_gs_bt_lds_lead_spots.py
"""
Bronze: Raw extraction of lead-spot events for visit rejection check.

Source: GeoSpot PostgreSQL — bt_lds_lead_spots
Filters: lds_event_id = 4 (Visit Scheduled) with valid appointment statuses.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.funnel_ptd.shared import query_bronze_source


@dg.asset(
    group_name="funnel_ptd_bronze",
    description="Bronze: lead-spot visit events for rejection check (lds_event_id=4).",
)
def raw_gs_bt_lds_lead_spots(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    query = """
    SELECT
        project_id,
        lds_event_id,
        lds_event_at,
        appointment_last_date_status_id
    FROM bt_lds_lead_spots
    WHERE lds_event_id = 4
      AND appointment_last_date_status_id IN (1, 2, 3, 4, 7, 9)
    """

    df = query_bronze_source(query, source_type="geospot_postgres", context=context)
    context.log.info(f"raw_gs_bt_lds_lead_spots: {df.height:,} rows, {df.width} columns")
    return df
