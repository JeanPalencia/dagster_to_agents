# defs/effective_supply/bronze/raw_gs_bt_lds_spot_added.py
"""
Bronze: Raw extraction of Spot Added events from GeoSpot bt_lds_lead_spots.

Source: GeoSpot PostgreSQL - bt_lds_lead_spots filtered by lds_event_id = 3.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.effective_supply.shared import query_bronze_source


@dg.asset(
    group_name="effective_supply_bronze",
    description="Bronze: raw spot added events from GeoSpot bt_lds_lead_spots (stage 3).",
)
def raw_gs_bt_lds_spot_added(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    query = """
    SELECT *
    FROM bt_lds_lead_spots
    WHERE lds_event_id = 3
    """

    df = query_bronze_source(query, source_type="geospot_postgres", context=context)
    context.log.info(
        f"raw_gs_bt_lds_spot_added: {df.height:,} rows, {df.width} columns"
    )
    return df
