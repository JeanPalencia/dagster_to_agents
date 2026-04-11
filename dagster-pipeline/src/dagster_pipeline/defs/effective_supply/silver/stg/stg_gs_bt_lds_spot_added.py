# defs/effective_supply/silver/stg/stg_gs_bt_lds_spot_added.py
"""
Silver STG: Normalize Spot Added events from Bronze.

Receives raw GeoSpot bt_lds_lead_spots extraction and normalizes types.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="effective_supply_silver",
    description="Silver STG: normalized spot added events from GeoSpot bt_lds_lead_spots.",
)
def stg_gs_bt_lds_spot_added(
    context: dg.AssetExecutionContext,
    raw_gs_bt_lds_spot_added: pl.DataFrame,
) -> pl.DataFrame:
    df = raw_gs_bt_lds_spot_added

    context.log.info(
        f"stg_gs_bt_lds_spot_added: {df.height:,} rows, {df.width} columns"
    )
    return df
