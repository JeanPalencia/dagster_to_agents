# defs/effective_supply/silver/stg/stg_gs_lk_spots.py
"""
Silver STG: Normalize spot attributes from Bronze.

Receives raw GeoSpot lk_spots extraction and normalizes types.
Filters already applied in Bronze (non-deleted, non-complex).
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="effective_supply_silver",
    description="Silver STG: normalized spot attributes from GeoSpot lk_spots.",
)
def stg_gs_lk_spots(
    context: dg.AssetExecutionContext,
    raw_gs_lk_spots: pl.DataFrame,
) -> pl.DataFrame:
    df = raw_gs_lk_spots

    context.log.info(
        f"stg_gs_lk_spots: {df.height:,} rows, {df.width} columns"
    )
    return df
