# defs/spot_state_transitions/silver/stg/stg_gs_active_spot_ids.py
"""
Silver STG: Normalizes raw_gs_active_spot_ids types.

Ensures spot_id is Int64. No queries.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="sst_silver",
    description="Silver STG: active spot_ids from GeoSpot lk_spots (type-normalized).",
)
def stg_gs_active_spot_ids(
    context: dg.AssetExecutionContext,
    raw_gs_active_spot_ids: pl.DataFrame,
) -> pl.DataFrame:
    """Ensures spot_id is Int64."""
    df = raw_gs_active_spot_ids.cast({"spot_id": pl.Int64}, strict=False)
    context.log.info(f"stg_gs_active_spot_ids: {df.height:,} active spots")
    return df
