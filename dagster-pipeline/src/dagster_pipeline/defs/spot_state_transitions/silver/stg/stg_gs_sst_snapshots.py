# defs/spot_state_transitions/silver/stg/stg_gs_sst_snapshots.py
"""
Silver STG: Normalizes raw_gs_sst_snapshots types.

Casts PostgreSQL columns to canonical Polars types. No queries.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_state_transitions.processing import TRANSITIONS_SCHEMA


@dg.asset(
    group_name="sst_silver",
    description="Silver STG: type-normalized snapshots from GeoSpot.",
)
def stg_gs_sst_snapshots(
    context: dg.AssetExecutionContext,
    raw_gs_sst_snapshots: pl.DataFrame,
) -> pl.DataFrame:
    """Normalizes PostgreSQL types to canonical schema."""
    if raw_gs_sst_snapshots.height == 0:
        context.log.info("stg_gs_sst_snapshots: 0 rows (empty input)")
        return raw_gs_sst_snapshots

    df = raw_gs_sst_snapshots.cast(TRANSITIONS_SCHEMA, strict=False)
    context.log.info(f"stg_gs_sst_snapshots: {df.height:,} rows, {df.width} columns")
    return df
