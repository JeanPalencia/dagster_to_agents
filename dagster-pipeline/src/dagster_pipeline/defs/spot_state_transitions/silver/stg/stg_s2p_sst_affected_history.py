# defs/spot_state_transitions/silver/stg/stg_s2p_sst_affected_history.py
"""
Silver STG: Normalizes raw_s2p_sst_affected_history types.

Casts MySQL columns to canonical Polars types. No queries.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_state_transitions.processing import TRANSITIONS_SCHEMA


@dg.asset(
    group_name="sst_silver",
    description="Silver STG: type-normalized spot_state_transitions history from MySQL S2P.",
)
def stg_s2p_sst_affected_history(
    context: dg.AssetExecutionContext,
    raw_s2p_sst_affected_history: pl.DataFrame,
) -> pl.DataFrame:
    """Normalizes MySQL types to canonical schema."""
    if raw_s2p_sst_affected_history.height == 0:
        context.log.info("stg_s2p_sst_affected_history: 0 rows (empty input)")
        return raw_s2p_sst_affected_history

    df = raw_s2p_sst_affected_history.cast(TRANSITIONS_SCHEMA, strict=False)
    context.log.info(f"stg_s2p_sst_affected_history: {df.height:,} rows, {df.width} columns")
    return df
