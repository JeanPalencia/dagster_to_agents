# defs/effective_supply/silver/stg/stg_gs_effective_supply_run_id.py
"""
Silver STG: Compute next run_id from Bronze max value.

Receives raw max(aud_run_id) from GeoSpot and returns next_run_id = max + 1.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="effective_supply_silver",
    description="Silver STG: next run_id for current pipeline execution.",
)
def stg_gs_effective_supply_run_id(
    context: dg.AssetExecutionContext,
    raw_gs_effective_supply_run_id: pl.DataFrame,
) -> int:
    df = raw_gs_effective_supply_run_id

    if df.height == 0 or df["max_run_id"][0] is None:
        next_run_id = 1
    else:
        next_run_id = int(df["max_run_id"][0]) + 1

    context.log.info(f"stg_gs_effective_supply_run_id: next run_id = {next_run_id}")
    return next_run_id
