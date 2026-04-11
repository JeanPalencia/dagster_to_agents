# defs/effective_supply/silver/core/core_lk_effective_supply.py
"""
Silver Core Final: Prepares core_effective_supply for gold by adding run_id.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="effective_supply_silver",
    description="Core Final: core_effective_supply with run_id for gold.",
)
def core_lk_effective_supply(
    context: dg.AssetExecutionContext,
    core_effective_supply: pl.DataFrame,
    stg_gs_effective_supply_run_id: int,
) -> pl.DataFrame:
    df = core_effective_supply.with_columns(
        pl.lit(stg_gs_effective_supply_run_id).cast(pl.Int64).alias("run_id")
    )

    context.log.info(
        f"core_lk_effective_supply: {df.height:,} rows, "
        f"{df.width} columns, run_id={stg_gs_effective_supply_run_id}"
    )
    return df
