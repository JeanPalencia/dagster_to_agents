# defs/effective_supply/gold/gold_lk_effective_supply_propensity_scores.py
"""
Gold layer: lk_effective_supply_propensity_scores table.

Receives core_lk_effective_supply_propensity_scores and adds audit fields.
No business logic — all transformations happen upstream in core.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields


@dg.asset(
    group_name="effective_supply_gold",
    description="Gold: propensity scores with audit fields.",
)
def gold_lk_effective_supply_propensity_scores(
    context: dg.AssetExecutionContext,
    core_lk_effective_supply_propensity_scores: pl.DataFrame,
) -> pl.DataFrame:
    df = core_lk_effective_supply_propensity_scores.rename({"run_id": "aud_run_id"})
    df = add_audit_fields(df, job_name="lk_effective_supply_propensity_scores")

    context.log.info(
        f"gold_lk_effective_supply_propensity_scores: {df.height:,} rows, {df.width} columns"
    )
    return df
