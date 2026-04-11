# defs/effective_supply/gold/gold_lk_effective_supply_rules.py
"""
Gold layer: lk_effective_supply_rules table.

Receives core_lk_effective_supply_rules and adds audit fields.
No business logic — all transformations happen upstream in core.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields


@dg.asset(
    group_name="effective_supply_gold",
    description="Gold: hierarchical rules with audit fields.",
)
def gold_lk_effective_supply_rules(
    context: dg.AssetExecutionContext,
    core_lk_effective_supply_rules: pl.DataFrame,
) -> pl.DataFrame:
    df = core_lk_effective_supply_rules.rename({"run_id": "aud_run_id"})
    df = add_audit_fields(df, job_name="lk_effective_supply_rules")

    context.log.info(
        f"gold_lk_effective_supply_rules: {df.height:,} rows, {df.width} columns"
    )
    return df
