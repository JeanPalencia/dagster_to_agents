# defs/effective_supply/gold/gold_lk_effective_supply_model_metrics.py
"""
Gold layer: lk_effective_supply_model_metrics table.

Receives core_lk_effective_supply_model_metrics and adds audit fields.
No business logic — all transformations happen upstream in core.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields


@dg.asset(
    group_name="effective_supply_gold",
    description="Gold: model evaluation metrics with audit fields.",
)
def gold_lk_effective_supply_model_metrics(
    context: dg.AssetExecutionContext,
    core_lk_effective_supply_model_metrics: pl.DataFrame,
) -> pl.DataFrame:
    df = core_lk_effective_supply_model_metrics.rename({"run_id": "aud_run_id"})
    df = add_audit_fields(df, job_name="lk_effective_supply_model_metrics")

    context.log.info(
        f"gold_lk_effective_supply_model_metrics: {df.height} rows, {df.width} columns"
    )
    return df
