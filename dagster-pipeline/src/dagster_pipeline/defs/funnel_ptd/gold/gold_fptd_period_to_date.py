# defs/funnel_ptd/gold/gold_fptd_period_to_date.py
"""
Gold: Period-to-Date funnel monitor with audit fields.

Receives core_fptd_final and adds standard audit metadata.
No business logic — all transformations happen upstream in Core.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields


@dg.asset(
    group_name="funnel_ptd_gold",
    description="Gold: period-to-date funnel monitor with audit fields.",
)
def gold_fptd_period_to_date(
    context: dg.AssetExecutionContext,
    core_fptd_final: pl.DataFrame,
) -> pl.DataFrame:
    df = add_audit_fields(core_fptd_final, job_name="funnel_ptd")

    context.log.info(
        f"gold_fptd_period_to_date: {df.height} rows, {df.width} columns"
    )
    return df
