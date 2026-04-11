"""
Gold layer: rpt_browse_visitors.

Adds audit fields for tracking inserts/updates.
No heavy business logic -- reads from Core only.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields


@dg.asset(
    group_name="bv_gold",
    description="Gold: rpt_browse_visitors with audit fields.",
)
def gold_rpt_browse_visitors(
    context: dg.AssetExecutionContext,
    core_browse_visitors: pl.DataFrame,
) -> pl.DataFrame:
    """Adds audit fields to core_browse_visitors."""
    df = add_audit_fields(core_browse_visitors, job_name="browse_visitors")
    context.log.info(f"gold_rpt_browse_visitors: {df.height:,} rows, {df.width} columns")
    return df
