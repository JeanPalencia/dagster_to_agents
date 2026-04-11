"""
Gold layer: rpt_gsc_spots_performance.

Adds audit fields for tracking inserts/updates.
No heavy business logic -- reads from Core only.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields


@dg.asset(
    group_name="gsp_gold",
    description="Gold: rpt_gsc_spots_performance with audit fields.",
)
def gold_rpt_gsc_spots_performance(
    context: dg.AssetExecutionContext,
    core_gsc_spots_performance: pl.DataFrame,
) -> pl.DataFrame:
    """Adds audit fields to core_gsc_spots_performance."""
    df = add_audit_fields(core_gsc_spots_performance, job_name="gsc_spots_performance")
    context.log.info(f"gold_rpt_gsc_spots_performance: {df.height:,} rows, {df.width} columns")
    return df
