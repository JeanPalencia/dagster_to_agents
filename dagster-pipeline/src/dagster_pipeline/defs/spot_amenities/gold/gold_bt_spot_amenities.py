"""
Gold layer: bt_spot_amenities.

Adds audit fields for tracking inserts/updates.
No heavy business logic — reads from Core only.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields


@dg.asset(
    group_name="spa_gold",
    description="Gold: bt_spot_amenities with audit fields.",
)
def gold_bt_spot_amenities(
    context: dg.AssetExecutionContext,
    core_bt_spot_amenities: pl.DataFrame,
) -> pl.DataFrame:
    """Adds audit fields to core_bt_spot_amenities."""
    df = add_audit_fields(core_bt_spot_amenities, job_name="bt_spot_amenities")
    context.log.info(f"gold_bt_spot_amenities: {df.height:,} rows, {df.width} columns")
    return df
