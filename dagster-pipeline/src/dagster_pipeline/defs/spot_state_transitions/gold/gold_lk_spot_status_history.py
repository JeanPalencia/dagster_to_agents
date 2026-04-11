# defs/spot_state_transitions/gold/gold_lk_spot_status_history.py
"""
Gold layer: lk_spot_status_history.

Adds:
- spot_hard_deleted_id (0/1) and spot_hard_deleted ("No"/"Yes"):
  spots not found in lk_spots are considered hard-deleted.
- Audit fields for tracking inserts/updates.

Reads from Core + STG — no database queries.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields


@dg.asset(
    group_name="sst_gold",
    description="Gold: lk_spot_status_history with hard-delete flag and audit fields.",
)
def gold_lk_spot_status_history(
    context: dg.AssetExecutionContext,
    core_sst_rebuild_history: pl.DataFrame,
    stg_gs_active_spot_ids: pl.DataFrame,
) -> pl.DataFrame:
    """Adds hard-delete flag and audit fields."""
    if core_sst_rebuild_history.height == 0:
        context.log.warning("Empty input — returning empty DataFrame.")
        return core_sst_rebuild_history

    active_set = set(stg_gs_active_spot_ids["spot_id"].to_list())

    df = core_sst_rebuild_history.with_columns(
        pl.col("spot_id")
        .map_elements(lambda x: 0 if x in active_set else 1, return_dtype=pl.Int64)
        .alias("spot_hard_deleted_id")
    )

    df = df.with_columns(
        pl.when(pl.col("spot_hard_deleted_id") == 0)
        .then(pl.lit("No"))
        .otherwise(pl.lit("Yes"))
        .alias("spot_hard_deleted")
    )

    n_deleted = df.filter(pl.col("spot_hard_deleted_id") == 1)["spot_id"].n_unique()
    n_deleted_rows = df.filter(pl.col("spot_hard_deleted_id") == 1).height
    context.log.info(f"Hard-deleted: {n_deleted:,} spots ({n_deleted_rows:,} rows)")

    df = add_audit_fields(df, job_name="lk_spot_status_history")

    context.log.info(
        f"gold_lk_spot_status_history: {df.height:,} rows, {df.width} columns"
    )
    return df
