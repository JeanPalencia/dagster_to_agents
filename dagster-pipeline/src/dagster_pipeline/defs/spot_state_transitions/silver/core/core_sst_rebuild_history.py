# defs/spot_state_transitions/silver/core/core_sst_rebuild_history.py
"""
Silver Core: Rebuilds transition history for affected spots.

Combines STG history (MySQL) + STG snapshots (GeoSpot), then:
1. Deduplicates consecutive (state, reason) runs
2. Adds prev/next state fields
3. Computes composite transition IDs (sstd_*)
4. Selects and renames final columns

Only reads from STG assets — no database queries.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_state_transitions.processing import (
    dedup_consecutive_runs,
    add_prev_fields,
    add_next_fields,
    add_sstd_ids,
    FINAL_COLUMNS,
    FINAL_RENAME,
)


@dg.asset(
    group_name="sst_silver",
    description="Silver Core: rebuilt transition history (dedup + prev/next + sstd IDs).",
)
def core_sst_rebuild_history(
    context: dg.AssetExecutionContext,
    stg_s2p_sst_affected_history: pl.DataFrame,
    stg_gs_sst_snapshots: pl.DataFrame,
) -> pl.DataFrame:
    """Combines, deduplicates, and enriches transition history."""
    context.log.info(
        f"Inputs: history={stg_s2p_sst_affected_history.height:,} rows, "
        f"snapshots={stg_gs_sst_snapshots.height:,} rows"
    )

    if stg_s2p_sst_affected_history.height == 0 and stg_gs_sst_snapshots.height == 0:
        context.log.warning("Both inputs are empty — returning empty DataFrame.")
        return pl.DataFrame(
            schema={col: pl.Int64 for col in FINAL_COLUMNS}
        ).rename(FINAL_RENAME)

    combined = pl.concat(
        [stg_gs_sst_snapshots, stg_s2p_sst_affected_history],
        how="diagonal_relaxed",
    )
    context.log.info(f"Combined: {combined.height:,} rows")

    if "source_sst_id" in combined.columns:
        combined = combined.cast({"source_sst_id": pl.Int64}, strict=False)

    cleaned = dedup_consecutive_runs(combined)
    context.log.info(f"After dedup: {cleaned.height:,} rows")

    enriched = add_prev_fields(cleaned)
    enriched = add_next_fields(enriched)
    enriched = add_sstd_ids(enriched)

    available_cols = [c for c in FINAL_COLUMNS if c in enriched.columns]
    result = enriched.select(available_cols).rename(FINAL_RENAME)

    context.log.info(f"core_sst_rebuild_history: {result.height:,} rows, {result.width} columns")
    return result
