# defs/data_lakehouse/silver/stg/stg_s2p_prs_project_spots.py
"""
Silver STG: Project-Spot relationships from project_requirement_spot table.

This asset transforms raw project-spot data, filtering out null spots
and keeping only the first record per (project_id, spot_id) group.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _transform_s2p_prs_project_spots(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms raw_s2p_project_requirement_spots:
    - Renames fields with project_spot_* prefix
    - Filters out records where spot_id is null
    - Groups by (project_id, spot_id) keeping the first created (min id)
    """

    # =========================================================================
    # 1. SELECT AND RENAME FIELDS
    # =========================================================================

    df_renamed = df.select([
        pl.col("id").alias("project_spot_id"),
        pl.col("project_requirement_id").alias("project_id"),
        pl.col("spot_id"),
        pl.col("spot_stage").alias("project_spot_status_id"),
        # TODO: Add project_spot_status (catalog) when available
        pl.col("created_at").alias("project_spot_created_at"),
        pl.col("created_at").dt.date().alias("project_spot_created_date"),
        pl.col("updated_at").alias("project_spot_updated_at"),
        pl.col("updated_at").dt.date().alias("project_spot_updated_date"),
    ])

    # =========================================================================
    # 2. FILTER OUT NULL SPOT_IDS
    # =========================================================================

    df_filtered = df_renamed.filter(pl.col("spot_id").is_not_null())

    # =========================================================================
    # 3. DEDUPLICATE: KEEP FIRST RECORD PER (project_id, spot_id)
    # The "first" is determined by the minimum project_spot_id
    # =========================================================================

    df_deduped = (
        df_filtered
        .sort("project_spot_id")  # Sort by id ascending
        .group_by(["project_id", "spot_id"])
        .first()  # Keep first row per group (smallest project_spot_id)
    )

    return df_deduped


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    description=(
        "Silver STG: Project-Spot relationships from project_requirement_spot. "
        "Filters null spots and deduplicates by (project_id, spot_id)."
    ),
)
def stg_s2p_prs_project_spots(
    context: dg.AssetExecutionContext,
    raw_s2p_project_requirement_spots: pl.DataFrame,
):
    """
    Transforms project_requirement_spot table:
    
    - project_requirement_id -> project_id
    - id -> project_spot_id
    - spot_stage -> project_spot_status_id
    - created_at/updated_at -> project_spot_created_at/updated_at
    
    Filters:
    - spot_id IS NOT NULL
    
    Deduplication:
    - Groups by (project_id, spot_id), keeps min(id) per group
    """
    def body():
        df = _transform_s2p_prs_project_spots(raw_s2p_project_requirement_spots)

        context.log.info(
            f"stg_s2p_prs_project_spots: {df.height} rows for partition {context.partition_key}"
        )
        return df

    yield from iter_job_wrapped_compute(context, body)
