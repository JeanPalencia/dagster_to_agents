"""
Silver STG: Normalizes raw_gs_mat_vtl.

Selects and renames columns needed for identity resolution and
lead enrichment in core_browse_visitors.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="bv_silver",
    description="Silver STG: MAT identity and lead metadata for browse visitor matching.",
)
def stg_gs_mat_vtl(
    context: dg.AssetExecutionContext,
    raw_gs_mat_vtl: pl.DataFrame,
) -> pl.DataFrame:
    """Selects and casts MAT columns for downstream join."""
    df = raw_gs_mat_vtl.select(
        pl.col("vis_user_pseudo_id").cast(pl.Utf8),
        pl.col("mat_canonical_visitor_id").cast(pl.Utf8),
        pl.col("lead_id").cast(pl.Int64, strict=False),
        pl.col("mat_channel").cast(pl.Utf8),
        pl.col("lead_sector").cast(pl.Utf8),
        pl.col("lead_cohort_type").cast(pl.Utf8),
        (pl.col("mat_with_match").cast(pl.Utf8) == "with_match").alias("mat_with_match"),
    ).unique(subset=["vis_user_pseudo_id"])

    context.log.info(f"stg_gs_mat_vtl: {df.height:,} rows (unique by vis_user_pseudo_id)")
    return df
