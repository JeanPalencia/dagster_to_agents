"""
Silver STG: Normalizes raw_gs_lk_spots_gsp.

Casts spot_id to Int64 (pipeline-internal type) and string fields to Utf8.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="gsp_silver",
    description="Silver STG: normalized lk_spots attributes with canonical types.",
)
def stg_gs_lk_spots_gsp(
    context: dg.AssetExecutionContext,
    raw_gs_lk_spots_gsp: pl.DataFrame,
) -> pl.DataFrame:
    """Normalizes types from raw GeoSpot lk_spots data."""
    df = raw_gs_lk_spots_gsp.with_columns(
        pl.col("spot_id").cast(pl.Int64),
        pl.col("spot_type").cast(pl.Utf8),
        pl.col("spot_sector").cast(pl.Utf8),
        pl.col("spot_status_full").cast(pl.Utf8),
        pl.col("spot_modality").cast(pl.Utf8),
        pl.col("spot_address").cast(pl.Utf8),
    )
    context.log.info(f"stg_gs_lk_spots_gsp: {df.height:,} rows, {df.width} columns")
    return df
