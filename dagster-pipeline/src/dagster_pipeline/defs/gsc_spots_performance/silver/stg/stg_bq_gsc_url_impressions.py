"""
Silver STG: Normalizes raw_bq_gsc_url_impressions.

Casts BigQuery-inferred types to canonical Polars types:
url to Utf8, numeric fields to their target types.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="gsp_silver",
    description="Silver STG: normalized GSC URL impressions with canonical types.",
)
def stg_bq_gsc_url_impressions(
    context: dg.AssetExecutionContext,
    raw_bq_gsc_url_impressions: pl.DataFrame,
) -> pl.DataFrame:
    """Normalizes types from raw BigQuery GSC data."""
    df = raw_bq_gsc_url_impressions.with_columns(
        pl.col("url").cast(pl.Utf8),
        pl.col("impressions").cast(pl.Int64),
        pl.col("clicks").cast(pl.Int64),
        pl.col("avg_position").cast(pl.Float64),
        pl.col("month").cast(pl.Int64),
        pl.col("year").cast(pl.Int64),
    )
    context.log.info(f"stg_bq_gsc_url_impressions: {df.height:,} rows, {df.width} columns")
    return df
