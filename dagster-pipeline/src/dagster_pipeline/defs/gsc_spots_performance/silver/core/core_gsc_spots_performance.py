"""
Silver Core: GSC Spots Performance.

Extracts spot_id from /spots URLs via regex, then INNER JOINs with
lk_spots attributes to produce the enriched performance report.

URL patterns covered:
  - https://spot2.mx/spots/{slug}/{spot_id}
  - https://spot2.mx/spots/{spot_id}
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="gsp_core",
    description="Core: extracts spot_id from URL, JOINs with lk_spots attributes.",
)
def core_gsc_spots_performance(
    context: dg.AssetExecutionContext,
    stg_bq_gsc_url_impressions: pl.DataFrame,
    stg_gs_lk_spots_gsp: pl.DataFrame,
) -> pl.DataFrame:
    """Extracts spot_id from URL and enriches with spot attributes."""
    df = stg_bq_gsc_url_impressions.with_columns(
        pl.col("url")
        .str.extract(r"/spots/(?:.+/)?(\d+)/?(?:\?.*)?$", 1)
        .cast(pl.Int64, strict=False)
        .alias("spot_id")
    )

    total = df.height
    nulls = df.filter(pl.col("spot_id").is_null()).height
    context.log.info(
        f"spot_id extracted: {total - nulls:,} / {total:,} "
        f"({(total - nulls) / total * 100:.2f}%)"
    )

    df_out = (
        df.join(stg_gs_lk_spots_gsp, on="spot_id", how="inner")
        .select(
            "spot_id",
            "spot_type",
            "spot_sector",
            "spot_status_full",
            "spot_modality",
            "spot_address",
            "url",
            "impressions",
            "clicks",
            "avg_position",
            "month",
            "year",
        )
    )

    dropped = total - df_out.height
    context.log.info(
        f"INNER JOIN: {df_out.height:,} rows final, "
        f"{dropped:,} dropped (no spot match or null spot_id)"
    )
    return df_out
