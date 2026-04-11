"""
Silver STG: Normalizes raw_bq_browse_sessions.

Casts types, ensures ga_session_id is Utf8, event_date is Date,
and extracts the browse sector from the first browse page URL.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="bv_silver",
    description="Silver STG: normalized browse sessions with sector extraction.",
)
def stg_bq_browse_sessions(
    context: dg.AssetExecutionContext,
    raw_bq_browse_sessions: pl.DataFrame,
) -> pl.DataFrame:
    """Normalizes types and extracts sector from browse page URLs."""
    df = raw_bq_browse_sessions.with_columns(
        pl.col("event_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d", strict=False).alias("event_date"),
        pl.col("ga_session_id").cast(pl.Utf8),
        pl.col("user_pseudo_id").cast(pl.Utf8),
        pl.col("source").cast(pl.Utf8),
        pl.col("medium").cast(pl.Utf8),
        pl.col("campaign_name").cast(pl.Utf8),
        pl.col("channel").cast(pl.Utf8),
        pl.col("traffic_type").cast(pl.Utf8),
        pl.col("is_scraping").cast(pl.Int64),
        pl.col("is_technical_waste").cast(pl.Int64),
    )

    # Extract sector from first_browse_page URL (first path segment after domain)
    df = df.with_columns(
        pl.col("first_browse_page")
        .str.extract(r"https://spot2\.mx/([^/?]+)", 1)
        .alias("browse_sector"),
    )

    context.log.info(f"stg_bq_browse_sessions: {df.height:,} rows, {df.width} columns")
    return df
