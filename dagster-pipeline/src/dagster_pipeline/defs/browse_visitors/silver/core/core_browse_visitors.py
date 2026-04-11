"""
Silver Core: core_browse_visitors.

Joins browse sessions with MAT identity data, resolves canonical visitor ID,
and aggregates to one row per unique visitor with session metrics.
"""
import dagster as dg
import polars as pl


def _mode(series: pl.Series) -> str | None:
    """Return the most frequent non-null value in a Series, or None."""
    counts = series.drop_nulls().value_counts(sort=True)
    if counts.height == 0:
        return None
    return counts[0, 0]


_FINAL_COLUMNS = [
    "canonical_visitor_id",
    "total_sessions",
    "total_page_views",
    "first_session_date",
    "last_session_date",
    "days_active",
    "primary_channel",
    "primary_traffic_type",
    "primary_source",
    "primary_medium",
    "sectors_visited",
    "is_converted",
    "lead_id",
    "lead_sector",
    "lead_cohort_type",
    "is_scraping",
]


@dg.asset(
    group_name="bv_silver",
    description="Silver Core: one row per unique visitor with aggregated browse session metrics.",
)
def core_browse_visitors(
    context: dg.AssetExecutionContext,
    stg_bq_browse_sessions: pl.DataFrame,
    stg_gs_mat_vtl: pl.DataFrame,
) -> pl.DataFrame:
    """JOIN sessions with MAT, resolve identity, aggregate per visitor."""
    n_null_upid = stg_bq_browse_sessions.filter(pl.col("user_pseudo_id").is_null()).height
    if n_null_upid:
        context.log.warning(
            f"Dropping {n_null_upid:,} sessions with NULL user_pseudo_id (unidentifiable)"
        )

    sessions = stg_bq_browse_sessions.filter(
        pl.col("user_pseudo_id").is_not_null()
        & (pl.col("is_scraping") == 0)
        & (pl.col("is_technical_waste") == 0)
    )
    context.log.info(
        f"Sessions after filtering scraping/waste/null-upid: {sessions.height:,} "
        f"(removed {stg_bq_browse_sessions.height - sessions.height:,})"
    )

    # LEFT JOIN with MAT to get canonical identity
    joined = sessions.join(
        stg_gs_mat_vtl.select(
            "vis_user_pseudo_id",
            "mat_canonical_visitor_id",
            "lead_id",
            "lead_sector",
            "lead_cohort_type",
            "mat_with_match",
        ),
        left_on="user_pseudo_id",
        right_on="vis_user_pseudo_id",
        how="left",
    )

    # Resolve canonical_visitor_id: MAT value if matched, else user_pseudo_id
    joined = joined.with_columns(
        pl.when(pl.col("mat_canonical_visitor_id").is_not_null())
        .then(pl.col("mat_canonical_visitor_id"))
        .otherwise(pl.col("user_pseudo_id"))
        .alias("canonical_visitor_id"),
        pl.col("mat_with_match").fill_null(False).alias("is_converted"),
        (pl.col("is_scraping") == 1).alias("is_scraping"),
    )

    # Aggregate per canonical_visitor_id
    agg = joined.group_by("canonical_visitor_id").agg(
        pl.len().alias("total_sessions"),
        pl.col("browse_page_views").sum().alias("total_page_views"),
        pl.col("event_date").min().alias("first_session_date"),
        pl.col("event_date").max().alias("last_session_date"),
        pl.col("event_date").n_unique().alias("days_active"),
        pl.col("channel").mode().first().alias("primary_channel"),
        pl.col("traffic_type").mode().first().alias("primary_traffic_type"),
        pl.col("source").mode().first().alias("primary_source"),
        pl.col("medium").mode().first().alias("primary_medium"),
        pl.col("browse_sector").drop_nulls().unique().sort().str.concat(", ").alias("sectors_visited"),
        pl.col("is_converted").max().alias("is_converted"),
        pl.col("lead_id").drop_nulls().first().alias("lead_id"),
        pl.col("lead_sector").drop_nulls().first().alias("lead_sector"),
        pl.col("lead_cohort_type").drop_nulls().first().alias("lead_cohort_type"),
        pl.col("is_scraping").max().alias("is_scraping"),
    )

    # Replace empty sectors_visited with null
    agg = agg.with_columns(
        pl.when(pl.col("sectors_visited") == "")
        .then(pl.lit(None))
        .otherwise(pl.col("sectors_visited"))
        .alias("sectors_visited"),
    )

    result = agg.select(_FINAL_COLUMNS)

    n_converted = result.filter(pl.col("is_converted")).height
    context.log.info(
        f"core_browse_visitors: {result.height:,} unique visitors "
        f"({n_converted:,} converted, {result.height - n_converted:,} not converted)"
    )
    return result
