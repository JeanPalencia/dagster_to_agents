# defs/effective_supply/silver/core/core_effective_supply_events.py
"""
Silver Core: Effective Supply Events - Combined event counts (UNION only).

This asset combines:
1. Contact + View events from BigQuery (event_type_id 1, 2)
2. Project events from GeoSpot bt_lds_lead_spots (event_type_id 3)

Output: Unified event rows per spot_id + event_type (no spot attributes yet).
"""
import dagster as dg
import polars as pl
from datetime import date, timedelta


def _get_six_month_window() -> tuple[date, date]:
    """
    Calculate the 6 closed months window.
    
    Returns:
        Tuple of (start_date, end_date) for the last 6 closed months.
        Example: If today is 2025-02-10, returns (2024-08-01, 2025-01-31)
    """
    today = date.today()
    # End of last closed month
    end_month_last_day = today.replace(day=1) - timedelta(days=1)
    end_month_start = end_month_last_day.replace(day=1)
    # Start: 5 months before end_month_start (total 6 months)
    start_month = (end_month_start - timedelta(days=150)).replace(day=1)
    # End date is end of end_month
    end_date = (end_month_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    
    return start_month, end_date


@dg.asset(
    group_name="effective_supply_silver",
    description=(
        "Silver Core: UNION of Contact, View, and Project events. "
        "Combines BigQuery events + GeoSpot project events into a unified event table."
    ),
)
def core_effective_supply_events(
    context: dg.AssetExecutionContext,
    stg_bq_spot_contact_view_event_counts: pl.DataFrame,
    stg_gs_bt_lds_spot_added: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds the unified event table for effective supply.
    
    Steps:
    1. Use Contact + View events from BigQuery as-is
    2. Aggregate Project events (spot added to project) from GeoSpot:
       - Filter last 6 closed months
       - Group by spot_id
       - Count occurrences
       - Add event_type_id=3, event_type="Project", event_name="spotAddedToProject"
    3. UNION ALL both event sources
    """
    # =========================================================================
    # 1. Contact + View events from BigQuery (already aggregated)
    # =========================================================================
    # Cast spot_id to Int64 for consistency (BigQuery may return as String)
    df_contact_view = stg_bq_spot_contact_view_event_counts.with_columns(
        pl.col("spot_id").cast(pl.Int64).alias("spot_id")
    )
    context.log.info(f"Contact/View events from BigQuery: {df_contact_view.height:,} rows")
    
    # =========================================================================
    # 2. Project events: aggregate from GeoSpot
    # =========================================================================
    start_date, end_date = _get_six_month_window()
    context.log.info(f"Project events window: {start_date} to {end_date}")
    
    df_project = (
        stg_gs_bt_lds_spot_added
        .filter(pl.col("spot_id").is_not_null())
        .with_columns(
            pl.col("lds_event_at").dt.date().alias("event_date")
        )
        .filter(pl.col("event_date") >= start_date)
        .filter(pl.col("event_date") <= end_date)
        .group_by("spot_id")
        .agg([
            pl.len().alias("event_count"),
            pl.col("event_date").min().alias("initial_date"),
            pl.col("event_date").max().alias("end_date"),
        ])
        # Add constant columns after aggregation (cast to match BigQuery types)
        .with_columns([
            pl.lit(3).cast(pl.Int64).alias("event_type_id"),
            pl.lit("Project").alias("event_type"),
            pl.lit("spotAddedToProject").alias("event_name"),
            pl.col("event_count").cast(pl.Int64),  # Match BigQuery Int64
        ])
        # Cast spot_id to Int64 for consistency
        .with_columns(
            pl.col("spot_id").cast(pl.Int64).alias("spot_id")
        )
    )
    context.log.info(f"Project events from GeoSpot: {df_project.height:,} rows")
    
    # =========================================================================
    # 3. UNION ALL - select columns explicitly for consistency
    # =========================================================================
    event_cols = [
        "spot_id", "event_type_id", "event_type", "event_name",
        "event_count", "initial_date", "end_date"
    ]
    df_contact_view_selected = df_contact_view.select(event_cols)
    df_project_selected = df_project.select(event_cols)
    
    df_events = pl.concat([df_contact_view_selected, df_project_selected])
    context.log.info(f"core_effective_supply_events: {df_events.height:,} rows after UNION ALL")
    
    return df_events
