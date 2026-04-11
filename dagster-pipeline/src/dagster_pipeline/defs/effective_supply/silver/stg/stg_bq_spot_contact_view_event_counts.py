# defs/effective_supply/silver/stg/stg_bq_spot_contact_view_event_counts.py
"""
Silver STG: Normalize spot contact and view event counts from Bronze.

Receives raw BigQuery extraction and normalizes types.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="effective_supply_silver",
    description="Silver STG: normalized spot contact/view event counts from BigQuery.",
)
def stg_bq_spot_contact_view_event_counts(
    context: dg.AssetExecutionContext,
    raw_bq_spot_contact_view_event_counts: pl.DataFrame,
) -> pl.DataFrame:
    df = raw_bq_spot_contact_view_event_counts

    context.log.info(
        f"stg_bq_spot_contact_view_event_counts: {df.height:,} rows, {df.width} columns"
    )
    return df
