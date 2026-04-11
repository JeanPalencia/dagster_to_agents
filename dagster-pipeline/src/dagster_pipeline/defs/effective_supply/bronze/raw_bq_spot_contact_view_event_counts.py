# defs/effective_supply/bronze/raw_bq_spot_contact_view_event_counts.py
"""
Bronze: Raw extraction of spot contact and view event counts from BigQuery.

Source: BigQuery scheduled query (spot2-mx-ga4-bq.analitics_spot2).
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.effective_supply.shared import query_bronze_source


@dg.asset(
    group_name="effective_supply_bronze",
    description="Bronze: raw spot contact/view event counts from BigQuery.",
)
def raw_bq_spot_contact_view_event_counts(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    query = """
    SELECT *
    FROM `spot2-mx-ga4-bq.analitics_spot2.spot_contact_view_event_counts`
    """

    df = query_bronze_source(query, source_type="bigquery", context=context)
    context.log.info(
        f"raw_bq_spot_contact_view_event_counts: {df.height:,} rows, {df.width} columns"
    )
    return df
