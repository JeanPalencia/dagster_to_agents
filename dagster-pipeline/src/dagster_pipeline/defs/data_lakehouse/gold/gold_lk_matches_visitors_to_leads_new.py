# defs/data_lakehouse/gold/gold_lk_matches_visitors_to_leads_new.py
"""
Gold layer: lk_matches_visitors_to_leads (NEW VERSION).

Reads stg_gs_lk_matches_visitors_to_leads_new from S3, adds audit fields,
writes parquet to S3 and loads to Geospot (lk_matches_visitors_to_leads_governance).
Same write pattern as other gold tables: single parquet write + load_to_geospot with same s3_key.

Replicates temp_matches_users_to_leads (legacy): output 27 columns (22 business including page_location + 5 aud_*) to match
lk_matches_visitors_to_leads_governance. page_location is propagated from silver for atribución (URLs spot2.mx, UTM, etc.).

Volume / deduplication (QA 26 Feb 2026):
- This asset does NOT apply explicit deduplication: it only reads silver, ensures columns, and adds aud_*.
- Volume differences vs legacy (~75 rows only in legacy, ~5 only in governance) may be due to:
  partition scope, source snapshot timing (Geospot temp_matches_users_to_leads), or legacy receiving
  incremental inserts while governance replaces by partition. If a filter or dedup is added later,
  document the criterion here.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_gold_s3_key,
    write_polars_to_s3,
    read_silver_from_s3,
    load_to_geospot,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.notifications import send_stale_data_notification
from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields

# Exact business column order to match temp_matches_users_to_leads (legacy).
# Table lk_matches_visitors_to_leads_governance: 22 business + 5 aud = 27 cols (includes page_location for atribución).
LK_MATCHES_VISITORS_TO_LEADS_COLUMN_ORDER = (
    "user_pseudo_id", "event_datetime_first", "event_name_first", "channel_first", "match_source",
    "client_id", "conversation_start", "event_datetime", "event_name", "channel",
    "source", "medium", "campaign_name", "page_location",
    "phone_number", "email_clients",
    "lead_min_at", "lead_min_type", "lead_type", "lead_date", "year_month_first", "with_match",
)
# Legacy 21 business cols (no page_location); kept for reference only.
LK_MATCHES_VISITORS_TO_LEADS_COLUMN_ORDER_LEGACY_21 = (
    "user_pseudo_id", "event_datetime_first", "event_name_first", "channel_first", "match_source",
    "client_id", "conversation_start", "event_datetime", "event_name", "channel",
    "source", "medium", "campaign_name",
    "phone_number", "email_clients",
    "lead_min_at", "lead_min_type", "lead_type", "lead_date", "year_month_first", "with_match",
)


@dg.asset(
    name="gold_lk_matches_visitors_to_leads_new",
    partitions_def=daily_partitions,
    group_name="gold",
    retry_policy=dg.RetryPolicy(max_retries=2),
    io_manager_key="s3_gold",
    description=(
        "Gold: lk_matches_visitors_to_leads. "
        "Reads stg_gs_lk_matches_visitors_to_leads_new from S3, adds audit, writes parquet and loads to Geospot (lk_matches_visitors_to_leads_governance)."
    ),
)
def gold_lk_matches_visitors_to_leads_new(context):
    """
    Builds the gold lk_matches_visitors_to_leads table from stg_gs_lk_matches_visitors_to_leads_new.
    Same pattern as gold_lk_spots_new: parquet only, one s3_key for write and load_to_geospot.
    No deduplication is applied; row count depends on silver (and partition/source snapshot).
    """
    def body():
        stale_tables = []
        stg_df, meta = read_silver_from_s3("stg_gs_lk_matches_visitors_to_leads_new", context)
        if meta.get("is_stale"):
            stale_tables.append({
                "table_name": "stg_gs_lk_matches_visitors_to_leads_new",
                "expected_date": context.partition_key,
                "available_date": meta.get("file_date", ""),
                "layer": "silver",
                "file_path": meta.get("file_path", ""),
            })

        for c in LK_MATCHES_VISITORS_TO_LEADS_COLUMN_ORDER:
            if c not in stg_df.columns:
                if c.endswith("_at") or "datetime" in c or c == "lead_date":
                    stg_df = stg_df.with_columns(pl.lit(None).cast(pl.Datetime).alias(c))
                elif c == "client_id":
                    stg_df = stg_df.with_columns(pl.lit(None).cast(pl.Float64).alias(c))
                else:
                    stg_df = stg_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
        stg_df = stg_df.select(LK_MATCHES_VISITORS_TO_LEADS_COLUMN_ORDER)
        df = add_audit_fields(stg_df, job_name="lk_matches_visitors_to_leads_new")
        s3_key = build_gold_s3_key(
            "lk_matches_visitors_to_leads_new", context.partition_key, file_format="parquet"
        )
        context.log.info(
            f"gold_lk_matches_visitors_to_leads_new: {df.height:,} rows, {df.width} columns "
            f"for partition {context.partition_key}. S3 key: {s3_key}"
        )

        write_polars_to_s3(df, s3_key, context, file_format="parquet")

        context.log.info(
            f"📤 Loading to Geospot: table=lk_matches_visitors_to_leads_governance, s3_key={s3_key}, mode=replace"
        )
        try:
            load_to_geospot(
                s3_key=s3_key,
                table_name="lk_matches_visitors_to_leads_governance",
                mode="replace",
                context=context,
            )
            context.log.info("✅ Geospot load completed for lk_matches_visitors_to_leads_governance")
        except Exception as e:
            context.log.error(f"❌ Geospot load failed: {e}")
            raise

        if stale_tables:
            send_stale_data_notification(
                context,
                stale_tables,
                current_asset_name="gold_lk_matches_visitors_to_leads_new",
                current_layer="gold",
            )

        return df

    yield from iter_job_wrapped_compute(context, body)
