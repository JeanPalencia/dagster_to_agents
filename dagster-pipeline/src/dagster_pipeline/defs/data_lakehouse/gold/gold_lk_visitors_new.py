# defs/data_lakehouse/gold/gold_lk_visitors_new.py
"""
Gold: lk_visitors_new from stg_bq_funnel_with_channel_new.

Reads stg_bq_funnel_with_channel_new from S3, applies lk_visitors logic in gold:
rename to vis_*, dedup by (user_pseudo_id, vis_create_date), vis_id, aud_*.
Writes parquet to S3 (lk_visitors_new) and loads to Geospot table lk_visitors.
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

# Columns that must not be in lk_visitors (dropped if present from silver/BQ)
COLUMNS_EXCLUDED_FROM_GOVERNANCE = ("vis_email", "vis_lead_id", "vis_lead_date")

# Final column order for lk_visitors (without vis_email, vis_lead_id, vis_lead_date)
LK_VISITORS_COLUMN_ORDER = (
    "vis_id",
    "user_pseudo_id",
    "vis_create_date",
    "vis_is_scraping",
    "vis_source",
    "vis_medium",
    "vis_campaign_name",
    "vis_channel",
    "vis_traffic_type",
    "aud_inserted_date",
    "aud_inserted_at",
    "aud_updated_date",
    "aud_updated_at",
    "aud_job",
)

# Funnel columns (stg_bq = bronze passthrough) -> lk_visitors
_BQ_TO_VIS_RENAME = {
    "user": "user_pseudo_id",
    "event_date": "vis_create_date",
    "user_sospechoso": "vis_is_scraping",
    "source": "vis_source",
    "medium": "vis_medium",
    "campaign_name": "vis_campaign_name",
}
_BQ_CHANNEL_ALIASES = ("Channel", "channel")
_BQ_TRAFFIC_ALIASES = ("Traffic_type", "traffic_type")


def _transform_funnel_to_lk_visitors(df: pl.DataFrame) -> pl.DataFrame:
    """Transform funnel (BQ columns) to lk_visitors schema: rename, dedup, vis_id, aud_*."""
    if df.height == 0:
        schema = {c: pl.Utf8 for c in LK_VISITORS_COLUMN_ORDER}
        schema["vis_create_date"] = pl.Date
        schema["vis_is_scraping"] = pl.Boolean
        schema["aud_inserted_date"] = pl.Date
        schema["aud_updated_date"] = pl.Date
        schema["aud_inserted_at"] = pl.Datetime
        schema["aud_updated_at"] = pl.Datetime
        return pl.DataFrame(schema=schema)

    rename = dict(_BQ_TO_VIS_RENAME)
    for alias in _BQ_CHANNEL_ALIASES:
        if alias in df.columns:
            rename[alias] = "vis_channel"
            break
    for alias in _BQ_TRAFFIC_ALIASES:
        if alias in df.columns:
            rename[alias] = "vis_traffic_type"
            break
    rename = {k: v for k, v in rename.items() if k in df.columns}
    out = df.rename(rename)

    # Normalize user_pseudo_id (cast + strip) to align with legacy and avoid COUNT(DISTINCT) differences
    if "user_pseudo_id" in out.columns:
        out = out.with_columns(
            pl.col("user_pseudo_id").cast(pl.Utf8).fill_null("").str.strip_chars().alias("user_pseudo_id")
        )
        # Same filter as legacy (funnel_with_channel.sql): user IS NOT NULL AND TRIM(user) != ''
        # Avoids vis_id starting with "_" and ensures non-null PK
        out = out.filter(pl.col("user_pseudo_id").str.len_chars() > 0)

    if "vis_create_date" in out.columns:
        out = out.with_columns(pl.col("vis_create_date").cast(pl.Date))

    out = out.unique(subset=["user_pseudo_id", "vis_create_date"], keep="first")

    out = out.with_columns(
        (
            pl.col("user_pseudo_id").cast(pl.Utf8).fill_null("")
            + pl.lit("_")
            + pl.col("vis_create_date").dt.strftime("%Y%m%d")
        ).alias("vis_id")
    )

    out = add_audit_fields(out, job_name="lk_visitors_new")

    # Drop columns that are not in governance (vis_email, vis_lead_id, vis_lead_date)
    for c in COLUMNS_EXCLUDED_FROM_GOVERNANCE:
        if c in out.columns:
            out = out.drop(c)

    for c in LK_VISITORS_COLUMN_ORDER:
        if c not in out.columns:
            out = out.with_columns(pl.lit(None).alias(c))
    return out.select(LK_VISITORS_COLUMN_ORDER)


@dg.asset(
    name="gold_lk_visitors_new",
    partitions_def=daily_partitions,
    group_name="gold",
    retry_policy=dg.RetryPolicy(max_retries=2),
    io_manager_key="s3_gold",
    description=(
        "Gold: lk_visitors_new. "
        "Reads stg_bq_funnel_with_channel_new from S3, applies dedup/vis_id/vis_*/aud_*, writes parquet (S3 lk_visitors_new) and loads to Geospot table lk_visitors."
    ),
)
def gold_lk_visitors_new(context):
    """Build gold lk_visitors table from stg_bq_funnel_with_channel_new. S3: lk_visitors_new, Geospot: lk_visitors."""
    def body():
        stale_tables = []
        funnel_df, meta = read_silver_from_s3("stg_bq_funnel_with_channel_new", context)
        if meta.get("is_stale"):
            stale_tables.append({
                "table_name": "stg_bq_funnel_with_channel_new",
                "expected_date": context.partition_key,
                "available_date": meta.get("file_date", ""),
                "layer": "silver",
                "file_path": meta.get("file_path", ""),
            })

        df = _transform_funnel_to_lk_visitors(funnel_df)
        context.log.info(
            f"gold_lk_visitors_new: {df.height:,} rows, {df.width} columns for partition {context.partition_key}"
        )

        s3_key = build_gold_s3_key("lk_visitors_new", context.partition_key, file_format="parquet")
        write_polars_to_s3(df, s3_key, context, file_format="parquet")

        context.log.info(
            f"📤 Loading to Geospot: table=lk_visitors, s3_key={s3_key}, mode=upsert"
        )
        try:
            load_to_geospot(
                s3_key=s3_key,
                table_name="lk_visitors",
                mode="upsert",
                context=context,
                conflict_columns=["vis_id"],
                update_columns=["vis_is_scraping", "aud_updated_date", "aud_updated_at"],
            )
            context.log.info("✅ Geospot load completed for lk_visitors")
        except Exception as e:
            context.log.error(f"❌ Geospot load failed: {e}")
            raise

        if stale_tables:
            send_stale_data_notification(
                context,
                stale_tables,
                current_asset_name="gold_lk_visitors_new",
                current_layer="gold",
            )

        return df

    yield from iter_job_wrapped_compute(context, body)
