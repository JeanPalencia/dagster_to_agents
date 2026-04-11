"""
Executes the full bt_lds_lead_spots pipeline locally (bronze -> silver -> gold)
for the channel attribution assets only.

The core bronze/silver assets (stg_s2p_clients_new, etc.) are already in S3
from the daily job. This script only materializes the 3 new bronze and 3 new
silver assets, then runs the gold transformation.

Usage:
    cd dagster-pipeline
    uv run python ../lakehouse-sdk/tests/bt_lds_channel_attribution/run_pipeline.py
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "dagster-pipeline", "src"))

import polars as pl
from datetime import date

from dagster_pipeline.defs.data_lakehouse.shared import (
    query_bronze_source,
    build_bronze_s3_key,
    build_silver_s3_key,
    write_polars_to_s3,
    read_polars_from_s3,
    S3_BUCKET,
    s3,
)


class FakeContext:
    """Minimal context to satisfy asset function signatures."""
    def __init__(self):
        self.partition_key = date.today().strftime("%Y-%m-%d")
        self.log = self

    def info(self, msg, *args):
        print(f"  [INFO] {msg}")

    def warning(self, msg, *args):
        print(f"  [WARN] {msg}")

    def error(self, msg, *args):
        print(f"  [ERROR] {msg}")


def write_df_to_s3(df: pl.DataFrame, key: str):
    """Write a DataFrame to S3 as parquet."""
    import io
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())


def run_bronze():
    ctx = FakeContext()
    print("\n=== BRONZE: raw_gs_recommendation_projects_new ===")
    q = """
    SELECT project_id, spots_suggested::text AS spots_suggested,
           white_list::text AS white_list, black_list::text AS black_list,
           created_at, updated_at
    FROM recommendation_projects
    """
    df = query_bronze_source(q, source_type="geospot_postgres", context=ctx)
    key = build_bronze_s3_key("raw_gs_recommendation_projects_new")
    write_df_to_s3(df, key)
    print(f"  -> {df.height:,} rows, {df.width} cols -> s3://{S3_BUCKET}/{key}")

    print("\n=== BRONZE: raw_gs_recommendation_chatbot_new ===")
    q = """
    SELECT id, spots_suggested::text AS spots_suggested,
           white_list::text AS white_list, black_list::text AS black_list,
           created_at, updated_at
    FROM recommendation_chatbot
    """
    df = query_bronze_source(q, source_type="geospot_postgres", context=ctx)
    key = build_bronze_s3_key("raw_gs_recommendation_chatbot_new")
    write_df_to_s3(df, key)
    print(f"  -> {df.height:,} rows, {df.width} cols -> s3://{S3_BUCKET}/{key}")

    print("\n=== BRONZE: raw_cb_conversation_events_new ===")
    q = """
    SELECT conversation_id, client_id, spot_id, event_data, created_at
    FROM conversation_events
    WHERE event_type = 'spot_confirmation'
    """
    df = query_bronze_source(q, source_type="staging_postgres", context=ctx)
    key = build_bronze_s3_key("raw_cb_conversation_events_new")
    write_df_to_s3(df, key)
    print(f"  -> {df.height:,} rows, {df.width} cols -> s3://{S3_BUCKET}/{key}")


def run_silver():
    print("\n=== SILVER: stg_gs_recommendation_projects_exploded_new ===")
    from dagster_pipeline.defs.data_lakehouse.silver.stg.stg_gs_recommendation_projects_exploded_new import (
        _transform_recommendation_projects_exploded,
    )
    key = build_bronze_s3_key("raw_gs_recommendation_projects_new")
    df_raw = read_polars_from_s3(key)
    df = _transform_recommendation_projects_exploded(df_raw)
    out_key = build_silver_s3_key("stg_gs_recommendation_projects_exploded_new")
    write_df_to_s3(df, out_key)
    print(f"  -> {df.height:,} rows, {df.width} cols -> s3://{S3_BUCKET}/{out_key}")

    print("\n=== SILVER: stg_gs_recommendation_chatbot_exploded_new ===")
    from dagster_pipeline.defs.data_lakehouse.silver.stg.stg_gs_recommendation_chatbot_exploded_new import (
        _transform_recommendation_chatbot_exploded,
    )
    key = build_bronze_s3_key("raw_gs_recommendation_chatbot_new")
    df_raw = read_polars_from_s3(key)
    df = _transform_recommendation_chatbot_exploded(df_raw)
    out_key = build_silver_s3_key("stg_gs_recommendation_chatbot_exploded_new")
    write_df_to_s3(df, out_key)
    print(f"  -> {df.height:,} rows, {df.width} cols -> s3://{S3_BUCKET}/{out_key}")

    print("\n=== SILVER: stg_cb_conversation_events_new ===")
    from dagster_pipeline.defs.data_lakehouse.silver.stg.stg_cb_conversation_events_new import (
        _transform_cb_conversation_events,
    )
    key = build_bronze_s3_key("raw_cb_conversation_events_new")
    df_raw = read_polars_from_s3(key)
    df = _transform_cb_conversation_events(df_raw)
    out_key = build_silver_s3_key("stg_cb_conversation_events_new")
    write_df_to_s3(df, out_key)
    print(f"  -> {df.height:,} rows, {df.width} cols -> s3://{S3_BUCKET}/{out_key}")


def run_gold():
    print("\n=== GOLD: gold_bt_lds_lead_spots_new (with channel attribution) ===")
    from dagster_pipeline.defs.data_lakehouse.gold.gold_bt_lds_lead_spots_new import (
        _transform_bt_lds_lead_spots_new,
        _normalize_silvers_for_lds,
        BT_LDS_LEAD_SPOTS_COLUMN_ORDER,
    )
    from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields
    from dagster_pipeline.defs.data_lakehouse.shared import (
        read_silver_from_s3,
        build_gold_s3_key,
        load_to_geospot,
    )

    ctx = FakeContext()

    def read_silver(name):
        df, _ = read_silver_from_s3(name, ctx)
        return df

    def read_silver_safe(name):
        try:
            return read_silver(name)
        except (ValueError, FileNotFoundError):
            print(f"  [WARN] Silver {name} not found -> empty DataFrame")
            return pl.DataFrame()

    stg_s2p_clients_new = read_silver("stg_s2p_clients_new")
    stg_s2p_projects_new = read_silver("stg_s2p_projects_new")
    stg_s2p_prs_project_spots_new = read_silver("stg_s2p_prs_project_spots_new")
    stg_s2p_appointments_new = read_silver("stg_s2p_appointments_new")
    stg_s2p_apd_appointment_dates_new = read_silver("stg_s2p_apd_appointment_dates_new")
    stg_s2p_alg_activity_log_projects_new = read_silver("stg_s2p_alg_activity_log_projects_new")
    stg_s2p_profiles_new = read_silver("stg_s2p_profiles_new")

    stg_rec_projects = read_silver_safe("stg_gs_recommendation_projects_exploded_new")
    stg_rec_chatbot = read_silver_safe("stg_gs_recommendation_chatbot_exploded_new")
    stg_conv_events = read_silver_safe("stg_cb_conversation_events_new")

    (
        stg_s2p_leads, stg_s2p_projects, stg_s2p_prs_project_spots,
        stg_s2p_appointments, stg_s2p_apd, stg_s2p_alg, stg_s2p_profiles,
    ) = _normalize_silvers_for_lds(
        stg_s2p_clients_new, stg_s2p_projects_new, stg_s2p_prs_project_spots_new,
        stg_s2p_appointments_new, stg_s2p_apd_appointment_dates_new,
        stg_s2p_alg_activity_log_projects_new, stg_s2p_profiles_new,
    )

    print(f"  Rec projects: {stg_rec_projects.height:,} rows")
    print(f"  Rec chatbot: {stg_rec_chatbot.height:,} rows")
    print(f"  Conv events: {stg_conv_events.height:,} rows")

    df = _transform_bt_lds_lead_spots_new(
        stg_s2p_leads=stg_s2p_leads,
        stg_s2p_projects=stg_s2p_projects,
        stg_s2p_prs_project_spots=stg_s2p_prs_project_spots,
        stg_s2p_appointments=stg_s2p_appointments,
        stg_s2p_apd_appointment_dates=stg_s2p_apd,
        stg_s2p_alg_activity_log_projects=stg_s2p_alg,
        stg_s2p_user_profiles=stg_s2p_profiles,
        stg_rec_projects=stg_rec_projects,
        stg_rec_chatbot=stg_rec_chatbot,
        stg_conv_events=stg_conv_events,
    )
    df = add_audit_fields(df, job_name="bt_lds_lead_spots_new")

    df = df.sort(
        by=["lead_id", "project_id", "spot_id", "lds_event_id", "lds_event_at",
            "apd_id", "appointment_id", "alg_id"],
        nulls_last=True,
    )

    id_cols_int32 = [
        "lead_id", "project_id", "spot_id", "appointment_id", "apd_id", "alg_id",
        "appointment_last_date_status_id", "apd_status_id",
        "lds_event_id", "lead_max_type_id", "spot_sector_id",
    ]
    for c in id_cols_int32:
        if c in df.columns:
            df = df.with_columns(pl.col(c).cast(pl.Int32))
    if "alg_event" in df.columns:
        df = df.with_columns(pl.col("alg_event").cast(pl.Utf8))
    if "lds_cohort_type_id" in df.columns:
        df = df.with_columns(pl.col("lds_cohort_type_id").cast(pl.Int16))

    for c in BT_LDS_LEAD_SPOTS_COLUMN_ORDER:
        if c not in df.columns:
            if c.endswith("_at"):
                df = df.with_columns(pl.lit(None).cast(pl.Datetime).alias(c))
            elif c.endswith("_date"):
                df = df.with_columns(pl.lit(None).cast(pl.Date).alias(c))
            elif "_id" in c:
                df = df.with_columns(pl.lit(None).cast(pl.Int32).alias(c))
            else:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    df_gov = df.select(BT_LDS_LEAD_SPOTS_COLUMN_ORDER)

    print(f"\n  Gold result: {df_gov.height:,} rows, {df_gov.width} columns")
    print(f"  Columns: {list(df_gov.columns)}")

    # Channel distribution
    df_evt3 = df_gov.filter(pl.col("lds_event_id") == 3)
    if df_evt3.height > 0 and "lds_channel_attribution_id" in df_evt3.columns:
        dist = df_evt3.group_by("lds_channel_attribution_id").len().sort("lds_channel_attribution_id")
        print(f"\n  Channel distribution (event 3, {df_evt3.height:,} rows):")
        for row in dist.iter_rows(named=True):
            pct = row["len"] / df_evt3.height * 100
            print(f"    Channel {row['lds_channel_attribution_id']}: {row['len']:,} ({pct:.1f}%)")

    # Write to S3
    partition_key = date.today().strftime("%Y-%m-%d")
    s3_key = build_gold_s3_key("bt_lds_lead_spots", partition_key, file_format="parquet")
    write_df_to_s3(df_gov, s3_key)
    print(f"\n  Written to s3://{S3_BUCKET}/{s3_key}")

    # Load to GeoSpot
    print("\n  Loading to GeoSpot...")
    try:
        load_to_geospot(s3_key=s3_key, table_name="bt_lds_lead_spots", mode="replace", context=ctx)
        print("  GeoSpot load completed.")
    except Exception as e:
        print(f"  GeoSpot load failed: {e}")


def main():
    print("=" * 70)
    print("bt_lds_lead_spots: Full pipeline execution (channel attribution)")
    print("=" * 70)

    run_bronze()
    run_silver()
    run_gold()

    print("\n" + "=" * 70)
    print("Pipeline complete. Run test_regression.py to validate.")
    print("=" * 70)


if __name__ == "__main__":
    main()
