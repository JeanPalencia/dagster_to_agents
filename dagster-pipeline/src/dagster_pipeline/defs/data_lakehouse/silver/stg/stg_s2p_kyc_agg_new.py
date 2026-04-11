# defs/data_lakehouse/silver/stg/stg_s2p_kyc_agg_new.py
"""
Silver: KYC aggregate per user for stg_s2p_users_new (users LEFT JOIN last inquiry).
Replicates the CTE kyc from stg_lk_users.sql so that brokers with real inquiry status
(Completed/Failed) are delivered to the join instead of falling back to Pending.

Logic:
- Universe: brokers (any profile with tenant_type=2) UNION internal users (email @spot2.mx / @spot2-services.com).
- Last inquiry per user: persona_inquiries, one row per user_id (latest by created_at).
- users LEFT JOIN last_inquiry → user_kyc_status_id (1=Completed, 2=Failed, 3=Pending), user_kyc_created_at.
- Output: user_id, user_kyc_status_id, user_kyc_status, user_kyc_created_at (consumed by stg_s2p_users_new with suffix _kyc).
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_SILVER
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_silver_s3_key,
    write_polars_to_s3,
    read_bronze_from_s3,
)


def _build_kyc_agg(
    df_users: pl.DataFrame,
    df_profiles: pl.DataFrame,
    df_inquiries: pl.DataFrame,
    context: dg.AssetExecutionContext | None = None,
) -> pl.DataFrame:
    """
    Build KYC aggregate: eligible users (brokers + internals) LEFT JOIN last inquiry per user.
    Replicates SQL legacy CTE kyc (users LEFT JOIN inquiry WHERE ranking=1, brokers OR internal).
    """
    # Normalize column names (MySQL may use id/user_id; legacy uses user_id, event_name, created_at)
    users_id = "id" if "id" in df_users.columns else "user_id"
    users_email = "email" if "email" in df_users.columns else None
    if users_email is None:
        # Fallback: no email → no internals; still build from brokers
        df_users = df_users.with_columns(pl.lit(None).cast(pl.Utf8).alias("email"))
        users_email = "email"
    inv_user_id = "user_id" if "user_id" in df_inquiries.columns else "model_id"  # some schemas use model_id
    inv_created = "created_at" if "created_at" in df_inquiries.columns else "created_at"
    inv_event = "event_name" if "event_name" in df_inquiries.columns else "event_name"

    # Last inquiry per user (ranking = 1 by created_at DESC)
    if df_inquiries.is_empty():
        last_inquiry = pl.DataFrame(
            schema={
                "user_id": pl.Int64,
                "event_name": pl.Utf8,
                "inquiry_created_at": pl.Datetime,
            }
        )
    else:
        inv = df_inquiries.select(
            pl.col(inv_user_id).cast(pl.Int64).alias("user_id"),
            pl.col(inv_event).cast(pl.Utf8).alias("event_name"),
            pl.col(inv_created).cast(pl.Datetime).alias("inquiry_created_at"),
        ).filter(pl.col("user_id").is_not_null())
        last_inquiry = (
            inv.sort(["user_id", "inquiry_created_at"], descending=[False, True])
            .group_by("user_id", maintain_order=True)
            .head(1)
        )

    # Brokers: user_ids with at least one profile tenant_type=2
    if "tenant_type" not in df_profiles.columns or df_profiles.is_empty():
        broker_user_ids = pl.DataFrame(schema={"user_id": pl.Int64})
    else:
        prof_user = "user_id" if "user_id" in df_profiles.columns else df_profiles.columns[0]
        broker_user_ids = (
            df_profiles.filter(pl.col("tenant_type") == 2)
            .select(pl.col(prof_user).alias("user_id"))
            .unique()
        )

    # Eligible = brokers UNION internals (legacy: WHERE (broker AND (ranking=1 OR null)) OR internal)
    # Cast user_id to Int64 to guarantee join key type matches last_inquiry (also Int64).
    eligible = df_users.select(
        pl.col(users_id).cast(pl.Int64).alias("user_id"),
        pl.col(users_email).alias("email"),
    )
    # Replica de: email REGEXP '(@spot2\.mx$|@spot2-services\.com$)' (case-insensitive en MySQL).
    internal_mask = pl.col("email").str.contains(r"(?i)(@spot2\.mx$|@spot2-services\.com$)")
    if not broker_user_ids.is_empty():
        broker_ids = broker_user_ids.get_column("user_id").cast(pl.Int64)
        eligible = eligible.filter(
            pl.col("user_id").is_in(broker_ids) | internal_mask
        )
    else:
        eligible = eligible.filter(internal_mask)

    # LEFT JOIN last inquiry — cast key to ensure type parity even after Parquet round-trip
    last_inquiry = last_inquiry.with_columns(pl.col("user_id").cast(pl.Int64))
    if context:
        context.log.info(
            f"[kyc_agg] eligible={eligible.height} (brokers+internals) | "
            f"last_inquiry={last_inquiry.height} unique users with any inquiry | "
            f"eligible user_id dtype={eligible.schema['user_id']} | "
            f"last_inquiry user_id dtype={last_inquiry.schema['user_id']}"
        )
    kyc = eligible.join(last_inquiry, on="user_id", how="left")
    _matched = kyc.filter(pl.col("event_name").is_not_null()).height
    if context:
        context.log.info(
            f"[kyc_agg] after LEFT JOIN: {kyc.height} rows | "
            f"with_inquiry (event_name not null)={_matched} | "
            f"without_inquiry={kyc.height - _matched}"
        )

    # user_kyc_status_id: legacy CASE
    # WHEN event_name IN ('inquiry.completed','inquiry.approved') THEN 1
    # WHEN email REGEXP @spot2 THEN 1
    # WHEN event_name IN ('inquiry.failed','inquiry.declined') THEN 2
    # WHEN inquiry.user_id IS NULL THEN 3 ELSE 3
    # Replica de: email REGEXP '(@spot2\.mx$|@spot2-services\.com$)' (case-insensitive en MySQL).
    is_internal = pl.col("email").str.contains(r"(?i)(@spot2\.mx$|@spot2-services\.com$)")
    completed_events = pl.col("event_name").is_in(["inquiry.completed", "inquiry.approved"])
    failed_events = pl.col("event_name").is_in(["inquiry.failed", "inquiry.declined"])
    user_kyc_status_id = (
        pl.when(completed_events)
        .then(1)
        .when(is_internal)
        .then(1)
        .when(failed_events)
        .then(2)
        .when(pl.col("event_name").is_null())
        .then(3)
        .otherwise(3)
        .cast(pl.Int64)
    )
    user_kyc_status = (
        pl.when(pl.col("user_kyc_status_id") == 1)
        .then(pl.lit("Completed"))
        .when(pl.col("user_kyc_status_id") == 2)
        .then(pl.lit("Failed"))
        .when(pl.col("user_kyc_status_id") == 3)
        .then(pl.lit("Pending"))
        .otherwise(pl.lit("Pending"))
    )
    # We need to add user_kyc_status_id first, then user_kyc_status (or compute in one with_columns)
    kyc = kyc.with_columns(
        user_kyc_status_id.alias("user_kyc_status_id"),
        pl.col("inquiry_created_at").alias("user_kyc_created_at"),
    )
    kyc = kyc.with_columns(
        pl.when(pl.col("user_kyc_status_id") == 1).then(pl.lit("Completed"))
        .when(pl.col("user_kyc_status_id") == 2).then(pl.lit("Failed"))
        .otherwise(pl.lit("Pending"))
        .alias("user_kyc_status")
    )

    return kyc.select(
        "user_id",
        "user_kyc_status_id",
        "user_kyc_status",
        "user_kyc_created_at",
    )


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    retry_policy=dg.RetryPolicy(max_retries=2),
    tags=TAGS_LK_SPOTS_SILVER,
    deps=["raw_s2p_users_new", "raw_s2p_profiles_new", "raw_s2p_persona_inquiries_new"],
    description=(
        "Silver KYC aggregate: one row per eligible user (brokers + internals) with user_kyc_status_id, "
        "user_kyc_created_at from users LEFT JOIN last persona_inquiry. Consumed by stg_s2p_users_new."
    ),
    io_manager_key="s3_silver",
)
def stg_s2p_kyc_agg_new(context):
    """
    Build KYC status per user from users, profiles, and persona_inquiries (replica of legacy CTE kyc).
    Writes to S3 for stg_s2p_users_new to read and join.
    """
    def body():
        df_users, _ = read_bronze_from_s3("raw_s2p_users_new", context)
        df_profiles, _ = read_bronze_from_s3("raw_s2p_profiles_new", context)
        df_inquiries, _ = read_bronze_from_s3("raw_s2p_persona_inquiries_new", context)

        if df_users.is_empty():
            context.log.warning("raw_s2p_users_new is empty; returning empty KYC agg")
            return pl.DataFrame(
                schema={
                    "user_id": pl.Int64,
                    "user_kyc_status_id": pl.Int64,
                    "user_kyc_status": pl.Utf8,
                    "user_kyc_created_at": pl.Datetime,
                }
            )

        context.log.info(
            f"[kyc_agg] inputs: df_users={df_users.height} rows, "
            f"df_profiles={df_profiles.height} rows, "
            f"df_inquiries={df_inquiries.height} rows | columns: {df_inquiries.columns}"
        )
        df_kyc = _build_kyc_agg(df_users, df_profiles, df_inquiries, context)
        _with_date = df_kyc.filter(pl.col("user_kyc_created_at").is_not_null()).height
        _completed = df_kyc.filter(pl.col("user_kyc_status_id") == 1).height
        _failed = df_kyc.filter(pl.col("user_kyc_status_id") == 2).height
        _pending = df_kyc.filter(pl.col("user_kyc_status_id") == 3).height
        context.log.info(
            f"stg_s2p_kyc_agg_new: {df_kyc.height} rows for partition {context.partition_key} | "
            f"Completed={_completed}, Failed={_failed}, Pending={_pending} | "
            f"user_kyc_created_at non-null={_with_date}"
        )
        s3_key = build_silver_s3_key("stg_s2p_kyc_agg_new", file_format="parquet")
        write_polars_to_s3(df_kyc, s3_key, context, file_format="parquet")
        return df_kyc

    yield from iter_job_wrapped_compute(context, body)
