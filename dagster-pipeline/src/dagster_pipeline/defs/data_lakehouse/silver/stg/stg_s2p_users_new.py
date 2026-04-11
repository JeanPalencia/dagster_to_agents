# defs/data_lakehouse/silver/stg/stg_s2p_users_new.py
"""
Silver STG: Complete transformation of raw_s2p_users_new using reusable functions.

Replicates the logic from stg_lk_users.sql:
- Joins users with latest profile per user (one row per user).
- user_type_id / user_type: one-to-one CASE from tenant_type (1=Tenant or Investor, 2=Broker, 4=Landlord, 5=Developer, 0=Unknown).
- user_last_log_at, user_days_since_last_log, user_total_log_count: from stg_s2p_user_activity_agg_new (S3).
- user_kyc_status_id, user_kyc_status, user_kyc_created_at: from stg_s2p_kyc_agg_new (S3). Fallback when join is null:
  internal → Completed (1), broker (tenant_type=2) → Pending (3), rest → Not Applicable. TODO: remove fallback
  when stg_s2p_kyc_agg_new replicates users LEFT JOIN inquiry (brokers without inquiry should come from agg).
- user_category / user_category_id: not in this STG; added downstream if present in gold.
- user_max_role_id / user_max_role: not in this STG; on lk_spots they come from the model_has_roles join in stg_lk_spots_new (presentation).
- user_industria_role / user_industria_role_id: from profiles.tenant_type (not model_has_roles).
"""
import dagster as dg
import polars as pl
from datetime import date, datetime

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_silver_s3_key,
    write_polars_to_s3,
    read_bronze_from_s3,
    read_silver_from_s3,
)
from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_SILVER
from dagster_pipeline.defs.data_lakehouse.notifications import send_stale_data_notification
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.silver.utils import (
    case_when,
    expr_mysql_numeric_int64,
    validate_and_clean_silver,
    validate_input_bronze,
    validate_output_silver,
)


def _transform_s2p_users(
    df_users: pl.DataFrame,
    df_profiles: pl.DataFrame,
    context: dg.AssetExecutionContext,
    df_activity_agg: pl.DataFrame | None = None,
    df_kyc_agg: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """
    Transforms raw_s2p_users_new maintaining original column names.
    Only adds calculated columns when necessary.
    
    Replicates the logic from stg_lk_users.sql:
    - Joins users with latest profile per user (one row per user)
    - Adds CASE statements for person_type, tenant_type, roles
    - Activity metrics: from df_activity_agg if provided, else placeholders
    - KYC fields: from df_kyc_agg if provided; else fallback internal→Completed, broker→Pending
    """
    for _old_level in ("Level_id", "levelId", "LEVEL_ID"):
        if _old_level in df_users.columns and "level_id" not in df_users.columns:
            df_users = df_users.rename({_old_level: "level_id"})
            break

    # ========== LATEST PROFILE PER USER ==========
    # One row per user: prefer the most recent profile that has name/last_name/phone (fixes user_id 19234:
    # legacy had profile with data, we were taking latest-by-created_at which had NULLs). If all profiles are empty, take latest.
    _has_data = (
        pl.col("name").is_not_null() | pl.col("last_name").is_not_null() | pl.col("phone_number").is_not_null()
    )
    _profiles_with_flag = df_profiles.with_columns(
        _has_data.cast(pl.Int8).alias("_has_profile_data")  # 1 = has data, 0 = empty
    )
    latest_profile_per_user = (
        _profiles_with_flag
        .sort(["user_id", "_has_profile_data", "created_at"], descending=[False, True, True])
        .group_by("user_id", maintain_order=True)
        .head(1)
        .drop("_has_profile_data")
    )
    
    # ========== JOIN USERS WITH LATEST PROFILE ==========
    df_joined = df_users.join(
        latest_profile_per_user,
        left_on="id",
        right_on="user_id",
        how="left",
        suffix="_profile"
    )
    if "level_id" in df_joined.columns:
        df_joined = df_joined.with_columns(expr_mysql_numeric_int64("level_id").alias("level_id"))

    # ========== LEFT JOIN ACTIVITY AND KYC AGGREGATES (from silver S3) ==========
    if df_activity_agg is not None and not df_activity_agg.is_empty():
        df_joined = df_joined.join(
            df_activity_agg,
            left_on="id",
            right_on="user_id",
            how="left",
            suffix="_alg",
        )
    if df_kyc_agg is not None and not df_kyc_agg.is_empty():
        # Rename KYC columns explicitly before joining so the _kyc suffix is always present,
        # regardless of whether df_joined already has those column names (Polars only adds
        # suffix on collision — without explicit rename _has_kyc_join would always be False).
        _kyc_to_join = df_kyc_agg.rename({
            "user_kyc_status_id": "user_kyc_status_id_kyc",
            "user_kyc_status": "user_kyc_status_kyc",
            "user_kyc_created_at": "user_kyc_created_at_kyc",
        })
        df_joined = df_joined.join(
            _kyc_to_join,
            left_on="id",
            right_on="user_id",
            how="left",
        )
    
    # ========== CASE STATEMENTS ==========
    # user_person_type
    user_person_type_expr = case_when(
        "person_type",
        {1: "Legal Entity", 2: "Individual"},
        default="Unknown",
        alias="user_person_type"
    )
    
    # user_type_id and user_type — one-to-one CASE from profiles.tenant_type (1=Tenant or Investor, 2=Broker, 4=Landlord, 5=Developer, else=Unknown).
    user_type_id_expr = (
        pl.when(pl.col("tenant_type") == 1).then(pl.lit(1))
        .when(pl.col("tenant_type").is_in([2, 4, 5])).then(pl.lit(2))
        .otherwise(pl.lit(0))
        .cast(pl.Int64)
        .alias("user_type_id")
    )
    user_type_expr = (
        pl.when(pl.col("tenant_type") == 1).then(pl.lit("Buyer"))
        .when(pl.col("tenant_type").is_in([2, 4, 5])).then(pl.lit("Seller"))
        .otherwise(pl.lit("Unknown"))
        .alias("user_type")
    )

    user_industria_role_expr = case_when(
    "tenant_type",
    {1: "Tenant", 2: "Broker", 4: "Landlord", 5: "Developer"},
    default=None,  # legacy uses NULL when no match, not "Unknown"
    alias="user_industria_role"
    )   
    
    # user_affiliation_id and user_affiliation
    _is_internal_email = pl.col("email").str.contains(r"(?i)(@spot2\.mx$|@spot2-services\.com$)")
    user_affiliation_id_expr = (
        pl.when(_is_internal_email)
        .then(1)
        .otherwise(0)
        .alias("user_affiliation_id")
    )

    user_affiliation_expr = (
        pl.when(_is_internal_email)
        .then(pl.lit("Internal User"))
        .otherwise(pl.lit("External User"))
        .alias("user_affiliation")
    )
    
    # KYC: replicate legacy SQL (stg_lk_users.sql). Order: join value → internal → broker → N/A.
    # TODO: remove internal/broker fallback when stg_s2p_kyc_agg_new replicates full CTE (users LEFT JOIN inquiry).
    # Replicate: email REGEXP '(@spot2\.mx$|@spot2-services\.com$)' (case-insensitive in MySQL).
    _is_internal = pl.col("email").str.contains(r"(?i)(@spot2\.mx$|@spot2-services\.com$)")
    # SQL: EXISTS (profiles WHERE tenant_type=2). Here only most recent profile; TODO: ANY profile when history exists.
    _is_broker = pl.col("tenant_type") == 2
    _has_kyc_join = "user_kyc_status_id_kyc" in df_joined.columns
    user_kyc_status_id_expr = (
        pl.when(pl.col("user_kyc_status_id_kyc").is_not_null())
        .then(pl.col("user_kyc_status_id_kyc").cast(pl.Int64))
        .when(_is_internal)
        .then(pl.lit(1).cast(pl.Int64))  # internal with no match → Completed (as in SQL)
        .when(_is_broker)
        .then(pl.lit(3).cast(pl.Int64))  # brokers without inquiry → Pending (as in SQL)
        .otherwise(pl.lit(0).cast(pl.Int64))
        if _has_kyc_join
        else (
            pl.when(_is_internal).then(pl.lit(1))
            .when(_is_broker).then(pl.lit(3))
            .otherwise(pl.lit(0))
            .cast(pl.Int64)
        )
    ).alias("user_kyc_status_id")
    # user_kyc_status is derived from user_kyc_status_id in a second with_columns (Polars evaluates the whole block against the same state).
    user_kyc_created_at_expr = (
        pl.col("user_kyc_created_at_kyc")
        if _has_kyc_join
        else pl.lit(None).cast(pl.Datetime)
    ).alias("user_kyc_created_at")
    
    # Activity: use joined stg_s2p_user_activity_agg_new columns when present, else placeholders
    _has_alg_join = "user_last_log_at_alg" in df_joined.columns
    user_last_log_at_expr = (
        pl.col("user_last_log_at_alg")
        if _has_alg_join
        else pl.lit(None).cast(pl.Datetime)
    ).alias("user_last_log_at")
    user_days_since_last_log_expr = (
        pl.col("user_days_since_last_log_alg").cast(pl.Int64)
        if _has_alg_join
        else pl.lit(None).cast(pl.Int64)
    ).alias("user_days_since_last_log")
    user_total_log_count_expr = (
        pl.col("user_total_log_count_alg").cast(pl.Int64)
        if _has_alg_join
        else pl.lit(0).cast(pl.Int64)
    ).alias("user_total_log_count")
    
    # ========== BUILD FINAL DATAFRAME ==========
    # Map columns from users and profiles to match stg_lk_users structure
    df_result = df_joined.with_columns([
        # Core user fields (from users table)
        pl.col("id").alias("user_id"),
        pl.col("uuid").alias("user_uuid"),
        pl.col("email").alias("user_email"),
        pl.col("owner_id").alias("user_owner_id"),
        pl.col("contact_id").alias("contact_id"),
        pl.col("hubspot_owner_id").alias("user_hubspot_owner_id"),
        pl.col("hubspot_user_id").alias("user_hubspot_user_id"),
        pl.col("created_at").alias("user_created_at"),
        pl.col("created_at").dt.date().alias("user_created_date"),
        pl.col("updated_at").alias("user_updated_at"),
        pl.col("updated_at").dt.date().alias("user_updated_date"),
        pl.col("deleted_at").alias("user_deleted_at"),
        pl.col("deleted_at").dt.date().alias("user_deleted_date"),
        
        # Profile fields (from latest profile per user - bronze columns)
        pl.col("name").alias("user_name"),
        pl.col("last_name").alias("user_last_name"),
        pl.col("mothers_last_name").alias("user_mothers_last_name"),
        # CONCAT_WS(' ', name, last_name, mothers_last_name) — SQL ignores nulls; replicate by filtering then joining.
        pl.concat_str(
            [
                pl.col("name"),
                pl.col("last_name"),
                pl.col("mothers_last_name"),
            ],
            separator=" ",
            ignore_nulls=True,
        ).str.strip_chars().alias("user_full_name"),
        pl.col("phone_indicator").alias("user_phone_indicator"),
        pl.col("phone_number").alias("user_phone_number"),
        # COALESCE(phone_full_number, CONCAT(phone_indicator, phone_number)) — legacy SQL.
        # Only concatenate when phone_number is not null (avoids "+52" when number is missing).
        pl.when(pl.col("phone_full_number").is_not_null())
        .then(pl.col("phone_full_number").cast(pl.String))
        .when(pl.col("phone_number").is_not_null())
        .then(
            pl.col("phone_indicator").fill_null("").cast(pl.String)
            + pl.col("phone_number").cast(pl.String)
        )
        .otherwise(pl.lit(None).cast(pl.String))
        .alias("user_phone_full_number"),
        pl.col("has_whatsapp").alias("user_has_whatsapp"),
        pl.col("id_profile").alias("user_profile_id"),  # Profile id
        
        # Calculated fields
        pl.col("person_type").fill_null(0).alias("user_person_type_id"),
        user_person_type_expr,
        user_type_id_expr,
        user_type_expr,
        user_affiliation_id_expr,
        user_affiliation_expr,
        
        # Activity log fields (placeholders)
        user_last_log_at_expr,
        user_days_since_last_log_expr,
        user_total_log_count_expr,
        
        # KYC fields
        user_kyc_status_id_expr,
        user_kyc_created_at_expr,
        
        # Domain extraction
        pl.col("email").str.split("@").list.get(-1).str.strip_chars().str.to_lowercase().alias("user_domain"),
        
        # Filter: user not deleted
        pl.when(pl.col("deleted_at").is_null())
        .then(1)
        .otherwise(0)
        .alias("filter"),
        pl.col("tenant_type").cast(pl.Int64).alias("user_industria_role_id"),
        user_industria_role_expr,
    ])
    if "level_id" not in df_result.columns:
        df_result = df_result.with_columns(pl.lit(None).cast(pl.Int64).alias("level_id"))

    # Second block: derive user_kyc_status from user_kyc_status_id already materialized (Polars does not allow referencing columns from the same with_columns).
    df_result = df_result.with_columns([
        pl.when(pl.col("user_kyc_status_id") == 1)
        .then(pl.lit("Completed"))
        .when(pl.col("user_kyc_status_id") == 2)
        .then(pl.lit("Failed"))
        .when(pl.col("user_kyc_status_id") == 3)
        .then(pl.lit("Pending"))
        .otherwise(pl.lit("Not Applicable"))
        .alias("user_kyc_status"),
    ])
    
    # Select only the columns we need (remove duplicate columns from join
    df_result = df_result.select([
        "id",
        "level_id",
        "user_id",
        "user_uuid",
        "user_name",
        "user_last_name",
        "user_mothers_last_name",
        "user_full_name",
        "user_email",
        "user_domain",
        "user_phone_indicator",
        "user_phone_number",
        "user_phone_full_number",
        "user_has_whatsapp",
        "user_profile_id",
        "user_owner_id",
        "contact_id",
        "user_hubspot_owner_id",
        "user_hubspot_user_id",
        "user_person_type_id",
        "user_person_type",
        "user_industria_role_id", 
        "user_industria_role", 
        "user_type_id",
        "user_type",
        "user_affiliation_id",
        "user_affiliation",
        "user_last_log_at",
        "user_days_since_last_log",
        "user_total_log_count",
        "user_kyc_status_id",
        "user_kyc_status",
        "user_kyc_created_at",
        "user_created_at",
        "user_created_date",
        "user_updated_at",
        "user_updated_date",
        "user_deleted_at",
        "user_deleted_date",
        "filter",
    ])
    
    return df_result


def _stg_s2p_users_new_impl(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Transforms raw_s2p_users_new maintaining original column names.
    Only adds calculated columns when necessary.

    Reads bronze data directly from S3 (no explicit Dagster dependencies).
    If bronze data is stale (from previous day), logs warning and sends notification.

    Replicates the logic from stg_lk_users.sql:
    - Joins users with latest profile per user (one row per user)
    - Adds CASE statements for person_type, tenant_type, roles
    - KYC: from stg_s2p_kyc_agg_new when present; fallback internal→Completed, broker→Pending (TODO: remove when KYC agg replicates users LEFT JOIN inquiry)
    - Activity log metrics (placeholder if activity_log not available)

    Validations are completely automatic - no configuration required.
    """
    # ========== READ BRONZE FROM S3 ==========
    stale_tables = []
    
    # Read users
    raw_s2p_users_new, meta_users = read_bronze_from_s3("raw_s2p_users_new", context)
    if meta_users['is_stale']:
        stale_tables.append({
            'table_name': 'raw_s2p_users_new',
            'expected_date': context.partition_key,
            'available_date': meta_users['file_date'],
            'layer': 'bronze',
            'file_path': meta_users['file_path'],
        })
    
    # Read profiles (for latest profile per user join)
    raw_s2p_profiles_new, meta_profiles = read_bronze_from_s3("raw_s2p_profiles_new", context)
    if meta_profiles['is_stale']:
        stale_tables.append({
            'table_name': 'raw_s2p_profiles_new',
            'expected_date': context.partition_key,
            'available_date': meta_profiles['file_date'],
            'layer': 'bronze',
            'file_path': meta_profiles['file_path'],
        })
    
    # Read activity and KYC aggregates (silvers); run after stg_s2p_user_activity_agg_new and stg_s2p_kyc_agg_new
    df_activity_agg = None
    df_kyc_agg = None
    try:
        df_activity_agg, meta_alg = read_silver_from_s3("stg_s2p_user_activity_agg_new", context)
        if meta_alg.get("is_stale"):
            stale_tables.append({"table_name": "stg_s2p_user_activity_agg_new", "expected_date": context.partition_key, "available_date": meta_alg.get("file_date"), "layer": "silver", "file_path": meta_alg.get("file_path", "")})
    except ValueError:
        context.log.warning("stg_s2p_user_activity_agg_new not found in S3; using activity placeholders")
    try:
        df_kyc_agg, meta_kyc = read_silver_from_s3("stg_s2p_kyc_agg_new", context)
        if meta_kyc.get("is_stale"):
            stale_tables.append({"table_name": "stg_s2p_kyc_agg_new", "expected_date": context.partition_key, "available_date": meta_kyc.get("file_date"), "layer": "silver", "file_path": meta_kyc.get("file_path", "")})
    except ValueError:
        context.log.warning("stg_s2p_kyc_agg_new not found in S3; using KYC heuristic from affiliation")
    
    # ========== INITIAL VALIDATION ==========
    input_validation = validate_input_bronze(raw_s2p_users_new, context)
    
    if input_validation["is_empty"]:
        context.log.warning("⚠️  Empty input - returning empty DataFrame")
        return raw_s2p_users_new
    
    # ========== TRANSFORMATION ==========
    df_users = _transform_s2p_users(
        raw_s2p_users_new,
        raw_s2p_profiles_new,
        context,
        df_activity_agg=df_activity_agg,
        df_kyc_agg=df_kyc_agg,
    )
    
    context.log.info(
        f"stg_s2p_users_new: {df_users.height} rows for partition {context.partition_key}"
    )
    
    # ========== FINAL VALIDATION ==========
    output_validation = validate_output_silver(
        df_users,
        raw_s2p_users_new,
        context,
        allow_row_loss=False,  # Should preserve all users
    )
    
    if not output_validation["validation_passed"]:
        context.log.warning(
            f"⚠️  Validation: {output_validation['rows_lost']:,} rows lost"
        )
    
    # ========== CLEANING AND QUALITY VALIDATIONS ==========
    df_users = validate_and_clean_silver(df_users, context)
    
    # ========== PERSISTENCE TO S3 ==========
    s3_key = build_silver_s3_key("stg_s2p_users_new", file_format="parquet")
    write_polars_to_s3(df_users, s3_key, context, file_format="parquet")
    
    # ========== SEND NOTIFICATIONS IF STALE DATA USED ==========
    if stale_tables:
        send_stale_data_notification(
            context, 
            stale_tables,
            current_asset_name="stg_s2p_users_new",
            current_layer="silver"
        )
    
    return df_users


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    retry_policy=dg.RetryPolicy(max_retries=2),
    io_manager_key="s3_silver",
    tags=TAGS_LK_SPOTS_SILVER,
    deps=["stg_s2p_kyc_agg_new"],
    description=(
        "Silver STG: Complete transformation of users from Spot2 Platform "
        "using reusable functions. Joins users with profiles and adds calculated fields."
    ),
)
def stg_s2p_users_new(context):
    def body():
        return _stg_s2p_users_new_impl(context)

    yield from iter_job_wrapped_compute(context, body)
