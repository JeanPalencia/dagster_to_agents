# defs/data_lakehouse/gold/gold_lk_users_new.py
"""
Gold layer: Final lk_users table ready for consumption (NEW VERSION).

Combines stg_s2p_users_new with leads (for agent_kam_id), spots statistics
to create comprehensive user view.
Replicates the logic from lk_users.sql.

agent_kam_full_name: same as legacy (Data_Lakehouse/sql/sqlite/lk_users.sql):
  LEFT JOIN stg_lk_users AS lkua ON lkl.agent_kam_id = lkua.user_id → lkua.user_full_name.
  We build a lookup from stg_s2p_users_new (user_id -> user_full_name) and join on agent_kam_id;
  join keys are cast to Int64 so the match resolves (avoids all-null when types differ).

Columns removed vs legacy:
- user_category / user_category_id: source lk_duc_domain_user_categories is DEPRECATED.
- user_industria_role / user_industria_role_id: source model_has_roles is DEPRECATED (kept in governance schema for compatibility).
- user_max_role_id / user_max_role: source model_has_roles is DEPRECATED.
- activity cols (activity_log disabled), filter.

contact_id is in lk_users (from stg_s2p_users_new) for HubSpot/integrations; position 12 after user_owner_id.
user_industria_role_id / user_industria_role are at positions 59-60 in Postgres (at end), not after user_person_type.

user_type_id / user_type: derived from profiles.tenant_type via CASE (in silver).
  1=Tenant, 2=Broker, 4=Landlord, 5=Developer, NULL=Unknown.

user_spot_count (lk_users): only non-deleted spots (stg_s2p_spots_new with deleted_at IS NULL).
Legacy may include deleted — see docs/data_lakehouse/governance-spot-counts.md.
"""
import dagster as dg
import polars as pl
from datetime import date, datetime

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_gold_s3_key,
    write_polars_to_s3,
    read_silver_from_s3,
    load_to_geospot,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.notifications import send_stale_data_notification
from dagster_pipeline.defs.data_lakehouse.gold.utils import (
    add_audit_fields,
    validate_gold_joins,
)


def _agg_spots_by_user(df_spots: pl.DataFrame, partition_date: str) -> pl.DataFrame:
    """
    Aggregate lk_spots by user_id for gold lk_users (counts, sqm, first/last dates, days since).
    Uses spot_sector (Industrial, Office, Retail, Land), spot_area_in_sqm, spot_created_at,
    spot_updated_at, spot_status_id (1 = public). If df_spots is empty, returns empty schema.
    """
    required = ["user_id", "spot_sector", "spot_area_in_sqm", "spot_created_at", "spot_updated_at"]
    if df_spots.is_empty() or not all(c in df_spots.columns for c in required):
        return pl.DataFrame(schema={
            "user_id": pl.Int64,
            "user_industrial_count": pl.Int64,
            "user_office_count": pl.Int64,
            "user_retail_count": pl.Int64,
            "user_land_count": pl.Int64,
            "user_spot_count": pl.Int64,
            "user_public_spot_count": pl.Int64,
            "user_total_industrial_sqm": pl.Float64,
            "user_total_office_sqm": pl.Float64,
            "user_total_retail_sqm": pl.Float64,
            "user_total_land_sqm": pl.Float64,
            "user_total_spot_sqm": pl.Float64,
            "user_first_spot_created_at": pl.Datetime,
            "user_last_spot_created_at": pl.Datetime,
            "user_last_spot_updated_at": pl.Datetime,
            "user_last_spot_activity_at": pl.Datetime,
            "user_days_since_last_spot_created": pl.Int64,
            "user_days_since_last_spot_updated": pl.Int64,
            "user_days_since_last_spot_activity": pl.Int64,
        })
    sector = pl.col("spot_sector").str.to_lowercase()
    is_industrial = sector.str.contains("industrial")
    is_office = sector.str.contains("office")
    is_retail = sector.str.contains("retail")
    is_land = sector.str.contains("land")
    is_public = pl.col("spot_status_id") == 1 if "spot_status_id" in df_spots.columns else pl.lit(False)
    area = pl.col("spot_area_in_sqm").fill_null(0)
    ref_dt = datetime.strptime(partition_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
    agg = df_spots.group_by("user_id").agg([
        is_industrial.sum().cast(pl.Int64).alias("user_industrial_count"),
        is_office.sum().cast(pl.Int64).alias("user_office_count"),
        is_retail.sum().cast(pl.Int64).alias("user_retail_count"),
        is_land.sum().cast(pl.Int64).alias("user_land_count"),
        pl.len().alias("user_spot_count"),
        is_public.sum().cast(pl.Int64).alias("user_public_spot_count"),
        (area * is_industrial).sum().alias("user_total_industrial_sqm"),
        (area * is_office).sum().alias("user_total_office_sqm"),
        (area * is_retail).sum().alias("user_total_retail_sqm"),
        (area * is_land).sum().alias("user_total_land_sqm"),
        area.sum().alias("user_total_spot_sqm"),
        pl.col("spot_created_at").min().alias("user_first_spot_created_at"),
        pl.col("spot_created_at").max().alias("user_last_spot_created_at"),
        pl.col("spot_updated_at").max().alias("user_last_spot_updated_at"),
        # TODO: legacy user_last_spot_activity_at may have used other activity sources; with activity_log disabled we use spot_updated_at as proxy.
        pl.col("spot_updated_at").max().alias("user_last_spot_activity_at"),
    ])
    agg = agg.with_columns([
        (pl.lit(ref_dt) - pl.col("user_last_spot_created_at")).dt.total_days().cast(pl.Int64).alias("user_days_since_last_spot_created"),
        (pl.lit(ref_dt) - pl.col("user_last_spot_updated_at")).dt.total_days().cast(pl.Int64).alias("user_days_since_last_spot_updated"),
        (pl.lit(ref_dt) - pl.col("user_last_spot_activity_at")).dt.total_days().cast(pl.Int64).alias("user_days_since_last_spot_activity"),
    ])
    return agg

# Legacy/build order: 60 cols for transform output. Same as governance (contact_id at pos 12; user_industria_role at end).
# user_raw_spot_count is built by transform but not in governance; we select only LK_USERS_GOVERNANCE_SCHEMA_ORDER for S3/Geospot.
LK_USERS_LEGACY_COLUMN_ORDER = (
    "user_id", "user_uuid", "user_name", "user_last_name", "user_mothers_last_name",
    "user_email", "user_domain", "user_phone_number", "user_has_whatsapp",
    "user_profile_id", "user_owner_id", "contact_id", "user_hubspot_owner_id", "user_hubspot_user_id",
    "user_person_type_id", "user_person_type",
    "user_type_id", "user_type",
    "user_affiliation_id", "user_affiliation",
    "user_industrial_count", "user_office_count", "user_retail_count", "user_land_count",
    "user_spot_count", "user_public_spot_count",
    "user_total_industrial_sqm", "user_total_office_sqm", "user_total_retail_sqm",
    "user_total_land_sqm", "user_total_spot_sqm",
    "user_first_spot_created_at", "user_last_spot_created_at", "user_last_spot_updated_at",
    "user_last_spot_activity_at", "user_days_since_last_spot_created", "user_days_since_last_spot_updated",
    "user_days_since_last_spot_activity",
    "user_kyc_status_id", "user_kyc_status", "user_kyc_created_at",
    "user_created_at", "user_created_date", "user_updated_at", "user_updated_date",
    "user_deleted_at", "user_deleted_date",
    "aud_inserted_at", "aud_inserted_date", "aud_updated_at", "aud_updated_date", "aud_job",
    "agent_kam_id", "user_is_kam", "user_full_name", "agent_kam_full_name",
    "user_phone_indicator", "user_phone_full_number",
    "user_industria_role_id", "user_industria_role",
)

# Exact column order for lk_users (Postgres). Must match table for Geospot load.
# 60 columns: contact_id at pos 12 (after user_owner_id). user_industria_role at end (59-60).
LK_USERS_GOVERNANCE_SCHEMA_ORDER = (
    "user_id", "user_uuid", "user_name", "user_last_name", "user_mothers_last_name",
    "user_email", "user_domain", "user_phone_number", "user_has_whatsapp",
    "user_profile_id", "user_owner_id", "contact_id", "user_hubspot_owner_id", "user_hubspot_user_id",
    "user_person_type_id", "user_person_type",
    "user_type_id", "user_type",
    "user_affiliation_id", "user_affiliation",
    "user_industrial_count", "user_office_count", "user_retail_count", "user_land_count",
    "user_spot_count", "user_public_spot_count",
    "user_total_industrial_sqm", "user_total_office_sqm", "user_total_retail_sqm",
    "user_total_land_sqm", "user_total_spot_sqm",
    "user_first_spot_created_at", "user_last_spot_created_at", "user_last_spot_updated_at",
    "user_last_spot_activity_at", "user_days_since_last_spot_created", "user_days_since_last_spot_updated",
    "user_days_since_last_spot_activity",
    "user_kyc_status_id", "user_kyc_status", "user_kyc_created_at",
    "user_created_at", "user_created_date", "user_updated_at", "user_updated_date",
    "user_deleted_at", "user_deleted_date",
    "aud_inserted_at", "aud_inserted_date", "aud_updated_at", "aud_updated_date", "aud_job",
    "agent_kam_id", "user_is_kam", "user_full_name", "agent_kam_full_name",
    "user_phone_indicator", "user_phone_full_number",
    "user_industria_role_id", "user_industria_role",
)


def _transform_lk_users_new(
    stg_s2p_users_new: pl.DataFrame,
    stg_s2p_clients_new: pl.DataFrame,
    context: dg.AssetExecutionContext,
    df_spots_agg: pl.DataFrame | None = None,
    df_raw_spots_agg: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """
    Builds the gold_lk_users_new table by joining stg_s2p_users_new with 
    clients (leads) and spots aggregation (if available).
    
    - Joins with stg_s2p_clients_new to get agent_kam_id (from kam_agent_id)
    - If df_spots_agg is provided (from stg_lk_spots_new aggregated by user_id), left-joins spot counts/sqm/dates
    - If df_raw_spots_agg is provided (from bronze raw_s2p_spots_new), left-joins user_raw_spot_count
    - Otherwise uses placeholders (0 / null) for spot columns
    """
    
    # ========== PREPARE LEADS DATA ==========
    # Get agent_kam_id from clients where user_id matches (the KAM assigned to this user's leads)
    # From lk_users.sql: LEFT JOIN lk_leads AS lkl ON lkl.user_id = users.user_id
    # We take the KAM from the most-recent lead (sort by updated_at desc) for a deterministic result.
    _clients_with_kam = stg_s2p_clients_new.filter(pl.col("kam_agent_id").is_not_null())
    if "updated_at" in _clients_with_kam.columns:
        _clients_with_kam = _clients_with_kam.sort("updated_at", descending=True)
    df_leads_agg = (
        _clients_with_kam
        .group_by("user_id")
        .agg([
            pl.col("kam_agent_id").first().cast(pl.Int64).alias("agent_kam_id"),
        ])
    )
    
    # Get agent_kam_full_name (join back to users to get the KAM's full name)
    # From lk_users.sql: LEFT JOIN stg_lk_users AS lkua ON lkl.agent_kam_id = lkua.user_id
    # Legacy: lkua.user_full_name AS agent_kam_full_name. Same type on join key so the match works.
    df_kam_lookup = (
        stg_s2p_users_new
        .select(["user_id", "user_full_name"])
        .with_columns(pl.col("user_id").cast(pl.Int64))  # Ensure same type as agent_kam_id for join
        .rename({"user_id": "agent_kam_id", "user_full_name": "agent_kam_full_name"})
    )
    
    # Merge agent_kam_id and agent_kam_full_name (join key both Int64)
    df_leads_final = df_leads_agg.join(
        df_kam_lookup,
        on="agent_kam_id",
        how="left"
    )
    
    # Check if user is KAM (exists in stg_s2p_clients_new as kam_agent_id for other users' leads)
    # From lk_users.sql: CASE WHEN EXISTS (SELECT 1 FROM lk_leads WHERE lk_leads.agent_kam_id = users.user_id) THEN 1 ELSE 0 END
    df_is_kam = (
        stg_s2p_clients_new
        .filter(pl.col("kam_agent_id").is_not_null())
        .select("kam_agent_id")
        .unique()
        .with_columns([
            pl.lit(1).alias("user_is_kam")
        ])
        .rename({"kam_agent_id": "user_id"})
    )
    
    # ========== JOIN USERS WITH LEADS ==========
    df_joined = (
        stg_s2p_users_new
        .join(df_leads_final, on="user_id", how="left")
        .join(df_is_kam, on="user_id", how="left")
    )
    
    # ========== JOIN SPOTS AGGREGATION (from stg_lk_spots_new by user_id) ==========
    if df_spots_agg is not None and not df_spots_agg.is_empty():
        df_joined = df_joined.join(df_spots_agg, on="user_id", how="left")

    # ========== JOIN RAW SPOTS AGGREGATION (from bronze raw_s2p_spots_new by user_id) ==========
    if df_raw_spots_agg is not None and not df_raw_spots_agg.is_empty():
        df_joined = df_joined.join(df_raw_spots_agg, on="user_id", how="left")

    # ========== SPOTS: ensure columns exist and fill nulls (placeholders when no spots / no agg) ==========
    spot_int_cols = [
        "user_industrial_count", "user_office_count", "user_retail_count", "user_land_count",
        "user_spot_count", "user_public_spot_count", "user_raw_spot_count",
        "user_days_since_last_spot_created", "user_days_since_last_spot_updated", "user_days_since_last_spot_activity",
    ]
    spot_float_cols = [
        "user_total_industrial_sqm", "user_total_office_sqm", "user_total_retail_sqm",
        "user_total_land_sqm", "user_total_spot_sqm",
    ]
    spot_ts_cols = [
        "user_first_spot_created_at", "user_last_spot_created_at", "user_last_spot_updated_at", "user_last_spot_activity_at",
    ]
    for c in spot_int_cols:
        if c not in df_joined.columns:
            df_joined = df_joined.with_columns(pl.lit(0).cast(pl.Int64).alias(c))
        else:
            df_joined = df_joined.with_columns(pl.col(c).fill_null(0).cast(pl.Int64).alias(c))
    for c in spot_float_cols:
        if c not in df_joined.columns:
            df_joined = df_joined.with_columns(pl.lit(0.0).cast(pl.Float64).alias(c))
        else:
            df_joined = df_joined.with_columns(pl.col(c).fill_null(0.0).cast(pl.Float64).alias(c))
    for c in spot_ts_cols:
        if c not in df_joined.columns:
            df_joined = df_joined.with_columns(pl.lit(None).cast(pl.Datetime).alias(c))
    
    # ========== BUILD FINAL DATAFRAME ==========
    # REMOVED: user_category_id_expr / user_category_expr (lk_duc_domain_user_categories deprecated)
    # REMOVED: user_industria_role (model_has_roles deprecated); user_type_id/user_type come from silver (profiles.tenant_type).
    df_result = df_joined.with_columns([
        pl.col("user_is_kam").fill_null(0).cast(pl.Int64),
        # Intentional divergence from legacy: internal users without KYC inquiry use user_created_at as fallback.
        pl.when(
            (pl.col("user_affiliation_id") == 1) & (pl.col("user_kyc_created_at").is_null())
        )
        .then(pl.col("user_created_at"))
        .otherwise(pl.col("user_kyc_created_at"))
        .alias("user_kyc_created_at_fixed"),
    ])
    
    # Replace user_kyc_created_at with the fixed version
    df_result = df_result.with_columns([
        pl.col("user_kyc_created_at_fixed").alias("user_kyc_created_at")
    ]).drop("user_kyc_created_at_fixed")

    # Audit fields (legacy has aud_* at columns 57-61)
    df_result = add_audit_fields(df_result, job_name="lk_users")

    # ========== CAST TYPES FOR lk_users (legacy: int8 -> Int64, float8 -> Float64) ==========
    int64_columns_legacy = [
        "user_id", "user_profile_id", "user_owner_id", "contact_id", "user_hubspot_owner_id", "user_hubspot_user_id",
        "user_person_type_id", "user_industria_role_id",
        "user_type_id", "user_affiliation_id",
        "user_industrial_count", "user_office_count", "user_retail_count", "user_land_count", "user_spot_count", "user_public_spot_count",
        "user_days_since_last_spot_created", "user_days_since_last_spot_updated", "user_days_since_last_spot_activity",
        "user_kyc_status_id", "agent_kam_id", "user_is_kam",
        "user_phone_number", "user_has_whatsapp",
    ]
    cast_exprs = [pl.col(c).cast(pl.Int64).alias(c) for c in int64_columns_legacy if c in df_result.columns]
    if cast_exprs:
        df_result = df_result.with_columns(cast_exprs)
    # Legacy float8: user_days_since_last_log, user_total_log_count (commented out with activity columns)

    # ========== OUTPUT: 60 columns in exact order (lk_users schema) ==========
    for c in LK_USERS_LEGACY_COLUMN_ORDER:
        if c not in df_result.columns:
            if c.endswith("_at"):
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Datetime).alias(c))
            elif c.endswith("_date"):
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Date).alias(c))
            elif c in ("user_is_kam", "user_has_whatsapp") or "_id" in c or "count" in c:
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Int64).alias(c))
            elif "sqm" in c:
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Float64).alias(c))
            else:
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    df_result = df_result.select(LK_USERS_LEGACY_COLUMN_ORDER)
    
    return df_result


def _gold_lk_users_new_impl(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """
    Creates the gold users table with full enrichment (NEW VERSION).
    
    Reads all silver tables directly from S3 (no explicit Dagster dependencies).
    If any data is stale (from previous day), logs warning and sends notification.
    
    This asset joins:
    - stg_s2p_users_new: Base user data (new version with original column names)
    - stg_s2p_clients_new: To get agent_kam_id (from kam_agent_id) and check if user is KAM
      - Join: users.user_id = clients.user_id
      - agent_kam_id comes from clients.kam_agent_id
    - stg_lk_spots_new: For user-level spot counts/sqm/dates (aggregated by user_id); if missing, spot columns are 0/null.

    user_type_id / user_type come from silver (profiles.tenant_type CASE). Schema 60 cols: contact_id at pos 12; user_industria_role at end.
    
    Required minimum columns:
    - user_id (PK): From stg_s2p_users_new.user_id
    - user_created_date: From stg_s2p_users_new.user_created_at
    - aud_* fields: Added automatically via add_audit_fields()
    
    The result contains all user fields plus KAM information and spots statistics.
    """
    # ========== READ ALL SILVERS FROM S3 ==========
    stale_tables = []
    
    # Read users (silver)
    stg_s2p_users_new, meta_users = read_silver_from_s3("stg_s2p_users_new", context)
    if meta_users['is_stale']:
        stale_tables.append({
            'table_name': 'stg_s2p_users_new',
            'expected_date': context.partition_key,
            'available_date': meta_users['file_date'],
            'layer': 'silver',
            'file_path': meta_users['file_path'],
        })
    
    # Read clients (silver) - for agent_kam_id (from kam_agent_id)
    stg_s2p_clients_new, meta_clients = read_silver_from_s3("stg_s2p_clients_new", context)
    if meta_clients['is_stale']:
        stale_tables.append({
            'table_name': 'stg_s2p_clients_new',
            'expected_date': context.partition_key,
            'available_date': meta_clients['file_date'],
            'layer': 'silver',
            'file_path': meta_clients['file_path'],
        })
    
    # Read spots (silver): stg_lk_spots_new for sqm/dates/sectors; stg_s2p_spots_new for raw user_spot_count (only non-deleted spots per user).
    df_spots_agg = None
    try:
        stg_lk_spots_new, meta_spots = read_silver_from_s3("stg_lk_spots_new", context)
        if meta_spots.get("is_stale"):
            context.log.warning(
                f"stg_lk_spots_new is stale (expected {context.partition_key}, got {meta_spots.get('file_date')}). "
                "Using previous day for spot columns; optional upstream, no notification sent."
            )
        df_spots_agg = _agg_spots_by_user(stg_lk_spots_new, context.partition_key)
    except (ValueError, FileNotFoundError, KeyError) as e:
        context.log.warning(f"stg_lk_spots_new not available for gold_lk_users_new: {e}. Spot columns will be 0/null.")

    # Raw count from silver stg_s2p_spots_new (non-deleted only) — replaces user_spot_count and feeds user_raw_spot_count (single read).
    df_raw_spots_agg = None
    try:
        stg_s2p_spots_new, _ = read_silver_from_s3("stg_s2p_spots_new", context)
        if "user_id" in stg_s2p_spots_new.columns:
            base = stg_s2p_spots_new
            if "deleted_at" in stg_s2p_spots_new.columns:
                base = base.filter(pl.col("deleted_at").is_null())
            df_user_spot_count = (
                base
                .group_by("user_id")
                .agg(pl.len().cast(pl.Int64).alias("user_spot_count_raw"))
                .with_columns(pl.col("user_id").cast(pl.Int64))
            )
            if df_spots_agg is not None and not df_spots_agg.is_empty():
                df_spots_agg = (
                    df_spots_agg.drop("user_spot_count")
                    .join(df_user_spot_count, on="user_id", how="left")
                    .rename({"user_spot_count_raw": "user_spot_count"})
                    .with_columns(pl.col("user_spot_count").fill_null(0))
                )
            else:
                df_spots_agg = (
                    df_user_spot_count
                    .rename({"user_spot_count_raw": "user_spot_count"})
                    .with_columns(pl.col("user_spot_count").fill_null(0))
                )
            # Same aggregation used for user_raw_spot_count (transform expects column user_raw_spot_count).
            df_raw_spots_agg = df_user_spot_count.rename({"user_spot_count_raw": "user_raw_spot_count"})
        else:
            context.log.warning("stg_s2p_spots_new missing user_id; user_spot_count and user_raw_spot_count not replaced.")
    except (ValueError, FileNotFoundError, KeyError) as e:
        context.log.warning(f"stg_s2p_spots_new not available: {e}. user_spot_count from stg_lk_spots_new or 0; user_raw_spot_count will be 0.")

    # ========== TRANSFORMATION ==========
    df_users = _transform_lk_users_new(
        stg_s2p_users_new=stg_s2p_users_new,
        stg_s2p_clients_new=stg_s2p_clients_new,
        context=context,
        df_spots_agg=df_spots_agg,
        df_raw_spots_agg=df_raw_spots_agg,
    )
    
    # Validate joins
    validate_gold_joins(
        df_main=stg_s2p_users_new,
        df_joined=df_users,
        context=context,
        main_name="stg_s2p_users_new"
    )

    # Select columns matching lk_users schema (60 cols including contact_id; no user_raw_spot_count) for S3 and Geospot
    df_users = df_users.select(LK_USERS_GOVERNANCE_SCHEMA_ORDER)

    # Validación preventiva: schema debe coincidir exactamente con la tabla Postgres antes del load
    expected_cols = list(LK_USERS_GOVERNANCE_SCHEMA_ORDER)
    actual_cols = df_users.columns
    if actual_cols != expected_cols:
        missing = set(expected_cols) - set(actual_cols)
        extra = set(actual_cols) - set(expected_cols)
        context.log.error(
            f"Schema mismatch antes de cargar a lk_users. "
            f"Faltantes: {missing}. Extra: {extra}."
        )
        raise ValueError(f"Schema mismatch lk_users: faltantes={missing}, extra={extra}")

    # Audit fields are added in _transform_lk_users_new (legacy order)
    context.log.info(
        f"gold_lk_users_new: {df_users.height} rows for partition {context.partition_key}"
    )
    
    # ========== COLUMN BREAKDOWN LOG ==========
    audit_count = len([c for c in df_users.columns if c.startswith("aud_")])
    context.log.info(
        f"gold_lk_users_new: {df_users.width} columns (lk_users schema), {df_users.height} rows. Audit: {audit_count} (aud_*)."
    )

    # ========== PERSIST TO S3 ==========
    # Column order = LK_USERS_GOVERNANCE_SCHEMA_ORDER. Types aligned to lk_users (int8/float8/timestamp/date).
    s3_key = build_gold_s3_key("lk_users", context.partition_key, file_format="parquet")
    write_polars_to_s3(df_users, s3_key, context, file_format="parquet")

    # Load to Geospot (tabla lk_users)
    context.log.info(
        f"📤 Loading to Geospot: table=lk_users, s3_key={s3_key}, mode=replace"
    )
    try:
        load_to_geospot(s3_key=s3_key, table_name="lk_users", mode="replace", context=context)
        context.log.info("✅ Geospot load completed for lk_users")
    except Exception as e:
        context.log.error(f"❌ Geospot load failed: {e}")
        raise

    # ========== SEND NOTIFICATIONS IF STALE DATA USED ==========
    if stale_tables:
        send_stale_data_notification(
            context,
            stale_tables,
            current_asset_name="gold_lk_users_new",
            current_layer="gold",
        )
    
    return df_users


@dg.asset(
    partitions_def=daily_partitions,
    group_name="gold",
    retry_policy=dg.RetryPolicy(max_retries=2),
    description=(
        "Gold: Final lk_users table ready for consumption (NEW VERSION). "
        "Combines stg_s2p_users_new with leads, spots statistics."
    ),
)
def gold_lk_users_new(context):
    def body():
        return _gold_lk_users_new_impl(context)

    yield from iter_job_wrapped_compute(context, body)
