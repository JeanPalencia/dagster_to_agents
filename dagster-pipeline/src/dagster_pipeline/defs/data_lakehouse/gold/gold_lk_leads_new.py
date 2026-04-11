# defs/data_lakehouse/gold/gold_lk_leads_new.py
"""
Gold layer: Final lk_leads table ready for consumption (NEW VERSION).

Combines stg_s2p_clients_new with stg_s2p_projects_new to create comprehensive lead view.
Uses reusable utilities for common patterns.

This version:
- Uses new silver tables (stg_s2p_clients_new, stg_s2p_projects_new)
- Maintains all columns from original gold_lk_leads
- Adds required minimum columns: lead_id, vis_pseudo_id, user_id, lead_creation_date

Replicates lk_leads (legacy): rows present in both tables should match. lk_leads tiene el mismo schema que lk_leads_v2 (107 columnas, sin lead_dollar_exchange ni filter).
"""
import dagster as dg
import polars as pl
from datetime import datetime, date

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
    aggregate_count_by,
    get_funnel_stage_priority,
    get_first_by_priority,
    validate_gold_joins,
    add_table_prefix_to_columns,
)
from dagster_pipeline.defs.data_lakehouse.silver.modules.lk_spots.exchange_rates import get_exchange_rate

# Replicate gold_lk_leads (legacy). Orden exacto de lk_leads_v2 (107 columnas, sin lead_dollar_exchange ni filter).
LK_LEADS_LEGACY_COLUMN_ORDER = (
    "lead_id", "lead_name", "lead_last_name", "lead_mothers_last_name", "lead_email", "lead_domain",
    "lead_phone_indicator", "lead_phone_number", "lead_full_phone_number", "lead_company", "lead_position",
    "lead_origin_id", "lead_origin",
    "lead_l0", "lead_l1", "lead_l2", "lead_l3", "lead_l4", "lead_supply", "lead_s0", "lead_s1", "lead_s3",
    "lead_max_type_id", "lead_max_type",
    "lead_client_type_id", "lead_client_type",
    "user_id", "agent_id", "lead_agent_by_api", "agent_kam_id",
    "spot_sector_id", "spot_sector", "lead_status_id", "lead_status",
    "lead_first_contact_time", "spot_min_square_space", "spot_max_square_space",
    "lead_sourse_id", "lead_sourse",
    "lead_hs_first_conversion", "lead_hs_utm_campaign",
    "lead_campaign_type_id", "lead_campaign_type",
    "lead_hs_analytics_source_data_1", "lead_hs_recent_conversion", "lead_hs_recent_conversion_date",
    "lead_hs_industrial_profiling", "lead_industrial_profile_state", "lead_industrial_profile_zone",
    "lead_industrial_profile_size_id", "lead_industrial_profile_size",
    "lead_industrial_profile_budget_id", "lead_industrial_profile_budget",
    "lead_hs_office_profiling", "lead_office_profile_state", "lead_office_profile_zone",
    "lead_office_profile_size_id", "lead_office_profile_size",
    "lead_office_profile_budget_id", "lead_office_profile_budget",
    "lead_project_funnel_relevant_id", "lead_project_funnel_relevant",
    "lead_lds_relevant_id", "lead_lds_relevant",
    "lead_projects_count",
    "lead_adv_project_id", "lead_adv_funnel_lead_date", "lead_adv_funnel_visit_created_date",
    "lead_adv_funnel_visit_realized_at", "lead_adv_funnel_visit_confirmed_at",
    "lead_adv_funnel_loi_date", "lead_adv_funnel_contract_date", "lead_adv_funnel_transaction_date", "lead_adv_funnel_flow",
    "lead_rec_project_id", "lead_rec_funnel_lead_date", "lead_rec_funnel_visit_created_date",
    "lead_rec_funnel_visit_realized_at", "lead_rec_funnel_visit_confirmed_at",
    "lead_rec_funnel_loi_date", "lead_rec_funnel_contract_date", "lead_rec_funnel_transaction_date", "lead_rec_funnel_flow",
    "lead_lead0_at", "lead_lead1_at", "lead_lead2_at", "lead_lead3_at", "lead_lead4_at",
    "lead_supply_at", "lead_supply0_at", "lead_supply1_at", "lead_supply3_at", "lead_client_at",
    "lead_type_datetime_start", "lead_type_date_start",
    "lead_profiling_completed_at",
    "lead_created_at", "lead_created_date", "lead_updated_at", "lead_updated_date",
    "lead_deleted_at", "lead_deleted_date",
    "aud_inserted_at", "aud_inserted_date", "aud_updated_at", "aud_updated_date", "aud_job",
)
# lk_leads = mismo schema que lk_leads_v2 (107 columnas).
LK_LEADS_GOVERNANCE_COLUMNS = LK_LEADS_LEGACY_COLUMN_ORDER

# Cols that may need filling if missing (no lead_dollar_exchange ni filter).
LK_LEADS_LEGACY_COLS_NO_AUD = tuple(c for c in LK_LEADS_LEGACY_COLUMN_ORDER if not c.startswith("aud_"))


def _transform_lk_leads_new(
    stg_s2p_clients_new: pl.DataFrame,
    stg_s2p_projects_new: pl.DataFrame,
    core_project_funnel_new: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds the gold_lk_leads_new table by enriching stg_s2p_clients_new with:
    - Project count from stg_s2p_projects_new
    - Most advanced project funnel data (furthest in funnel)
    - Most recent project funnel data (latest created)
    
    Joins (all LEFT to keep all leads):
    - stg_s2p_projects_new: Grouped by client_id to get lead_projects_count
    - core_project_funnel_new: Aggregated by lead_id for adv/rec projects
    """
    
    # =========================================================================
    # 1. PREPARE BASE: map columns to match gold_lk_leads (legacy) names
    
    df_leads_base = stg_s2p_clients_new.with_columns([
        # Identification
        pl.col("id").alias("lead_id"),
        pl.col("name").alias("lead_name"),
        pl.col("lastname").alias("lead_last_name"),
        pl.col("mothers_lastname").alias("lead_mothers_last_name"),
        pl.col("email").alias("lead_email"),
        pl.col("email_domain").alias("lead_domain"),
        pl.col("phone_indicator").alias("lead_phone_indicator"),
        pl.col("phone_number").alias("lead_phone_number"),
        pl.col("full_phone_number").alias("lead_full_phone_number"),
        pl.col("company").alias("lead_company"),
        pl.col("position").alias("lead_position"),
        
        # Origin
        pl.col("origin").alias("lead_origin_id"),
        pl.col("origin_text").alias("lead_origin"),
        
        # Lead flags (L0-L4, Supply, S0-S3)
        pl.col("has_lead0").alias("lead_l0"),
        pl.col("has_lead1").alias("lead_l1"),
        pl.col("has_lead2").alias("lead_l2"),
        pl.col("has_lead3").alias("lead_l3"),
        pl.col("has_lead4").alias("lead_l4"),
        pl.col("has_supply").alias("lead_supply"),
        pl.col("has_supply0").alias("lead_s0"),
        pl.col("has_supply1").alias("lead_s1"),
        pl.col("has_supply3").alias("lead_s3"),
        
        # Max type
        pl.col("lead_max_type_id").alias("lead_max_type_id"),
        pl.col("lead_max_type").alias("lead_max_type"),
        
        # Client type
        pl.col("client_type").alias("lead_client_type_id"),
        pl.col("client_type_text").alias("lead_client_type"),
        
        # Users and Agents (mapped to names without lead_ prefix)
        pl.col("user_id").alias("user_id"),
        pl.col("client_agent_id").alias("agent_id"),
        pl.col("client_agent_by_api").alias("lead_agent_by_api"),
        pl.col("kam_agent_id").alias("agent_kam_id"),
        
        # Sector
        pl.col("spot_sector_id").alias("spot_sector_id"),
        pl.col("spot_type_text").alias("spot_sector"),
        
        # Status (from source only; mapping client_state -> text is identical to stg_s2p_leads)
        pl.col("client_state").alias("lead_status_id"),
        pl.col("client_state_text").alias("lead_status"),
        
        # Contact
        pl.col("client_first_contact_time").alias("lead_first_contact_time"),
        
        # Space requirements
        pl.col("min_square_space").alias("spot_min_square_space"),
        pl.col("max_square_space").alias("spot_max_square_space"),
        
        # Source
        pl.col("hs_client_source").alias("lead_sourse_id"),
        pl.col("hs_client_source_text").alias("lead_sourse"),
        
        # HubSpot fields
        pl.col("hs_first_conversion").alias("lead_hs_first_conversion"),
        pl.col("hs_utm_campaign").alias("lead_hs_utm_campaign"),
        
        # Campaign type
        pl.col("campaign_type_id").alias("lead_campaign_type_id"),
        pl.col("campaign_type").alias("lead_campaign_type"),
        
        # Analytics
        pl.col("hs_analytics_source_data_1").alias("lead_hs_analytics_source_data_1"),
        pl.col("hs_recent_conversion").alias("lead_hs_recent_conversion"),
        pl.col("hs_recent_conversion_date").alias("lead_hs_recent_conversion_date"),
        
        # Industrial profiling
        pl.col("hs_industrial_profiling").alias("lead_hs_industrial_profiling"),
        pl.col("industrial_profile_state").alias("lead_industrial_profile_state"),
        pl.col("industrial_profile_region").alias("lead_industrial_profile_zone"),
        pl.col("industrial_profile_size_id").alias("lead_industrial_profile_size_id"),
        pl.col("industrial_profile_size_text").alias("lead_industrial_profile_size"),
        pl.col("industrial_profile_budget_id").alias("lead_industrial_profile_budget_id"),
        pl.col("industrial_profile_budget_text").alias("lead_industrial_profile_budget"),
        
        # Office profiling
        pl.col("hs_office_profiling").alias("lead_hs_office_profiling"),
        pl.col("office_profile_state").alias("lead_office_profile_state"),
        pl.col("office_profile_region").alias("lead_office_profile_zone"),
        pl.col("office_profile_size_id").alias("lead_office_profile_size_id"),
        pl.col("office_profile_size_text").alias("lead_office_profile_size"),
        pl.col("office_profile_budget_id").alias("lead_office_profile_budget_id"),
        pl.col("office_profile_budget_text").alias("lead_office_profile_budget"),
        
        # Funnel relevance
        pl.col("project_funnel_relevant_id").alias("lead_project_funnel_relevant_id"),
        pl.col("project_funnel_relevant").alias("lead_project_funnel_relevant"),
        
        # LDS relevance
        pl.col("lds_relevant_id").alias("lead_lds_relevant_id"),
        pl.col("lds_relevant").alias("lead_lds_relevant"),
        
        # VTL-aligned (same as lk_matches_visitors_to_leads/queries/clients.sql)
        pl.col("lead_min_at").alias("lead_min_at"),
        pl.col("lead_min_type").alias("lead_min_type"),
        pl.col("lead_type").alias("lead_type"),
        
        # Projects count (will be added in join)
        
        # Lead timestamps
        pl.col("lead0_at").alias("lead_lead0_at"),
        pl.col("lead1_at").alias("lead_lead1_at"),
        pl.col("lead2_at").alias("lead_lead2_at"),
        pl.col("lead3_at").alias("lead_lead3_at"),
        pl.col("lead4_at").alias("lead_lead4_at"),
        pl.col("supply_at").alias("lead_supply_at"),
        pl.col("supply0_at").alias("lead_supply0_at"),
        pl.col("supply1_at").alias("lead_supply1_at"),
        pl.col("supply3_at").alias("lead_supply3_at"),
        pl.col("client_at").alias("lead_client_at"),
        
        # Type date/datetime start
        pl.col("lead_type_datetime_start").alias("lead_type_datetime_start"),
        pl.col("lead_type_date_start").alias("lead_type_date_start"),
        
        # Profiling completed
        pl.col("profiling_completed_at").alias("lead_profiling_completed_at"),
        
        # Created/Updated/Deleted timestamps (from source only; do not overwrite with run date).
        # lead_updated_* comes from clients.updated_at; if governance shows run date, the source (MySQL) had that value when the silver was materialized.
        pl.col("created_at").alias("lead_created_at"),
        pl.col("created_date").alias("lead_created_date"),
        pl.col("updated_at").alias("lead_updated_at"),
        pl.col("updated_date").alias("lead_updated_date"),
        pl.col("deleted_at").alias("lead_deleted_at"),
        pl.col("deleted_date").alias("lead_deleted_date"),
    ])
    
    # Add required field lead_creation_date (derived from lead_created_at)
    df_leads_base = df_leads_base.with_columns(
        pl.col("lead_created_at").alias("lead_creation_date")
    )
    
    # =========================================================================
    # 2. PREPARE PROJECT COUNT (group by lead_id)
    # =========================================================================
    # stg_s2p_projects_new outputs lead_id (alias of client_id from bronze).
    df_project_count = (
        stg_s2p_projects_new
        .group_by("lead_id")
        .agg(pl.len().alias("lead_projects_count"))
    )
    
    # =========================================================================
    # 3. PREPARE FUNNEL DATA WITH STAGE PRIORITY
    # =========================================================================
    
    # Add priority to determine most advanced project
    df_funnel = get_funnel_stage_priority(core_project_funnel_new)
    
    # Select only needed columns from funnel
    funnel_cols = [
        "lead_id",
        "project_id",
        "project_created_date",
        "_funnel_stage_priority",
        "project_funnel_lead_date",
        "project_funnel_visit_created_date",
        "project_funnel_visit_realized_at",
        "project_funnel_visit_confirmed_at",
        "project_funnel_loi_date",
        "project_funnel_contract_date",
        "project_funnel_transaction_date",
        "project_funnel_flow",
    ]
    df_funnel = df_funnel.select(funnel_cols).unique()
    
    # =========================================================================
    # 4. MOST ADVANCED PROJECT (furthest in funnel, then most recent)
    # =========================================================================
    
    df_adv = get_first_by_priority(
        df_funnel,
        group_by_col="lead_id",
        priority_cols=["_funnel_stage_priority", "project_created_date", "project_id"],
        priority_desc=[True, True, True],  # Descending for all (priority, created_date, project_id)
        prefix="lead_adv_"
    ).select([
        pl.col("lead_id"),
        pl.col("project_id").alias("lead_adv_project_id"),
        pl.col("project_funnel_lead_date").alias("lead_adv_funnel_lead_date"),
        pl.col("project_funnel_visit_created_date").alias("lead_adv_funnel_visit_created_date"),
        pl.col("project_funnel_visit_realized_at").alias("lead_adv_funnel_visit_realized_at"),
        pl.col("project_funnel_visit_confirmed_at").alias("lead_adv_funnel_visit_confirmed_at"),
        pl.col("project_funnel_loi_date").alias("lead_adv_funnel_loi_date"),
        pl.col("project_funnel_contract_date").alias("lead_adv_funnel_contract_date"),
        pl.col("project_funnel_transaction_date").alias("lead_adv_funnel_transaction_date"),
        pl.col("project_funnel_flow").alias("lead_adv_funnel_flow"),
    ])
    
    # Most recent project: latest created; desempate por project_id desc para alinear con legacy y evitar L0/L1 invertidos en la frontera.
    df_rec = get_first_by_priority(
        df_funnel,
        group_by_col="lead_id",
        priority_cols=["project_created_date", "project_id"],
        priority_desc=[True, True],  # Descending for both; project_id is deterministic tiebreaker
        prefix="lead_rec_"
    ).select([
        pl.col("lead_id"),
        pl.col("project_id").alias("lead_rec_project_id"),
        pl.col("project_funnel_lead_date").alias("lead_rec_funnel_lead_date"),
        pl.col("project_funnel_visit_created_date").alias("lead_rec_funnel_visit_created_date"),
        pl.col("project_funnel_visit_realized_at").alias("lead_rec_funnel_visit_realized_at"),
        pl.col("project_funnel_visit_confirmed_at").alias("lead_rec_funnel_visit_confirmed_at"),
        pl.col("project_funnel_loi_date").alias("lead_rec_funnel_loi_date"),
        pl.col("project_funnel_contract_date").alias("lead_rec_funnel_contract_date"),
        pl.col("project_funnel_transaction_date").alias("lead_rec_funnel_transaction_date"),
        pl.col("project_funnel_flow").alias("lead_rec_funnel_flow"),
    ])
    
    # =========================================================================
    # 6. LEFT JOINS: leads <- project_count <- adv_funnel <- rec_funnel
    # =========================================================================
    
    df_joined = (
        df_leads_base
        .join(df_project_count, on="lead_id", how="left")
        .join(df_adv, on="lead_id", how="left")
        .join(df_rec, on="lead_id", how="left")
    )
    
    # Fill null project counts with 0
    df_joined = df_joined.with_columns(
        pl.col("lead_projects_count").fill_null(0)
    )
    
    # =========================================================================
    # 7. ALIGN WITH LEGACY: filter, sector y agent_id
    # =========================================================================

    # filter: 1 para activos, 0 para eliminados
    df_joined = df_joined.with_columns(
        pl.when(pl.col("lead_deleted_at").is_null())
          .then(1)
          .otherwise(0)
          .cast(pl.Int64)
          .alias("filter")
    )

    # spot_sector_id y spot_sector: derivar SIEMPRE desde spot_type_id del cliente
    # Independientemente de si el lead tiene proyectos o no.
    # spot_type_id viene de stg_s2p_clients_new (siempre presente en df_joined).
    df_joined = df_joined.with_columns([
        pl.when(pl.col("spot_type_id").is_in([13, 11, 9, 15]))
          .then(pl.col("spot_type_id"))
          .otherwise(pl.lit(13))
          .cast(pl.Int64)
          .alias("spot_sector_id"),
        pl.when(pl.col("spot_type_id") == 11).then(pl.lit("Office"))
          .when(pl.col("spot_type_id") == 9).then(pl.lit("Industrial"))
          .when(pl.col("spot_type_id") == 15).then(pl.lit("Land"))
          .otherwise(pl.lit("Retail"))
          .cast(pl.Utf8)
          .alias("spot_sector"),
    ])

    # Eliminar la sobreescritura de agent_id — client_agent_id ya fue mapeado
    # correctamente en el paso 1. No sobreescribir con user_id.
    
    # =========================================================================
    # 8. (Optional) Extra columns for internal use; not written to legacy output
    # =========================================================================
    
    # =========================================================================
    # 9. BUILD RESULT: legacy schema columns (107 to match gold_lk_leads); fill missing with null
    # =========================================================================
    cols_legacy_no_aud = [c for c in LK_LEADS_LEGACY_COLS_NO_AUD if c in df_joined.columns]
    df_result = df_joined.select([pl.col(c) for c in cols_legacy_no_aud])
    # Ensure any column from schema that was dropped (e.g. by select) is present as null
    for c in LK_LEADS_LEGACY_COLS_NO_AUD:
        if c not in df_result.columns:
            if c == "lead_dollar_exchange":
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Float64).alias(c))
            elif "_id" in c or "count" in c:
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Int64).alias(c))
            elif "_at" in c or ("_date" in c and "lead_" in c):
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Datetime if "_at" in c else pl.Date).alias(c))
            else:
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    
    # =========================================================================
    # 10. ADD AUDIT FIELDS (using reusable utility)
    # =========================================================================
    df_result = add_audit_fields(df_result, job_name="lk_leads")

    # =========================================================================
    # 11. OUTPUT: columns in legacy order (107 + aud = 112); add any missing as null
    # =========================================================================
    for c in LK_LEADS_LEGACY_COLUMN_ORDER:
        if c not in df_result.columns:
            if c.endswith("_at") or c.endswith("_date") and "aud_" in c:
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Datetime if c.endswith("_at") else pl.Date).alias(c))
            elif c.endswith("_date"):
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Date).alias(c))
            elif "_id" in c or c == "filter":
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Int64).alias(c))
            elif "time" in c or "exchange" in c or "count" in c:
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Float64 if c == "lead_dollar_exchange" or "time" in c else pl.Int64).alias(c))
            else:
                df_result = df_result.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    df_result = df_result.select(LK_LEADS_LEGACY_COLUMN_ORDER)
    
    return df_result


@dg.asset(
    partitions_def=daily_partitions,
    group_name="gold",
    retry_policy=dg.RetryPolicy(max_retries=2),
    io_manager_key="s3_gold",
    description=(
        "Gold: Final lk_leads table ready for consumption (NEW VERSION). "
        "Combines stg_s2p_clients_new with stg_s2p_projects_new using reusable utilities."
    ),
)
def gold_lk_leads_new(
    context,
):
    """
    Creates the gold leads table with project and funnel enrichment (NEW VERSION).
    Reads silver tables from S3.
    """
    def body():
        return _gold_lk_leads_new_body(context)

    yield from iter_job_wrapped_compute(context, body)


def _gold_lk_leads_new_body(context: dg.AssetExecutionContext) -> pl.DataFrame:
    # ========== READ SILVERS FROM S3 ==========
    stale_tables = []
    stg_s2p_clients_new, meta_clients = read_silver_from_s3("stg_s2p_clients_new", context)
    if meta_clients.get("is_stale"):
        stale_tables.append({
            "table_name": "stg_s2p_clients_new",
            "expected_date": context.partition_key,
            "available_date": meta_clients.get("file_date"),
            "layer": "silver",
            "file_path": meta_clients.get("file_path", ""),
        })
    stg_s2p_projects_new, meta_projects = read_silver_from_s3("stg_s2p_projects_new", context)
    if meta_projects.get("is_stale"):
        stale_tables.append({
            "table_name": "stg_s2p_projects_new",
            "expected_date": context.partition_key,
            "available_date": meta_projects.get("file_date"),
            "layer": "silver",
            "file_path": meta_projects.get("file_path", ""),
        })
    core_project_funnel_new, meta_funnel = read_silver_from_s3("core_project_funnel_new", context)
    if meta_funnel.get("is_stale"):
        stale_tables.append({
            "table_name": "core_project_funnel_new",
            "expected_date": context.partition_key,
            "available_date": meta_funnel.get("file_date"),
            "layer": "silver",
            "file_path": meta_funnel.get("file_path", ""),
        })
    # Exchange rate (lead_dollar_exchange) no está en lk_leads; se omite la lectura de stg_s2p_exchanges_new
    # para evitar alertas de stale cuando ese silver no corre en el job de leads.
    # ========== TRANSFORMATION ==========
    df_leads = _transform_lk_leads_new(
        stg_s2p_clients_new=stg_s2p_clients_new,
        stg_s2p_projects_new=stg_s2p_projects_new,
        core_project_funnel_new=core_project_funnel_new,
    )
    
    df_leads = df_leads.select(LK_LEADS_LEGACY_COLUMN_ORDER)
    
    # Mismo schema que lk_leads_v2 (107 columnas); escribir y cargar a lk_leads.
    df_governance = df_leads.select(
        [pl.col(c) for c in LK_LEADS_GOVERNANCE_COLUMNS if c in df_leads.columns]
    )
    for c in LK_LEADS_GOVERNANCE_COLUMNS:
        if c not in df_governance.columns:
            if c in ("spot_min_square_space", "spot_max_square_space"):
                df_governance = df_governance.with_columns(pl.lit(None).cast(pl.Float64).alias(c))
            elif c.endswith("_at") or c.endswith("_date"):
                df_governance = df_governance.with_columns(
                    pl.lit(None).cast(pl.Datetime if c.endswith("_at") else pl.Date).alias(c)
                )
            elif c == "lead_projects_count" or "_id" in c:
                df_governance = df_governance.with_columns(pl.lit(None).cast(pl.Int64).alias(c))
            else:
                df_governance = df_governance.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    df_governance = df_governance.select(LK_LEADS_GOVERNANCE_COLUMNS)

    # Validate joins
    validate_gold_joins(
        df_main=stg_s2p_clients_new,
        df_joined=df_leads,
        context=context,
        main_name="stg_s2p_clients_new"
    )
    
    context.log.info(
        f"gold_lk_leads_new: {df_leads.height} rows for partition {context.partition_key}"
    )
    
    # ========== COLUMN BREAKDOWN LOG ==========
    audit_count = len([c for c in df_governance.columns if c.startswith("aud_")])
    context.log.info(
        f"📊 gold_lk_leads_new: {df_governance.width} columns (governance schema), {df_governance.height} rows. Audit: {audit_count} (aud_*)."
    )
    
    # Persist to S3 (solo columnas de governance para que el load a Geospot cargue todas las filas)
    s3_key = build_gold_s3_key("lk_leads", context.partition_key, file_format="parquet")
    write_polars_to_s3(df_governance, s3_key, context, file_format="parquet")

    # Load to Geospot (tabla lk_leads)
    context.log.info(
        f"📤 Loading to Geospot: table=lk_leads, s3_key={s3_key}, mode=replace"
    )
    try:
        load_to_geospot(s3_key=s3_key, table_name="lk_leads", mode="replace", context=context)
        context.log.info("✅ Geospot load completed for lk_leads")
    except Exception as e:
        context.log.error(f"❌ Geospot load failed: {e}")
        raise

    # ========== SEND NOTIFICATIONS IF STALE DATA USED ==========
    if stale_tables:
        send_stale_data_notification(
            context, 
            stale_tables,
            current_asset_name="gold_lk_leads_new",
            current_layer="gold"
        )
    
    return df_leads
