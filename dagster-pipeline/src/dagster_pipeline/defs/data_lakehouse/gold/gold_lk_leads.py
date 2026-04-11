# defs/data_lakehouse/gold/gold_lk_leads.py
"""
Gold layer: Final lk_leads table ready for consumption.

Combines lead data from stg_s2p_leads with:
- Project count from stg_s2p_projects
- Most advanced project funnel data from core_project_funnel
- Most recent project funnel data from core_project_funnel

This creates a comprehensive view for analysis and reporting.
"""
import dagster as dg
import polars as pl
from datetime import datetime, date

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _get_funnel_stage_priority(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds a funnel_stage_priority column to determine how far a project advanced.
    
    Priority (higher = more advanced):
    - 6: Has transaction date (won)
    - 5: Has contract date
    - 4: Has LOI date
    - 3: Has visit confirmed
    - 2: Has visit realized
    - 1: Has visit created
    - 0: Lead only (no visits)
    """
    return df.with_columns(
        pl.when(pl.col("project_funnel_transaction_date").is_not_null())
        .then(6)
        .when(pl.col("project_funnel_contract_date").is_not_null())
        .then(5)
        .when(pl.col("project_funnel_loi_date").is_not_null())
        .then(4)
        .when(pl.col("project_funnel_visit_confirmed_at").is_not_null())
        .then(3)
        .when(pl.col("project_funnel_visit_realized_at").is_not_null())
        .then(2)
        .when(pl.col("project_funnel_visit_created_date").is_not_null())
        .then(1)
        .otherwise(0)
        .alias("_funnel_stage_priority")
    )


def _transform_lk_leads(
    stg_s2p_leads: pl.DataFrame,
    stg_s2p_projects: pl.DataFrame,
    core_project_funnel: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds the gold_lk_leads table by enriching stg_s2p_leads with:
    - Project count from stg_s2p_projects
    - Most advanced project funnel data (furthest in funnel)
    - Most recent project funnel data (latest created)
    
    Joins (all LEFT to keep all leads):
    - stg_s2p_projects: Grouped by lead_id to get lead_projects_count
    - core_project_funnel: Aggregated by lead_id for adv/rec projects
    """

    # =========================================================================
    # 1. PREPARE PROJECT COUNT (group by lead_id)
    # =========================================================================

    df_project_count = (
        stg_s2p_projects
        .group_by("lead_id")
        .agg(pl.len().alias("lead_projects_count"))
    )

    # =========================================================================
    # 2. PREPARE FUNNEL DATA WITH STAGE PRIORITY
    # =========================================================================

    # Add priority to determine most advanced project
    df_funnel = _get_funnel_stage_priority(core_project_funnel)

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
    # 3. MOST ADVANCED PROJECT (furthest in funnel, then most recent)
    # =========================================================================

    # Sort by priority (desc) then by created_date (desc) then by project_id (desc) to break ties
    df_funnel_sorted_adv = df_funnel.sort(
        by=["lead_id", "_funnel_stage_priority", "project_created_date", "project_id"],
        descending=[False, True, True, True]
    )

    # Take first row per lead (most advanced)
    df_adv = (
        df_funnel_sorted_adv
        .group_by("lead_id")
        .first()
        .select([
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
    )

    # =========================================================================
    # 4. MOST RECENT PROJECT (latest created)
    # =========================================================================

    # Sort by created_date (desc), then project_id (desc) for deterministic tie-breaking
    df_funnel_sorted_rec = df_funnel.sort(
        by=["lead_id", "project_created_date", "project_id"],
        descending=[False, True, True]
    )

    # Take first row per lead (most recent)
    df_rec = (
        df_funnel_sorted_rec
        .group_by("lead_id")
        .first()
        .select([
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
    )

    # =========================================================================
    # 5. LEFT JOINS: leads <- project_count <- adv_funnel <- rec_funnel
    # =========================================================================

    df_joined = (
        stg_s2p_leads
        .join(df_project_count, on="lead_id", how="left")
        .join(df_adv, on="lead_id", how="left")
        .join(df_rec, on="lead_id", how="left")
    )

    # Fill null project counts with 0
    df_joined = df_joined.with_columns(
        pl.col("lead_projects_count").fill_null(0)
    )

    # =========================================================================
    # 6. FINAL SELECT (explicit field order for flexibility)
    # =========================================================================

    df_result = df_joined.select([
        # ---------------------------------------------------------------------
        # Identification
        # ---------------------------------------------------------------------
        pl.col("lead_id"),
        pl.col("lead_name"),
        pl.col("lead_last_name"),
        pl.col("lead_mothers_last_name"),
        pl.col("lead_email"),
        pl.col("lead_domain"),
        pl.col("lead_phone_indicator"),
        pl.col("lead_phone_number"),
        pl.col("lead_full_phone_number"),
        pl.col("lead_company"),
        pl.col("lead_position"),

        # ---------------------------------------------------------------------
        # Origin
        # ---------------------------------------------------------------------
        pl.col("lead_origin_id"),
        pl.col("lead_origin"),

        # ---------------------------------------------------------------------
        # Lead flags (L0-L4, Supply, S0-S3)
        # ---------------------------------------------------------------------
        pl.col("lead_l0"),
        pl.col("lead_l1"),
        pl.col("lead_l2"),
        pl.col("lead_l3"),
        pl.col("lead_l4"),
        pl.col("lead_supply"),
        pl.col("lead_s0"),
        pl.col("lead_s1"),
        pl.col("lead_s3"),

        # ---------------------------------------------------------------------
        # Max type (extra from STG)
        # ---------------------------------------------------------------------
        pl.col("lead_max_type_id"),
        pl.col("lead_max_type"),

        # ---------------------------------------------------------------------
        # Client type
        # ---------------------------------------------------------------------
        pl.col("lead_client_type_id"),
        pl.col("lead_client_type"),

        # ---------------------------------------------------------------------
        # Users and Agents
        # ---------------------------------------------------------------------
        pl.col("user_id"),
        pl.col("agent_id"),
        pl.col("lead_agent_by_api"),
        pl.col("agent_kam_id"),

        # ---------------------------------------------------------------------
        # Sector
        # ---------------------------------------------------------------------
        pl.col("spot_sector_id"),
        pl.col("spot_sector"),

        # ---------------------------------------------------------------------
        # Status
        # ---------------------------------------------------------------------
        pl.col("lead_status_id"),
        pl.col("lead_status"),

        # ---------------------------------------------------------------------
        # Contact
        # ---------------------------------------------------------------------
        pl.col("lead_first_contact_time"),

        # ---------------------------------------------------------------------
        # Space requirements
        # ---------------------------------------------------------------------
        pl.col("spot_min_square_space"),
        pl.col("spot_max_square_space"),

        # ---------------------------------------------------------------------
        # Source
        # ---------------------------------------------------------------------
        pl.col("lead_sourse_id"),
        pl.col("lead_sourse"),

        # ---------------------------------------------------------------------
        # HubSpot fields
        # ---------------------------------------------------------------------
        pl.col("lead_hs_first_conversion"),
        pl.col("lead_hs_utm_campaign"),

        # ---------------------------------------------------------------------
        # Campaign type (extra from STG)
        # ---------------------------------------------------------------------
        pl.col("lead_campaign_type_id"),
        pl.col("lead_campaign_type"),

        # ---------------------------------------------------------------------
        # Analytics
        # ---------------------------------------------------------------------
        pl.col("lead_hs_analytics_source_data_1"),
        pl.col("lead_hs_recent_conversion"),
        pl.col("lead_hs_recent_conversion_date"),

        # ---------------------------------------------------------------------
        # Industrial profiling
        # ---------------------------------------------------------------------
        pl.col("lead_hs_industrial_profiling"),
        pl.col("lead_industrial_profile_state"),
        pl.col("lead_industrial_profile_zone"),
        pl.col("lead_industrial_profile_size_id"),
        pl.col("lead_industrial_profile_size"),
        pl.col("lead_industrial_profile_budget_id"),
        pl.col("lead_industrial_profile_budget"),

        # ---------------------------------------------------------------------
        # Office profiling
        # ---------------------------------------------------------------------
        pl.col("lead_hs_office_profiling"),
        pl.col("lead_office_profile_state"),
        pl.col("lead_office_profile_zone"),
        pl.col("lead_office_profile_size_id"),
        pl.col("lead_office_profile_size"),
        pl.col("lead_office_profile_budget_id"),
        pl.col("lead_office_profile_budget"),

        # ---------------------------------------------------------------------
        # Funnel relevance (extra from STG)
        # ---------------------------------------------------------------------
        pl.col("lead_project_funnel_relevant_id"),
        pl.col("lead_project_funnel_relevant"),

        # ---------------------------------------------------------------------
        # LDS relevance (for BT_LDS_LEAD_SPOTS)
        # ---------------------------------------------------------------------
        pl.col("lead_lds_relevant_id"),
        pl.col("lead_lds_relevant"),

        # ---------------------------------------------------------------------
        # Projects count (from JOIN with stg_s2p_projects)
        # ---------------------------------------------------------------------
        pl.col("lead_projects_count"),

        # =====================================================================
        # MOST ADVANCED PROJECT (furthest in funnel)
        # =====================================================================
        pl.col("lead_adv_project_id"),
        pl.col("lead_adv_funnel_lead_date"),
        pl.col("lead_adv_funnel_visit_created_date"),
        pl.col("lead_adv_funnel_visit_realized_at"),
        pl.col("lead_adv_funnel_visit_confirmed_at"),
        pl.col("lead_adv_funnel_loi_date"),
        pl.col("lead_adv_funnel_contract_date"),
        pl.col("lead_adv_funnel_transaction_date"),
        pl.col("lead_adv_funnel_flow"),

        # =====================================================================
        # MOST RECENT PROJECT (latest created)
        # =====================================================================
        pl.col("lead_rec_project_id"),
        pl.col("lead_rec_funnel_lead_date"),
        pl.col("lead_rec_funnel_visit_created_date"),
        pl.col("lead_rec_funnel_visit_realized_at"),
        pl.col("lead_rec_funnel_visit_confirmed_at"),
        pl.col("lead_rec_funnel_loi_date"),
        pl.col("lead_rec_funnel_contract_date"),
        pl.col("lead_rec_funnel_transaction_date"),
        pl.col("lead_rec_funnel_flow"),

        # ---------------------------------------------------------------------
        # Lead timestamps
        # ---------------------------------------------------------------------
        pl.col("lead_lead0_at"),
        pl.col("lead_lead1_at"),
        pl.col("lead_lead2_at"),
        pl.col("lead_lead3_at"),
        pl.col("lead_lead4_at"),
        pl.col("lead_supply_at"),
        pl.col("lead_supply0_at"),
        pl.col("lead_supply1_at"),
        pl.col("lead_supply3_at"),
        pl.col("lead_client_at"),

        # ---------------------------------------------------------------------
        # Type date/datetime start (extra from STG)
        # ---------------------------------------------------------------------
        pl.col("lead_type_datetime_start"),  # Full timestamp
        pl.col("lead_type_date_start"),      # Date only

        # ---------------------------------------------------------------------
        # Profiling completed (extra from STG)
        # ---------------------------------------------------------------------
        pl.col("lead_profiling_completed_at"),

        # ---------------------------------------------------------------------
        # Created/Updated/Deleted timestamps
        # ---------------------------------------------------------------------
        pl.col("lead_created_at"),
        pl.col("lead_created_date"),
        pl.col("lead_updated_at"),
        pl.col("lead_updated_date"),
        pl.col("lead_deleted_at"),
        pl.col("lead_deleted_date"),

        # =====================================================================
        # Audit fields
        # =====================================================================
        pl.lit(datetime.now()).alias("aud_inserted_at"),
        pl.lit(date.today()).alias("aud_inserted_date"),
        pl.lit(datetime.now()).alias("aud_updated_at"),
        pl.lit(date.today()).alias("aud_updated_date"),
        pl.lit("lk_leads").alias("aud_job"),
    ])

    return df_result


@dg.asset(
    partitions_def=daily_partitions,
    group_name="gold",
    description=(
        "Gold: Final lk_leads table ready for consumption. "
        "Combines stg_s2p_leads with project count and funnel data."
    ),
)
def gold_lk_leads(
    context: dg.AssetExecutionContext,
    stg_s2p_leads: pl.DataFrame,
    stg_s2p_projects: pl.DataFrame,
    core_project_funnel: pl.DataFrame,
) -> pl.DataFrame:
    """
    Creates the gold leads table with project and funnel enrichment.
    
    This asset joins:
    - stg_s2p_leads: Base lead data with transformations
    - stg_s2p_projects: Grouped by lead_id to count projects per lead
    - core_project_funnel: Aggregated by lead_id for:
        - Most advanced project (furthest in funnel)
        - Most recent project (latest created)
    
    The result contains all lead fields plus project count and funnel data,
    enabling comprehensive analysis of lead activity and conversion.
    """
    def body() -> pl.DataFrame:
        df_leads = _transform_lk_leads(
            stg_s2p_leads=stg_s2p_leads,
            stg_s2p_projects=stg_s2p_projects,
            core_project_funnel=core_project_funnel,
        )

        context.log.info(
            f"gold_lk_leads: {df_leads.height} rows for partition {context.partition_key}"
        )
        return df_leads

    yield from iter_job_wrapped_compute(context, body)
