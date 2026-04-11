# defs/data_lakehouse/gold/gold_lk_projects.py
"""
Gold layer: Final lk_projects table ready for consumption.

Combines project data with lead attributes, user profiles, and funnel metrics
to create a comprehensive view for analysis and reporting.
"""
import dagster as dg
import polars as pl
from datetime import datetime, date

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _transform_lk_projects(
    stg_s2p_projects: pl.DataFrame,
    stg_s2p_leads: pl.DataFrame,
    stg_s2p_user_profiles: pl.DataFrame,
    core_project_funnel: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds the gold_lk_projects table by joining stg_s2p_projects with 
    leads, user profiles, and funnel data to create a comprehensive project view.
    
    Joins (all LEFT to keep all projects):
    - stg_s2p_leads: Lead attributes (sector, type, campaign, relevance)
    - stg_s2p_user_profiles: User industry role
    - core_project_funnel: Funnel metrics (visits, LOI, contracts, transactions)
    """

    # =========================================================================
    # 1. PREPARE SOURCE DATA (select only needed columns, avoid duplicates)
    # =========================================================================

    # Leads: fields removed from funnel, now recovered here
    # Dedupe by lead_id so the join does not multiply project rows (one row per project).
    df_leads = (
        stg_s2p_leads
        .select([
            "lead_id",
            "spot_sector",
            "lead_max_type",
            "lead_campaign_type",
            "lead_profiling_completed_at",
            "lead_project_funnel_relevant_id",
        ])
        .unique(subset=["lead_id"], keep="first")
    )

    # User profiles: industry role
    df_profiles = stg_s2p_user_profiles.select([
        "user_id",
        "user_industria_role_id",
        "user_industria_role",
    ])

    # Funnel: aggregate by project_id to avoid duplicates
    # project_funnel_event is aggregated as a list (multiple events per project)
    # Sort so .first() is deterministic when multiple rows exist per project_id.
    df_funnel = (
        core_project_funnel
        .sort("project_funnel_lead_date", nulls_last=True)
        .group_by("project_id")
        .agg([
            # Take first non-null value for scalar fields
            pl.col("project_funnel_lead_date").first(),
            pl.col("project_funnel_visit_created_date").first(),
            pl.col("project_funnel_visit_created_per_project_date").first(),
            pl.col("project_funnel_visit_status").first(),
            pl.col("project_funnel_visit_realized_at").first(),
            pl.col("project_funnel_visit_realized_per_project_date").first(),
            pl.col("project_funnel_visit_confirmed_at").first(),
            pl.col("project_funnel_visit_confirmed_per_project_date").first(),
            pl.col("project_funnel_loi_date").first(),
            pl.col("project_funnel_loi_per_project_date").first(),
            pl.col("project_funnel_contract_date").first(),
            pl.col("project_funnel_contract_per_project_date").first(),
            pl.col("project_funnel_transaction_date").first(),
            pl.col("project_funnel_transaction_per_project_date").first(),
            pl.col("project_funnel_flow").first(),
            # Aggregate events as unique list (native List type for Parquet efficiency)
            pl.col("project_funnel_event").drop_nulls().unique().sort().alias("project_funnel_events"),
        ])
    )

    # =========================================================================
    # 2. LEFT JOINS: projects <- leads <- profiles <- funnel
    # =========================================================================

    df_joined = (
        stg_s2p_projects
        .join(df_leads, on="lead_id", how="left")
        .join(df_profiles, on="user_id", how="left")
        .join(df_funnel, on="project_id", how="left")
    )

    # =========================================================================
    # 3. FINAL SELECT (explicit field order for flexibility)
    # =========================================================================

    df_result = df_joined.select([
        # ---------------------------------------------------------------------
        # STG Projects: Core identifiers
        # ---------------------------------------------------------------------
        pl.col("project_id"),
        pl.col("project_name"),
        pl.col("lead_id"),

        # ---------------------------------------------------------------------
        # STG Leads: Lead attributes (recovered from funnel)
        # ---------------------------------------------------------------------
        pl.col("spot_sector"),
        pl.col("project_state_ids"),
        pl.col("lead_max_type"),
        pl.col("lead_campaign_type"),
        pl.col("lead_profiling_completed_at"),
        pl.col("lead_project_funnel_relevant_id"),

        # ---------------------------------------------------------------------
        # STG Projects: User identifier
        # ---------------------------------------------------------------------
        pl.col("user_id"),

        # ---------------------------------------------------------------------
        # STG User Profiles: User attributes (recovered from funnel)
        # ---------------------------------------------------------------------
        pl.col("user_industria_role_id"),
        pl.col("user_industria_role"),

        # ---------------------------------------------------------------------
        # STG Projects: Spot sector
        # ---------------------------------------------------------------------
        pl.col("spot_sector_id"),

        # ---------------------------------------------------------------------
        # STG Projects: Space requirements
        # ---------------------------------------------------------------------
        pl.col("project_min_square_space"),
        pl.col("project_max_square_space"),

        # ---------------------------------------------------------------------
        # STG Projects: Currency
        # ---------------------------------------------------------------------
        pl.col("project_currency_id"),
        pl.col("spot_currency_sale_id"),
        pl.col("spot_currency_sale"),

        # ---------------------------------------------------------------------
        # STG Projects: Pricing
        # ---------------------------------------------------------------------
        pl.col("spot_price_sqm"),
        pl.col("project_min_rent_price"),
        pl.col("project_max_rent_price"),
        pl.col("project_min_sale_price"),
        pl.col("project_max_sale_price"),
        pl.col("spot_price_sqm_mxn_sale_min"),
        pl.col("spot_price_sqm_mxn_sale_max"),
        pl.col("spot_price_sqm_mxn_rent_min"),
        pl.col("spot_price_sqm_mxn_rent_max"),

        # ---------------------------------------------------------------------
        # STG Projects: Terms
        # ---------------------------------------------------------------------
        pl.col("project_rent_months"),
        pl.col("project_commission"),

        # ---------------------------------------------------------------------
        # STG Projects: Stage and status
        # ---------------------------------------------------------------------
        pl.col("project_last_spot_stage_id"),
        pl.col("project_last_spot_stage"),
        pl.col("project_enable_id"),
        pl.col("project_enable"),
        pl.col("project_disable_reason_id"),
        pl.col("project_disable_reason"),

        # ---------------------------------------------------------------------
        # STG Projects: Funnel relevance flags
        # ---------------------------------------------------------------------
        pl.col("project_funnel_relevant_id"),
        pl.col("project_funnel_relevant"),

        # ---------------------------------------------------------------------
        # STG Projects: Won date
        # ---------------------------------------------------------------------
        pl.col("project_won_date"),

        # =====================================================================
        # FUNNEL FIELDS (inserted after project_won_date)
        # =====================================================================

        # Funnel: Lead date
        pl.col("project_funnel_lead_date"),

        # Funnel: Visit created
        pl.col("project_funnel_visit_created_date"),
        pl.col("project_funnel_visit_created_per_project_date"),
        pl.col("project_funnel_visit_status"),

        # Funnel: Visit realized
        pl.col("project_funnel_visit_realized_at"),
        pl.col("project_funnel_visit_realized_per_project_date"),

        # Funnel: Visit confirmed
        pl.col("project_funnel_visit_confirmed_at"),
        pl.col("project_funnel_visit_confirmed_per_project_date"),

        # Funnel: LOI
        pl.col("project_funnel_loi_date"),
        pl.col("project_funnel_loi_per_project_date"),

        # Funnel: Contract
        pl.col("project_funnel_contract_date"),
        pl.col("project_funnel_contract_per_project_date"),

        # Funnel: Transaction (won)
        pl.col("project_funnel_transaction_date"),
        pl.col("project_funnel_transaction_per_project_date"),

        # Funnel: Flow attribution
        pl.col("project_funnel_flow"),
        pl.col("project_funnel_events"),  # List of unique events per project

        # =====================================================================
        # STG Projects: Remaining fields (timestamps)
        # =====================================================================
        pl.col("project_created_at"),
        pl.col("project_created_date"),
        pl.col("project_updated_at"),
        pl.col("project_updated_date"),

        # =====================================================================
        # Audit fields
        # =====================================================================
        pl.lit(datetime.now()).alias("aud_inserted_at"),
        pl.lit(date.today()).alias("aud_inserted_date"),
        pl.lit(datetime.now()).alias("aud_updated_at"),
        pl.lit(date.today()).alias("aud_updated_date"),
        pl.lit("lk_projects").alias("aud_job"),
    ])

    return df_result


@dg.asset(
    partitions_def=daily_partitions,
    group_name="gold",
    description=(
        "Gold: Final lk_projects table ready for consumption. "
        "Combines stg_s2p_projects with leads, profiles, and funnel metrics."
    ),
)
def gold_lk_projects(
    context: dg.AssetExecutionContext,
    stg_s2p_projects: pl.DataFrame,
    stg_s2p_leads: pl.DataFrame,
    stg_s2p_user_profiles: pl.DataFrame,
    core_project_funnel: pl.DataFrame,
) -> pl.DataFrame:
    """
    Creates the gold projects table with full enrichment.
    
    This asset joins:
    - stg_s2p_projects: Base project data with transformations
    - stg_s2p_leads: Lead attributes (sector, type, campaign, relevance)
    - stg_s2p_user_profiles: User industry role
    - core_project_funnel: Funnel metrics (visits, LOI, contracts, transactions)
    
    The result contains all project fields plus lead, user, and funnel data,
    enabling comprehensive analysis of project lifecycle and conversion rates.
    """
    def body() -> pl.DataFrame:
        df_projects = _transform_lk_projects(
            stg_s2p_projects=stg_s2p_projects,
            stg_s2p_leads=stg_s2p_leads,
            stg_s2p_user_profiles=stg_s2p_user_profiles,
            core_project_funnel=core_project_funnel,
        )

        context.log.info(
            f"gold_lk_projects: {df_projects.height} rows for partition {context.partition_key}"
        )
        return df_projects

    yield from iter_job_wrapped_compute(context, body)
