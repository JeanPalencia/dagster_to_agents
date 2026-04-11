# defs/data_lakehouse/silver/stg/stg_s2p_alg_activity_log_projects_new.py
"""
Silver STG: Complete transformation of raw_s2p_activity_log_projects_new using reusable functions.

This asset maintains original bronze column names and only adds calculated columns when necessary.
"""
import polars as pl
from datetime import date

from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset

# ============ Project event constants ============

# LOI (Letter of Intent) events
LOI_EVENTS = [
    "projectSpotIntentionLetter",
    "projectSpotDocumentation",
    "projectSpotContract",
    "projectSpotWon",
]

# Contract events (subset of LOI_EVENTS)
CONTRACT_EVENTS = [
    "projectSpotContract",
    "projectSpotWon",
]


def _transform_s2p_alg_activity_log_projects(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms raw_s2p_activity_log_projects_new maintaining original column names.
    Only adds calculated columns when necessary.
    """

    # Date cutoff for relevance
    fecha_corte_inicio = date(2024, 1, 1)
    fecha_corte_fin = date.today()

    # -------------------------
    # project_id and spot_id: extract from JSON (all records are project-related)
    # -------------------------
    project_id_expr = (
        pl.col("properties")
        .str.json_path_match("$.project_id")
        .cast(pl.Int64)
        .alias("project_id")
    )

    spot_id_expr = (
        pl.col("properties")
        .str.json_path_match("$.spot_id")
        .cast(pl.Int64)
        .alias("spot_id")
    )

    # -------------------------
    # LOI (Letter of Intent) - using case_when
    # -------------------------
    is_loi_event = pl.col("event").is_in(LOI_EVENTS)

    alg_loi_event_id_expr = (
        pl.when(is_loi_event)
        .then(1)
        .otherwise(0)
        .alias("alg_loi_event_id")
    )

    alg_loi_event_expr = (
        pl.when(is_loi_event)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("alg_loi_event")
    )

    # -------------------------
    # Contract
    # -------------------------
    is_contract_event = pl.col("event").is_in(CONTRACT_EVENTS)

    alg_contract_event_id_expr = (
        pl.when(is_contract_event)
        .then(1)
        .otherwise(0)
        .alias("alg_contract_event_id")
    )

    alg_contract_event_expr = (
        pl.when(is_contract_event)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("alg_contract_event")
    )

    # -------------------------
    # Relevance (date filter + event type)
    # -------------------------
    created_date = pl.col("created_at").dt.date()
    is_in_date_range = (created_date >= fecha_corte_inicio) & (created_date <= fecha_corte_fin)

    # LOI relevant for project funnel
    is_loi_relevant = is_in_date_range & is_loi_event

    alg_project_funnel_loi_relevant_id_expr = (
        pl.when(is_loi_relevant)
        .then(1)
        .otherwise(0)
        .alias("alg_project_funnel_loi_relevant_id")
    )

    alg_project_funnel_loi_relevant_expr = (
        pl.when(is_loi_relevant)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("alg_project_funnel_loi_relevant")
    )

    # Contract relevant for project funnel
    is_contract_relevant = is_in_date_range & is_contract_event

    alg_project_funnel_contract_relevant_id_expr = (
        pl.when(is_contract_relevant)
        .then(1)
        .otherwise(0)
        .alias("alg_project_funnel_contract_relevant_id")
    )

    alg_project_funnel_contract_relevant_expr = (
        pl.when(is_contract_relevant)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("alg_project_funnel_contract_relevant")
    )

    # -------------------------
    # Maintain all original columns and add calculated ones
    # -------------------------
    return df.with_columns([
        # Extracted columns from JSON
        project_id_expr,
        spot_id_expr,

        # Rename key column
        pl.col("id").alias("alg_id"),
        pl.col("event").alias("alg_event"),

        # LOI columns
        alg_loi_event_id_expr,
        alg_loi_event_expr,

        # Contract columns
        alg_contract_event_id_expr,
        alg_contract_event_expr,

        # LOI relevance for project funnel
        alg_project_funnel_loi_relevant_id_expr,
        alg_project_funnel_loi_relevant_expr,

        # Contract relevance for project funnel
        alg_project_funnel_contract_relevant_id_expr,
        alg_project_funnel_contract_relevant_expr,

        # Date columns (derived from timestamps)
        pl.col("created_at").dt.date().alias("alg_created_date"),
        pl.col("updated_at").dt.date().alias("alg_updated_date"),
    ])


stg_s2p_alg_activity_log_projects_new = make_silver_stg_asset(
    "raw_s2p_activity_log_projects_new",
    _transform_s2p_alg_activity_log_projects,
    silver_asset_name="stg_s2p_alg_activity_log_projects_new",
    description=(
        "Silver STG: Complete transformation of activity_log_projects from Spot2 Platform "
        "using reusable functions. Maintains original column names and adds calculated fields."
    ),
)
