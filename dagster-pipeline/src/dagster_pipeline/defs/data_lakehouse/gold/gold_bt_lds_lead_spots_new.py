# defs/data_lakehouse/gold/gold_bt_lds_lead_spots_new.py
"""
Gold layer: BT_LDS_LEAD_SPOTS (NEW pipeline).

Independent fork of the legacy _transform_bt_lds_lead_spots with channel
attribution logic integrated into Block 3 (Spot Added).

Reads _new silver tables from S3, builds the 10-block event log, enriches
Block 3 with channel attribution and recommendation flags, writes to S3
and loads to bt_lds_lead_spots in Geospot.

Channel attribution (Block 3 only):
  - lds_channel_attribution_id/lds_channel_attribution: 1=Algorithm, 2=Chatbot, 3=Manual
  - lds_algorithm_evidence_at: updated_at from recommendation_projects white_list
  - lds_chatbot_evidence_at: created_at from conversation_events spot_confirmation
  - is_recommended_algorithm_id/is_recommended_algorithm: any list in recommendation_projects
  - is_recommended_chatbot_id/is_recommended_chatbot: any list in recommendation_chatbot
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

CHANNEL_ATTRIBUTION_COLS = (
    "lds_channel_attribution_id", "lds_channel_attribution",
    "lds_algorithm_evidence_at", "lds_chatbot_evidence_at",
    "is_recommended_algorithm_id", "is_recommended_algorithm",
    "is_recommended_chatbot_id", "is_recommended_chatbot",
)

BT_LDS_LEAD_SPOTS_COLUMN_ORDER = (
    "lead_id", "project_id", "spot_id", "appointment_id", "apd_id", "alg_id",
    "appointment_last_date_status_id", "appointment_last_status_at", "apd_status_id", "alg_event",
    "lds_event_id", "lds_event", "lds_event_at",
    "lead_max_type_id", "lead_max_type", "spot_sector_id", "spot_sector",
    "user_industria_role_id", "user_industria_role",
    "lds_cohort_type_id", "lds_cohort_type", "lds_cohort_at",
    *CHANNEL_ATTRIBUTION_COLS,
    "aud_inserted_at", "aud_inserted_date", "aud_updated_at", "aud_updated_date", "aud_job",
)


def _null_channel_cols() -> list[pl.Expr]:
    """Returns null expressions for the 8 channel attribution columns."""
    return [
        pl.lit(None).cast(pl.Int16).alias("lds_channel_attribution_id"),
        pl.lit(None).cast(pl.Utf8).alias("lds_channel_attribution"),
        pl.lit(None).cast(pl.Datetime).alias("lds_algorithm_evidence_at"),
        pl.lit(None).cast(pl.Datetime).alias("lds_chatbot_evidence_at"),
        pl.lit(None).cast(pl.Int16).alias("is_recommended_algorithm_id"),
        pl.lit(None).cast(pl.Utf8).alias("is_recommended_algorithm"),
        pl.lit(None).cast(pl.Int16).alias("is_recommended_chatbot_id"),
        pl.lit(None).cast(pl.Utf8).alias("is_recommended_chatbot"),
    ]


def _enrich_spot_added_with_channel_attribution(
    df_spot_events: pl.DataFrame,
    stg_rec_projects: pl.DataFrame,
    stg_rec_chatbot: pl.DataFrame,
    stg_conv_events: pl.DataFrame,
) -> pl.DataFrame:
    """
    Enriches Block 3 (Spot Added) events with channel attribution and
    recommendation indicators.

    Args:
        df_spot_events: Block 3 DataFrame with (lead_id, project_id, spot_id, ...)
        stg_rec_projects: Exploded recommendation_projects (project_id, spot_id, list_type, updated_at)
        stg_rec_chatbot: Exploded recommendation_chatbot (recommendation_chatbot_id, spot_id, list_type, updated_at)
        stg_conv_events: Conversation events (lead_id, spot_id, created_at)
    """
    # --- Algorithm evidence: white_list entries from recommendation_projects ---
    algo_evidence = (
        stg_rec_projects
        .filter(pl.col("list_type") == "white_list")
        .group_by(["project_id", "spot_id"])
        .agg(pl.col("updated_at").min().alias("lds_algorithm_evidence_at"))
    )

    df_spot_events = df_spot_events.join(
        algo_evidence,
        on=["project_id", "spot_id"],
        how="left",
    )

    # --- Chatbot evidence: spot_confirmation from conversation_events ---
    if stg_conv_events.height > 0 and "lead_id" in stg_conv_events.columns:
        chatbot_evidence = (
            stg_conv_events
            .group_by(["lead_id", "spot_id"])
            .agg(pl.col("created_at").min().alias("lds_chatbot_evidence_at"))
        )
        df_spot_events = df_spot_events.join(
            chatbot_evidence,
            on=["lead_id", "spot_id"],
            how="left",
        )
    else:
        df_spot_events = df_spot_events.with_columns(
            pl.lit(None).cast(pl.Datetime).alias("lds_chatbot_evidence_at")
        )

    # Normalize datetime types for comparison (sources may differ in precision/tz)
    for ts_col in ("lds_algorithm_evidence_at", "lds_chatbot_evidence_at"):
        if ts_col in df_spot_events.columns:
            dt = df_spot_events[ts_col].dtype
            if hasattr(dt, "time_zone") and dt.time_zone is not None:
                df_spot_events = df_spot_events.with_columns(
                    pl.col(ts_col).dt.replace_time_zone(None).alias(ts_col)
                )
            if hasattr(dt, "time_unit") and dt.time_unit != "us":
                df_spot_events = df_spot_events.with_columns(
                    pl.col(ts_col).cast(pl.Datetime("us")).alias(ts_col)
                )

    # --- Channel attribution logic ---
    has_algo = pl.col("lds_algorithm_evidence_at").is_not_null()
    has_chat = pl.col("lds_chatbot_evidence_at").is_not_null()
    algo_first = pl.col("lds_algorithm_evidence_at") <= pl.col("lds_chatbot_evidence_at")

    df_spot_events = df_spot_events.with_columns([
        pl.when(has_algo & has_chat)
            .then(pl.when(algo_first).then(1).otherwise(2))
            .when(has_algo)
            .then(1)
            .when(has_chat)
            .then(2)
            .otherwise(3)
            .cast(pl.Int16)
            .alias("lds_channel_attribution_id"),

        pl.when(has_algo & has_chat)
            .then(pl.when(algo_first).then(pl.lit("Algorithm")).otherwise(pl.lit("Chatbot")))
            .when(has_algo)
            .then(pl.lit("Algorithm"))
            .when(has_chat)
            .then(pl.lit("Chatbot"))
            .otherwise(pl.lit("Manual"))
            .alias("lds_channel_attribution"),
    ])

    # --- is_recommended_algorithm: any list in recommendation_projects ---
    algo_any = (
        stg_rec_projects
        .select(["project_id", "spot_id"])
        .unique()
        .with_columns(pl.lit(1).cast(pl.Int16).alias("is_recommended_algorithm_id"))
    )
    df_spot_events = df_spot_events.join(
        algo_any,
        on=["project_id", "spot_id"],
        how="left",
    )
    df_spot_events = df_spot_events.with_columns([
        pl.col("is_recommended_algorithm_id").fill_null(0).alias("is_recommended_algorithm_id"),
        pl.when(pl.col("is_recommended_algorithm_id").fill_null(0) == 1)
            .then(pl.lit("Yes"))
            .otherwise(pl.lit("No"))
            .alias("is_recommended_algorithm"),
    ])

    # --- is_recommended_chatbot: any list in recommendation_chatbot, joined via lead_id ---
    if stg_rec_chatbot.height > 0:
        chatbot_any = (
            stg_rec_chatbot
            .select("spot_id")
            .unique()
            .with_columns(pl.lit(1).cast(pl.Int16).alias("is_recommended_chatbot_id"))
        )
        df_spot_events = df_spot_events.join(
            chatbot_any,
            on="spot_id",
            how="left",
        )
    else:
        df_spot_events = df_spot_events.with_columns(
            pl.lit(None).cast(pl.Int16).alias("is_recommended_chatbot_id")
        )

    df_spot_events = df_spot_events.with_columns([
        pl.col("is_recommended_chatbot_id").fill_null(0).alias("is_recommended_chatbot_id"),
        pl.when(pl.col("is_recommended_chatbot_id").fill_null(0) == 1)
            .then(pl.lit("Yes"))
            .otherwise(pl.lit("No"))
            .alias("is_recommended_chatbot"),
    ])

    return df_spot_events


def _transform_bt_lds_lead_spots_new(
    stg_s2p_leads: pl.DataFrame,
    stg_s2p_projects: pl.DataFrame,
    stg_s2p_prs_project_spots: pl.DataFrame,
    stg_s2p_appointments: pl.DataFrame,
    stg_s2p_apd_appointment_dates: pl.DataFrame,
    stg_s2p_alg_activity_log_projects: pl.DataFrame,
    stg_s2p_user_profiles: pl.DataFrame,
    stg_rec_projects: pl.DataFrame,
    stg_rec_chatbot: pl.DataFrame,
    stg_conv_events: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds the gold_bt_lds_lead_spots event log table with channel attribution.

    Fork of _transform_bt_lds_lead_spots (legacy) with:
    - Channel attribution integrated in Block 3
    - No audit fields (added separately by caller)
    - Independent from legacy module
    """

    # =========================================================================
    # BLOCK 1: LEAD CREATED EVENTS
    # =========================================================================
    df_lead_events = (
        stg_s2p_leads
        .filter(pl.col("lead_lds_relevant_id") == 1)
        .select([
            pl.col("lead_id"),
            pl.lit(None).cast(pl.Int64).alias("project_id"),
            pl.lit(None).cast(pl.Int64).alias("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(1).alias("lds_event_id"),
            pl.lit("Lead Created").alias("lds_event"),
            pl.col("lead_type_datetime_start").alias("lds_event_at"),
            *_null_channel_cols(),
        ])
    )

    # =========================================================================
    # BLOCK 2: PROJECT CREATED EVENTS
    # =========================================================================
    relevant_lead_ids = (
        stg_s2p_leads
        .filter(pl.col("lead_lds_relevant_id") == 1)
        .select("lead_id")
    )

    df_project_events = (
        stg_s2p_projects
        .join(relevant_lead_ids, on="lead_id", how="inner")
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.lit(None).cast(pl.Int64).alias("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(2).alias("lds_event_id"),
            pl.lit("Project Created").alias("lds_event"),
            pl.col("project_created_at").alias("lds_event_at"),
            *_null_channel_cols(),
        ])
    )

    # =========================================================================
    # BASE: RELEVANT SPOTS (used for blocks 3, 4, etc.)
    # =========================================================================
    relevant_projects = (
        stg_s2p_projects
        .join(relevant_lead_ids, on="lead_id", how="inner")
        .select(["project_id", "lead_id"])
    )

    df_relevant_spots = (
        stg_s2p_prs_project_spots
        .join(relevant_projects, on="project_id", how="inner")
    )

    # =========================================================================
    # BLOCK 3: SPOT ADDED EVENTS (with channel attribution)
    # =========================================================================
    df_spot_events = (
        df_relevant_spots
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(3).alias("lds_event_id"),
            pl.lit("Spot Added").alias("lds_event"),
            pl.col("project_spot_created_at").alias("lds_event_at"),
        ])
    )

    df_spot_events = _enrich_spot_added_with_channel_attribution(
        df_spot_events,
        stg_rec_projects,
        stg_rec_chatbot,
        stg_conv_events,
    )
    # Reorder columns to match the other blocks (base cols + channel cols in canonical order)
    base_cols = [c for c in df_spot_events.columns if c not in CHANNEL_ATTRIBUTION_COLS]
    df_spot_events = df_spot_events.select(base_cols + list(CHANNEL_ATTRIBUTION_COLS))

    # =========================================================================
    # BLOCK 4: VISIT CREATED EVENTS
    # =========================================================================
    df_last_status_date = (
        stg_s2p_appointments
        .join(stg_s2p_apd_appointment_dates, on="appointment_id", how="inner")
        .filter(
            pl.col("apd_status_id") == pl.col("appointment_last_date_status_id")
        )
        .with_columns(
            pl.min_horizontal("appointment_updated_at", "apd_updated_at")
              .alias("status_updated_at")
        )
        .group_by("appointment_id")
        .agg(
            pl.col("status_updated_at").max().alias("appointment_last_status_at")
        )
    )

    df_visits_base = (
        df_relevant_spots
        .select(["lead_id", "project_id", "spot_id"])
        .join(
            stg_s2p_appointments,
            on=["project_id", "spot_id"],
            how="inner"
        )
        .join(
            df_last_status_date,
            on="appointment_id",
            how="left"
        )
    )

    df_visit_created_events = (
        df_visits_base
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.col("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.col("appointment_last_date_status_id"),
            pl.col("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(4).alias("lds_event_id"),
            pl.lit("Visit Created").alias("lds_event"),
            pl.col("appointment_created_at").alias("lds_event_at"),
            *_null_channel_cols(),
        ])
    )

    # =========================================================================
    # BLOCK 5: VISIT CONFIRMED EVENTS
    # =========================================================================
    confirmed_statuses = [3, 4, 6, 7, 8, 9, 18, 19]

    df_visit_confirmed_events = (
        df_visits_base
        .join(
            stg_s2p_apd_appointment_dates,
            on="appointment_id",
            how="inner"
        )
        .filter(pl.col("apd_status_id").is_in(confirmed_statuses))
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.col("appointment_id"),
            pl.col("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.col("appointment_last_date_status_id"),
            pl.col("appointment_last_status_at"),
            pl.col("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(5).alias("lds_event_id"),
            pl.lit("Visit Confirmed").alias("lds_event"),
            pl.col("apd_at").alias("lds_event_at"),
            *_null_channel_cols(),
        ])
    )

    # =========================================================================
    # BLOCK 6: VISIT REALIZED EVENTS
    # =========================================================================
    df_visit_realized_events = (
        df_visits_base
        .join(
            stg_s2p_apd_appointment_dates,
            on="appointment_id",
            how="inner"
        )
        .filter(pl.col("apd_status_id") == 4)
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.col("appointment_id"),
            pl.col("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.col("appointment_last_date_status_id"),
            pl.col("appointment_last_status_at"),
            pl.col("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(6).alias("lds_event_id"),
            pl.lit("Visit Realized").alias("lds_event"),
            pl.col("apd_at").alias("lds_event_at"),
            *_null_channel_cols(),
        ])
    )

    # =========================================================================
    # BLOCK 7: LOI (LETTER OF INTENT) EVENTS
    # =========================================================================
    df_loi_events = (
        df_relevant_spots
        .select(["lead_id", "project_id", "spot_id"])
        .unique()
        .join(
            stg_s2p_alg_activity_log_projects.filter(pl.col("alg_loi_event_id") == 1),
            on=["project_id", "spot_id"],
            how="inner"
        )
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.col("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.col("alg_event").cast(pl.Utf8),
            pl.lit(7).alias("lds_event_id"),
            pl.lit("LOI").alias("lds_event"),
            pl.col("alg_created_at").alias("lds_event_at"),
            *_null_channel_cols(),
        ])
    )

    # =========================================================================
    # BLOCK 8: CONTRACT EVENTS
    # =========================================================================
    df_contract_events = (
        df_relevant_spots
        .select(["lead_id", "project_id", "spot_id"])
        .unique()
        .join(
            stg_s2p_alg_activity_log_projects.filter(pl.col("alg_contract_event_id") == 1),
            on=["project_id", "spot_id"],
            how="inner"
        )
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.col("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.col("alg_event").cast(pl.Utf8),
            pl.lit(8).alias("lds_event_id"),
            pl.lit("Contract").alias("lds_event"),
            pl.col("alg_created_at").alias("lds_event_at"),
            *_null_channel_cols(),
        ])
    )

    # =========================================================================
    # BLOCK 9: SPOT WON EVENTS
    # =========================================================================
    df_spot_won_events = (
        df_relevant_spots
        .select(["lead_id", "project_id", "spot_id"])
        .unique()
        .join(
            stg_s2p_alg_activity_log_projects.filter(pl.col("alg_event") == "projectSpotWon"),
            on=["project_id", "spot_id"],
            how="inner"
        )
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.col("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.col("alg_event").cast(pl.Utf8),
            pl.lit(9).alias("lds_event_id"),
            pl.lit("Spot Won").alias("lds_event"),
            pl.col("alg_created_at").alias("lds_event_at"),
            *_null_channel_cols(),
        ])
    )

    # =========================================================================
    # BLOCK 10: TRANSACTION (PROJECT LEVEL)
    # =========================================================================
    df_transaction_events = (
        df_relevant_spots
        .select(["lead_id", "project_id"])
        .unique()
        .join(
            stg_s2p_projects.filter(pl.col("project_won_date").is_not_null()),
            on="project_id",
            how="inner"
        )
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.lit(None).cast(pl.Int64).alias("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(10).alias("lds_event_id"),
            pl.lit("Transaction").alias("lds_event"),
            pl.col("project_won_date").cast(pl.Datetime).alias("lds_event_at"),
            *_null_channel_cols(),
        ])
    )

    # =========================================================================
    # UNION ALL EVENTS
    # =========================================================================
    df_result = pl.concat([
        df_lead_events,
        df_project_events,
        df_spot_events,
        df_visit_created_events,
        df_visit_confirmed_events,
        df_visit_realized_events,
        df_loi_events,
        df_contract_events,
        df_spot_won_events,
        df_transaction_events,
    ])

    # =========================================================================
    # ADD SEGMENTATION FIELDS
    # =========================================================================
    df_lead_segments = (
        stg_s2p_leads
        .select([
            "lead_id",
            "user_id",
            "lead_max_type_id",
            "lead_max_type",
            "spot_sector_id",
            "spot_sector",
            "lead_type_datetime_start",
        ])
    )

    df_user_segments = (
        stg_s2p_user_profiles
        .select([
            "user_id",
            "user_industria_role_id",
            "user_industria_role",
        ])
    )

    df_lead_with_profile = (
        df_lead_segments
        .join(df_user_segments, on="user_id", how="left")
        .drop("user_id")
    )

    df_result = df_result.join(
        df_lead_with_profile,
        on="lead_id",
        how="left"
    )

    # =========================================================================
    # ADD PROJECT DATES FOR COHORT CALCULATION
    # =========================================================================
    df_project_dates = (
        stg_s2p_projects
        .select(["project_id", "project_created_date"])
    )

    df_result = df_result.join(
        df_project_dates,
        on="project_id",
        how="left"
    )

    # =========================================================================
    # CALCULATE COHORT FIELDS
    # =========================================================================
    lead_date = pl.col("lead_type_datetime_start").dt.date()
    project_date = pl.col("project_created_date")
    days_diff = (project_date - lead_date).dt.total_days()

    is_recent = (
        pl.col("project_id").is_null() |
        (days_diff <= 30)
    )

    df_result = df_result.with_columns([
        pl.when(is_recent).then(1).otherwise(2).alias("lds_cohort_type_id"),
        pl.when(is_recent)
            .then(pl.lit("New"))
            .otherwise(pl.lit("Reactivated"))
            .alias("lds_cohort_type"),
        pl.when(is_recent)
            .then(lead_date)
            .otherwise(project_date)
            .alias("lds_cohort_at"),
    ])

    df_result = df_result.drop(["lead_type_datetime_start", "project_created_date"])

    return df_result


def _normalize_silvers_for_lds(
    stg_s2p_clients_new: pl.DataFrame,
    stg_s2p_projects_new: pl.DataFrame,
    stg_s2p_prs_project_spots_new: pl.DataFrame,
    stg_s2p_appointments_new: pl.DataFrame,
    stg_s2p_apd_appointment_dates_new: pl.DataFrame,
    stg_s2p_alg_activity_log_projects_new: pl.DataFrame,
    stg_s2p_profiles_new: pl.DataFrame,
) -> tuple:
    """
    Normalize _new silver DataFrames to the column names expected by the transform.
    """
    stg_s2p_leads = stg_s2p_clients_new.select([
        pl.col("id").alias("lead_id"),
        pl.col("lds_relevant_id").alias("lead_lds_relevant_id"),
        pl.col("lead_type_datetime_start"),
        pl.col("user_id"),
        pl.col("lead_max_type_id"),
        pl.col("lead_max_type"),
        pl.col("spot_sector_id"),
        pl.col("spot_type_text").alias("spot_sector"),
    ])

    stg_s2p_projects = stg_s2p_projects_new.select([
        pl.col("lead_id"),
        pl.col("project_id"),
        pl.col("project_created_at"),
        pl.col("project_created_date"),
        pl.col("project_won_date"),
    ])

    stg_s2p_prs_project_spots = stg_s2p_prs_project_spots_new

    stg_s2p_appointments = stg_s2p_appointments_new
    stg_s2p_apd_appointment_dates = stg_s2p_apd_appointment_dates_new

    alg = stg_s2p_alg_activity_log_projects_new
    if "alg_created_at" not in alg.columns and "created_at" in alg.columns:
        alg = alg.with_columns(pl.col("created_at").alias("alg_created_at"))
    stg_s2p_alg_activity_log_projects = alg

    stg_s2p_user_profiles = stg_s2p_profiles_new

    return (
        stg_s2p_leads,
        stg_s2p_projects,
        stg_s2p_prs_project_spots,
        stg_s2p_appointments,
        stg_s2p_apd_appointment_dates,
        stg_s2p_alg_activity_log_projects,
        stg_s2p_user_profiles,
    )


def _gold_bt_lds_lead_spots_new_impl(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """
    Builds gold_bt_lds_lead_spots from _new silver tables with channel attribution.
    """
    stale_tables = []

    def read_and_track_stale(name: str):
        df, meta = read_silver_from_s3(name, context)
        if meta.get("is_stale"):
            stale_tables.append({
                "table_name": name,
                "expected_date": context.partition_key,
                "available_date": meta.get("file_date", ""),
                "layer": "silver",
                "file_path": meta.get("file_path", ""),
            })
        return df

    def read_silver_safe(name: str):
        """Read silver from S3, return empty DataFrame if not found."""
        try:
            return read_and_track_stale(name)
        except (ValueError, FileNotFoundError):
            context.log.warning(f"Silver table {name} not found in S3 - using empty DataFrame")
            return pl.DataFrame()

    stg_s2p_clients_new = read_and_track_stale("stg_s2p_clients_new")
    stg_s2p_projects_new = read_and_track_stale("stg_s2p_projects_new")
    stg_s2p_prs_project_spots_new = read_and_track_stale("stg_s2p_prs_project_spots_new")
    stg_s2p_appointments_new = read_and_track_stale("stg_s2p_appointments_new")
    stg_s2p_apd_appointment_dates_new = read_and_track_stale("stg_s2p_apd_appointment_dates_new")
    stg_s2p_alg_activity_log_projects_new = read_and_track_stale("stg_s2p_alg_activity_log_projects_new")
    stg_s2p_profiles_new = read_and_track_stale("stg_s2p_profiles_new")

    stg_rec_projects = read_silver_safe("stg_gs_recommendation_projects_exploded_new")
    stg_rec_chatbot = read_silver_safe("stg_gs_recommendation_chatbot_exploded_new")
    stg_conv_events = read_silver_safe("stg_cb_conversation_events_new")

    (
        stg_s2p_leads,
        stg_s2p_projects,
        stg_s2p_prs_project_spots,
        stg_s2p_appointments,
        stg_s2p_apd_appointment_dates,
        stg_s2p_alg_activity_log_projects,
        stg_s2p_user_profiles,
    ) = _normalize_silvers_for_lds(
        stg_s2p_clients_new,
        stg_s2p_projects_new,
        stg_s2p_prs_project_spots_new,
        stg_s2p_appointments_new,
        stg_s2p_apd_appointment_dates_new,
        stg_s2p_alg_activity_log_projects_new,
        stg_s2p_profiles_new,
    )

    df = _transform_bt_lds_lead_spots_new(
        stg_s2p_leads=stg_s2p_leads,
        stg_s2p_projects=stg_s2p_projects,
        stg_s2p_prs_project_spots=stg_s2p_prs_project_spots,
        stg_s2p_appointments=stg_s2p_appointments,
        stg_s2p_apd_appointment_dates=stg_s2p_apd_appointment_dates,
        stg_s2p_alg_activity_log_projects=stg_s2p_alg_activity_log_projects,
        stg_s2p_user_profiles=stg_s2p_user_profiles,
        stg_rec_projects=stg_rec_projects,
        stg_rec_chatbot=stg_rec_chatbot,
        stg_conv_events=stg_conv_events,
    )
    df = add_audit_fields(df, job_name="bt_lds_lead_spots_new")

    df = df.sort(
        by=[
            "lead_id", "project_id", "spot_id", "lds_event_id", "lds_event_at",
            "apd_id", "appointment_id", "alg_id",
        ],
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
    df_governance = df.select(BT_LDS_LEAD_SPOTS_COLUMN_ORDER)

    context.log.info(
        f"gold_bt_lds_lead_spots_new: {df_governance.height:,} rows, {df_governance.width} columns for partition {context.partition_key}"
    )
    context.log.info(
        f"Governance schema (for bt_lds_lead_spots): columns={list(df_governance.columns)}, "
        f"dtypes={dict(zip(df_governance.columns, [str(t) for t in df_governance.dtypes]))}"
    )

    s3_key = build_gold_s3_key("bt_lds_lead_spots", context.partition_key, file_format="parquet")
    write_polars_to_s3(df_governance, s3_key, context, file_format="parquet")

    context.log.info(
        f"Loading to Geospot: table=bt_lds_lead_spots, s3_key={s3_key}, mode=replace"
    )
    context.log.warning(
        "Geospot load is asynchronous (API returns 200 when the job is queued). "
        "If bt_lds_lead_spots has 0 rows, check: (1) table/schema name in Postgres, "
        "(2) Geospot server logs for COPY/INSERT errors, (3) parquet column names match table."
    )
    try:
        load_to_geospot(s3_key=s3_key, table_name="bt_lds_lead_spots", mode="replace", context=context)
        context.log.info("Geospot load completed for bt_lds_lead_spots")
    except Exception as e:
        context.log.error(f"Geospot load failed: {e}")
        raise

    if stale_tables:
        send_stale_data_notification(
            context,
            stale_tables,
            current_asset_name="gold_bt_lds_lead_spots_new",
            current_layer="gold",
        )

    return df_governance


@dg.asset(
    name="gold_bt_lds_lead_spots_new",
    partitions_def=daily_partitions,
    group_name="gold",
    retry_policy=dg.RetryPolicy(max_retries=2),
    io_manager_key="s3_gold",
    description=(
        "Gold: BT_LDS_LEAD_SPOTS event log (NEW pipeline). "
        "Reads _new silver tables from S3, builds 10-block event log with "
        "channel attribution on Block 3, writes to S3 and loads to GeoSpot."
    ),
)
def gold_bt_lds_lead_spots_new(context):
    def body():
        return _gold_bt_lds_lead_spots_new_impl(context)

    yield from iter_job_wrapped_compute(context, body)
