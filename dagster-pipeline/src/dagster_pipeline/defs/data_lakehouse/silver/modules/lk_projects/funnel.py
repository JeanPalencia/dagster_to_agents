"""
Silver lk_projects: funnel transform.

Builds the Lead-Project Funnel by joining clients, projects, appointments,
appointment_dates, activity_log_projects, and lead_event_histories.
Used by core_project_funnel_new.
"""
import polars as pl


def transform_project_funnel_new(
    stg_s2p_clients_new: pl.DataFrame,
    stg_s2p_projects_new: pl.DataFrame,
    stg_s2p_appointments_new: pl.DataFrame,
    stg_s2p_apd_appointment_dates_new: pl.DataFrame,
    stg_s2p_alg_activity_log_projects_new: pl.DataFrame,
    stg_s2p_leh_lead_event_histories_new: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds the Lead-Project Funnel by joining multiple STG assets (NEW VERSION).

    - Filters relevant clients and projects (project_funnel_relevant_id == 1)
    - Aggregates visit data (created, confirmed, realized)
    - Joins LOI and contract events from activity_log
    - Adds flow data from lead_event_histories

    stg_s2p_projects_new must expose: project_id, lead_id, project_funnel_relevant_id,
    project_funnel_won_relevant_date, project_created_date.
    """
    # 1. Prepare base dataframes
    df_clients = (
        stg_s2p_clients_new
        .filter(pl.col("project_funnel_relevant_id") == 1)
        .select([
            pl.col("id").alias("lead_id"),
            "lead_type_date_start",
        ])
    )

    df_projects = (
        stg_s2p_projects_new
        .filter(pl.col("project_funnel_relevant_id") == 1)
        .select([
            "project_id",
            "lead_id",
            "project_funnel_won_relevant_date",
            "project_created_date",
        ])
    )

    # 2. Aggregate visit data
    df_created_visits = (
        stg_s2p_appointments_new
        .group_by("project_id")
        .agg([
            pl.col("appointment_created_date").min().alias("visit_created_first_date"),
            pl.col("appointment_id").max().alias("visit_last_appointment_id"),
            pl.col("appointment_last_date_status_id")
            .sort_by(["appointment_created_at", "appointment_id"])
            .last()
            .alias("visit_last_status_id"),
            pl.col("appointment_last_date_status")
            .sort_by(["appointment_created_at", "appointment_id"])
            .last()
            .alias("visit_last_status"),
        ])
    )

    df_confirmed_visits = (
        stg_s2p_appointments_new
        .join(stg_s2p_apd_appointment_dates_new, on="appointment_id", how="inner")
        .filter(pl.col("apd_status_id").is_in([3, 4, 6, 7, 8, 9, 18, 19]))
        .group_by("project_id")
        .agg(pl.col("apd_at").min().alias("visit_confirmed_first_at"))
    )

    df_realized_visits = (
        stg_s2p_appointments_new
        .join(stg_s2p_apd_appointment_dates_new, on="appointment_id", how="inner")
        .filter(pl.col("apd_status_id") == 4)
        .group_by("project_id")
        .agg(pl.col("apd_at").min().alias("visit_realized_first_at"))
    )

    # 3. Activity log (LOI & contract)
    df_alg_loi = (
        stg_s2p_alg_activity_log_projects_new
        .filter(pl.col("alg_project_funnel_loi_relevant_id") == 1)
        .group_by("project_id")
        .agg(pl.col("alg_created_date").min().alias("project_loi_first_date"))
    )

    df_alg_contract = (
        stg_s2p_alg_activity_log_projects_new
        .filter(pl.col("alg_project_funnel_contract_relevant_id") == 1)
        .group_by("project_id")
        .agg(pl.col("alg_created_date").min().alias("project_contract_first_date"))
    )

    # 4. Flow data
    df_data_flow = (
        stg_s2p_leh_lead_event_histories_new
        .filter(pl.col("lhe_first_project_funnel_event_id") == 1)
        .select(["lead_id", "lhe_event", "lhe_flow"])
    )

    # 5. Join all
    df_funnel = (
        df_clients
        .join(df_projects, on="lead_id", how="left")
        .join(df_created_visits, on="project_id", how="left")
        .join(df_realized_visits, on="project_id", how="left")
        .join(df_confirmed_visits, on="project_id", how="left")
        .join(df_alg_loi, on="project_id", how="left")
        .join(df_alg_contract, on="project_id", how="left")
        .join(df_data_flow, on="lead_id", how="left")
    )

    # 6. Derived *_per_project dates
    df_funnel = df_funnel.with_columns([
        pl.when(pl.col("visit_created_first_date").is_not_null())
        .then(pl.col("project_created_date"))
        .otherwise(None)
        .alias("visit_created_project_date"),
        pl.when(pl.col("visit_realized_first_at").is_not_null())
        .then(pl.col("project_created_date"))
        .otherwise(None)
        .alias("visit_realized_project_date"),
        pl.when(pl.col("visit_confirmed_first_at").is_not_null())
        .then(pl.col("project_created_date"))
        .otherwise(None)
        .alias("visit_confirmed_project_date"),
        pl.when(pl.col("project_loi_first_date").is_not_null())
        .then(pl.col("project_created_date"))
        .otherwise(None)
        .alias("loi_project_date"),
        pl.when(pl.col("project_contract_first_date").is_not_null())
        .then(pl.col("project_created_date"))
        .otherwise(None)
        .alias("contract_project_date"),
        pl.when(pl.col("project_funnel_won_relevant_date").is_not_null())
        .then(pl.col("project_created_date"))
        .otherwise(None)
        .alias("transaction_project_date"),
    ])

    # 7. Final select with renamed columns
    df_result = df_funnel.select([
        pl.col("lead_type_date_start").alias("project_funnel_lead_date"),
        pl.col("project_created_date"),
        pl.col("visit_created_first_date").alias("project_funnel_visit_created_date"),
        pl.col("visit_created_project_date").alias("project_funnel_visit_created_per_project_date"),
        pl.col("visit_last_appointment_id").alias("appointment_id"),
        pl.col("visit_last_status").alias("project_funnel_visit_status"),
        pl.col("visit_realized_first_at").alias("project_funnel_visit_realized_at"),
        pl.col("visit_realized_project_date").alias("project_funnel_visit_realized_per_project_date"),
        pl.col("visit_confirmed_first_at").alias("project_funnel_visit_confirmed_at"),
        pl.col("visit_confirmed_project_date").alias("project_funnel_visit_confirmed_per_project_date"),
        pl.col("project_loi_first_date").alias("project_funnel_loi_date"),
        pl.col("loi_project_date").alias("project_funnel_loi_per_project_date"),
        pl.col("project_contract_first_date").alias("project_funnel_contract_date"),
        pl.col("contract_project_date").alias("project_funnel_contract_per_project_date"),
        pl.col("project_funnel_won_relevant_date").alias("project_funnel_transaction_date"),
        pl.col("transaction_project_date").alias("project_funnel_transaction_per_project_date"),
        pl.col("lead_id"),
        pl.col("project_id"),
        pl.col("lhe_flow").alias("project_funnel_flow"),
        pl.col("lhe_event").alias("project_funnel_event"),
    ])

    df_result = df_result.sort(
        by=[
            pl.coalesce(["project_created_date", "project_funnel_lead_date"]),
            "project_id",
            "lead_id",
        ]
    )

    return df_result
