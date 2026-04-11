# defs/data_lakehouse/silver/core/core_project_funnel.py
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _transform_project_funnel(
    stg_s2p_leads: pl.DataFrame,
    stg_s2p_projects: pl.DataFrame,
    stg_s2p_appointments: pl.DataFrame,
    stg_s2p_apd_appointment_dates: pl.DataFrame,
    stg_s2p_alg_activity_log_projects: pl.DataFrame,
    stg_s2p_leh_lead_event_histories: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds the Lead-Project Funnel by joining multiple STG assets.
    
    This transformation replicates the funnel.sql query logic:
    - Filters relevant leads and projects (created >= 2024-01-01)
    - Aggregates visit data (created, confirmed, realized)
    - Joins LOI and contract events from activity_log
    - Adds flow data from lead_event_histories
    """

    # =========================================================================
    # 1. PREPARE BASE DATAFRAMES
    # =========================================================================

    # Leads: filter relevant only and select minimal columns for funnel
    df_leads = (
        stg_s2p_leads
        .filter(pl.col("lead_project_funnel_relevant_id") == 1)
        .select([
            "lead_id",
            "lead_type_date_start",
        ])
    )

    # Projects: filter relevant only and select funnel columns
    df_projects = (
        stg_s2p_projects
        .filter(pl.col("project_funnel_relevant_id") == 1)
        .select([
            "project_id",
            "lead_id",
            "project_funnel_won_relevant_date",
            "project_created_date",
        ])
    )

    # =========================================================================
    # 2. AGGREGATE VISIT DATA
    # =========================================================================

    # Created visits: first date, last appointment, last status per project
    df_created_visits = (
        stg_s2p_appointments
        .group_by("project_id")
        .agg([
            pl.col("appointment_created_date")
            .min()
            .alias("visit_created_first_date"),

            pl.col("appointment_id")
            .max()
            .alias("visit_last_appointment_id"),

            # Last status_id by project (sorted by created_at, then id)
            pl.col("appointment_last_date_status_id")
            .sort_by(["appointment_created_at", "appointment_id"])
            .last()
            .alias("visit_last_status_id"),

            # Last status text by project
            pl.col("appointment_last_date_status")
            .sort_by(["appointment_created_at", "appointment_id"])
            .last()
            .alias("visit_last_status"),
        ])
    )

    # Confirmed visits: first confirmed datetime per project
    # Status IN (3=AgentConfirmed, 4=Visited, 6=Canceled, 7=ClientConfirmed,
    #            8=ClientCanceled, 9=ContactConfirmed, 18=ContactCanceled, 19=RescheduledCanceled)
    df_confirmed_visits = (
        stg_s2p_appointments
        .join(stg_s2p_apd_appointment_dates, on="appointment_id", how="inner")
        .filter(pl.col("apd_status_id").is_in([3, 4, 6, 7, 8, 9, 18, 19]))
        .group_by("project_id")
        .agg(
            pl.col("apd_at")
            .min()
            .alias("visit_confirmed_first_at")
        )
    )

    # Realized visits: first visited datetime per project (status = 4 = Visited)
    df_realized_visits = (
        stg_s2p_appointments
        .join(stg_s2p_apd_appointment_dates, on="appointment_id", how="inner")
        .filter(pl.col("apd_status_id") == 4)
        .group_by("project_id")
        .agg(
            pl.col("apd_at")
            .min()
            .alias("visit_realized_first_at")
        )
    )

    # =========================================================================
    # 3. AGGREGATE ACTIVITY LOG DATA (LOI & CONTRACT)
    # =========================================================================

    # LOI events: first LOI date per project
    df_alg_loi = (
        stg_s2p_alg_activity_log_projects
        .filter(pl.col("alg_project_funnel_loi_relevant_id") == 1)
        .group_by("project_id")
        .agg(
            pl.col("alg_created_date")
            .min()
            .alias("project_loi_first_date")
        )
    )

    # Contract events: first contract date per project
    df_alg_contract = (
        stg_s2p_alg_activity_log_projects
        .filter(pl.col("alg_project_funnel_contract_relevant_id") == 1)
        .group_by("project_id")
        .agg(
            pl.col("alg_created_date")
            .min()
            .alias("project_contract_first_date")
        )
    )

    # =========================================================================
    # 4. FLOW DATA (first flow event per lead)
    # =========================================================================

    df_data_flow = (
        stg_s2p_leh_lead_event_histories
        .filter(pl.col("lhe_first_project_funnel_event_id") == 1)
        .select([
            "lead_id",
            "lhe_event",
            "lhe_flow",
        ])
    )

    # =========================================================================
    # 5. BUILD FUNNEL: JOIN ALL DATAFRAMES
    # =========================================================================

    # Assemble complete funnel starting from leads
    df_funnel = (
        df_leads
        # Projects
        .join(df_projects, on="lead_id", how="left")
        # Created visits
        .join(df_created_visits, on="project_id", how="left")
        # Realized visits
        .join(df_realized_visits, on="project_id", how="left")
        # Confirmed visits
        .join(df_confirmed_visits, on="project_id", how="left")
        # LOI
        .join(df_alg_loi, on="project_id", how="left")
        # Contracts
        .join(df_alg_contract, on="project_id", how="left")
        # Flow (first flow event)
        .join(df_data_flow, on="lead_id", how="left")
    )

    # =========================================================================
    # 6. DERIVED COLUMNS (*_per_project dates)
    # =========================================================================

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

    # =========================================================================
    # 7. FINAL SELECT WITH RENAMED COLUMNS
    # =========================================================================

    df_result = df_funnel.select([
        # Lead/Project dates
        pl.col("lead_type_date_start").alias("project_funnel_lead_date"),
        pl.col("project_created_date"),

        # Created visits
        pl.col("visit_created_first_date").alias("project_funnel_visit_created_date"),
        pl.col("visit_created_project_date").alias("project_funnel_visit_created_per_project_date"),
        pl.col("visit_last_appointment_id").alias("appointment_id"),
        pl.col("visit_last_status").alias("project_funnel_visit_status"),

        # Realized visits
        pl.col("visit_realized_first_at").alias("project_funnel_visit_realized_at"),
        pl.col("visit_realized_project_date").alias("project_funnel_visit_realized_per_project_date"),

        # Confirmed visits
        pl.col("visit_confirmed_first_at").alias("project_funnel_visit_confirmed_at"),
        pl.col("visit_confirmed_project_date").alias("project_funnel_visit_confirmed_per_project_date"),

        # LOI
        pl.col("project_loi_first_date").alias("project_funnel_loi_date"),
        pl.col("loi_project_date").alias("project_funnel_loi_per_project_date"),

        # Contract
        pl.col("project_contract_first_date").alias("project_funnel_contract_date"),
        pl.col("contract_project_date").alias("project_funnel_contract_per_project_date"),

        # Transaction (won)
        pl.col("project_funnel_won_relevant_date").alias("project_funnel_transaction_date"),
        pl.col("transaction_project_date").alias("project_funnel_transaction_per_project_date"),

        # Lead
        pl.col("lead_id"),

        # Project
        pl.col("project_id"),

        # Flow
        pl.col("lhe_flow").alias("project_funnel_flow"),
        pl.col("lhe_event").alias("project_funnel_event"),
    ])

    # =========================================================================
    # 8. SORT (equivalent to ORDER BY in SQL)
    # =========================================================================

    df_result = df_result.sort(
        by=[
            pl.coalesce(["project_created_date", "project_funnel_lead_date"]),
            "project_id",
            "lead_id",
        ]
    )

    return df_result


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    description=(
        "Silver Core: Lead-Project Funnel combining leads, projects, visits, "
        "LOI, contracts, and flow data for funnel analytics."
    ),
)
def core_project_funnel(
    context: dg.AssetExecutionContext,
    stg_s2p_leads: pl.DataFrame,
    stg_s2p_projects: pl.DataFrame,
    stg_s2p_appointments: pl.DataFrame,
    stg_s2p_apd_appointment_dates: pl.DataFrame,
    stg_s2p_alg_activity_log_projects: pl.DataFrame,
    stg_s2p_leh_lead_event_histories: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds the Project Funnel by joining all STG assets.
    
    This is the main funnel table for analytics, containing:
    - Lead information (relevant leads from 2024-01-01)
    - Project stages and status
    - Visit milestones (created, confirmed, realized)
    - LOI and contract dates
    - Flow attribution (first interaction channel)
    """
    def body() -> pl.DataFrame:
        df_funnel = _transform_project_funnel(
            stg_s2p_leads=stg_s2p_leads,
            stg_s2p_projects=stg_s2p_projects,
            stg_s2p_appointments=stg_s2p_appointments,
            stg_s2p_apd_appointment_dates=stg_s2p_apd_appointment_dates,
            stg_s2p_alg_activity_log_projects=stg_s2p_alg_activity_log_projects,
            stg_s2p_leh_lead_event_histories=stg_s2p_leh_lead_event_histories,
        )

        context.log.info(
            f"core_project_funnel: {df_funnel.height} rows for partition {context.partition_key}"
        )
        return df_funnel

    yield from iter_job_wrapped_compute(context, body)
