# defs/data_lakehouse/silver/core/core_project_funnel_new.py
"""
Silver Core: Lead-Project Funnel (NEW VERSION) using new standardized STG tables.

Uses silver.modules.lk_projects for the transform (many intermediate tables).
Combines clients, projects, visits, LOI, contracts, and flow data for funnel analytics.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_silver_s3_key,
    write_polars_to_s3,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.silver.modules.lk_projects import (
    transform_project_funnel_new,
)


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    retry_policy=dg.RetryPolicy(max_retries=2),
    io_manager_key="s3_silver",
    description=(
        "Silver Core: Lead-Project Funnel (NEW VERSION) combining clients, projects, visits, "
        "LOI, contracts, and flow data for funnel analytics. Uses new standardized STG tables. "
        "Receives upstream STG assets as Dagster inputs (no S3 read for silvers)."
    ),
)
def core_project_funnel_new(
    context,
    stg_s2p_clients_new: pl.DataFrame,
    stg_s2p_projects_new: pl.DataFrame,
    stg_s2p_appointments_new: pl.DataFrame,
    stg_s2p_apd_appointment_dates_new: pl.DataFrame,
    stg_s2p_alg_activity_log_projects_new: pl.DataFrame,
    stg_s2p_leh_lead_event_histories_new: pl.DataFrame,
):
    """
    Builds the Project Funnel by joining all STG assets (NEW VERSION).

    Receives the 6 silver STG assets as Dagster upstream inputs (same partition).
    No read from S3 for silvers; dependency order is enforced by the graph.

    This is the main funnel table for analytics, containing:
    - Client information (mapped to lead_id, relevant clients from 2024-01-01)
    - Project stages and status
    - Visit milestones (created, confirmed, realized)
    - LOI and contract dates
    - Flow attribution (first interaction channel)

    Uses new standardized tables:
    - stg_s2p_clients_new (instead of stg_s2p_leads)
    - stg_s2p_projects_new (instead of stg_s2p_projects)
    """
    def body():
        df_funnel = transform_project_funnel_new(
            stg_s2p_clients_new=stg_s2p_clients_new,
            stg_s2p_projects_new=stg_s2p_projects_new,
            stg_s2p_appointments_new=stg_s2p_appointments_new,
            stg_s2p_apd_appointment_dates_new=stg_s2p_apd_appointment_dates_new,
            stg_s2p_alg_activity_log_projects_new=stg_s2p_alg_activity_log_projects_new,
            stg_s2p_leh_lead_event_histories_new=stg_s2p_leh_lead_event_histories_new,
        )

        context.log.info(
            f"core_project_funnel_new: {df_funnel.height} rows for partition {context.partition_key}"
        )

        s3_key = build_silver_s3_key("core_project_funnel_new", file_format="parquet")
        write_polars_to_s3(df_funnel, s3_key, context, file_format="parquet")

        return df_funnel

    yield from iter_job_wrapped_compute(context, body)
