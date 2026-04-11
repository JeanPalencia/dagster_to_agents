# defs/data_lakehouse/silver/stg/stg_s2p_appointments_new.py
"""
Silver STG: Complete transformation of raw_s2p_calendar_appointments_new using reusable functions.

This asset maintains original bronze column names and only adds calculated columns when necessary.
"""
import polars as pl

from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset
from dagster_pipeline.defs.data_lakehouse.silver.utils import case_when


def _transform_s2p_appointments(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms raw_s2p_calendar_appointments_new maintaining original column names.
    Only adds calculated columns when necessary.
    """

    # appointment_last_date_status (CASE last_date_status ...)
    # Using case_when for cleaner code
    appointment_last_date_status_expr = case_when(
        "last_date_status",
        {
            1: "NewAppointmentDate",
            2: "ToConfirm",
            3: "AgentConfirmed",
            4: "Visited",
            5: "Rejected",
            6: "Canceled",
            7: "ClientConfirmed",
            8: "ClientCanceled",
            9: "ContactConfirmed",
            10: "ContactRejected",
            11: "OfferNotAvailableRejected",
            12: "ScheduleNotAvailableRejected",
            13: "UnsuitableSpotRejected",
            14: "NoContactAnswerRejected",
            15: "NoClientAnswerRejected",
            16: "ScheduleErrorRejected",
            17: "NotSharedComissionRejected",
            18: "ContactCanceled",
            19: "RescheduledCanceled",
        },
        default="Unknown",
        alias="appointment_last_date_status"
    )

    # Maintain all original columns and add calculated ones
    return df.with_columns([
        # Rename key columns (maintain original names where possible)
        pl.col("id").alias("appointment_id"),
        pl.col("project_requirement_id").alias("project_id"),
        pl.col("last_date_status").alias("appointment_last_date_status_id"),

        # Calculated columns
        appointment_last_date_status_expr,

        # Timestamp columns (keep for sorting in funnel)
        pl.col("created_at").alias("appointment_created_at"),
        pl.col("updated_at").alias("appointment_updated_at"),
        pl.col("deleted_at").alias("appointment_deleted_at"),

        # Date columns (derived from timestamps)
        pl.col("created_at").dt.date().alias("appointment_created_date"),
        pl.col("updated_at").dt.date().alias("appointment_updated_date"),
        pl.col("deleted_at").dt.date().alias("appointment_deleted_date"),
    ])


stg_s2p_appointments_new = make_silver_stg_asset(
    "raw_s2p_calendar_appointments_new",
    _transform_s2p_appointments,
    silver_asset_name="stg_s2p_appointments_new",
    description=(
        "Silver STG: Complete transformation of calendar_appointments from Spot2 Platform "
        "using reusable functions. Maintains original column names and adds calculated fields."
    ),
    partitioned=False,
)
