# defs/data_lakehouse/silver/stg/stg_s2p_appointments.py
import dagster as dg
import polars as pl
from datetime import date  # opcional aquí, pero lo dejamos por consistencia

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _transform_s2p_appointments(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma raw_s2p_calendar_appointments (calendar_appointments en MySQL)
    replicando la query stg_s2p_appointments.
    """

    # appointment_last_date_status (CASE last_date_status ...)
    lds = pl.col("last_date_status")
    appointment_last_date_status_expr = (
        pl.when(lds == 1).then(pl.lit("NewAppointmentDate"))
        .when(lds == 2).then(pl.lit("ToConfirm"))
        .when(lds == 3).then(pl.lit("AgentConfirmed"))
        .when(lds == 4).then(pl.lit("Visited"))
        .when(lds == 5).then(pl.lit("Rejected"))
        .when(lds == 6).then(pl.lit("Canceled"))
        .when(lds == 7).then(pl.lit("ClientConfirmed"))
        .when(lds == 8).then(pl.lit("ClientCanceled"))
        .when(lds == 9).then(pl.lit("ContactConfirmed"))
        .when(lds == 10).then(pl.lit("ContactRejected"))
        .when(lds == 11).then(pl.lit("OfferNotAvailableRejected"))
        .when(lds == 12).then(pl.lit("ScheduleNotAvailableRejected"))
        .when(lds == 13).then(pl.lit("UnsuitableSpotRejected"))
        .when(lds == 14).then(pl.lit("NoContactAnswerRejected"))
        .when(lds == 15).then(pl.lit("NoClientAnswerRejected"))
        .when(lds == 16).then(pl.lit("ScheduleErrorRejected"))
        .when(lds == 17).then(pl.lit("NotSharedComissionRejected"))
        .when(lds == 18).then(pl.lit("ContactCanceled"))
        .when(lds == 19).then(pl.lit("RescheduledCanceled"))
        .otherwise(pl.lit("Unknown"))
        .alias("appointment_last_date_status")
    )

    return df.select(
        [
            pl.col("id").alias("appointment_id"),
            pl.col("project_requirement_id").alias("project_id"),
            pl.col("spot_id"),
            pl.col("created_at").alias("appointment_created_at"),
            pl.col("last_date_status").alias("appointment_last_date_status_id"),
            appointment_last_date_status_expr,
            pl.col("created_at").dt.date().alias("appointment_created_date"),
            pl.col("updated_at").alias("appointment_updated_at"),
            pl.col("updated_at").dt.date().alias("appointment_updated_date"),
            pl.col("deleted_at").alias("appointment_deleted_at"),
            pl.col("deleted_at").dt.date().alias("appointment_deleted_date"),
        ]
    )


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    description=(
        "Silver STG: simple transformation de calendar_appointments "
        "to appointments/proyectos Spot2."
    ),
)
def stg_s2p_appointments(
    context: dg.AssetExecutionContext,
    raw_s2p_calendar_appointments: pl.DataFrame,
):
    """
    Emula:

    SELECT
        id AS appointment_id,
        project_requirement_id AS project_id,
        spot_id,
        created_at AS appointment_created_at,
        last_date_status AS appointment_last_date_status_id,
        CASE last_date_status ... END AS appointment_last_date_status,
        DATE(created_at) AS appointment_created_date,
        updated_at AS appointment_updated_at,
        DATE(updated_at) AS appointment_updated_date,
        deleted_at AS appointment_deleted_at,
        DATE(deleted_at) AS appointment_deleted_date
    FROM calendar_appointments;
    """
    def body():
        df_appointments = _transform_s2p_appointments(raw_s2p_calendar_appointments)

        context.log.info(
            f"stg_s2p_appointments: {df_appointments.height} rows for partition {context.partition_key}"
        )
        return df_appointments

    yield from iter_job_wrapped_compute(context, body)
