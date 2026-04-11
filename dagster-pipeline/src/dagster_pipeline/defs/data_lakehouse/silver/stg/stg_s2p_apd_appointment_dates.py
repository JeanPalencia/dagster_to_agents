# defs/data_lakehouse/silver/stg/stg_s2p_apd_appointment_dates.py
import dagster as dg
import polars as pl
from datetime import date  # sólo por consistencia con otros assets

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _transform_s2p_apd_appointment_dates(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma raw_s2p_calendar_appointment_dates (calendar_appointment_dates en MySQL)
    replicando la query de stg_s2p_apd_appointment_dates.
    """

    return df.select(
        [
            pl.col("id").alias("apd_id"),
            pl.col("calendar_appointment_id").alias("appointment_id"),
            pl.col("status").alias("apd_status_id"),
            pl.col("date").alias("apd_at"),
            pl.col("created_at").alias("apd_created_at"),
            pl.col("created_at").dt.date().alias("apd_created_date"),
            pl.col("updated_at").alias("apd_updated_at"),
            pl.col("updated_at").dt.date().alias("apd_updated_date"),
        ]
    )


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    description=(
        "Silver STG: simple transformation de calendar_appointment_dates "
        "a fechas de citas de proyectos Spot2."
    ),
)
def stg_s2p_apd_appointment_dates(
    context: dg.AssetExecutionContext,
    raw_s2p_calendar_appointment_dates: pl.DataFrame,
):
    """
    Emula:

    SELECT
        id AS apd_id,
        calendar_appointment_id AS appointment_id,
        status AS apd_status_id,
        date AS apd_at,
        created_at AS apd_created_at,
        DATE(created_at) AS apd_created_date,
        updated_at AS apd_updated_at,
        DATE(updated_at) AS apd_updated_date
    FROM calendar_appointment_dates;
    """
    def body():
        df_apd = _transform_s2p_apd_appointment_dates(raw_s2p_calendar_appointment_dates)

        context.log.info(
            f"stg_s2p_apd_appointment_dates: {df_apd.height} rows for partition {context.partition_key}"
        )
        return df_apd

    yield from iter_job_wrapped_compute(context, body)
