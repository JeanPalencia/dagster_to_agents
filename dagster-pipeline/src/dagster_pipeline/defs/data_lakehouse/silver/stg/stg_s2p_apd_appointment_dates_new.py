# defs/data_lakehouse/silver/stg/stg_s2p_apd_appointment_dates_new.py
"""
Silver STG: Complete transformation of raw_s2p_calendar_appointment_dates_new using reusable functions.

This asset maintains original bronze column names and only adds calculated columns when necessary.
"""
import polars as pl

from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset


def _transform_s2p_apd_appointment_dates(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms raw_s2p_calendar_appointment_dates_new maintaining original column names.
    Only adds calculated columns when necessary.
    """

    # Maintain all original columns and add calculated ones
    return df.with_columns([
        # Rename key columns (maintain original names where possible)
        pl.col("id").alias("apd_id"),
        pl.col("calendar_appointment_id").alias("appointment_id"),
        pl.col("status").alias("apd_status_id"),
        pl.col("date").alias("apd_at"),

        # Timestamp columns (keep for potential sorting/filtering)
        pl.col("created_at").alias("apd_created_at"),
        pl.col("updated_at").alias("apd_updated_at"),

        # Date columns (derived from timestamps)
        pl.col("created_at").dt.date().alias("apd_created_date"),
        pl.col("updated_at").dt.date().alias("apd_updated_date"),
    ])


stg_s2p_apd_appointment_dates_new = make_silver_stg_asset(
    "raw_s2p_calendar_appointment_dates_new",
    _transform_s2p_apd_appointment_dates,
    silver_asset_name="stg_s2p_apd_appointment_dates_new",
    description=(
        "Silver STG: Complete transformation of calendar_appointment_dates from Spot2 Platform "
        "using reusable functions. Maintains original column names and adds calculated fields."
    ),
)
