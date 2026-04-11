# defs/data_lakehouse/bronze/raw_s2p_calendar_appointments_new.py
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset

raw_s2p_calendar_appointments_new = make_bronze_asset(
    "mysql_prod", table_name="calendar_appointments", partitioned=False
)
