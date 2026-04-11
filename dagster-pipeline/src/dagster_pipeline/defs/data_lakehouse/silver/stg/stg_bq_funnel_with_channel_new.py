# defs/data_lakehouse/silver/stg/stg_bq_funnel_with_channel_new.py
"""
Silver STG: Transformation of raw_bq_funnel_with_channel_new from BigQuery.
"""
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset

stg_bq_funnel_with_channel_new = make_silver_stg_asset(
    "raw_bq_funnel_with_channel_new",
    description="Silver STG: transformation of funnel_with_channel from BigQuery.",
)
