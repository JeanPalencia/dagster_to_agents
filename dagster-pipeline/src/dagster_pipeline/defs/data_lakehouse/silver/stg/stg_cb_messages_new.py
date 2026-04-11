# defs/data_lakehouse/silver/stg/stg_cb_messages_new.py
"""
Silver STG: Transformation of raw_cb_messages_new from Chatbot PostgreSQL.
"""
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset

stg_cb_messages_new = make_silver_stg_asset(
    "raw_cb_messages_new",
    description="Silver STG: transformation of messages from Chatbot PostgreSQL.",
    partitioned=False,
)
