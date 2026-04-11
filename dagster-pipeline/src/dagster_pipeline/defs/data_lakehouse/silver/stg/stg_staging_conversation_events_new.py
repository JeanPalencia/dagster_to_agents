# defs/data_lakehouse/silver/stg/stg_staging_conversation_events_new.py
"""
Silver STG: Transformation of raw_staging_conversation_events_new (conversation_events from Staging).
Keeps schema aligned with conversation_analysis conv_raw_events for use in gold_bt_conv_conversations_new.
"""
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset

stg_staging_conversation_events_new = make_silver_stg_asset(
    "raw_staging_conversation_events_new",
    description="Silver STG: conversation events from Staging for bt_conv_conversations pipeline.",
    partitioned=False,
)
