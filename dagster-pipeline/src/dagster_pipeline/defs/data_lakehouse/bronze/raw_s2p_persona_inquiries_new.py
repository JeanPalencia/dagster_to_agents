# defs/data_lakehouse/bronze/raw_s2p_persona_inquiries_new.py
"""
Bronze: raw extraction of persona_inquiries from Spot2 MySQL.
Used by stg_s2p_kyc_agg_new to build KYC status per user (users LEFT JOIN last inquiry).
Legacy: stg_lk_users.sql CTE inquiry from persona_inquiries (user_id, event_name, created_at).
"""
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset
from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_BRONZE

raw_s2p_persona_inquiries_new = make_bronze_asset("mysql_prod", tags=TAGS_LK_SPOTS_BRONZE, table_name="persona_inquiries")
