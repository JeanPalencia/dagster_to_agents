# defs/data_lakehouse/silver/stg/stg_gs_lk_matches_visitors_to_leads_new.py
"""
Silver STG: Transformation of raw_gs_lk_matches_visitors_to_leads_new from Geospot PostgreSQL.
"""
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset

stg_gs_lk_matches_visitors_to_leads_new = make_silver_stg_asset(
    "raw_gs_lk_matches_visitors_to_leads_new",
    description="Silver STG: transformation of lk_matches_visitors_to_leads from Geospot PostgreSQL.",
    silver_asset_name="stg_gs_lk_matches_visitors_to_leads_new",
)
