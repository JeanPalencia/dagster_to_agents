# defs/data_lakehouse/bronze/raw_gs_lk_matches_visitors_to_leads_new.py
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset

# Source table in Geospot PostgreSQL remains temp_matches_users_to_leads; asset name is lk_matches_visitors_to_leads_new.
raw_gs_lk_matches_visitors_to_leads_new = make_bronze_asset(
    "geospot_postgres",
    table_name="temp_matches_users_to_leads",
    asset_name="raw_gs_lk_matches_visitors_to_leads_new",
)
