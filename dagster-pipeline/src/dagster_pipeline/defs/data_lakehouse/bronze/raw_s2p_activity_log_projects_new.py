# defs/data_lakehouse/bronze/raw_s2p_activity_log_projects_new.py
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset

raw_s2p_activity_log_projects_new = make_bronze_asset(
    "mysql_prod",
    table_name="activity_log",
    where_clause=(
        "event IN ("
        "'projectSpotIntentionLetter', "
        "'projectSpotDocumentation', "
        "'projectSpotContract', "
        "'projectSpotWon'"
        ")"
    ),
    description_suffix=" for projects",
    asset_name="raw_s2p_activity_log_projects_new",
)
