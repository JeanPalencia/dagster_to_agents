# defs/data_lakehouse/bronze/raw_s2p_report_history_new.py
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset
from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_BRONZE

# report_history is a CTE in SQL; base table is spot_reports (same as raw_s2p_spot_reports_new)
raw_s2p_report_history_new = make_bronze_asset(
    "mysql_prod", table_name="spot_reports", asset_name="raw_s2p_report_history_new"
)
