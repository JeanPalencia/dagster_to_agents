# defs/data_lakehouse/bronze/raw_s2p_prices_new.py
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset
from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_BRONZE

raw_s2p_prices_new = make_bronze_asset("mysql_prod", tags=TAGS_LK_SPOTS_BRONZE, table_name="prices")
