# defs/data_lakehouse/bronze/raw_s2p_spot_photos_new.py
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset
from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_BRONZE

# spot_photos is a CTE in SQL; base table is photos (same as raw_s2p_photos_new)
raw_s2p_spot_photos_new = make_bronze_asset(
    "mysql_prod", table_name="photos", asset_name="raw_s2p_spot_photos_new"
)
