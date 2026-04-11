# defs/data_lakehouse/bronze/raw_s2p_model_has_roles_new.py
"""Bronze: model_has_roles from S2P MySQL (stg_lk_spots.sql mhr → user_max_role_*)."""
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset
from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_BRONZE

raw_s2p_model_has_roles_new = make_bronze_asset(
    "mysql_prod",
    tags=TAGS_LK_SPOTS_BRONZE,
    table_name="model_has_roles",
)
