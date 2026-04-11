# defs/data_lakehouse/silver/stg/stg_s2p_data_states_new.py
"""Silver STG: Passthrough from raw_s2p_data_states_new (lk_spots upstream)."""
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset
from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_SILVER

stg_s2p_data_states_new = make_silver_stg_asset(
    "raw_s2p_data_states_new",
    tags=TAGS_LK_SPOTS_SILVER,
    description="Silver STG: data_states from S2P MySQL (lk_spots upstream).",
)
