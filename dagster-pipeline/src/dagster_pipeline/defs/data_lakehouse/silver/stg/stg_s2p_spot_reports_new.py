# defs/data_lakehouse/silver/stg/stg_s2p_spot_reports_new.py
"""Silver STG: Passthrough from raw_s2p_spot_reports_new (lk_spots upstream)."""
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset
from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_SILVER

stg_s2p_spot_reports_new = make_silver_stg_asset(
    "raw_s2p_spot_reports_new",
    tags=TAGS_LK_SPOTS_SILVER,
    description="Silver STG: spot_reports from S2P MySQL (lk_spots upstream).",
)
