# defs/data_lakehouse/silver/stg/stg_s2p_prs_project_spots_new.py
"""
Silver STG: Project-Spot relationships from project_requirement_spot (NEW pipeline).

Reads raw_s2p_project_requirement_spots_new from S3, applies the same transform as legacy:
rename fields (project_requirement_id -> project_id, etc.), filter null spot_id,
deduplicate by (project_id, spot_id) keeping first by project_spot_id.
"""
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset
from dagster_pipeline.defs.data_lakehouse.silver.stg.stg_s2p_prs_project_spots import (
    _transform_s2p_prs_project_spots,
)

stg_s2p_prs_project_spots_new = make_silver_stg_asset(
    "raw_s2p_project_requirement_spots_new",
    _transform_s2p_prs_project_spots,
    silver_asset_name="stg_s2p_prs_project_spots_new",
    description=(
        "Silver STG: Project-Spot relationships from project_requirement_spot. "
        "Filters null spots and deduplicates by (project_id, spot_id)."
    ),
    allow_row_loss=True,  # Filter null spot_id + dedup reduces rows
)
