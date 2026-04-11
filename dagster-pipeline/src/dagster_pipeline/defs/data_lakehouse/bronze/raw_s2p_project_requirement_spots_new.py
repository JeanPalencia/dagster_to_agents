# defs/data_lakehouse/bronze/raw_s2p_project_requirement_spots_new.py
"""
Bronze: raw extraction of project_requirement_spot (MySQL) for the NEW pipeline.

Table project_requirement_spot links project_requirements to spots (project_id ↔ spot_id).
Asset name uses plural 'spots' to align with legacy raw_s2p_project_requirement_spots.
"""
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset

raw_s2p_project_requirement_spots_new = make_bronze_asset(
    "mysql_prod",
    table_name="project_requirement_spot",
    asset_name="raw_s2p_project_requirement_spots_new",
    description="Bronze: raw extraction of project_requirement_spot from Spot2 Platform (MySQL). Links projects to spots.",
)
