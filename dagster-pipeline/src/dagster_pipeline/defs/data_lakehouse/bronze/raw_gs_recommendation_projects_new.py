# defs/data_lakehouse/bronze/raw_gs_recommendation_projects_new.py
"""
Bronze: recommendation_projects from GeoSpot PostgreSQL.
Used for channel attribution in bt_lds_lead_spots (algorithm recommendations).

Uses explicit column selection with JSONB->text casts to avoid pyarrow
"cannot mix list and non-list" errors when converting to Polars.
"""
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset

_QUERY = """
SELECT
    project_id,
    spots_suggested::text AS spots_suggested,
    white_list::text AS white_list,
    black_list::text AS black_list,
    created_at,
    updated_at
FROM recommendation_projects
"""

raw_gs_recommendation_projects_new = make_bronze_asset(
    "geospot_postgres",
    query=_QUERY,
    asset_name="raw_gs_recommendation_projects_new",
    description=(
        "Bronze: recommendation_projects from GeoSpot. "
        "Contains algorithm recommendations per project (spots_suggested, white_list, black_list)."
    ),
)
