# defs/spot_state_transitions/bronze/raw_gs_active_spot_ids.py
"""
Bronze External: Active spot IDs from GeoSpot lk_spots.

Independent extraction (no dependencies on other Bronze assets).
Used by Gold layer to determine hard-deleted spots.
Has a corresponding STG asset.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_state_transitions.shared import query_bronze_source


@dg.asset(
    group_name="sst_bronze",
    description="Bronze: active spot_ids from GeoSpot lk_spots.",
)
def raw_gs_active_spot_ids(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Queries GeoSpot for all spot_ids in lk_spots."""
    query = "SELECT spot_id FROM lk_spots"
    df = query_bronze_source(query, source_type="geospot_postgres", context=context)

    context.log.info(f"raw_gs_active_spot_ids: {df.height:,} active spots")
    return df
