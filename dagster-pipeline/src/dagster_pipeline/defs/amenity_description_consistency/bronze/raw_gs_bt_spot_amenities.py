"""
Bronze External: bt_spot_amenities from GeoSpot PostgreSQL.

Extracts spot-amenity associations with amenity names for the
consistency analysis.

Has a corresponding STG asset: stg_gs_bt_spot_amenities.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.amenity_description_consistency.shared import (
    query_bronze_source,
)


@dg.asset(
    group_name="adc_bronze",
    description="Bronze: bt_spot_amenities (spot_id + amenity name) from GeoSpot.",
)
def adc_raw_gs_bt_spot_amenities(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    query = """
    SELECT spot_id, spa_amenity_name
    FROM bt_spot_amenities
    """
    df = query_bronze_source(query, source_type="geospot_postgres", context=context)
    context.log.info(f"adc_raw_gs_bt_spot_amenities: {df.height:,} rows")
    return df
