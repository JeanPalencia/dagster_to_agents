"""
Bronze External: lk_spots from GeoSpot PostgreSQL.

Extracts Single + Subspace spots (public + disabled) that have
a non-empty description. This is the universe for the amenity-
description consistency analysis.

Has a corresponding STG asset: stg_gs_lk_spots.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.amenity_description_consistency.shared import (
    query_bronze_source,
)


@dg.asset(
    group_name="adc_bronze",
    description="Bronze: lk_spots (Single+Subspace, public+disabled, with description) from GeoSpot.",
)
def adc_raw_gs_lk_spots(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    query = """
    SELECT
        spot_id,
        spot_description,
        spot_type,
        spot_status_full
    FROM lk_spots
    WHERE spot_type_id IN (1, 3)
      AND spot_status_full_id IN (100, 305, 306, 309, 315, 308)
      AND spot_description IS NOT NULL
      AND LENGTH(TRIM(spot_description)) > 0
    """
    df = query_bronze_source(query, source_type="geospot_postgres", context=context)
    context.log.info(f"adc_raw_gs_lk_spots: {df.height:,} rows")
    return df
