# defs/effective_supply/bronze/raw_gs_lk_spots.py
"""
Bronze: Raw extraction of spot attributes from GeoSpot lk_spots.

Source: GeoSpot PostgreSQL - lk_spots (non-deleted, non-complex only).
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.effective_supply.shared import query_bronze_source


@dg.asset(
    group_name="effective_supply_bronze",
    description="Bronze: raw spot attributes from GeoSpot lk_spots.",
)
def raw_gs_lk_spots(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    query = """
    SELECT
        spot_id,
        spot_sector_id,
        spot_sector,
        spot_type_id,
        spot_type,
        spot_state_id,
        spot_state,
        spot_municipality_id,
        spot_municipality,
        spot_zip_code_id,
        spot_zip_code,
        spot_corridor_id,
        spot_corridor,
        spot_latitude,
        spot_longitude,
        spot_area_in_sqm,
        spot_modality_id,
        spot_modality,
        spot_price_total_mxn_rent,
        spot_price_total_mxn_sale,
        spot_price_sqm_mxn_rent,
        spot_price_sqm_mxn_sale
    FROM lk_spots
    WHERE spot_deleted_at IS NULL
      AND spot_type_id IN (1, 3)
    """

    df = query_bronze_source(query, source_type="geospot_postgres", context=context)
    context.log.info(
        f"raw_gs_lk_spots: {df.height:,} rows, {df.width} columns"
    )
    return df
