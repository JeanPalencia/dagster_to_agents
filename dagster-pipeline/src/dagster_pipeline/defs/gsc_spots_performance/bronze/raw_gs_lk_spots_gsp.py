"""
Bronze External: Spot attributes from GeoSpot PostgreSQL (lk_spots).

Extracts spot_id plus classification and location fields used to enrich
the GSC performance report.

Has a corresponding STG asset: stg_gs_lk_spots_gsp.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.gsc_spots_performance.shared import query_bronze_source


_LK_SPOTS_SQL = """
SELECT spot_id, spot_type, spot_sector, spot_status_full, spot_modality, spot_address
FROM lk_spots
"""


@dg.asset(
    group_name="gsp_bronze",
    description="Bronze: spot attributes (type, sector, status, modality, address) from GeoSpot.",
)
def raw_gs_lk_spots_gsp(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Extracts spot classification and location data from GeoSpot."""
    df = query_bronze_source(_LK_SPOTS_SQL, source_type="geospot_postgres", context=context)
    context.log.info(f"raw_gs_lk_spots_gsp: {df.height:,} rows, {df.width} columns")
    return df
