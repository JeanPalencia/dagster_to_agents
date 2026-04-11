"""
Bronze External: Spot-amenity associations from MySQL S2P.

Has a corresponding STG asset: stg_s2p_spot_amenities.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_amenities.shared import query_bronze_source


@dg.asset(
    group_name="spa_bronze",
    description="Bronze: spot-amenity associations from MySQL S2P.",
)
def raw_s2p_spot_amenities(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Extracts all spot_amenities from MySQL S2P."""
    query = "SELECT id, spot_id, amenity_id, created_at, updated_at FROM spot_amenities"
    df = query_bronze_source(query, source_type="mysql_prod", context=context)
    context.log.info(f"raw_s2p_spot_amenities: {df.height:,} rows")
    return df
