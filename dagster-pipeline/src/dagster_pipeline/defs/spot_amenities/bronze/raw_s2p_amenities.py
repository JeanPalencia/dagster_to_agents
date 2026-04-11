"""
Bronze External: Amenities catalog from MySQL S2P.

Has a corresponding STG asset: stg_s2p_amenities.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_amenities.shared import query_bronze_source


@dg.asset(
    group_name="spa_bronze",
    description="Bronze: amenities catalog from MySQL S2P.",
)
def raw_s2p_amenities(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Extracts all amenities from MySQL S2P."""
    query = "SELECT id, name, description, category, created_at, updated_at FROM amenities"
    df = query_bronze_source(query, source_type="mysql_prod", context=context)
    context.log.info(f"raw_s2p_amenities: {df.height:,} rows")
    return df
