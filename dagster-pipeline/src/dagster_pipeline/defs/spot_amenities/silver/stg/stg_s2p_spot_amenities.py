"""
Silver STG: Normalizes raw_s2p_spot_amenities.

Renames columns and casts types. No joins, no external data.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="spa_silver",
    description="Silver STG: spot-amenity associations (type-normalized).",
)
def stg_s2p_spot_amenities(
    context: dg.AssetExecutionContext,
    raw_s2p_spot_amenities: pl.DataFrame,
) -> pl.DataFrame:
    """Renames and casts columns from raw_s2p_spot_amenities."""
    df = raw_s2p_spot_amenities.select(
        pl.col("id").cast(pl.Int64, strict=False).alias("spa_id"),
        pl.col("spot_id").cast(pl.Int64, strict=False),
        pl.col("amenity_id").cast(pl.Int64, strict=False).alias("spa_amenity_id"),
        pl.col("created_at").cast(pl.Datetime, strict=False).alias("spa_created_at"),
        pl.col("updated_at").cast(pl.Datetime, strict=False).alias("spa_updated_at"),
    )
    context.log.info(f"stg_s2p_spot_amenities: {df.height:,} rows")
    return df
