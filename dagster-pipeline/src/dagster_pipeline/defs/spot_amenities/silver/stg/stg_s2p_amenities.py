"""
Silver STG: Normalizes raw_s2p_amenities.

Renames columns, casts types, and computes category fields
(derived exclusively from its own RAW).
"""
import dagster as dg
import polars as pl


_CATEGORY_ID_MAP = {0: 1, 1: 2}
_CATEGORY_NAME_MAP = {0: "Servicio", 1: "Amenidad"}
_CATEGORY_ID_DEFAULT = 0
_CATEGORY_NAME_DEFAULT = "Unknown"


@dg.asset(
    group_name="spa_silver",
    description="Silver STG: amenities catalog with category classification.",
)
def stg_s2p_amenities(
    context: dg.AssetExecutionContext,
    raw_s2p_amenities: pl.DataFrame,
) -> pl.DataFrame:
    """Renames, casts, and computes spa_amenity_category_id / spa_amenity_category."""
    df = raw_s2p_amenities.select(
        pl.col("id").cast(pl.Int64, strict=False).alias("spa_amenity_id"),
        pl.col("name").cast(pl.Utf8).alias("spa_amenity_name"),
        pl.col("description").cast(pl.Utf8).alias("spa_amenity_description"),
        pl.col("category")
        .cast(pl.Int64, strict=False)
        .replace(_CATEGORY_ID_MAP, default=_CATEGORY_ID_DEFAULT)
        .alias("spa_amenity_category_id"),
        pl.col("category")
        .cast(pl.Int64, strict=False)
        .replace(_CATEGORY_NAME_MAP, default=_CATEGORY_NAME_DEFAULT)
        .cast(pl.Utf8)
        .alias("spa_amenity_category"),
        pl.col("created_at").cast(pl.Datetime, strict=False).alias("spa_amenity_created_at"),
        pl.col("updated_at").cast(pl.Datetime, strict=False).alias("spa_amenity_updated_at"),
    )
    context.log.info(f"stg_s2p_amenities: {df.height:,} rows")
    return df
