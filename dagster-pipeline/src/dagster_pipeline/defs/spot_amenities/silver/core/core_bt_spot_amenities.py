"""
Silver Core: bt_spot_amenities.

LEFT JOIN between stg_s2p_spot_amenities and stg_s2p_amenities
to produce the final bridge table with enriched amenity metadata.
"""
import dagster as dg
import polars as pl


_FINAL_COLUMNS = [
    "spa_id",
    "spot_id",
    "spa_amenity_id",
    "spa_amenity_name",
    "spa_amenity_description",
    "spa_amenity_category_id",
    "spa_amenity_category",
    "spa_amenity_created_at",
    "spa_amenity_updated_at",
    "spa_created_at",
    "spa_updated_at",
]


@dg.asset(
    group_name="spa_silver",
    description="Silver Core: bridge table joining spot-amenity associations with amenity catalog.",
)
def core_bt_spot_amenities(
    context: dg.AssetExecutionContext,
    stg_s2p_spot_amenities: pl.DataFrame,
    stg_s2p_amenities: pl.DataFrame,
) -> pl.DataFrame:
    """LEFT JOIN spot_amenities with amenities on spa_amenity_id."""
    df = stg_s2p_spot_amenities.join(
        stg_s2p_amenities,
        on="spa_amenity_id",
        how="left",
    ).select(_FINAL_COLUMNS)

    context.log.info(f"core_bt_spot_amenities: {df.height:,} rows, {df.width} columns")
    return df
