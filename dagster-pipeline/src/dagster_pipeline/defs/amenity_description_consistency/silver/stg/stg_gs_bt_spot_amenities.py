"""
Silver STG: Normalizes adc_raw_gs_bt_spot_amenities.

Casts types and ensures consistent schema. No joins, no external data.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="adc_silver",
    description="Silver STG: bt_spot_amenities normalized for consistency analysis.",
)
def adc_stg_gs_bt_spot_amenities(
    context: dg.AssetExecutionContext,
    adc_raw_gs_bt_spot_amenities: pl.DataFrame,
) -> pl.DataFrame:
    df = adc_raw_gs_bt_spot_amenities.select(
        pl.col("spot_id").cast(pl.Int64, strict=False),
        pl.col("spa_amenity_name").cast(pl.Utf8),
    )
    context.log.info(f"adc_stg_gs_bt_spot_amenities: {df.height:,} rows")
    return df
