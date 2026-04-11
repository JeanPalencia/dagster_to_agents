"""
Silver STG: Normalizes adc_raw_gs_lk_spots.

Casts types and ensures consistent schema. No joins, no external data.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="adc_silver",
    description="Silver STG: lk_spots normalized for consistency analysis.",
)
def adc_stg_gs_lk_spots(
    context: dg.AssetExecutionContext,
    adc_raw_gs_lk_spots: pl.DataFrame,
) -> pl.DataFrame:
    df = adc_raw_gs_lk_spots.select(
        pl.col("spot_id").cast(pl.Int64, strict=False),
        pl.col("spot_description").cast(pl.Utf8),
        pl.col("spot_type").cast(pl.Utf8),
        pl.col("spot_status_full").cast(pl.Utf8),
    )
    context.log.info(f"adc_stg_gs_lk_spots: {df.height:,} rows")
    return df
