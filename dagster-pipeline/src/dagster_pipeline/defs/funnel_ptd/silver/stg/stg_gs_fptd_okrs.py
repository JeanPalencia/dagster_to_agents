# defs/funnel_ptd/silver/stg/stg_gs_fptd_okrs.py
"""
STG: Staging for OKR targets — type normalization.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="funnel_ptd_stg",
    description="STG: OKR monthly targets with normalized types.",
)
def stg_gs_fptd_okrs(
    context: dg.AssetExecutionContext,
    raw_gs_lk_okrs: pl.DataFrame,
) -> pl.DataFrame:
    df = raw_gs_lk_okrs

    df = df.with_columns([
        pl.col("okr_month_start_ts").cast(pl.Date, strict=False).alias("okr_month_start"),
        pl.col("okr_leads").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("okr_projects").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("okr_scheduled_visits").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("okr_confirmed_visits").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("okr_completed_visits").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("okr_lois").cast(pl.Float64, strict=False).fill_null(0),
    ])

    df = df.drop("okr_month_start_ts")

    context.log.info(f"stg_gs_fptd_okrs: {df.height:,} rows")
    return df
