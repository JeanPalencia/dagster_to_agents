# defs/funnel_ptd/silver/stg/stg_gs_fptd_leads.py
"""
STG: Staging for leads — computes lead_first_datetime and lead_first_date.

Calculated fields derived exclusively from raw_gs_lk_leads.
Filters leads with valid dates (>= 2021-01-01, < today, < sentinel).
"""
from datetime import date, datetime

import dagster as dg
import polars as pl


SENTINEL_TS = datetime(9999, 12, 31)
SENTINEL_DATE = date(9999, 12, 31)
MIN_DATE = datetime(2021, 1, 1)


@dg.asset(
    group_name="funnel_ptd_stg",
    description="STG: leads with lead_first_datetime and lead_first_date.",
)
def stg_gs_fptd_leads(
    context: dg.AssetExecutionContext,
    raw_gs_lk_leads: pl.DataFrame,
) -> pl.DataFrame:
    df = raw_gs_lk_leads

    lead_at_cols = [
        "lead_lead0_at", "lead_lead1_at", "lead_lead2_at",
        "lead_lead3_at", "lead_lead4_at",
    ]
    for col in lead_at_cols:
        df = df.with_columns(
            pl.col(col).cast(pl.Datetime("us"), strict=False)
        )

    df = df.with_columns(
        pl.min_horizontal([
            pl.col(c).fill_null(SENTINEL_TS) for c in lead_at_cols
        ]).alias("lead_first_datetime")
    )

    df = df.filter(
        (pl.col("lead_first_datetime") >= MIN_DATE)
        & (pl.col("lead_first_datetime") < pl.lit(datetime.combine(date.today(), datetime.min.time())))
        & (pl.col("lead_first_datetime") < SENTINEL_TS)
    )

    df = df.with_columns(
        pl.col("lead_first_datetime").cast(pl.Date).alias("lead_first_date")
    )

    df = df.select(["lead_id", "lead_first_datetime", "lead_first_date"])

    context.log.info(f"stg_gs_fptd_leads: {df.height:,} rows")
    return df
