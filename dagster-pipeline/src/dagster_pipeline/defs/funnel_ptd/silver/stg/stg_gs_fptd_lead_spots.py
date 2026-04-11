# defs/funnel_ptd/silver/stg/stg_gs_fptd_lead_spots.py
"""
STG: Staging for bt_lds_lead_spots — type normalization for rejection check.
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="funnel_ptd_stg",
    description="STG: lead-spot visit events with normalized types.",
)
def stg_gs_fptd_lead_spots(
    context: dg.AssetExecutionContext,
    raw_gs_bt_lds_lead_spots: pl.DataFrame,
) -> pl.DataFrame:
    df = raw_gs_bt_lds_lead_spots

    df = df.with_columns([
        pl.col("project_id").cast(pl.Int64, strict=False),
        pl.col("lds_event_id").cast(pl.Int32, strict=False),
        pl.col("lds_event_at").cast(pl.Datetime("us"), strict=False),
        pl.col("appointment_last_date_status_id").cast(pl.Int32, strict=False),
    ])

    context.log.info(f"stg_gs_fptd_lead_spots: {df.height:,} rows")
    return df
