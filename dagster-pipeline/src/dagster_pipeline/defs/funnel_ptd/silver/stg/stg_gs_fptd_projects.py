# defs/funnel_ptd/silver/stg/stg_gs_fptd_projects.py
"""
STG: Staging for projects — type normalization and column renaming.

cohort_date_real is NOT computed here (requires join with leads → goes to Core).
"""
import dagster as dg
import polars as pl


@dg.asset(
    group_name="funnel_ptd_stg",
    description="STG: projects with normalized types and renamed funnel columns.",
)
def stg_gs_fptd_projects(
    context: dg.AssetExecutionContext,
    raw_gs_lk_projects_v2: pl.DataFrame,
) -> pl.DataFrame:
    df = raw_gs_lk_projects_v2

    df = df.with_columns([
        pl.col("project_created_at").cast(pl.Datetime("us"), strict=False),
        pl.col("project_funnel_visit_confirmed_at").cast(pl.Datetime("us"), strict=False),
        pl.col("project_funnel_visit_realized_at").cast(pl.Datetime("us"), strict=False),
        pl.col("project_funnel_visit_created_date").cast(pl.Date, strict=False),
        pl.col("project_funnel_loi_date").cast(pl.Date, strict=False),
        pl.col("project_won_date").cast(pl.Date, strict=False),
        pl.col("project_enable_id").cast(pl.Int32, strict=False),
    ])

    df = df.rename({
        "project_funnel_visit_created_date": "visit_scheduled_at",
        "project_funnel_visit_confirmed_at": "visit_confirmed_at",
        "project_funnel_visit_realized_at": "visit_completed_at",
    })

    context.log.info(f"stg_gs_fptd_projects: {df.height:,} rows")
    return df
