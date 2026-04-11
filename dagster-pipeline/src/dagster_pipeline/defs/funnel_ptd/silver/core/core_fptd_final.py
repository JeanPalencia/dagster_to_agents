# defs/funnel_ptd/silver/core/core_fptd_final.py
"""
Core: Final output — joins current counts with projections,
produces Actuals and Actuals + Forecast rows.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.funnel_ptd.silver.silver_shared import (
    PROJECTION_JOIN_KEYS,
)


def _join_counts_projections(
    current_counts: pl.DataFrame,
    projections: pl.DataFrame,
) -> pl.DataFrame:
    """LEFT JOIN current counts with projections."""
    df = current_counts.join(projections, on=PROJECTION_JOIN_KEYS, how="left")

    for col in ["scheduled_visits_proj", "completed_visits_proj", "lois_proj", "n_projects_in_projection_scope"]:
        df = df.with_columns(
            pl.when(
                (pl.col("counting_method") == "Rolling")
                & (pl.col("period_end") < pl.col("as_of_date"))
            ).then(0)
            .otherwise(pl.col(col).fill_null(0))
            .alias(col)
        )

    return df


def _build_final_output(joined: pl.DataFrame) -> pl.DataFrame:
    """Generate Actuals + Actuals+Forecast rows with PTD rates and linear projections."""
    _RATE_METRICS = [
        "leads", "projects", "scheduled_visits",
        "confirmed_visits", "completed_visits", "lois",
    ]

    base_cols = [
        "counting_method", "as_of_date", "ptd_type", "period_start",
        "period_end", "cohort_type",
        "leads_current", "projects_current",
        "scheduled_visits_current", "confirmed_visits_current",
        "completed_visits_current", "lois_current", "won_current",
        "leads_ptd", "projects_ptd",
        "scheduled_visits_ptd", "confirmed_visits_ptd",
        "completed_visits_ptd", "lois_ptd",
        "okr_leads_total", "okr_projects_total",
        "okr_scheduled_visits_total", "okr_confirmed_visits_total",
        "okr_completed_visits_total", "okr_lois_total",
        "scheduled_visits_proj", "completed_visits_proj",
        "lois_proj", "n_projects_in_projection_scope",
    ]

    actual_cols = [c for c in base_cols if c in joined.columns]

    actuals = joined.select(actual_cols).with_columns(
        pl.lit("Actuals").alias("value_type")
    )

    forecast = joined.select(actual_cols).with_columns([
        pl.lit("\u26a0\ufe0f Actuals + Forecast").alias("value_type"),
        (pl.col("scheduled_visits_current") + pl.col("scheduled_visits_proj")).alias("scheduled_visits_current"),
        (pl.col("completed_visits_current") + pl.col("completed_visits_proj")).alias("completed_visits_current"),
        (pl.col("lois_current") + pl.col("lois_proj")).alias("lois_current"),
    ])

    rate_exprs = [
        pl.when(pl.col(f"{m}_ptd") != 0)
        .then(pl.col(f"{m}_current") / pl.col(f"{m}_ptd"))
        .otherwise(None)
        .alias(f"{m}_ptd_rate")
        for m in _RATE_METRICS
    ]

    actuals = actuals.with_columns(rate_exprs)
    actuals = actuals.with_columns([
        pl.col(f"{m}_current").cast(pl.Float64).alias(f"{m}_count")
        for m in _RATE_METRICS
    ])

    forecast = forecast.with_columns(rate_exprs)
    forecast = forecast.with_columns([
        (pl.col(f"{m}_ptd_rate") * pl.col(f"okr_{m}_total")).alias(f"{m}_count")
        for m in _RATE_METRICS
    ])

    result = pl.concat([actuals, forecast], how="diagonal_relaxed")

    result = result.sort([
        "counting_method", "ptd_type", "period_start", "cohort_type",
        pl.when(pl.col("value_type") == "Actuals").then(1).otherwise(2),
    ])

    return result


@dg.asset(
    group_name="funnel_ptd_core",
    description="Core: final PTD output (Actuals + Forecast) joining counts and projections.",
)
def core_fptd_final(
    context: dg.AssetExecutionContext,
    core_fptd_current_counts: pl.DataFrame,
    core_fptd_projections: pl.DataFrame,
) -> pl.DataFrame:
    joined = _join_counts_projections(core_fptd_current_counts, core_fptd_projections)
    result = _build_final_output(joined)

    context.log.info(f"core_fptd_final: {result.height} rows, {result.width} columns")
    return result
