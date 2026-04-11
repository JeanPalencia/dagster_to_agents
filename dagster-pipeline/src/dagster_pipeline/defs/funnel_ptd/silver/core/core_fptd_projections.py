# defs/funnel_ptd/silver/core/core_fptd_projections.py
"""
Core: KM-based projections — probability of completing each funnel stage by EOP.

Aggregates per-project probabilities into projected completion counts.
"""
from datetime import date, datetime, timedelta

import dagster as dg
import polars as pl

from dagster_pipeline.defs.funnel_ptd.silver.silver_shared import (
    PROJECTION_JOIN_KEYS,
)

KM_MIN_EVENTS = 30


def _build_projection_scope(monitor: pl.DataFrame) -> pl.DataFrame:
    """Filter to active projects in stages 1-3 with On time or Late status."""
    df = monitor.filter(
        (pl.col("project_enable_id") == 1)
        & (pl.col("current_stage_id").is_in([1, 2, 3]))
    )

    df = df.with_columns(
        pl.when(pl.col("current_stage_id") == 1).then(pl.col("visit_scheduled_status_id"))
        .when(pl.col("current_stage_id") == 2).then(pl.col("visit_completed_status_id"))
        .when(pl.col("current_stage_id") == 3).then(pl.col("loi_signed_status_id"))
        .otherwise(None)
        .alias("current_stage_status_id")
    )

    df = df.filter(pl.col("current_stage_status_id").is_in([3, 4]))
    return df


def _compute_projection_horizon(scope: pl.DataFrame) -> pl.DataFrame:
    """Compute eop_date and days_remaining_to_eop."""
    df = scope

    def _eop(ptd_type: str, aod: date) -> date:
        if ptd_type == "Month to Date":
            next_m = (aod.replace(day=28) + timedelta(days=4)).replace(day=1)
            return next_m - timedelta(days=1)
        elif ptd_type == "Quarter to Date":
            q_month = ((aod.month - 1) // 3) * 3 + 1
            q_start = aod.replace(month=q_month, day=1)
            q_next = q_start.month + 3
            if q_next > 12:
                return date(q_start.year + 1, q_next - 12, 1) - timedelta(days=1)
            return date(q_start.year, q_next, 1) - timedelta(days=1)
        return aod

    eop_dates = []
    days_rem = []
    last_stage_at = []
    days_since = []

    for row in df.iter_rows(named=True):
        eop = _eop(row["ptd_type"], row["as_of_date"])
        eop_dates.append(eop)
        days_rem.append(max(0, (eop - row["as_of_date"]).days))

        if row["visit_scheduled_at"] is None:
            lca = row["project_created_at"]
        elif row["visit_completed_at"] is None:
            lca = row["visit_scheduled_at"]
        elif row["project_funnel_loi_date"] is None:
            lca = row["visit_completed_at"]
        else:
            lca = row["project_funnel_loi_date"]

        if lca is not None:
            if isinstance(lca, datetime):
                lca = lca.date()
            elif not isinstance(lca, date):
                lca = row["as_of_date"]
        else:
            lca = row["as_of_date"]

        last_stage_at.append(lca)
        days_since.append((row["as_of_date"] - lca).days)

    df = df.with_columns([
        pl.Series("eop_date", eop_dates, dtype=pl.Date),
        pl.Series("days_remaining_to_eop", days_rem, dtype=pl.Int32),
        pl.Series("last_completed_stage_at", last_stage_at, dtype=pl.Date),
        pl.Series("days_since_last_completed_stage", days_since, dtype=pl.Int32),
    ])

    return df


def _choose_curve(
    horizon: pl.DataFrame,
    km_n: pl.DataFrame,
) -> pl.DataFrame:
    """Select KM curve level (L3 -> L2 -> L1) based on min_events threshold."""
    n3 = km_n.filter(pl.col("curve_level") == "L3").select([
        pl.col("counting_method"), pl.col("cohort_type"),
        pl.col("stage_id"), pl.col("n_events").alias("n_events_l3"),
        pl.col("max_t").alias("max_t_l3"),
    ])
    n2 = km_n.filter(pl.col("curve_level") == "L2").select([
        pl.col("counting_method"),
        pl.col("stage_id"), pl.col("n_events").alias("n_events_l2"),
        pl.col("max_t").alias("max_t_l2"),
    ])
    n1 = km_n.filter(pl.col("curve_level") == "L1").select([
        pl.col("stage_id"), pl.col("n_events").alias("n_events_l1"),
        pl.col("max_t").alias("max_t_l1"),
    ])

    df = horizon
    df = df.join(n3, left_on=["counting_method", "cohort_type", "current_stage_id"],
                 right_on=["counting_method", "cohort_type", "stage_id"], how="left")
    df = df.join(n2, left_on=["counting_method", "current_stage_id"],
                 right_on=["counting_method", "stage_id"], how="left")
    df = df.join(n1, left_on="current_stage_id", right_on="stage_id", how="left")

    df = df.with_columns(
        pl.when(pl.col("n_events_l3").fill_null(0) >= KM_MIN_EVENTS).then(pl.lit("L3"))
        .when(pl.col("n_events_l2").fill_null(0) >= KM_MIN_EVENTS).then(pl.lit("L2"))
        .when(pl.col("n_events_l1").fill_null(0) >= KM_MIN_EVENTS).then(pl.lit("L1"))
        .otherwise(None)
        .alias("used_curve_level")
    )
    df = df.with_columns(
        pl.when(pl.col("used_curve_level") == "L3").then(pl.col("counting_method"))
        .when(pl.col("used_curve_level") == "L2").then(pl.col("counting_method"))
        .when(pl.col("used_curve_level") == "L1").then(pl.lit("All"))
        .otherwise(None)
        .alias("used_counting_method")
    )
    df = df.with_columns(
        pl.when(pl.col("used_curve_level") == "L3").then(pl.col("cohort_type"))
        .when(pl.col("used_curve_level") == "L2").then(pl.lit("All"))
        .when(pl.col("used_curve_level") == "L1").then(pl.lit("All"))
        .otherwise(None)
        .alias("used_cohort_type")
    )

    df = df.with_columns(
        pl.when(pl.col("used_curve_level") == "L3").then(pl.col("max_t_l3"))
        .when(pl.col("used_curve_level") == "L2").then(pl.col("max_t_l2"))
        .when(pl.col("used_curve_level") == "L1").then(pl.col("max_t_l1"))
        .otherwise(None)
        .alias("max_t")
    )

    df = df.with_columns([
        pl.col("days_since_last_completed_stage").alias("t0"),
        (pl.col("days_since_last_completed_stage") + pl.col("days_remaining_to_eop")).alias("t1"),
    ])

    df = df.with_columns([
        pl.min_horizontal("t0", pl.col("max_t").fill_null(pl.col("t0"))).alias("t0_c"),
        pl.min_horizontal("t1", pl.col("max_t").fill_null(pl.col("t1"))).alias("t1_c"),
    ])

    return df


def _compute_probabilities(
    curve_choice: pl.DataFrame,
    km_survival: pl.DataFrame,
) -> pl.DataFrame:
    """Compute P(complete stage by EOP) = 1 - S(t1)/S(t0)."""
    km = km_survival.select([
        "curve_level", "counting_method", "cohort_type", "stage_id", "t", "s_t",
    ])

    df = curve_choice.join(
        km.rename({"s_t": "s_t0"}),
        left_on=["used_curve_level", "used_counting_method", "used_cohort_type", "current_stage_id", "t0_c"],
        right_on=["curve_level", "counting_method", "cohort_type", "stage_id", "t"],
        how="left",
    )

    df = df.join(
        km.rename({"s_t": "s_t1"}),
        left_on=["used_curve_level", "used_counting_method", "used_cohort_type", "current_stage_id", "t1_c"],
        right_on=["curve_level", "counting_method", "cohort_type", "stage_id", "t"],
        how="left",
    )

    df = df.with_columns(
        pl.when(pl.col("used_curve_level").is_null()).then(None)
        .when(pl.col("s_t0").is_null() | (pl.col("s_t0") == 0)).then(0.0)
        .when(pl.col("s_t1").is_null()).then(None)
        .otherwise(
            (1.0 - pl.col("s_t1") / pl.col("s_t0")).clip(0.0, 1.0)
        )
        .alias("prob_complete_current_stage_by_eop")
    )

    return df


def _aggregate_projections(probabilities: pl.DataFrame) -> pl.DataFrame:
    """Aggregate probabilities by group into projected counts."""
    df = probabilities
    group_keys = PROJECTION_JOIN_KEYS

    result = df.group_by(group_keys).agg([
        pl.when(pl.col("current_stage_id") == 1)
        .then(pl.col("prob_complete_current_stage_by_eop").fill_null(0))
        .otherwise(0)
        .sum()
        .alias("scheduled_visits_proj"),

        pl.when(pl.col("current_stage_id") == 2)
        .then(pl.col("prob_complete_current_stage_by_eop").fill_null(0))
        .otherwise(0)
        .sum()
        .alias("completed_visits_proj"),

        pl.when(pl.col("current_stage_id") == 3)
        .then(pl.col("prob_complete_current_stage_by_eop").fill_null(0))
        .otherwise(0)
        .sum()
        .alias("lois_proj"),

        pl.len().alias("n_projects_in_projection_scope"),
    ])

    return result


@dg.asset(
    group_name="funnel_ptd_core",
    description="Core: KM-based projections of funnel completions by end of period.",
)
def core_fptd_projections(
    context: dg.AssetExecutionContext,
    core_fptd_monitor: pl.DataFrame,
    core_fptd_km_survival: dict,
) -> pl.DataFrame:
    km_survival = core_fptd_km_survival["km_survival"]
    km_n = core_fptd_km_survival["km_n"]

    scope = _build_projection_scope(core_fptd_monitor)
    context.log.info(f"projection_scope: {scope.height:,} projects")

    if scope.height == 0:
        context.log.info("No projects in projection scope — returning empty projections")
        return pl.DataFrame(schema={
            "counting_method": pl.Utf8, "as_of_date": pl.Date,
            "ptd_type": pl.Utf8, "period_start": pl.Date, "period_end": pl.Date,
            "cohort_type": pl.Utf8,
            "scheduled_visits_proj": pl.Int64, "completed_visits_proj": pl.Int64,
            "lois_proj": pl.Int64, "n_projects_in_projection_scope": pl.UInt32,
        })

    horizon = _compute_projection_horizon(scope)
    curve_choice = _choose_curve(horizon, km_n)
    probabilities = _compute_probabilities(curve_choice, km_survival)
    result = _aggregate_projections(probabilities)

    context.log.info(f"core_fptd_projections: {result.height} rows")
    return result
