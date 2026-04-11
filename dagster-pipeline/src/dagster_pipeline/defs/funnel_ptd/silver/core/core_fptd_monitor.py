# defs/funnel_ptd/silver/core/core_fptd_monitor.py
"""
Core: Project-level monitor — percentiles, eligibility, scoring, presentable.

Replicates the `monitor` CTE block from the projections section of the SQL query.
"""
from datetime import date, datetime, timedelta
from typing import Optional

import dagster as dg
import polars as pl

from dagster_pipeline.defs.funnel_ptd.silver.silver_shared import (
    _build_projects_base,
    _build_ptd_windows,
)


ACTIVITY_CATALOG = pl.DataFrame({
    "status_id": [1, 2, 3],
    "project_activity_status": ["Active Project", "Inactive Project", "Rejected or Canceled"],
}).cast({"status_id": pl.Int32})

STATUS_CATALOG = pl.DataFrame({
    "status_id": [1, 3, 4, 5, 6],
    "status_label": ["Completed", "On time", "Late", "Inactive Project", "Rejected or Canceled"],
}).cast({"status_id": pl.Int32})


def _compute_percentiles(
    projects_base: pl.DataFrame,
    ref_date: date,
) -> pl.DataFrame:
    """P75 thresholds for stage transitions, with mature fallback."""
    q_month = ((ref_date.month - 1) // 3) * 3 + 1
    q_start = ref_date.replace(month=q_month, day=1)
    m = q_start.month - 6
    y = q_start.year
    if m <= 0:
        m += 12
        y -= 1
    window_start = date(y, m, 1)

    pb = projects_base.filter(
        pl.col("project_created_at").is_not_null()
        & (pl.col("project_created_at").cast(pl.Date) < q_start)
    )
    pb = pb.with_columns([
        pl.col("project_created_at").cast(pl.Date).alias("created_dt"),
        pl.col("visit_scheduled_at").cast(pl.Date).alias("scheduled_dt"),
        pl.col("visit_completed_at").cast(pl.Date).alias("completed_dt"),
        pl.col("project_funnel_loi_date").cast(pl.Date).alias("loi_dt"),
    ])

    results = []
    for method in ["Cohort", "Rolling"]:
        anchor_col = "cohort_date_real" if method == "Cohort" else "created_dt"
        sub = pb.filter(
            (pl.col(anchor_col) >= window_start) & (pl.col(anchor_col) < q_start)
        )

        def _p75_diff(df: pl.DataFrame, col_a: str, col_b: str) -> Optional[int]:
            valid = df.filter(
                pl.col(col_a).is_not_null() & pl.col(col_b).is_not_null()
            ).with_columns(
                ((pl.col(col_a) - pl.col(col_b)).dt.total_days()).alias("diff")
            ).filter(pl.col("diff") >= 0)
            if valid.height == 0:
                return None
            return int(valid["diff"].quantile(0.75, "lower"))

        p75_ca = _p75_diff(sub, "scheduled_dt", "created_dt")
        p75_ar = _p75_diff(sub, "completed_dt", "scheduled_dt")
        p75_rl = _p75_diff(sub, "loi_dt", "completed_dt")

        def _p75_mature(df: pl.DataFrame, col_a: str, col_b: str, threshold: Optional[int]) -> Optional[int]:
            if threshold is None:
                return None
            valid = df.filter(
                pl.col(col_a).is_not_null() & pl.col(col_b).is_not_null()
                & (pl.col(col_b) <= pl.lit(ref_date - timedelta(days=threshold)))
            ).with_columns(
                ((pl.col(col_a) - pl.col(col_b)).dt.total_days()).alias("diff")
            ).filter(pl.col("diff") >= 0)
            if valid.height == 0:
                return None
            return int(valid["diff"].quantile(0.75, "lower"))

        p75_ca_m = _p75_mature(sub, "scheduled_dt", "created_dt", p75_ca)
        p75_ar_m = _p75_mature(sub, "completed_dt", "scheduled_dt", p75_ar)
        p75_rl_m = _p75_mature(sub, "loi_dt", "completed_dt", p75_rl)

        results.append({
            "counting_method": method,
            "p75_created_to_agenda": p75_ca_m if p75_ca_m is not None else p75_ca,
            "p75_scheduled_to_completed": p75_ar_m if p75_ar_m is not None else p75_ar,
            "p75_completed_to_loi": p75_rl_m if p75_rl_m is not None else p75_rl,
        })

    return pl.DataFrame(results)


def _build_projects_cutoff(
    projects_base: pl.DataFrame,
    windows: pl.DataFrame,
    ref_date: date,
) -> pl.DataFrame:
    """Cutoff future events at ref_ts_end (start of today)."""
    ref_ts_end = datetime.combine(ref_date + timedelta(days=1), datetime.min.time())

    pb = projects_base.filter(
        pl.col("project_created_at").is_not_null()
        & (pl.col("project_created_at") < ref_ts_end)
    )

    pb = pb.with_columns([
        pl.when(pl.col("project_created_at") < ref_ts_end)
        .then(pl.col("project_created_at")).otherwise(None).alias("project_created_at"),
        pl.when(pl.col("visit_scheduled_at").is_not_null() & (pl.col("visit_scheduled_at").cast(pl.Datetime("us")) < ref_ts_end))
        .then(pl.col("visit_scheduled_at")).otherwise(None).alias("visit_scheduled_at"),
        pl.when(pl.col("visit_completed_at").is_not_null() & (pl.col("visit_completed_at") < ref_ts_end))
        .then(pl.col("visit_completed_at")).otherwise(None).alias("visit_completed_at"),
        pl.when(pl.col("project_funnel_loi_date").is_not_null() & (pl.col("project_funnel_loi_date").cast(pl.Datetime("us")) < ref_ts_end))
        .then(pl.col("project_funnel_loi_date")).otherwise(None).alias("project_funnel_loi_date"),
    ])

    frames = []
    for w in windows.iter_rows(named=True):
        chunk = pb.with_columns([
            pl.lit(w["as_of_date"]).alias("as_of_date"),
            pl.lit(w["ptd_type"]).alias("ptd_type"),
            pl.lit(w["period_start"]).alias("period_start"),
            pl.lit(w["period_end"]).alias("period_end"),
        ])
        frames.append(chunk)

    return pl.concat(frames) if frames else pb


def _build_monitor_scope(cutoff: pl.DataFrame) -> pl.DataFrame:
    """Build scope_cohort + scope_rolling -> monitor_scope."""
    cohort = cutoff.filter(
        (pl.col("cohort_date_real") >= pl.col("period_start"))
        & (pl.col("cohort_date_real") <= pl.col("period_end"))
    ).with_columns([
        pl.lit("Cohort").alias("counting_method"),
        pl.when(pl.col("cohort_date_real") == pl.col("lead_first_date"))
        .then(pl.lit("New"))
        .otherwise(pl.lit("Reactivated"))
        .alias("cohort_type"),
    ])

    rolling = cutoff.filter(
        (pl.col("period_end") == pl.col("as_of_date"))
        & (pl.col("project_created_at").is_not_null())
        & (pl.col("project_created_at").cast(pl.Date) <= pl.col("as_of_date"))
    ).with_columns([
        pl.lit("Rolling").alias("counting_method"),
        pl.lit("New").alias("cohort_type"),
    ])

    return pl.concat([cohort, rolling], how="diagonal_relaxed")


def _compute_eligibility(scope: pl.DataFrame) -> pl.DataFrame:
    """Compute candidacy flags and backlog indicators per stage."""
    df = scope

    df = df.with_columns(
        (
            pl.col("visit_scheduled_at").is_not_null()
            & (pl.col("visit_scheduled_at").cast(pl.Date) <= pl.col("as_of_date"))
            & pl.col("visit_completed_at").is_null()
        ).alias("stage2_backlog_in_window")
    )

    df = df.with_columns(
        pl.when(pl.col("counting_method") == "Cohort")
        .then(
            pl.col("visit_completed_at").is_not_null()
            & (pl.col("visit_completed_at").cast(pl.Date) >= pl.col("period_start"))
            & (pl.col("visit_completed_at").cast(pl.Date) <= pl.col("period_end"))
            & pl.col("project_funnel_loi_date").is_null()
        )
        .otherwise(
            pl.col("visit_completed_at").is_not_null()
            & (pl.col("visit_completed_at").cast(pl.Date) <= pl.col("as_of_date"))
            & pl.col("project_funnel_loi_date").is_null()
        )
        .alias("stage3_backlog_in_window")
    )

    df = df.with_columns([
        pl.lit(True).alias("is_schedule_visit_candidate"),
        (
            pl.col("visit_scheduled_at").is_not_null()
            & (pl.col("visit_scheduled_at").cast(pl.Date) <= pl.col("as_of_date"))
        ).alias("is_complete_visit_candidate"),
        (
            pl.col("visit_completed_at").is_not_null()
            & (pl.col("visit_completed_at").cast(pl.Date) <= pl.col("as_of_date"))
        ).alias("is_sign_loi_candidate"),
    ])

    return df


def _score_projects(
    eligible: pl.DataFrame,
    lead_spots: pl.DataFrame,
    pct: pl.DataFrame,
) -> pl.DataFrame:
    """Join percentiles and compute visit rejection flag."""
    df = eligible.join(pct, on="counting_method", how="left")

    rejection_ids_by_window = {}
    for row in df.select(["project_id", "counting_method", "as_of_date", "period_start"]).unique().iter_rows(named=True):
        pid = row["project_id"]
        aod = row["as_of_date"]
        ps = row["period_start"]
        method = row["counting_method"]

        aod_next = datetime.combine(aod + timedelta(days=1), datetime.min.time())
        if method == "Cohort":
            start_filter = datetime.combine(ps, datetime.min.time())
        else:
            start_filter = datetime.combine(aod - timedelta(days=365), datetime.min.time())

        has_event = lead_spots.filter(
            (pl.col("project_id") == pid)
            & (pl.col("lds_event_at") < aod_next)
            & (pl.col("lds_event_at") >= start_filter)
        ).height > 0

        rejection_ids_by_window[(pid, method, aod, ps)] = has_event

    df = df.with_row_index("_row_idx")
    rejection_flags = []
    for row in df.iter_rows(named=True):
        pid = row["project_id"]
        enable = row["project_enable_id"]
        backlog = row["stage2_backlog_in_window"]
        method = row["counting_method"]
        aod = row["as_of_date"]
        ps = row["period_start"]

        if enable == 0 or not backlog:
            rejection_flags.append(False)
        else:
            has_event = rejection_ids_by_window.get((pid, method, aod, ps), False)
            rejection_flags.append(not has_event)

    df = df.with_columns(pl.Series("is_visit_rejected", rejection_flags))
    df = df.drop("_row_idx")
    return df


def _build_presentable(scored: pl.DataFrame) -> pl.DataFrame:
    """Compute status IDs per stage, current_stage, days metrics."""
    df = scored

    df = df.with_columns(
        pl.when(pl.col("project_enable_id") == 0).then(2)
        .when(pl.col("is_visit_rejected")).then(3)
        .otherwise(1)
        .cast(pl.Int32)
        .alias("project_activity_status_id")
    )

    df = df.with_columns(
        pl.when(~pl.col("is_schedule_visit_candidate")).then(None)
        .when(
            (pl.col("counting_method") == "Rolling")
            & pl.col("visit_scheduled_at").is_not_null()
            & (pl.col("visit_scheduled_at").cast(pl.Date) >= pl.col("period_start"))
            & (pl.col("visit_scheduled_at").cast(pl.Date) <= pl.col("period_end"))
        ).then(1)
        .when(
            (pl.col("counting_method") == "Cohort")
            & pl.col("visit_scheduled_at").is_not_null()
            & (pl.col("visit_scheduled_at").cast(pl.Date) <= pl.col("as_of_date"))
        ).then(1)
        .when(pl.col("project_enable_id") == 0).then(5)
        .when(
            (pl.col("counting_method") == "Rolling")
            & (pl.col("period_end") < pl.col("as_of_date"))
            & ~(
                (pl.col("project_created_at").cast(pl.Date) >= pl.col("period_start"))
                & (pl.col("project_created_at").cast(pl.Date) <= pl.col("period_end"))
            )
        ).then(None)
        .when(pl.col("p75_created_to_agenda").is_null()).then(None)
        .when(
            (pl.col("as_of_date") - pl.col("project_created_at").cast(pl.Date)).dt.total_days()
            > pl.col("p75_created_to_agenda")
        ).then(4)
        .otherwise(3)
        .cast(pl.Int32)
        .alias("visit_scheduled_status_id")
    )

    df = df.with_columns(
        pl.when(~pl.col("is_complete_visit_candidate")).then(None)
        .when(pl.col("visit_completed_at").is_not_null()).then(1)
        .when(pl.col("project_enable_id") == 0).then(5)
        .when(pl.col("is_visit_rejected")).then(6)
        .when(pl.col("p75_scheduled_to_completed").is_null()).then(None)
        .when(
            (pl.col("as_of_date") - pl.col("visit_scheduled_at").cast(pl.Date)).dt.total_days()
            > pl.col("p75_scheduled_to_completed")
        ).then(4)
        .otherwise(3)
        .cast(pl.Int32)
        .alias("visit_completed_status_id")
    )

    df = df.with_columns(
        pl.when(~pl.col("is_sign_loi_candidate")).then(None)
        .when(pl.col("project_funnel_loi_date").is_not_null()).then(1)
        .when(pl.col("project_enable_id") == 0).then(5)
        .when(pl.col("p75_completed_to_loi").is_null()).then(None)
        .when(
            (pl.col("as_of_date") - pl.col("visit_completed_at").cast(pl.Date)).dt.total_days()
            > pl.col("p75_completed_to_loi")
        ).then(4)
        .otherwise(3)
        .cast(pl.Int32)
        .alias("loi_signed_status_id")
    )

    df = df.with_columns(
        pl.when(pl.col("visit_scheduled_at").is_null()).then(1)
        .when(pl.col("visit_completed_at").is_null()).then(2)
        .when(pl.col("project_funnel_loi_date").is_null()).then(3)
        .otherwise(4)
        .cast(pl.Int32)
        .alias("current_stage_id")
    )

    return df


def _build_monitor(presentable: pl.DataFrame) -> pl.DataFrame:
    """Join with activity and status catalogs."""
    df = presentable.join(
        ACTIVITY_CATALOG.rename({"status_id": "project_activity_status_id", "project_activity_status": "project_activity_status"}),
        on="project_activity_status_id", how="left",
    )

    for src_col, dst_col in [
        ("visit_scheduled_status_id", "visit_scheduled_status"),
        ("visit_completed_status_id", "visit_completed_status"),
        ("loi_signed_status_id", "loi_signed_status"),
    ]:
        cat = STATUS_CATALOG.rename({"status_id": src_col, "status_label": dst_col})
        df = df.join(cat, on=src_col, how="left")

    return df


@dg.asset(
    group_name="funnel_ptd_core",
    description="Core: project-level monitor with stage status and rejection flags.",
)
def core_fptd_monitor(
    context: dg.AssetExecutionContext,
    stg_gs_fptd_leads: pl.DataFrame,
    stg_gs_fptd_projects: pl.DataFrame,
    stg_gs_fptd_lead_spots: pl.DataFrame,
) -> pl.DataFrame:
    ref_date = date.today() - timedelta(days=1)

    projects_base = _build_projects_base(stg_gs_fptd_leads, stg_gs_fptd_projects)
    windows = _build_ptd_windows(ref_date)

    pct = _compute_percentiles(projects_base, ref_date)
    context.log.info(f"Percentiles computed: {pct}")

    cutoff = _build_projects_cutoff(projects_base, windows, ref_date)
    context.log.info(f"projects_cutoff: {cutoff.height:,} rows")

    scope = _build_monitor_scope(cutoff)
    context.log.info(f"monitor_scope: {scope.height:,} rows")

    eligible = _compute_eligibility(scope)
    scored = _score_projects(eligible, stg_gs_fptd_lead_spots, pct)
    presentable = _build_presentable(scored)
    monitor = _build_monitor(presentable)

    context.log.info(f"core_fptd_monitor: {monitor.height:,} rows, {monitor.width} columns")
    return monitor
