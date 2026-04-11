# defs/funnel_ptd/silver/core/core_fptd_current_counts.py
"""
Core: Current counts — Cohort + Rolling counting methods with PTD targets and OKR totals.

Replicates the `current_counts` mega-CTE from the period_to_date SQL query.
"""
from datetime import date, timedelta

import dagster as dg
import polars as pl

from dagster_pipeline.defs.funnel_ptd.silver.silver_shared import (
    WINDOW_JOIN_KEYS,
    _build_projects_base,
    _build_ptd_windows,
)


def _compute_ptd_targets(
    windows: pl.DataFrame,
    okrs: pl.DataFrame,
) -> pl.DataFrame:
    """PTD targets: OKR * month_fraction for each window."""
    rows = []
    for w in windows.iter_rows(named=True):
        ps, pe = w["period_start"], w["period_end"]
        m = ps.replace(day=1)
        while m <= pe:
            next_m = (m.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end = next_m - timedelta(days=1)
            days_in_month = month_end.day
            capped_end = min(month_end, pe)
            days_elapsed = (capped_end - m).days + 1
            fraction = days_elapsed / days_in_month if days_in_month > 0 else 0

            rows.append({
                **{k: w[k] for k in WINDOW_JOIN_KEYS},
                "month_start": m,
                "month_fraction": fraction,
            })
            m = next_m

    if not rows:
        return windows.select(WINDOW_JOIN_KEYS).with_columns([
            pl.lit(0.0).alias(f"{m}_ptd")
            for m in ["leads", "projects", "scheduled_visits",
                       "confirmed_visits", "completed_visits", "lois"]
        ])

    pm = pl.DataFrame(rows).cast({"month_start": pl.Date})
    pm = pm.join(okrs, left_on="month_start", right_on="okr_month_start", how="left")

    metrics = ["leads", "projects", "scheduled_visits",
               "confirmed_visits", "completed_visits", "lois"]
    agg_exprs = [
        (pl.col(f"okr_{m}").fill_null(0) * pl.col("month_fraction")).sum().alias(f"{m}_ptd")
        for m in metrics
    ]
    return pm.group_by(WINDOW_JOIN_KEYS).agg(agg_exprs)


def _compute_okr_totals(
    windows: pl.DataFrame,
    okrs: pl.DataFrame,
) -> pl.DataFrame:
    """Full OKR totals: MTD = single month, QTD = 3 months of the quarter."""
    rows = []
    for w in windows.iter_rows(named=True):
        ps = w["period_start"]
        if w["ptd_type"] == "Quarter to Date":
            end_month = (ps.replace(day=28) + timedelta(days=4)).replace(day=1)
            end_month = (end_month.replace(day=28) + timedelta(days=4)).replace(day=1)
            months_end = end_month
        else:
            months_end = ps

        m = ps.replace(day=1)
        while m <= months_end:
            rows.append({**{k: w[k] for k in WINDOW_JOIN_KEYS}, "month_start": m})
            m = (m.replace(day=28) + timedelta(days=4)).replace(day=1)

    if not rows:
        return windows.select(WINDOW_JOIN_KEYS).with_columns([
            pl.lit(0.0).alias(f"okr_{m}_total")
            for m in ["leads", "projects", "scheduled_visits",
                       "confirmed_visits", "completed_visits", "lois"]
        ])

    tm = pl.DataFrame(rows).cast({"month_start": pl.Date})
    tm = tm.join(okrs, left_on="month_start", right_on="okr_month_start", how="left")

    metrics = ["leads", "projects", "scheduled_visits",
               "confirmed_visits", "completed_visits", "lois"]
    agg_exprs = [
        pl.col(f"okr_{m}").fill_null(0).sum().alias(f"okr_{m}_total")
        for m in metrics
    ]
    return tm.group_by(WINDOW_JOIN_KEYS).agg(agg_exprs)


def _compute_cohort_counts(
    leads: pl.DataFrame,
    projects_base: pl.DataFrame,
    windows: pl.DataFrame,
) -> pl.DataFrame:
    """Cohort counting method: leads and projects counted by cohort_date_real."""
    lead_rows = leads.select([
        pl.col("lead_id"),
        pl.col("lead_first_date"),
        pl.col("lead_first_date").alias("cohort_date_real"),
    ])
    proj_rows = projects_base.select([
        "lead_id", "cohort_date_real",
    ]).join(leads.select(["lead_id", "lead_first_date"]), on="lead_id", how="inner")
    proj_rows = proj_rows.select(["lead_id", "lead_first_date", "cohort_date_real"])
    all_cohort_rows = pl.concat([lead_rows, proj_rows])

    lead_assigns = []
    for w in windows.iter_rows(named=True):
        ps, pe = w["period_start"], w["period_end"]
        chunk = all_cohort_rows.filter(
            (pl.col("cohort_date_real") >= ps) & (pl.col("cohort_date_real") <= pe)
        )
        assigned = chunk.group_by("lead_id").agg([
            pl.col("lead_first_date").first(),
            pl.col("cohort_date_real").min().alias("assigned_cohort_date"),
        ])
        assigned = assigned.with_columns([
            pl.when(pl.col("assigned_cohort_date") == pl.col("lead_first_date"))
            .then(pl.lit("New"))
            .otherwise(pl.lit("Reactivated"))
            .alias("cohort_type"),
        ])
        lead_counts = assigned.group_by("cohort_type").agg(
            pl.len().alias("leads_current")
        )
        for ct_row in lead_counts.iter_rows(named=True):
            lead_assigns.append({
                **{k: w[k] for k in WINDOW_JOIN_KEYS},
                "cohort_type": ct_row["cohort_type"],
                "leads_current": ct_row["leads_current"],
            })

    cohort_leads = pl.DataFrame(lead_assigns).cast({
        "as_of_date": pl.Date, "period_start": pl.Date, "period_end": pl.Date,
        "leads_current": pl.Int64,
    }) if lead_assigns else pl.DataFrame(schema={
        **{k: pl.Date for k in ["as_of_date", "period_start", "period_end"]},
        "ptd_type": pl.Utf8, "cohort_type": pl.Utf8, "leads_current": pl.Int64,
    })

    proj_rows_list = []
    pb = projects_base.join(leads.select(["lead_id", "lead_first_date"]), on="lead_id", how="inner", suffix="_ld")
    for w in windows.iter_rows(named=True):
        ps, pe, aod = w["period_start"], w["period_end"], w["as_of_date"]
        chunk = pb.filter(
            (pl.col("cohort_date_real") >= ps) & (pl.col("cohort_date_real") <= pe)
        )
        chunk = chunk.with_columns(
            pl.when(pl.col("cohort_date_real") == pl.col("lead_first_date"))
            .then(pl.lit("New"))
            .otherwise(pl.lit("Reactivated"))
            .alias("cohort_type")
        )

        for ct in ["New", "Reactivated"]:
            sub = chunk.filter(pl.col("cohort_type") == ct)
            proj_rows_list.append({
                **{k: w[k] for k in WINDOW_JOIN_KEYS},
                "cohort_type": ct,
                "projects_current": sub.filter(
                    pl.col("project_created_at").is_not_null()
                    & (pl.col("project_created_at").cast(pl.Date) <= aod)
                )["project_id"].n_unique(),
                "scheduled_visits_current": sub.filter(
                    pl.col("visit_scheduled_at").is_not_null()
                    & (pl.col("visit_scheduled_at") <= pl.lit(aod))
                )["project_id"].n_unique(),
                "confirmed_visits_current": sub.filter(
                    pl.col("visit_scheduled_at").is_not_null()
                    & (pl.col("visit_scheduled_at") <= pl.lit(aod))
                    & pl.col("visit_confirmed_at").is_not_null()
                )["project_id"].n_unique(),
                "completed_visits_current": sub.filter(
                    pl.col("visit_completed_at").is_not_null()
                    & (pl.col("visit_completed_at").cast(pl.Date) <= aod)
                )["project_id"].n_unique(),
                "lois_current": sub.filter(
                    pl.col("project_funnel_loi_date").is_not_null()
                    & (pl.col("project_funnel_loi_date") <= aod)
                )["project_id"].n_unique(),
                "won_current": sub.filter(
                    pl.col("project_won_date").is_not_null()
                    & (pl.col("project_won_date") <= aod)
                )["project_id"].n_unique(),
            })

    cohort_projects = pl.DataFrame(proj_rows_list).cast({
        "as_of_date": pl.Date, "period_start": pl.Date, "period_end": pl.Date,
    }) if proj_rows_list else pl.DataFrame()

    join_keys = WINDOW_JOIN_KEYS + ["cohort_type"]
    if cohort_leads.height > 0 and cohort_projects.height > 0:
        result = cohort_leads.join(cohort_projects, on=join_keys, how="outer_coalesce")
    elif cohort_leads.height > 0:
        result = cohort_leads
    else:
        result = cohort_projects

    result = result.with_columns(pl.lit("Cohort").alias("counting_method"))
    return result


def _compute_rolling_counts(
    leads: pl.DataFrame,
    projects_base: pl.DataFrame,
    windows: pl.DataFrame,
) -> pl.DataFrame:
    """Rolling counting method: events counted by their own date within the period."""
    rows = []
    pb = projects_base.join(leads.select(["lead_id", "lead_first_date"]), on="lead_id", how="inner", suffix="_ld")

    for w in windows.iter_rows(named=True):
        ps, pe, aod = w["period_start"], w["period_end"], w["as_of_date"]

        lead_count = leads.filter(
            (pl.col("lead_first_date") >= ps) & (pl.col("lead_first_date") <= pe)
        )["lead_id"].n_unique()

        if w["ptd_type"] == "Month to Date":
            next_m = (ps.replace(day=28) + timedelta(days=4)).replace(day=1)
            confirmed_end = next_m - timedelta(days=1)
        elif w["ptd_type"] == "Quarter to Date":
            q_month = ((ps.month - 1) // 3) * 3 + 1
            q_start = ps.replace(month=q_month, day=1)
            q_next = q_start.month + 3
            if q_next > 12:
                confirmed_end = date(q_start.year + 1, q_next - 12, 1) - timedelta(days=1)
            else:
                confirmed_end = date(q_start.year, q_next, 1) - timedelta(days=1)
        else:
            confirmed_end = pe

        rows.append({
            **{k: w[k] for k in WINDOW_JOIN_KEYS},
            "cohort_type": "New",
            "leads_current": lead_count,
            "projects_current": pb.filter(
                pl.col("project_created_at").is_not_null()
                & (pl.col("project_created_at").cast(pl.Date) >= ps)
                & (pl.col("project_created_at").cast(pl.Date) <= pe)
            )["project_id"].n_unique(),
            "scheduled_visits_current": pb.filter(
                pl.col("visit_scheduled_at").is_not_null()
                & (pl.col("visit_scheduled_at").cast(pl.Date) >= ps)
                & (pl.col("visit_scheduled_at").cast(pl.Date) <= pe)
            )["project_id"].n_unique(),
            "confirmed_visits_current": pb.filter(
                pl.col("visit_confirmed_at").is_not_null()
                & (pl.col("visit_confirmed_at").cast(pl.Date) >= ps)
                & (pl.col("visit_confirmed_at").cast(pl.Date) <= confirmed_end)
            )["project_id"].n_unique(),
            "completed_visits_current": pb.filter(
                pl.col("visit_completed_at").is_not_null()
                & (pl.col("visit_completed_at").cast(pl.Date) >= ps)
                & (pl.col("visit_completed_at").cast(pl.Date) <= pe)
            )["project_id"].n_unique(),
            "lois_current": pb.filter(
                pl.col("project_funnel_loi_date").is_not_null()
                & (pl.col("project_funnel_loi_date") >= ps)
                & (pl.col("project_funnel_loi_date") <= pe)
            )["project_id"].n_unique(),
            "won_current": pb.filter(
                pl.col("project_won_date").is_not_null()
                & (pl.col("project_won_date") >= ps)
                & (pl.col("project_won_date") <= pe)
            )["project_id"].n_unique(),
        })

    result = pl.DataFrame(rows).cast({
        "as_of_date": pl.Date, "period_start": pl.Date, "period_end": pl.Date,
    })
    result = result.with_columns(pl.lit("Rolling").alias("counting_method"))
    return result


def _combine_current_counts(
    cohort: pl.DataFrame,
    rolling: pl.DataFrame,
    targets: pl.DataFrame,
    okr_totals: pl.DataFrame,
    windows: pl.DataFrame,
) -> pl.DataFrame:
    """Union Cohort + Rolling, cross-join Reactivated zeros for Rolling, join targets/OKRs."""
    rolling_react = rolling.clone()
    rolling_react = rolling_react.with_columns(pl.lit("Reactivated").alias("cohort_type"))
    int_cols = [c for c in rolling_react.columns
                if c.endswith("_current") and rolling_react[c].dtype in (pl.Int64, pl.Int32, pl.UInt32)]
    rolling_react = rolling_react.with_columns([pl.lit(0).cast(pl.Int64).alias(c) for c in int_cols])

    combined = pl.concat([cohort, rolling, rolling_react], how="diagonal_relaxed")

    metric_cols = [c for c in combined.columns if c.endswith("_current")]
    combined = combined.with_columns([pl.col(c).fill_null(0) for c in metric_cols])

    combined = combined.join(targets, on=WINDOW_JOIN_KEYS, how="left")
    ptd_cols = [c for c in combined.columns if c.endswith("_ptd")]
    combined = combined.with_columns([pl.col(c).fill_null(0) for c in ptd_cols])

    combined = combined.join(okr_totals, on=WINDOW_JOIN_KEYS, how="left")
    okr_cols = [c for c in combined.columns if c.endswith("_total")]
    combined = combined.with_columns([pl.col(c).fill_null(0) for c in okr_cols])

    return combined


@dg.asset(
    group_name="funnel_ptd_core",
    description="Core: current counts (Cohort + Rolling) with PTD targets and OKR totals.",
)
def core_fptd_current_counts(
    context: dg.AssetExecutionContext,
    stg_gs_fptd_leads: pl.DataFrame,
    stg_gs_fptd_projects: pl.DataFrame,
    stg_gs_fptd_okrs: pl.DataFrame,
) -> pl.DataFrame:
    ref_date = date.today() - timedelta(days=1)
    context.log.info(f"ref_date = {ref_date}")

    projects_base = _build_projects_base(stg_gs_fptd_leads, stg_gs_fptd_projects)
    context.log.info(f"projects_base: {projects_base.height:,} rows")

    windows = _build_ptd_windows(ref_date)
    context.log.info(f"ptd_windows: {windows.height} rows")

    targets = _compute_ptd_targets(windows, stg_gs_fptd_okrs)
    okr_totals = _compute_okr_totals(windows, stg_gs_fptd_okrs)

    cohort = _compute_cohort_counts(stg_gs_fptd_leads, projects_base, windows)
    context.log.info(f"cohort counts: {cohort.height} rows")

    rolling = _compute_rolling_counts(stg_gs_fptd_leads, projects_base, windows)
    context.log.info(f"rolling counts: {rolling.height} rows")

    result = _combine_current_counts(cohort, rolling, targets, okr_totals, windows)
    context.log.info(f"core_fptd_current_counts: {result.height} rows, {result.width} columns")
    return result
