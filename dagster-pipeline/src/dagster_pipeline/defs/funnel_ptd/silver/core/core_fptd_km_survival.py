# defs/funnel_ptd/silver/core/core_fptd_km_survival.py
"""
Core: Kaplan-Meier survival curves for stage transitions.

Computes S(t) = P(T > t) for each (curve_level, counting_method, cohort_type, stage_id).
Uses 3 fallback levels (L3, L2, L1) with GROUPING SETS emulation.
"""
from datetime import date, timedelta

import dagster as dg
import polars as pl

from dagster_pipeline.defs.funnel_ptd.silver.silver_shared import (
    _build_projects_base,
)


def _build_km_raw(
    projects_base: pl.DataFrame,
    ref_date: date,
) -> pl.DataFrame:
    """Build KM observations: one row per (project, stage, method)."""
    q_month = ((ref_date.month - 1) // 3) * 3 + 1
    q_start = ref_date.replace(month=q_month, day=1)
    m = q_start.month - 6
    y = q_start.year
    if m <= 0:
        m += 12
        y -= 1
    window_start = date(y, m, 1)

    pb = projects_base.filter(
        (pl.col("project_enable_id") == 1)
        & pl.col("project_created_at").is_not_null()
        & (pl.col("project_created_at").cast(pl.Date) <= ref_date)
    )

    frames = []

    for method in ["Cohort", "Rolling"]:
        if method == "Cohort":
            sub = pb.filter(
                (pl.col("cohort_date_real") >= window_start)
                & (pl.col("cohort_date_real") < q_start)
            )
        else:
            sub = pb

        cohort_expr = (
            pl.when(pl.col("cohort_date_real") == pl.col("lead_first_date"))
            .then(pl.lit("New")).otherwise(pl.lit("Reactivated"))
        ).alias("cohort_type") if method == "Cohort" else pl.lit("New").alias("cohort_type")

        # Stage 1: created -> agenda
        s1_filter = pl.col("project_created_at").is_not_null()
        if method == "Rolling":
            s1_filter = (
                s1_filter
                & (pl.col("project_created_at").cast(pl.Date) >= window_start)
                & (pl.col("project_created_at").cast(pl.Date) < q_start)
            )
        s1 = sub.filter(s1_filter).select([
            pl.lit(method).alias("counting_method"),
            cohort_expr,
            pl.lit(1).cast(pl.Int32).alias("stage_id"),
            pl.col("project_created_at").cast(pl.Date).alias("stage_start_dt"),
            pl.col("visit_scheduled_at").cast(pl.Date).alias("stage_end_dt"),
        ])
        frames.append(s1)

        # Stage 2: agenda -> realizada
        s2_filter = (
            pl.col("visit_scheduled_at").is_not_null()
            & (pl.col("visit_scheduled_at").cast(pl.Date) <= ref_date)
        )
        if method == "Rolling":
            s2_filter = (
                s2_filter
                & (pl.col("visit_scheduled_at").cast(pl.Date) >= window_start)
                & (pl.col("visit_scheduled_at").cast(pl.Date) < q_start)
            )
        s2 = sub.filter(s2_filter).select([
            pl.lit(method).alias("counting_method"),
            cohort_expr,
            pl.lit(2).cast(pl.Int32).alias("stage_id"),
            pl.col("visit_scheduled_at").cast(pl.Date).alias("stage_start_dt"),
            pl.col("visit_completed_at").cast(pl.Date).alias("stage_end_dt"),
        ])
        frames.append(s2)

        # Stage 3: realizada -> loi
        s3_filter = (
            pl.col("visit_completed_at").is_not_null()
            & (pl.col("visit_completed_at").cast(pl.Date) <= ref_date)
        )
        if method == "Rolling":
            s3_filter = (
                s3_filter
                & (pl.col("visit_completed_at").cast(pl.Date) >= window_start)
                & (pl.col("visit_completed_at").cast(pl.Date) < q_start)
            )
        s3 = sub.filter(s3_filter).select([
            pl.lit(method).alias("counting_method"),
            cohort_expr,
            pl.lit(3).cast(pl.Int32).alias("stage_id"),
            pl.col("visit_completed_at").cast(pl.Date).alias("stage_start_dt"),
            pl.col("project_funnel_loi_date").cast(pl.Date).alias("stage_end_dt"),
        ])
        frames.append(s3)

    return pl.concat(frames, how="diagonal_relaxed")


def _compute_km_observations(km_raw: pl.DataFrame, ref_date: date) -> pl.DataFrame:
    """Compute event flag and observed time in days."""
    df = km_raw.filter(pl.col("stage_start_dt").is_not_null())

    df = df.with_columns([
        pl.when(pl.col("stage_end_dt").is_not_null()).then(1).otherwise(0).cast(pl.Int32).alias("event"),
        pl.when(pl.col("stage_end_dt").is_not_null())
        .then((pl.col("stage_end_dt") - pl.col("stage_start_dt")).dt.total_days())
        .otherwise((pl.lit(ref_date) - pl.col("stage_start_dt")).dt.total_days())
        .cast(pl.Int32)
        .alias("observed_time"),
    ])

    df = df.filter(pl.col("observed_time") >= 0)
    return df


def _compute_km_counts_grouping(observations: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Replicate GROUPING SETS with 3 explicit levels:
    L3 = (counting_method, cohort_type, stage_id, t)
    L2 = (counting_method, stage_id, t) with cohort_type='All'
    L1 = (stage_id, t) with both='All'

    Returns (km_counts, km_n).
    """
    obs = observations.select(["counting_method", "cohort_type", "stage_id", "event", "observed_time"])

    def _agg_counts(df: pl.DataFrame, group_cols: list[str], level: str) -> pl.DataFrame:
        result = df.group_by(group_cols + ["observed_time"]).agg([
            pl.col("event").filter(pl.col("event") == 1).count().alias("d_t"),
            pl.col("event").filter(pl.col("event") == 0).count().alias("c_t"),
        ])
        result = result.rename({"observed_time": "t"})
        if "counting_method" not in group_cols:
            result = result.with_columns(pl.lit("All").alias("counting_method"))
        if "cohort_type" not in group_cols:
            result = result.with_columns(pl.lit("All").alias("cohort_type"))
        result = result.with_columns(pl.lit(level).alias("curve_level"))
        return result

    l3 = _agg_counts(obs, ["counting_method", "cohort_type", "stage_id"], "L3")
    l2 = _agg_counts(obs, ["counting_method", "stage_id"], "L2")
    l1 = _agg_counts(obs, ["stage_id"], "L1")
    km_counts = pl.concat([l3, l2, l1], how="diagonal_relaxed")

    def _agg_n(df: pl.DataFrame, group_cols: list[str], level: str) -> pl.DataFrame:
        result = df.group_by(group_cols).agg([
            pl.len().alias("n_total"),
            pl.col("event").filter(pl.col("event") == 1).count().alias("n_events"),
            pl.col("observed_time").max().alias("max_t"),
        ])
        if "counting_method" not in group_cols:
            result = result.with_columns(pl.lit("All").alias("counting_method"))
        if "cohort_type" not in group_cols:
            result = result.with_columns(pl.lit("All").alias("cohort_type"))
        result = result.with_columns(pl.lit(level).alias("curve_level"))
        return result

    n3 = _agg_n(obs, ["counting_method", "cohort_type", "stage_id"], "L3")
    n2 = _agg_n(obs, ["counting_method", "stage_id"], "L2")
    n1 = _agg_n(obs, ["stage_id"], "L1")
    km_n = pl.concat([n3, n2, n1], how="diagonal_relaxed")

    return km_counts, km_n


def _compute_km_survival(
    km_counts: pl.DataFrame,
    km_n: pl.DataFrame,
) -> pl.DataFrame:
    """Compute full KM survival curve S(t) for each (curve_level, method, cohort, stage)."""
    group_key = ["curve_level", "counting_method", "cohort_type", "stage_id"]

    grids = []
    for row in km_n.iter_rows(named=True):
        max_t = row["max_t"]
        if max_t is None or max_t < 0:
            continue
        g = pl.DataFrame({"t": list(range(int(max_t) + 1))}).cast({"t": pl.Int32})
        for k in group_key:
            g = g.with_columns(pl.lit(row[k]).alias(k))
        g = g.with_columns(pl.lit(row["n_total"]).cast(pl.Int64).alias("n_total"))
        grids.append(g)

    if not grids:
        return pl.DataFrame(schema={
            **{k: pl.Utf8 for k in group_key[:3]},
            "stage_id": pl.Int32, "t": pl.Int32,
            "n_total": pl.Int64, "n_at_risk": pl.Int64,
            "d_t": pl.Int64, "c_t": pl.Int64, "s_t": pl.Float64,
        })

    grid = pl.concat(grids, how="diagonal_relaxed")
    grid = grid.cast({"t": pl.Int32, "stage_id": pl.Int32})
    km_counts = km_counts.cast({"t": pl.Int32, "stage_id": pl.Int32, "d_t": pl.Int64, "c_t": pl.Int64})

    steps = grid.join(
        km_counts.select(group_key + ["t", "d_t", "c_t"]),
        on=group_key + ["t"],
        how="left",
    )
    steps = steps.with_columns([
        pl.col("d_t").fill_null(0),
        pl.col("c_t").fill_null(0),
    ])

    steps = steps.sort(group_key + ["t"])
    steps = steps.with_columns(
        (pl.col("d_t") + pl.col("c_t")).alias("_exits")
    )
    steps = steps.with_columns(
        pl.col("_exits")
        .cum_sum()
        .shift(1)
        .over(group_key)
        .fill_null(0)
        .alias("exits_before_t")
    )
    steps = steps.with_columns(
        (pl.col("n_total") - pl.col("exits_before_t")).alias("n_at_risk")
    )

    steps = steps.with_columns(
        pl.when((pl.col("n_at_risk") > 0) & (pl.col("d_t") > 0) & (pl.col("d_t") >= pl.col("n_at_risk")))
        .then(1)
        .otherwise(0)
        .alias("step_zero")
    )
    steps = steps.with_columns(
        pl.col("step_zero")
        .cum_max()
        .over(group_key)
        .alias("cum_zero")
    )
    steps = steps.with_columns(
        pl.when((pl.col("n_at_risk") <= 0) | (pl.col("d_t") == 0))
        .then(0.0)
        .when(pl.col("d_t") >= pl.col("n_at_risk"))
        .then(0.0)
        .otherwise(
            ((pl.col("n_at_risk") - pl.col("d_t")).cast(pl.Float64) / pl.col("n_at_risk").cast(pl.Float64)).log()
        )
        .alias("ln_step")
    )

    steps = steps.with_columns(
        pl.when(pl.col("cum_zero") == 1)
        .then(0.0)
        .otherwise(
            pl.col("ln_step").cum_sum().over(group_key).exp()
        )
        .alias("s_t")
    )

    return steps.select(group_key + ["t", "n_total", "n_at_risk", "d_t", "c_t", "s_t"])


@dg.asset(
    group_name="funnel_ptd_core",
    description="Core: Kaplan-Meier survival curves for funnel stage transitions.",
)
def core_fptd_km_survival(
    context: dg.AssetExecutionContext,
    stg_gs_fptd_leads: pl.DataFrame,
    stg_gs_fptd_projects: pl.DataFrame,
) -> dict:
    ref_date = date.today() - timedelta(days=1)

    projects_base = _build_projects_base(stg_gs_fptd_leads, stg_gs_fptd_projects)

    km_raw = _build_km_raw(projects_base, ref_date)
    context.log.info(f"km_raw: {km_raw.height:,} observations")

    km_obs = _compute_km_observations(km_raw, ref_date)
    context.log.info(f"km_obs: {km_obs.height:,} valid observations")

    km_counts, km_n = _compute_km_counts_grouping(km_obs)
    context.log.info(f"km_counts: {km_counts.height:,} rows, km_n: {km_n.height} groups")

    km_survival = _compute_km_survival(km_counts, km_n)
    context.log.info(f"km_survival: {km_survival.height:,} rows")

    return {"km_survival": km_survival, "km_n": km_n}
