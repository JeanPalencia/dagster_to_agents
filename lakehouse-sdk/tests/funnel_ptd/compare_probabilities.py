#!/usr/bin/env python3
"""
Diagnostic: compare per-project KM probabilities between PostgreSQL and Polars.

Extracts individual prob_complete_current_stage_by_eop values (before SUM
aggregation) from both systems and compares them side by side to locate
the source of the lois_proj discrepancy.

Usage (from dagster-pipeline directory):
    cd dagster-pipeline && uv run python ../../lakehouse-sdk/tests/funnel_ptd/compare_probabilities.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import dagster as dg
import polars as pl

DAGSTER_SRC = Path(__file__).resolve().parents[3] / "dagster-pipeline" / "src"
sys.path.insert(0, str(DAGSTER_SRC))

from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source
from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_projections import (
    _build_projection_scope,
    _compute_projection_horizon,
    _choose_curve,
    _compute_probabilities,
)

SQL_FILE = Path(__file__).resolve().parent / ".." / "period_to_date" / "period_to_date.sql"

FILTER_COUNTING = "Rolling"
FILTER_PTD = "Month to Date"
FILTER_PERIOD_PREFIX = "2026-03"
FILTER_STAGE = 3

PROB_COLS = [
    "project_id", "t0_c", "t1_c", "s_t0", "s_t1",
    "prob_complete_current_stage_by_eop",
    "used_curve_level", "used_counting_method", "used_cohort_type",
]


_CUSTOM_SELECT = """

SELECT
    p.project_id,
    p.counting_method,
    p.ptd_type,
    p.period_start,
    p.current_stage_id,
    p.t0_c,
    p.t1_c,
    p.s_t0,
    p.s_t1,
    p.prob_complete_current_stage_by_eop,
    p.used_curve_level,
    p.used_counting_method,
    p.used_cohort_type
FROM probabilities p
WHERE p.counting_method = '{counting}'
  AND p.ptd_type = '{ptd}'
  AND p.period_start >= '{prefix}-01'
  AND p.current_stage_id = {stage}
ORDER BY p.project_id
"""


def _extract_inner_ctes() -> str:
    """Extract inner CTEs (params..probabilities) from the projections block."""
    full_sql = SQL_FILE.read_text()

    inner_start_marker = "\tWITH params AS ("
    inner_start = full_sql.find(inner_start_marker)
    if inner_start == -1:
        raise ValueError("Cannot find inner CTE start (WITH params AS)")

    agg_marker = ")\n\n\tSELECT\n\t    p.counting_method"
    agg_idx = full_sql.find(agg_marker)
    if agg_idx == -1:
        raise ValueError("Cannot find the aggregation SELECT after probabilities CTE")

    return full_sql[inner_start : agg_idx + 1]


def _build_pg_query(*, force_float8: bool = False) -> str:
    """Build query to extract per-project probabilities.

    If force_float8=True, casts KM intermediate calculations to float8
    (double precision) to match Polars f64 precision.
    """
    inner_ctes = _extract_inner_ctes()

    if force_float8:
        # Force ln_step to use float8 instead of numeric
        inner_ctes = inner_ctes.replace(
            "ELSE LN(((r.n_at_risk - r.d_t)::numeric) / NULLIF(r.n_at_risk, 0))",
            "ELSE LN(((r.n_at_risk - r.d_t)::float8) / NULLIF(r.n_at_risk::float8, 0))",
        )
        # Force S(t) exp/sum to use float8
        inner_ctes = inner_ctes.replace(
            "WHEN cum_zero = 1 THEN 0::numeric",
            "WHEN cum_zero = 1 THEN 0::float8",
        )

    select = _CUSTOM_SELECT.format(
        counting=FILTER_COUNTING,
        ptd=FILTER_PTD,
        prefix=FILTER_PERIOD_PREFIX,
        stage=FILTER_STAGE,
    )
    return inner_ctes + select


def extract_pg_probabilities(*, force_float8: bool = False) -> pl.DataFrame:
    """Run modified SQL against GeoSpot and return per-project probabilities."""
    label = "PG-float8" if force_float8 else "PG-numeric"
    sql = _build_pg_query(force_float8=force_float8)
    print(f"Executing {label} query ({len(sql):,} chars)...")
    df = query_bronze_source(sql, source_type="geospot_postgres")
    print(f"{label}: {df.height} rows returned (stage {FILTER_STAGE}, {FILTER_COUNTING} {FILTER_PTD})")
    return df


def extract_polars_probabilities() -> pl.DataFrame:
    """Materialize pipeline assets and compute per-project probabilities."""
    from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_lk_leads import raw_gs_lk_leads
    from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_lk_projects_v2 import raw_gs_lk_projects_v2
    from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_lk_okrs import raw_gs_lk_okrs
    from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_bt_lds_lead_spots import raw_gs_bt_lds_lead_spots
    from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_leads import stg_gs_fptd_leads
    from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_projects import stg_gs_fptd_projects
    from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_okrs import stg_gs_fptd_okrs
    from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_lead_spots import stg_gs_fptd_lead_spots
    from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_current_counts import core_fptd_current_counts
    from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_monitor import core_fptd_monitor
    from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_km_survival import core_fptd_km_survival
    from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_projections import core_fptd_projections

    all_assets = [
        raw_gs_lk_leads, raw_gs_lk_projects_v2, raw_gs_lk_okrs, raw_gs_bt_lds_lead_spots,
        stg_gs_fptd_leads, stg_gs_fptd_projects, stg_gs_fptd_okrs, stg_gs_fptd_lead_spots,
        core_fptd_current_counts, core_fptd_monitor, core_fptd_km_survival,
        core_fptd_projections,
    ]

    print("Materializing Dagster assets...")
    result = dg.materialize(
        assets=all_assets,
        selection=dg.AssetSelection.assets("core_fptd_projections").upstream(),
    )

    monitor = result.output_for_node("core_fptd_monitor")
    km_dict = result.output_for_node("core_fptd_km_survival")
    km_survival = km_dict["km_survival"]
    km_n = km_dict["km_n"]

    scope = _build_projection_scope(monitor)
    horizon = _compute_projection_horizon(scope)
    curve_choice = _choose_curve(horizon, km_n)
    probs = _compute_probabilities(curve_choice, km_survival)

    filtered = probs.filter(
        (pl.col("counting_method") == FILTER_COUNTING)
        & (pl.col("ptd_type") == FILTER_PTD)
        & (pl.col("period_start").cast(pl.Utf8).str.starts_with(FILTER_PERIOD_PREFIX))
        & (pl.col("current_stage_id") == FILTER_STAGE)
    ).select(PROB_COLS).sort("project_id")

    print(f"Polars: {filtered.height} rows (stage {FILTER_STAGE}, {FILTER_COUNTING} {FILTER_PTD})")
    return filtered


def compare(polars_df: pl.DataFrame, pg_df: pl.DataFrame, label: str = "PG") -> None:
    """Join both DataFrames on project_id and report differences."""
    left = polars_df.select([
        pl.col("project_id"),
        pl.col("t0_c").alias("polars_t0_c"),
        pl.col("t1_c").alias("polars_t1_c"),
        pl.col("s_t0").alias("polars_s_t0"),
        pl.col("s_t1").alias("polars_s_t1"),
        pl.col("prob_complete_current_stage_by_eop").alias("polars_prob"),
        pl.col("used_curve_level").alias("polars_curve"),
    ])

    right = pg_df.select([
        pl.col("project_id").cast(pl.Int64),
        pl.col("t0_c").cast(pl.Int32).alias("pg_t0_c"),
        pl.col("t1_c").cast(pl.Int32).alias("pg_t1_c"),
        pl.col("s_t0").cast(pl.Float64).alias("pg_s_t0"),
        pl.col("s_t1").cast(pl.Float64).alias("pg_s_t1"),
        pl.col("prob_complete_current_stage_by_eop").cast(pl.Float64).alias("pg_prob"),
        pl.col("used_curve_level").alias("pg_curve"),
    ])

    merged = left.join(right, on="project_id", how="full", coalesce=True)

    only_polars = merged.filter(pl.col("pg_prob").is_null() & pl.col("polars_prob").is_not_null())
    only_pg = merged.filter(pl.col("polars_prob").is_null() & pl.col("pg_prob").is_not_null())
    both = merged.filter(pl.col("polars_prob").is_not_null() & pl.col("pg_prob").is_not_null())

    print("\n" + "=" * 70)
    print(f"COMPARISON: Polars vs {label}")
    print("=" * 70)
    print(f"Only in Polars:     {only_polars.height}")
    print(f"Only in PostgreSQL: {only_pg.height}")
    print(f"In both:            {both.height}")

    if only_polars.height > 0:
        print("\nProjects only in Polars:")
        print(only_polars.select(["project_id", "polars_t0_c", "polars_t1_c", "polars_prob"]))

    if only_pg.height > 0:
        print("\nProjects only in PostgreSQL:")
        print(only_pg.select(["project_id", "pg_t0_c", "pg_t1_c", "pg_prob"]))

    if both.height == 0:
        print("\nNo common projects to compare.")
        return

    # Curve level mismatches
    curve_mismatch = both.filter(pl.col("polars_curve") != pl.col("pg_curve"))
    print(f"\nCurve level mismatches: {curve_mismatch.height}")
    if curve_mismatch.height > 0:
        print(curve_mismatch.select(["project_id", "polars_curve", "pg_curve"]).head(10))

    # t0/t1 mismatches
    t_mismatch = both.filter(
        (pl.col("polars_t0_c") != pl.col("pg_t0_c"))
        | (pl.col("polars_t1_c") != pl.col("pg_t1_c"))
    )
    print(f"t0/t1 mismatches:       {t_mismatch.height}")
    if t_mismatch.height > 0:
        print(t_mismatch.select([
            "project_id", "polars_t0_c", "pg_t0_c", "polars_t1_c", "pg_t1_c",
        ]).head(10))

    # S(t) differences
    both = both.with_columns([
        (pl.col("polars_s_t0") - pl.col("pg_s_t0")).abs().alias("s_t0_diff"),
        (pl.col("polars_s_t1") - pl.col("pg_s_t1")).abs().alias("s_t1_diff"),
        (pl.col("polars_prob") - pl.col("pg_prob")).abs().alias("prob_diff"),
    ])

    print(f"\nS(t0) diffs > 1e-10:    {both.filter(pl.col('s_t0_diff') > 1e-10).height}")
    print(f"S(t0) diffs > 1e-6:     {both.filter(pl.col('s_t0_diff') > 1e-6).height}")
    print(f"S(t0) diffs > 0.001:    {both.filter(pl.col('s_t0_diff') > 0.001).height}")

    print(f"\nS(t1) diffs > 1e-10:    {both.filter(pl.col('s_t1_diff') > 1e-10).height}")
    print(f"S(t1) diffs > 1e-6:     {both.filter(pl.col('s_t1_diff') > 1e-6).height}")
    print(f"S(t1) diffs > 0.001:    {both.filter(pl.col('s_t1_diff') > 0.001).height}")

    print(f"\nProb diffs > 1e-10:     {both.filter(pl.col('prob_diff') > 1e-10).height}")
    print(f"Prob diffs > 1e-6:      {both.filter(pl.col('prob_diff') > 1e-6).height}")
    print(f"Prob diffs > 0.001:     {both.filter(pl.col('prob_diff') > 0.001).height}")
    print(f"Prob diffs > 0.01:      {both.filter(pl.col('prob_diff') > 0.01).height}")
    print(f"Prob diffs > 0.1:       {both.filter(pl.col('prob_diff') > 0.1).height}")

    sum_polars = both["polars_prob"].sum()
    sum_pg = both["pg_prob"].sum()
    print(f"\nSum Polars prob:  {sum_polars:.10f}")
    print(f"Sum PG prob:      {sum_pg:.10f}")
    print(f"Difference:       {sum_polars - sum_pg:.10f}")
    print(f"Polars rounded:   {round(sum_polars)}")
    print(f"PG rounded:       {round(sum_pg)}")

    # Top 10 largest prob differences
    top = both.sort("prob_diff", descending=True).head(10)
    print("\nTop 10 largest prob differences:")
    print(top.select([
        "project_id",
        "polars_t0_c", "pg_t0_c",
        "polars_t1_c", "pg_t1_c",
        "polars_s_t0", "pg_s_t0",
        "polars_s_t1", "pg_s_t1",
        "polars_prob", "pg_prob",
        "prob_diff",
    ]))


def _build_km_curve_query() -> str:
    """Extract km_survival_final values from PostgreSQL for the relevant curve."""
    inner_ctes = _extract_inner_ctes()

    # Cut right after km_survival_final closes: "FROM km_zero\n\t),"
    end_marker = "FROM km_zero\n\t),"
    end_idx = inner_ctes.find(end_marker)
    if end_idx == -1:
        raise ValueError("Cannot find end of km_survival_final (FROM km_zero)")

    # Include the closing paren but not the comma
    trimmed = inner_ctes[: end_idx + len(end_marker) - 1]

    return trimmed + """

SELECT
    curve_level,
    counting_method,
    cohort_type,
    stage_id,
    t,
    n_total,
    n_at_risk,
    d_t,
    c_t,
    s_t
FROM km_survival_final
ORDER BY curve_level, counting_method, cohort_type, stage_id, t
"""


def _build_km_n_query() -> str:
    """Extract km_n values from PostgreSQL."""
    inner_ctes = _extract_inner_ctes()

    # Cut right after km_n CTE closes, before km_grid
    # km_n ends with "GROUP BY ..." then ")," before km_grid
    end_marker = "\tkm_grid AS ("
    end_idx = inner_ctes.find(end_marker)
    if end_idx == -1:
        raise ValueError("Cannot find km_grid CTE")

    trimmed = inner_ctes[:end_idx].rstrip().rstrip(",")

    return trimmed + """

SELECT
    curve_level,
    counting_method,
    cohort_type,
    stage_id,
    n_total,
    n_events,
    max_t
FROM km_n
ORDER BY curve_level, counting_method, cohort_type, stage_id
"""


def compare_km_curves(polars_probs: pl.DataFrame) -> None:
    """Compare KM survival curves between PostgreSQL and Polars."""
    from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_km_survival import (
        _compute_km_survival,
    )

    # Determine which curve the 311 projects use
    curve_info = polars_probs.select([
        "used_curve_level", "used_counting_method", "used_cohort_type",
    ]).unique()
    print("\n" + "=" * 70)
    print("KM CURVE COMPARISON")
    print("=" * 70)
    print(f"Curves used by filtered projects:")
    print(curve_info)

    # --- Get Polars KM data from the pipeline ---
    from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_lk_leads import raw_gs_lk_leads
    from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_lk_projects_v2 import raw_gs_lk_projects_v2
    from dagster_pipeline.defs.funnel_ptd.bronze.raw_gs_bt_lds_lead_spots import raw_gs_bt_lds_lead_spots
    from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_leads import stg_gs_fptd_leads
    from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_projects import stg_gs_fptd_projects
    from dagster_pipeline.defs.funnel_ptd.silver.stg.stg_gs_fptd_lead_spots import stg_gs_fptd_lead_spots
    from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_km_survival import core_fptd_km_survival

    result = dg.materialize(
        assets=[
            raw_gs_lk_leads, raw_gs_lk_projects_v2, raw_gs_bt_lds_lead_spots,
            stg_gs_fptd_leads, stg_gs_fptd_projects, stg_gs_fptd_lead_spots,
            core_fptd_km_survival,
        ],
        selection=dg.AssetSelection.assets("core_fptd_km_survival").upstream(),
    )
    km_dict = result.output_for_node("core_fptd_km_survival")
    polars_km = km_dict["km_survival"]
    polars_km_n = km_dict["km_n"]

    # --- Get PG KM data ---
    print("Querying PG km_survival_final...")
    pg_km_sql = _build_km_curve_query()
    pg_km = query_bronze_source(pg_km_sql, source_type="geospot_postgres")
    print(f"PG km_survival: {pg_km.height} rows")

    print("Querying PG km_n...")
    pg_km_n_sql = _build_km_n_query()
    pg_km_n = query_bronze_source(pg_km_n_sql, source_type="geospot_postgres")
    print(f"PG km_n: {pg_km_n.height} rows")

    # --- Compare km_n ---
    print("\n--- km_n comparison ---")
    pk = polars_km_n.sort(["curve_level", "counting_method", "cohort_type", "stage_id"])
    gk = pg_km_n.sort(["curve_level", "counting_method", "cohort_type", "stage_id"])
    print(f"Polars km_n: {pk.height} rows, PG km_n: {gk.height} rows")

    # Filter to stage 3 for detailed view
    pk3 = pk.filter(pl.col("stage_id") == FILTER_STAGE)
    gk3 = gk.filter(pl.col("stage_id").cast(pl.Int32) == FILTER_STAGE)
    print(f"\nPolars km_n (stage {FILTER_STAGE}):")
    print(pk3)
    print(f"\nPG km_n (stage {FILTER_STAGE}):")
    print(gk3)

    # --- Compare KM curves for the specific curve used ---
    for row in curve_info.iter_rows(named=True):
        cl = row["used_curve_level"]
        cm = row["used_counting_method"]
        ct = row["used_cohort_type"]
        print(f"\n--- KM curve: {cl}, {cm}, {ct}, stage {FILTER_STAGE} ---")

        p_curve = polars_km.filter(
            (pl.col("curve_level") == cl)
            & (pl.col("counting_method") == cm)
            & (pl.col("cohort_type") == ct)
            & (pl.col("stage_id") == FILTER_STAGE)
        ).sort("t")

        g_curve = pg_km.filter(
            (pl.col("curve_level") == cl)
            & (pl.col("counting_method") == cm)
            & (pl.col("cohort_type") == ct)
            & (pl.col("stage_id").cast(pl.Int32) == FILTER_STAGE)
        ).sort("t")

        print(f"Polars curve: {p_curve.height} time points")
        print(f"PG curve:     {g_curve.height} time points")

        if p_curve.height == 0 or g_curve.height == 0:
            print("SKIP: empty curve")
            continue

        # Join on t
        merged = p_curve.select([
            pl.col("t"),
            pl.col("n_total").alias("p_n_total"),
            pl.col("n_at_risk").alias("p_n_at_risk"),
            pl.col("d_t").alias("p_d_t"),
            pl.col("c_t").alias("p_c_t"),
            pl.col("s_t").alias("p_s_t"),
        ]).join(
            g_curve.select([
                pl.col("t").cast(pl.Int32),
                pl.col("n_total").cast(pl.Int64).alias("g_n_total"),
                pl.col("n_at_risk").cast(pl.Int64).alias("g_n_at_risk"),
                pl.col("d_t").cast(pl.Int64).alias("g_d_t"),
                pl.col("c_t").cast(pl.Int64).alias("g_c_t"),
                pl.col("s_t").cast(pl.Float64).alias("g_s_t"),
            ]),
            on="t",
            how="full",
            coalesce=True,
        ).sort("t")

        # Check n_total
        n_total_diff = merged.filter(pl.col("p_n_total") != pl.col("g_n_total"))
        print(f"\nn_total mismatches:   {n_total_diff.height}")
        if n_total_diff.height > 0:
            print(n_total_diff.head(5))

        # Check d_t
        d_t_diff = merged.filter(pl.col("p_d_t") != pl.col("g_d_t"))
        print(f"d_t mismatches:       {d_t_diff.height}")
        if d_t_diff.height > 0:
            print(d_t_diff.select(["t", "p_d_t", "g_d_t", "p_n_at_risk", "g_n_at_risk"]).head(10))

        # Check c_t
        c_t_diff = merged.filter(pl.col("p_c_t") != pl.col("g_c_t"))
        print(f"c_t mismatches:       {c_t_diff.height}")
        if c_t_diff.height > 0:
            print(c_t_diff.select(["t", "p_c_t", "g_c_t"]).head(10))

        # Check n_at_risk
        n_risk_diff = merged.filter(pl.col("p_n_at_risk") != pl.col("g_n_at_risk"))
        print(f"n_at_risk mismatches: {n_risk_diff.height}")
        if n_risk_diff.height > 0:
            print(n_risk_diff.select(["t", "p_n_at_risk", "g_n_at_risk", "p_d_t", "g_d_t", "p_c_t", "g_c_t"]).head(10))

        # S(t) differences
        merged = merged.with_columns(
            (pl.col("p_s_t") - pl.col("g_s_t")).abs().alias("s_t_diff")
        )
        print(f"\nS(t) diffs > 1e-15:   {merged.filter(pl.col('s_t_diff') > 1e-15).height}")
        print(f"S(t) diffs > 1e-10:   {merged.filter(pl.col('s_t_diff') > 1e-10).height}")
        print(f"S(t) diffs > 0.001:   {merged.filter(pl.col('s_t_diff') > 0.001).height}")

        # Show first few rows where S(t) starts diverging
        first_diff = merged.filter(pl.col("s_t_diff") > 1e-15).head(5)
        if first_diff.height > 0:
            print(f"\nFirst time points where S(t) diverges:")
            print(first_diff.select(["t", "p_n_at_risk", "g_n_at_risk", "p_d_t", "g_d_t", "p_c_t", "g_c_t", "p_s_t", "g_s_t", "s_t_diff"]))


def main() -> None:
    pg_numeric = extract_pg_probabilities(force_float8=False)
    polars_df = extract_polars_probabilities()

    compare(polars_df, pg_numeric, label="PG-numeric (original)")

    # Deep dive: compare KM curves directly
    compare_km_curves(polars_df)


if __name__ == "__main__":
    main()
