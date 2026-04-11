#!/usr/bin/env python3
"""
Test: PoP-Cohorts with Attribution Segmenters.

Runs the original PoP-Cohorts query and the modified version (with channel,
traffic_type, campaign_name) against GeoSpot PostgreSQL, then verifies that
collapsing the new query's extra dimensions reproduces the original results.

Usage:
    cd dagster-pipeline
    uv run python ../lakehouse-sdk/tests/pop_cohorts/test_pop_cohorts_attribution.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import polars as pl


def _ensure_import_paths() -> None:
    root = Path(__file__).resolve().parents[3]
    dagster_src = root / "dagster-pipeline" / "src"
    if str(dagster_src) not in sys.path:
        sys.path.insert(0, str(dagster_src))


def _separator(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}\n")


SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "old"

ORIGINAL_DIMS = [
    "comparison_type",
    "current_period_start",
    "current_period_end",
    "previous_period_start",
    "previous_period_end",
    "cohort_type",
    "lead_max_type",
    "spot_sector",
    "mode",
]

METRIC_COLS = [
    "leads_current",
    "projects_current",
    "scheduled_visits_current",
    "completed_visits_current",
    "lois_current",
    "won_current",
    "leads_prev",
    "projects_prev",
    "scheduled_visits_prev",
    "completed_visits_prev",
    "lois_prev",
    "won_prev",
]

NEW_DIMS = ["channel", "traffic_type", "campaign_name"]


def main() -> None:
    _ensure_import_paths()

    from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

    _separator("1. Running ORIGINAL PoP-Cohorts query")
    original_sql = (SQL_DIR / "PoP-Cohorts.sql").read_text()
    t0 = time.time()
    df_original = query_bronze_source(original_sql, source_type="geospot_postgres")
    t_orig = time.time() - t0
    print(f"  Rows: {df_original.height:,}  |  Columns: {df_original.width}")
    print(f"  Time: {t_orig:.1f}s")
    print(f"  Columns: {df_original.columns}")

    _separator("2. Running MODIFIED PoP-Cohorts query (with attribution)")
    modified_sql = (SQL_DIR / "PoP-Cohorts-with-attribution.sql").read_text()
    t0 = time.time()
    df_modified = query_bronze_source(modified_sql, source_type="geospot_postgres")
    t_mod = time.time() - t0
    print(f"  Rows: {df_modified.height:,}  |  Columns: {df_modified.width}")
    print(f"  Time: {t_mod:.1f}s")
    print(f"  Columns: {df_modified.columns}")

    _separator("3. New segmenters: distinct values")
    for col in NEW_DIMS:
        if col in df_modified.columns:
            vals = df_modified[col].unique().sort().to_list()
            print(f"  {col}: {len(vals)} distinct values")
            for v in vals[:20]:
                print(f"    - {v}")
            if len(vals) > 20:
                print(f"    ... and {len(vals) - 20} more")
        else:
            print(f"  WARNING: column '{col}' not found in modified output!")

    _separator("4. Collapsing modified query and comparing with original")

    agg_exprs = [pl.col(c).sum().alias(c) for c in METRIC_COLS if c in df_modified.columns]

    df_collapsed = (
        df_modified
        .group_by(ORIGINAL_DIMS)
        .agg(agg_exprs)
        .sort(ORIGINAL_DIMS)
    )

    df_orig_sorted = df_original.sort(ORIGINAL_DIMS)

    print(f"  Original rows:  {df_orig_sorted.height:,}")
    print(f"  Collapsed rows: {df_collapsed.height:,}")

    if df_orig_sorted.height != df_collapsed.height:
        print(f"\n  MISMATCH: row count differs!")
        print(f"  Original dims not in collapsed:")
        orig_keys = set(
            df_orig_sorted.select(ORIGINAL_DIMS).unique().rows()
        )
        collapsed_keys = set(
            df_collapsed.select(ORIGINAL_DIMS).unique().rows()
        )
        only_orig = orig_keys - collapsed_keys
        only_collapsed = collapsed_keys - orig_keys
        if only_orig:
            print(f"    Only in original ({len(only_orig)}):")
            for r in list(only_orig)[:5]:
                print(f"      {r}")
        if only_collapsed:
            print(f"    Only in collapsed ({len(only_collapsed)}):")
            for r in list(only_collapsed)[:5]:
                print(f"      {r}")
    else:
        print(f"  Row count: MATCH")

    available_metrics = [c for c in METRIC_COLS if c in df_orig_sorted.columns and c in df_collapsed.columns]

    df_orig_sorted = df_orig_sorted.select(ORIGINAL_DIMS + available_metrics)
    df_collapsed = df_collapsed.select(ORIGINAL_DIMS + available_metrics)

    df_orig_sorted = df_orig_sorted.cast(
        {c: pl.Int64 for c in available_metrics}
    )
    df_collapsed = df_collapsed.cast(
        {c: pl.Int64 for c in available_metrics}
    )

    joined = df_orig_sorted.join(
        df_collapsed,
        on=ORIGINAL_DIMS,
        how="outer",
        suffix="_new",
    )

    mismatches = 0
    for metric in available_metrics:
        col_orig = metric
        col_new = f"{metric}_new"
        if col_new not in joined.columns:
            continue
        diffs = joined.filter(
            pl.col(col_orig).fill_null(0) != pl.col(col_new).fill_null(0)
        )
        if diffs.height > 0:
            mismatches += diffs.height
            print(f"\n  DIFF in '{metric}': {diffs.height} rows differ")
            sample = diffs.head(5).select(
                ORIGINAL_DIMS + [col_orig, col_new]
            )
            print(sample)

    _separator("5. Result")
    if mismatches == 0:
        print("  ALL METRICS MATCH — the 3 new segmenters do not alter")
        print("  any aggregate values. Safe to use in production.")
    else:
        print(f"  TOTAL MISMATCHES: {mismatches}")
        print("  Review the differences above.")

    print(f"\n  Performance: original {t_orig:.1f}s vs modified {t_mod:.1f}s")


if __name__ == "__main__":
    main()
