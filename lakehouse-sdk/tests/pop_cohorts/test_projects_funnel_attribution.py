#!/usr/bin/env python3
"""
Test: Projects-Funnel with Attribution Segmenters.

Runs the original Projects-Funnel query and the modified version (with channel,
traffic_type, campaign_name) against GeoSpot PostgreSQL, then verifies that
dropping the 3 new columns reproduces the original results exactly.

Usage:
    cd dagster-pipeline
    uv run python ../lakehouse-sdk/tests/pop_cohorts/test_projects_funnel_attribution.py
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
NEW_COLS = ["channel", "traffic_type", "campaign_name"]


def main() -> None:
    _ensure_import_paths()

    from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

    _separator("1. Running ORIGINAL Projects-Funnel query")
    original_sql = (SQL_DIR / "Projects-Funnel.sql").read_text()
    t0 = time.time()
    df_original = query_bronze_source(original_sql, source_type="geospot_postgres")
    t_orig = time.time() - t0
    print(f"  Rows: {df_original.height:,}  |  Columns: {df_original.width}")
    print(f"  Time: {t_orig:.1f}s")
    print(f"  Columns: {df_original.columns}")

    _separator("2. Running MODIFIED Projects-Funnel query (with attribution)")
    modified_sql = (SQL_DIR / "Projects-Funnel-with-attribution.sql").read_text()
    t0 = time.time()
    df_modified = query_bronze_source(modified_sql, source_type="geospot_postgres")
    t_mod = time.time() - t0
    print(f"  Rows: {df_modified.height:,}  |  Columns: {df_modified.width}")
    print(f"  Time: {t_mod:.1f}s")
    print(f"  Columns: {df_modified.columns}")

    _separator("3. Column check")
    orig_cols = set(df_original.columns)
    mod_cols = set(df_modified.columns)
    extra_cols = mod_cols - orig_cols
    missing_cols = orig_cols - mod_cols
    print(f"  Original columns:  {len(orig_cols)}")
    print(f"  Modified columns:  {len(mod_cols)}")
    print(f"  Extra (new):       {extra_cols}")
    if missing_cols:
        print(f"  MISSING (ERROR):   {missing_cols}")
    expected_extra = set(NEW_COLS)
    if extra_cols == expected_extra:
        print(f"  Column diff: MATCH (exactly {NEW_COLS})")
    else:
        print(f"  Column diff: UNEXPECTED — expected {expected_extra}, got {extra_cols}")

    _separator("4. New segmenters: distinct values")
    for col in NEW_COLS:
        if col in df_modified.columns:
            vals = df_modified[col].unique().sort().to_list()
            print(f"  {col}: {len(vals)} distinct values")
            for v in vals[:15]:
                print(f"    - {v}")
            if len(vals) > 15:
                print(f"    ... and {len(vals) - 15} more")

    _separator("5. Row count comparison")
    print(f"  Original: {df_original.height:,}")
    print(f"  Modified: {df_modified.height:,}")
    if df_original.height == df_modified.height:
        print(f"  Row count: MATCH")
    else:
        print(f"  Row count: MISMATCH (expected same count)")
        print(f"  NOTE: This may indicate a problem with the JOIN")

    _separator("6. Data integrity: drop new cols and compare")

    df_mod_stripped = df_modified.drop(NEW_COLS)

    shared_cols = [c for c in df_original.columns if c in df_mod_stripped.columns]
    df_orig_cmp = df_original.select(shared_cols)
    df_mod_cmp = df_mod_stripped.select(shared_cols)

    for col in shared_cols:
        if df_orig_cmp[col].dtype != df_mod_cmp[col].dtype:
            try:
                target = df_orig_cmp[col].dtype
                df_mod_cmp = df_mod_cmp.with_columns(pl.col(col).cast(target))
            except Exception:
                pass

    if df_orig_cmp.height != df_mod_cmp.height:
        print(f"  Cannot compare: row counts differ ({df_orig_cmp.height} vs {df_mod_cmp.height})")
    else:
        mismatches = 0
        for col in shared_cols:
            orig_series = df_orig_cmp[col]
            mod_series = df_mod_cmp[col]
            ne = (orig_series.ne_missing(mod_series)).sum()
            if ne > 0:
                mismatches += ne
                print(f"  DIFF in '{col}': {ne} rows differ")
                diff_idx = (orig_series.ne_missing(mod_series)).arg_true()
                for i in diff_idx[:3].to_list():
                    print(f"    row {i}: original={orig_series[i]} vs modified={mod_series[i]}")

        if mismatches == 0:
            print(f"  ALL {len(shared_cols)} COLUMNS MATCH across {df_orig_cmp.height:,} rows")
            print(f"  The 3 new segmenters do not alter any existing data.")

    _separator("7. Result")
    row_ok = df_original.height == df_modified.height
    col_ok = extra_cols == expected_extra and not missing_cols
    if row_ok and col_ok and mismatches == 0:
        print("  PASS — Safe to use in production.")
    else:
        problems = []
        if not row_ok:
            problems.append("row count mismatch")
        if not col_ok:
            problems.append("column mismatch")
        if mismatches > 0:
            problems.append(f"{mismatches} data mismatches")
        print(f"  ISSUES: {', '.join(problems)}")

    print(f"\n  Performance: original {t_orig:.1f}s vs modified {t_mod:.1f}s")


if __name__ == "__main__":
    main()
