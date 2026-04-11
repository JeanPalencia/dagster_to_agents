#!/usr/bin/env python3
"""
Test: Rolling-Projects with Attribution Segmenters.

Runs the original Rolling-Projects query and the modified version (with channel,
traffic_type, campaign_name) against GeoSpot PostgreSQL, then verifies that
dropping the 3 new columns reproduces the original results row-by-row.

Usage:
    cd dagster-pipeline
    uv run python ../lakehouse-sdk/tests/pop_cohorts/test_rolling_projects_attribution.py
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

    _separator("1. Running ORIGINAL Rolling-Projects query")
    original_sql = (SQL_DIR / "Rolling-Projects.sql").read_text()
    t0 = time.time()
    df_original = query_bronze_source(original_sql, source_type="geospot_postgres")
    t_orig = time.time() - t0
    print(f"  Rows: {df_original.height:,}  |  Columns: {df_original.width}")
    print(f"  Time: {t_orig:.1f}s")
    print(f"  Columns: {df_original.columns}")

    _separator("2. Running MODIFIED Rolling-Projects query (with attribution)")
    modified_sql = (SQL_DIR / "Rolling-Projects-with-attribution.sql").read_text()
    t0 = time.time()
    df_modified = query_bronze_source(modified_sql, source_type="geospot_postgres")
    t_mod = time.time() - t0
    print(f"  Rows: {df_modified.height:,}  |  Columns: {df_modified.width}")
    print(f"  Time: {t_mod:.1f}s")
    print(f"  Columns: {df_modified.columns}")

    _separator("3. Column check")
    expected_extra = 3
    actual_extra = df_modified.width - df_original.width
    print(f"  Original columns:  {df_original.width}")
    print(f"  Modified columns:  {df_modified.width}")
    print(f"  Extra columns:     {actual_extra} (expected {expected_extra})")
    if actual_extra != expected_extra:
        print(f"  MISMATCH in column count!")
        return

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
    print(f"  Original rows: {df_original.height:,}")
    print(f"  Modified rows: {df_modified.height:,}")
    if df_original.height != df_modified.height:
        print(f"  MISMATCH: row counts differ!")
        return
    print(f"  Row count: MATCH")

    _separator("6. Row-by-row comparison (stripping new columns)")
    df_stripped = df_modified.drop(NEW_COLS)

    orig_sorted = df_original.sort(df_original.columns)
    stripped_sorted = df_stripped.sort(df_stripped.columns)

    orig_str = orig_sorted.cast({c: pl.Utf8 for c in orig_sorted.columns})
    stripped_str = stripped_sorted.cast({c: pl.Utf8 for c in stripped_sorted.columns})

    mismatches = 0
    for col in orig_str.columns:
        diffs = (orig_str[col] != stripped_str[col]).sum()
        if diffs > 0:
            mismatches += diffs
            print(f"  DIFF in '{col}': {diffs} rows differ")

    _separator("7. Result")
    if mismatches == 0:
        print("  ALL ROWS MATCH — the 3 new segmenters do not alter")
        print("  any existing column values. Safe to use in production.")
    else:
        print(f"  TOTAL MISMATCHES: {mismatches}")
        print("  Review the differences above.")

    print(f"\n  Performance: original {t_orig:.1f}s vs modified {t_mod:.1f}s")


if __name__ == "__main__":
    main()
