#!/usr/bin/env python3
"""
Validation script: compares Pandas original pipeline vs Dagster/Polars pipeline
for Spot State Transitions (SST).

Steps:
1. Runs the original Pandas pipeline (via update_spot_status_history_v3_dagster.py)
2. Materializes Dagster assets via dg.materialize() up to gold_lk_spot_status_history
3. Compares both DataFrames row-by-row, column-by-column

Usage:
    cd dagster-pipeline
    uv run python ../lakehouse-sdk/tests/spot_state_transitions/validate_sst_dagster.py
"""

import os
import sys
import pandas as pd
import polars as pl
import numpy as np

# Paths
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
DAGSTER_DIR = os.path.realpath(os.path.join(SCRIPT_DIR, "..", "..", "..", "dagster-pipeline"))

# Columns to compare (exclude aud_* audit fields, which only exist in Dagster)
COMPARABLE_COLUMNS = [
    "source_sst_id", "spot_id",
    "spot_status_id", "spot_status_reason_id", "spot_status_full_id",
    "prev_spot_status_id", "prev_spot_status_reason_id", "prev_spot_status_full_id",
    "next_spot_status_id", "next_spot_status_reason_id", "next_spot_status_full_id",
    "sstd_status_final_id", "sstd_status_id",
    "sstd_status_full_final_id", "sstd_status_full_id",
    "prev_source_sst_created_at", "next_source_sst_created_at",
    "minutes_since_prev_state", "minutes_until_next_state",
    "source_sst_created_at", "source_sst_updated_at",
    "source_id", "source",
    "spot_hard_deleted_id", "spot_hard_deleted",
]

SORT_KEYS = ["spot_id", "source_sst_created_at", "source_sst_id"]
FLOAT_TOLERANCE = 0.01


def run_pandas_pipeline() -> pd.DataFrame:
    """Runs the original Pandas pipeline and returns the result DataFrame."""
    sys.path.insert(0, SCRIPT_DIR)
    from update_spot_status_history_v3_dagster import update_incremental
    return update_incremental()


def run_dagster_pipeline() -> pl.DataFrame:
    """
    Runs the Dagster SST pipeline via dg.materialize() and returns the gold DataFrame.

    Uses the real assets — any change in the pipeline is automatically reflected here.
    """
    src_path = os.path.join(DAGSTER_DIR, "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    import dagster as dg
    from dagster_pipeline.defs.spot_state_transitions import (
        rawi_gs_sst_watermark,
        rawi_s2p_sst_new_transitions,
        raw_s2p_sst_affected_history,
        raw_gs_sst_snapshots,
        raw_gs_active_spot_ids,
        stg_s2p_sst_affected_history,
        stg_gs_sst_snapshots,
        stg_gs_active_spot_ids,
        core_sst_rebuild_history,
        gold_lk_spot_status_history,
    )

    all_assets = [
        rawi_gs_sst_watermark,
        rawi_s2p_sst_new_transitions,
        raw_s2p_sst_affected_history,
        raw_gs_sst_snapshots,
        raw_gs_active_spot_ids,
        stg_s2p_sst_affected_history,
        stg_gs_sst_snapshots,
        stg_gs_active_spot_ids,
        core_sst_rebuild_history,
        gold_lk_spot_status_history,
    ]

    gold_selection = dg.AssetSelection.assets(
        "gold_lk_spot_status_history"
    ).upstream()

    print("  Materializing Dagster assets...")
    result = dg.materialize(assets=all_assets, selection=gold_selection)

    if not result.success:
        print("  PIPELINE FAILED!")
        sys.exit(1)

    gold = result.output_for_node("gold_lk_spot_status_history")
    print(f"  Result: {gold.height} rows, {gold.width} columns")
    return gold


def normalize_pandas_for_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizes a Pandas DataFrame for comparison."""
    df = df.copy()

    # Select only comparable columns
    available = [c for c in COMPARABLE_COLUMNS if c in df.columns]
    df = df[available]

    # Sort deterministically
    sort_keys = [k for k in SORT_KEYS if k in df.columns]
    df = df.sort_values(sort_keys, kind="mergesort").reset_index(drop=True)

    # Normalize types
    for col in df.columns:
        if df[col].dtype.name in ("Int64", "int64"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif "datetime" in str(df[col].dtype):
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def normalize_polars_for_comparison(df: pl.DataFrame) -> pd.DataFrame:
    """Converts a Polars DataFrame to Pandas, normalized for comparison."""
    # Select only comparable columns
    available = [c for c in COMPARABLE_COLUMNS if c in df.columns]
    df = df.select(available)

    # Sort deterministically
    sort_keys = [k for k in SORT_KEYS if k in df.columns]
    df = df.sort(sort_keys)

    # Convert to Pandas
    df_pd = df.to_pandas()

    # Normalize types
    for col in df_pd.columns:
        if df_pd[col].dtype.name in ("Int64", "int64"):
            df_pd[col] = pd.to_numeric(df_pd[col], errors="coerce")
        elif "datetime" in str(df_pd[col].dtype):
            df_pd[col] = pd.to_datetime(df_pd[col], errors="coerce")

    return df_pd.reset_index(drop=True)


def compare_dataframes(df_pandas: pd.DataFrame, df_dagster: pd.DataFrame):
    """Compares two DataFrames row-by-row, column-by-column."""
    print(f"\n  Pandas shape:  {df_pandas.shape}")
    print(f"  Dagster shape: {df_dagster.shape}")

    if df_pandas.shape[0] != df_dagster.shape[0]:
        print(f"\n  ROW COUNT MISMATCH: Pandas={df_pandas.shape[0]}, Dagster={df_dagster.shape[0]}")

    common_cols = [c for c in df_pandas.columns if c in df_dagster.columns]
    pandas_only = [c for c in df_pandas.columns if c not in df_dagster.columns]
    dagster_only = [c for c in df_dagster.columns if c not in df_pandas.columns]

    if pandas_only:
        print(f"  Columns only in Pandas: {pandas_only}")
    if dagster_only:
        print(f"  Columns only in Dagster: {dagster_only}")

    print(f"\n  Comparing {len(common_cols)} common columns...")
    print(f"  {'Column':<35} {'Match':>8} {'Diff':>8} {'Status':>10}")
    print(f"  {'-'*35} {'-'*8} {'-'*8} {'-'*10}")

    all_match = True
    total_diffs = 0

    n_rows = min(df_pandas.shape[0], df_dagster.shape[0])

    for col in common_cols:
        pd_vals = df_pandas[col].iloc[:n_rows].tolist()
        dg_vals = df_dagster[col].iloc[:n_rows].tolist()

        matches = 0
        diffs = 0
        first_diffs = []

        for i, (a, b) in enumerate(zip(pd_vals, dg_vals)):
            a_null = a is None or a is pd.NaT or a is pd.NA
            b_null = b is None or b is pd.NaT or b is pd.NA
            if isinstance(a, float) and np.isnan(a):
                a_null = True
            if isinstance(b, float) and np.isnan(b):
                b_null = True

            if a_null and b_null:
                matches += 1
            elif a_null or b_null:
                diffs += 1
                if len(first_diffs) < 3:
                    first_diffs.append((i, a, b))
            elif isinstance(a, (int, float, np.integer, np.floating)) and isinstance(b, (int, float, np.integer, np.floating)):
                fa, fb = float(a), float(b)
                # For integer-valued floats, compare exactly
                if fa == fb or abs(fa - fb) < FLOAT_TOLERANCE:
                    matches += 1
                else:
                    diffs += 1
                    if len(first_diffs) < 3:
                        first_diffs.append((i, a, b))
            else:
                if str(a) == str(b):
                    matches += 1
                else:
                    diffs += 1
                    if len(first_diffs) < 3:
                        first_diffs.append((i, a, b))

        status = "PASS" if diffs == 0 else "FAIL"
        print(f"  {col:<35} {matches:>8} {diffs:>8} {status:>10}")

        if diffs > 0:
            all_match = False
            total_diffs += diffs
            for row_idx, val_a, val_b in first_diffs:
                print(f"    row {row_idx}: pandas={val_a!r} vs dagster={val_b!r}")

    return all_match, total_diffs


def main():
    print("=" * 70)
    print("VALIDATION: Pandas (original) vs Dagster (Polars) SST Pipeline")
    print("=" * 70)

    # Step 1: Run Pandas pipeline
    print("\n[1/3] Running Pandas pipeline...")
    df_pandas_raw = run_pandas_pipeline()
    if df_pandas_raw is None or df_pandas_raw.empty:
        print("  WARNING: Pandas pipeline returned empty/None")
        return

    # Step 2: Run Dagster pipeline (programmatic, same DB queries)
    print("\n[2/3] Running Dagster/Polars pipeline...")
    df_dagster_raw = run_dagster_pipeline()
    if df_dagster_raw.height == 0:
        print("  WARNING: Dagster pipeline returned empty")
        return
    print(f"  Result: {df_dagster_raw.height} rows, {df_dagster_raw.width} columns")

    # Step 3: Compare
    print("\n[3/3] Comparing results...")
    df_pandas_norm = normalize_pandas_for_comparison(df_pandas_raw)
    df_dagster_norm = normalize_polars_for_comparison(df_dagster_raw)

    all_match, total_diffs = compare_dataframes(df_pandas_norm, df_dagster_norm)

    print("\n" + "=" * 70)
    if all_match and df_pandas_norm.shape[0] == df_dagster_norm.shape[0]:
        print(f"RESULT: 100% MATCH ({df_pandas_norm.shape[0]} rows, {len(COMPARABLE_COLUMNS)} columns)")
    else:
        print(f"RESULT: DIFFERENCES FOUND ({total_diffs} total mismatches)")
    print("=" * 70)


if __name__ == "__main__":
    main()
