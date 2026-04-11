#!/usr/bin/env python3
"""
Test: funnel_with_channel LLM classification fix.

Validates that moving the "Organic LLMs" rule BEFORE "Nulo-Vacío" in the
Channel CASE WHEN fixes misclassified LLM leads (ChatGPT, Perplexity, etc.)
without affecting any other channel assignments.

Uses a single-pass comparison query (compare_channel_diff.sql) to avoid
double-scanning BigQuery, then optionally runs both full queries for
aggregate validation.

Usage:
    cd dagster-pipeline
    uv run python ../lakehouse-sdk/tests/funnel_channel_llm/test_funnel_llm_fix.py
"""
from __future__ import annotations

import re
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
    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}\n")


SQL_DIR = Path(__file__).resolve().parent

LLM_SOURCE_PATTERN = re.compile(
    r"(chatgpt|perplexity|claude|bard|gemini|anthropic|openai|llm|ai\s*chat|ai\s*assistant)",
    re.IGNORECASE,
)


def main() -> None:
    _ensure_import_paths()
    from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

    # ------------------------------------------------------------------
    # STEP 1: Single-pass comparison (efficient — one BQ scan)
    # ------------------------------------------------------------------
    _separator("1. Running comparison query (single BigQuery scan)")
    compare_sql = (SQL_DIR / "compare_channel_diff.sql").read_text()
    t0 = time.time()
    df_diff = query_bronze_source(compare_sql, source_type="bigquery")
    t_compare = time.time() - t0

    if hasattr(df_diff, "to_pandas"):
        df_diff_pd = df_diff.to_pandas()
    else:
        df_diff_pd = df_diff

    print(f"  Time: {t_compare:.1f}s")
    print(f"  Rows with different classification: {len(df_diff_pd)}")

    if df_diff_pd.empty:
        _separator("RESULT")
        print("  No differences found between original and fixed classification.")
        print("  This means either there are no LLM sources with empty medium,")
        print("  or the fix has no effect. Check the data range.")
        return

    # ------------------------------------------------------------------
    # STEP 2: Analyze which sources changed
    # ------------------------------------------------------------------
    _separator("2. Differences detail (original -> fixed)")

    total_sessions = int(df_diff_pd["user_sessions"].sum())
    total_users = int(df_diff_pd["unique_users"].sum())
    print(f"  Total sessions affected: {total_sessions:,}")
    print(f"  Total unique users affected: {total_users:,}")

    print(f"\n  Transitions (channel_original -> channel_fixed):")
    transitions = (
        df_diff_pd.groupby(["channel_original", "channel_fixed"])
        .agg({"user_sessions": "sum", "unique_users": "sum"})
        .sort_values("unique_users", ascending=False)
        .reset_index()
    )
    for _, row in transitions.iterrows():
        print(
            f"    {row['channel_original']!s:20s} -> {row['channel_fixed']!s:20s}"
            f"  | {int(row['unique_users']):>6,} users, {int(row['user_sessions']):>6,} sessions"
        )

    # ------------------------------------------------------------------
    # STEP 3: Verify ONLY LLM sources changed
    # ------------------------------------------------------------------
    _separator("3. Safety check: only LLM sources should change")

    df_diff_pd["source_str"] = df_diff_pd["source"].fillna("").astype(str)
    df_diff_pd["is_llm_source"] = df_diff_pd["source_str"].apply(
        lambda s: bool(LLM_SOURCE_PATTERN.search(s))
    )

    non_llm_changes = df_diff_pd[~df_diff_pd["is_llm_source"]]
    if non_llm_changes.empty:
        print("  PASS: All changed rows have LLM sources.")
    else:
        print(f"  WARNING: {len(non_llm_changes)} non-LLM source(s) also changed!")
        print("  These need manual review:")
        for _, row in non_llm_changes.iterrows():
            print(
                f"    source={row['source']!r}, medium={row['medium']!r}, "
                f"campaign={row['campaign_name']!r}: "
                f"{row['channel_original']} -> {row['channel_fixed']} "
                f"({int(row['unique_users'])} users)"
            )

    # ------------------------------------------------------------------
    # STEP 4: Show affected LLM sources detail
    # ------------------------------------------------------------------
    _separator("4. LLM sources detail")

    llm_changes = df_diff_pd[df_diff_pd["is_llm_source"]].sort_values(
        "unique_users", ascending=False
    )
    if llm_changes.empty:
        print("  No LLM sources found in differences.")
    else:
        print(f"  {len(llm_changes)} distinct (source, medium, campaign) combos:")
        for _, row in llm_changes.iterrows():
            print(
                f"    source={row['source']!r:30s} medium={row['medium']!r:15s} "
                f"campaign={str(row['campaign_name'])[:30]:30s} | "
                f"{row['channel_original']:15s} -> {row['channel_fixed']:15s} "
                f"| {int(row['unique_users']):>5,} users"
            )

    # ------------------------------------------------------------------
    # STEP 5: Run both full queries for aggregate comparison
    # ------------------------------------------------------------------
    _separator("5. Full query comparison (aggregate Channel counts)")

    print("  Running original full query...")
    original_sql = (SQL_DIR / "funnel_original.sql").read_text()
    t0 = time.time()
    df_orig = query_bronze_source(original_sql, source_type="bigquery")
    t_orig = time.time() - t0
    if hasattr(df_orig, "to_pandas"):
        df_orig = df_orig.to_pandas()
    print(f"  Original: {len(df_orig):,} rows in {t_orig:.1f}s")

    print("  Running fixed full query...")
    fixed_sql = (SQL_DIR / "funnel_fixed.sql").read_text()
    t0 = time.time()
    df_fixed = query_bronze_source(fixed_sql, source_type="bigquery")
    t_fixed = time.time() - t0
    if hasattr(df_fixed, "to_pandas"):
        df_fixed = df_fixed.to_pandas()
    print(f"  Fixed:    {len(df_fixed):,} rows in {t_fixed:.1f}s")

    print(f"\n  Row count match: {'YES' if len(df_orig) == len(df_fixed) else 'NO — INVESTIGATE!'}")

    channel_col = "Channel" if "Channel" in df_orig.columns else "channel"

    counts_orig = (
        df_orig[channel_col]
        .value_counts()
        .reset_index()
        .rename(columns={channel_col: "channel", "count": "original"})
    )
    counts_fixed = (
        df_fixed[channel_col]
        .value_counts()
        .reset_index()
        .rename(columns={channel_col: "channel", "count": "fixed"})
    )

    counts = counts_orig.merge(counts_fixed, on="channel", how="outer").fillna(0)
    counts["original"] = counts["original"].astype(int)
    counts["fixed"] = counts["fixed"].astype(int)
    counts["diff"] = counts["fixed"] - counts["original"]
    counts = counts.sort_values("original", ascending=False)

    print(f"\n  {'Channel':<25s} {'Original':>10s} {'Fixed':>10s} {'Diff':>10s}")
    print(f"  {'-' * 57}")
    for _, row in counts.iterrows():
        diff_str = f"{row['diff']:+,d}" if row["diff"] != 0 else "0"
        marker = " <--" if row["diff"] != 0 else ""
        print(
            f"  {str(row['channel']):<25s} {row['original']:>10,d} {row['fixed']:>10,d} {diff_str:>10s}{marker}"
        )

    tt_col = "Traffic_type" if "Traffic_type" in df_orig.columns else "traffic_type"
    if tt_col in df_orig.columns and tt_col in df_fixed.columns:
        print(f"\n  {'Traffic_type':<25s} {'Original':>10s} {'Fixed':>10s} {'Diff':>10s}")
        print(f"  {'-' * 57}")
        tt_orig = df_orig[tt_col].value_counts().reset_index().rename(columns={tt_col: "tt", "count": "original"})
        tt_fixed = df_fixed[tt_col].value_counts().reset_index().rename(columns={tt_col: "tt", "count": "fixed"})
        tt_counts = tt_orig.merge(tt_fixed, on="tt", how="outer").fillna(0)
        tt_counts["original"] = tt_counts["original"].astype(int)
        tt_counts["fixed"] = tt_counts["fixed"].astype(int)
        tt_counts["diff"] = tt_counts["fixed"] - tt_counts["original"]
        tt_counts = tt_counts.sort_values("original", ascending=False)
        for _, row in tt_counts.iterrows():
            diff_str = f"{row['diff']:+,d}" if row["diff"] != 0 else "0"
            marker = " <--" if row["diff"] != 0 else ""
            print(
                f"  {str(row['tt']):<25s} {row['original']:>10,d} {row['fixed']:>10,d} {diff_str:>10s}{marker}"
            )

    # ------------------------------------------------------------------
    # STEP 6: Summary
    # ------------------------------------------------------------------
    _separator("6. Summary")

    all_llm = non_llm_changes.empty
    row_match = len(df_orig) == len(df_fixed)

    if all_llm and row_match:
        print("  PASS: Fix only affects LLM sources (Nulo-Vacio -> Organic LLMs).")
        print("  PASS: Row counts match between original and fixed.")
        print("  SAFE to apply fix to the scheduled BigQuery query.")
    else:
        if not all_llm:
            print("  WARN: Some non-LLM sources were also affected — review above.")
        if not row_match:
            print("  WARN: Row counts differ — review above.")

    print(f"\n  Timing: compare={t_compare:.1f}s, original={t_orig:.1f}s, fixed={t_fixed:.1f}s")


if __name__ == "__main__":
    main()
