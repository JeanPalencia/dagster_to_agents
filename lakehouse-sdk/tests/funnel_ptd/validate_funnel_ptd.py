#!/usr/bin/env python3
"""
Validation: compare Dagster funnel_ptd Gold output against the original SQL query.

Usage (from dagster-pipeline directory):
    uv run python -m lakehouse_sdk.tests.funnel_ptd.validate_funnel_ptd

Or directly:
    cd dagster-pipeline && uv run python ../../lakehouse-sdk/tests/funnel_ptd/validate_funnel_ptd.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import dagster as dg
import polars as pl

# Ensure dagster-pipeline/src is on the path
DAGSTER_SRC = Path(__file__).resolve().parents[3] / "dagster-pipeline" / "src"
sys.path.insert(0, str(DAGSTER_SRC))

from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

FLOAT_TOLERANCE = 0.01

SQL_FILE = Path(__file__).resolve().parent / ".." / "period_to_date" / "period_to_date.sql"

SORT_KEYS = [
    "counting_method", "ptd_type", "period_start", "cohort_type", "value_type",
]

# Columns that exist in the SQL output (no audit fields)
COMPARE_COLUMNS = [
    "value_type",
    "counting_method", "as_of_date", "ptd_type", "period_start", "period_end",
    "cohort_type",
    "leads_current", "projects_current",
    "scheduled_visits_current", "confirmed_visits_current",
    "completed_visits_current", "lois_current", "won_current",
    "leads_ptd", "projects_ptd",
    "scheduled_visits_ptd", "confirmed_visits_ptd",
    "completed_visits_ptd", "lois_ptd",
    "okr_leads_total", "okr_projects_total",
    "okr_scheduled_visits_total", "okr_confirmed_visits_total",
    "okr_completed_visits_total", "okr_lois_total",
    "scheduled_visits_proj", "completed_visits_proj",
    "lois_proj", "n_projects_in_projection_scope",
    "leads_ptd_rate", "projects_ptd_rate",
    "scheduled_visits_ptd_rate", "confirmed_visits_ptd_rate",
    "completed_visits_ptd_rate", "lois_ptd_rate",
    "leads_count", "projects_count",
    "scheduled_visits_count", "confirmed_visits_count",
    "completed_visits_count", "lois_count",
]


def run_sql_query() -> pl.DataFrame:
    """Execute the original SQL query against GeoSpot."""
    sql = SQL_FILE.read_text()
    print(f"Executing SQL query ({len(sql):,} chars)...")
    df = query_bronze_source(sql, source_type="geospot_postgres")
    print(f"SQL result: {df.height} rows, {df.width} columns")
    return df


def run_dagster_pipeline() -> pl.DataFrame:
    """Materialize Dagster assets and extract Gold output."""
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
    from dagster_pipeline.defs.funnel_ptd.silver.core.core_fptd_final import core_fptd_final
    from dagster_pipeline.defs.funnel_ptd.gold.gold_fptd_period_to_date import gold_fptd_period_to_date

    all_assets = [
        raw_gs_lk_leads, raw_gs_lk_projects_v2, raw_gs_lk_okrs, raw_gs_bt_lds_lead_spots,
        stg_gs_fptd_leads, stg_gs_fptd_projects, stg_gs_fptd_okrs, stg_gs_fptd_lead_spots,
        core_fptd_current_counts, core_fptd_monitor, core_fptd_km_survival,
        core_fptd_projections, core_fptd_final,
        gold_fptd_period_to_date,
    ]

    gold_selection = dg.AssetSelection.assets("gold_fptd_period_to_date").upstream()

    print("Materializing Dagster assets...")
    result = dg.materialize(assets=all_assets, selection=gold_selection)

    if not result.success:
        print("ERROR: Dagster materialization failed!")
        for event in result.all_events:
            if event.is_failure:
                print(f"  FAILED: {event.asset_key} — {event.message}")
        sys.exit(1)

    df = result.output_for_node("gold_fptd_period_to_date")
    print(f"Dagster result: {df.height} rows, {df.width} columns")
    return df


def compare_dataframes(df_sql: pl.DataFrame, df_dagster: pl.DataFrame) -> bool:
    """Compare SQL and Dagster outputs column by column."""
    # Select only comparable columns
    sql_cols = [c for c in COMPARE_COLUMNS if c in df_sql.columns]
    dag_cols = [c for c in COMPARE_COLUMNS if c in df_dagster.columns]

    missing_in_dagster = set(sql_cols) - set(dag_cols)
    missing_in_sql = set(dag_cols) - set(sql_cols)
    if missing_in_dagster:
        print(f"WARNING: columns in SQL but not in Dagster: {missing_in_dagster}")
    if missing_in_sql:
        print(f"WARNING: columns in Dagster but not in SQL: {missing_in_sql}")

    common_cols = [c for c in sql_cols if c in dag_cols]

    # Drop audit columns from Dagster output
    audit_cols = [c for c in df_dagster.columns if c.startswith("aud_")]
    df_dag = df_dagster.drop(audit_cols) if audit_cols else df_dagster
    df_dag = df_dag.select([c for c in common_cols if c in df_dag.columns])

    df_sq = df_sql.select([c for c in common_cols if c in df_sql.columns])

    # Sort both by the same keys
    sort_cols = [c for c in SORT_KEYS if c in common_cols]
    df_sq = df_sq.sort(sort_cols)
    df_dag = df_dag.sort(sort_cols)

    if df_sq.height != df_dag.height:
        print(f"FAIL: Row count mismatch: SQL={df_sq.height}, Dagster={df_dag.height}")
        return False

    print(f"Row count: {df_sq.height} (match)")

    total_mismatches = 0
    for col in common_cols:
        if col not in df_sq.columns or col not in df_dag.columns:
            continue

        sql_series = df_sq[col]
        dag_series = df_dag[col]

        # Cast for comparison
        if sql_series.dtype != dag_series.dtype:
            try:
                dag_series = dag_series.cast(sql_series.dtype, strict=False)
            except Exception:
                try:
                    sql_series = sql_series.cast(dag_series.dtype, strict=False)
                except Exception:
                    sql_series = sql_series.cast(pl.Utf8)
                    dag_series = dag_series.cast(pl.Utf8)

        if sql_series.dtype in (pl.Float64, pl.Float32):
            diffs = (sql_series.fill_null(0) - dag_series.fill_null(0)).abs()
            mismatches = diffs.filter(diffs > FLOAT_TOLERANCE).len()
        else:
            sql_null = sql_series.is_null()
            dag_null = dag_series.is_null()
            both_null = sql_null & dag_null
            both_set = ~sql_null & ~dag_null

            if sql_series.dtype in (pl.Int64, pl.Int32, pl.UInt32):
                matches = both_null | (both_set & (sql_series == dag_series))
            else:
                matches = both_null | (
                    both_set & (sql_series.cast(pl.Utf8) == dag_series.cast(pl.Utf8))
                )
            mismatches = (~matches).sum()

        if mismatches > 0:
            total_mismatches += mismatches
            print(f"  MISMATCH in '{col}': {mismatches} rows differ")
            # Show first few mismatches
            if sql_series.dtype in (pl.Float64, pl.Float32):
                diffs = (sql_series.fill_null(0) - dag_series.fill_null(0)).abs()
                idx = diffs.arg_sort(descending=True)[:3]
            else:
                sql_str = sql_series.cast(pl.Utf8).fill_null("NULL")
                dag_str = dag_series.cast(pl.Utf8).fill_null("NULL")
                idx = (sql_str != dag_str).arg_true()[:3]

            for i in idx:
                i = int(i)
                print(f"    row {i}: SQL={sql_series[i]}, Dagster={dag_series[i]}")
        else:
            print(f"  OK: '{col}'")

    if total_mismatches == 0:
        print(f"\nSUCCESS: All {len(common_cols)} columns match across {df_sq.height} rows!")
        return True
    else:
        print(f"\nFAIL: {total_mismatches} total mismatches across columns")
        return False


def main():
    print("=" * 80)
    print("Funnel PTD Validation: SQL query vs Dagster pipeline")
    print("=" * 80)

    df_sql = run_sql_query()
    df_dagster = run_dagster_pipeline()

    print("\n--- Comparison ---")
    success = compare_dataframes(df_sql, df_dagster)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
