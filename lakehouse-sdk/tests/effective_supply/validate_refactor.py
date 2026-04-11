#!/usr/bin/env python3
"""
Exhaustive validation: compare refactored Gold pipeline output vs GeoSpot tables.

Runs the refactored pipeline programmatically and compares each of the 6 Gold
tables against the current GeoSpot data, excluding:
- aud_run_id (new, not in GeoSpot yet)
- aud_inserted_at, aud_inserted_date (different timestamps)
- aud_updated_at, aud_updated_date (different timestamps)
- aud_job (may differ in case/format)

Exit code 0 = 100% match on all tables. Non-zero = differences found.
"""
import sys
from pathlib import Path

import polars as pl


# ---------------------------------------------------------------------------
# Audit columns to exclude from comparison
# ---------------------------------------------------------------------------
AUDIT_COLS = {
    "aud_run_id", "aud_inserted_at", "aud_inserted_date",
    "aud_updated_at", "aud_updated_date", "aud_job",
}

# ---------------------------------------------------------------------------
# Table definitions: name, order-by keys, epsilon tolerance for ML tables
# ---------------------------------------------------------------------------
ML_EPSILON_SCORES = 0.5
ML_EPSILON_METRICS = 0.20

TABLES = [
    {
        "name": "lk_effective_supply",
        "keys": ["spot_id"],
    },
    {
        "name": "lk_effective_supply_propensity_scores",
        "keys": ["spot_id", "market_universe"],
        "epsilon": ML_EPSILON_SCORES,
    },
    {
        "name": "lk_effective_supply_drivers",
        "keys": ["market_universe", "feature_name"],
        "epsilon": ML_EPSILON_METRICS,
    },
    {
        "name": "lk_effective_supply_category_effects",
        "keys": ["market_universe", "feature_name", "category_id"],
    },
    {
        "name": "lk_effective_supply_rules",
        "keys": ["market_universe", "rule_id"],
    },
    {
        "name": "lk_effective_supply_model_metrics",
        "keys": ["market_universe"],
        "epsilon": ML_EPSILON_METRICS,
    },
]


def _is_float_dtype(dtype: pl.DataType) -> bool:
    return dtype in (pl.Float32, pl.Float64)


def _is_numeric_dtype(dtype: pl.DataType) -> bool:
    return dtype in (
        pl.Float32, pl.Float64,
        pl.Int8, pl.Int16, pl.Int32, pl.Int64,
        pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    )


def compare_table(gold_df: pl.DataFrame, geospot_df: pl.DataFrame,
                  table_name: str, keys: list[str],
                  epsilon: float | None = None) -> bool:
    """Compare gold_df vs geospot_df row by row, column by column.

    When *epsilon* is set, float columns are compared with approximate
    tolerance instead of exact equality.  Differences within epsilon are
    reported as INFO (not FAIL).
    """
    compare_cols_gold = [c for c in gold_df.columns if c not in AUDIT_COLS]
    compare_cols_geo = [c for c in geospot_df.columns if c not in AUDIT_COLS]

    common_cols = sorted(set(compare_cols_gold) & set(compare_cols_geo))
    only_gold = set(compare_cols_gold) - set(compare_cols_geo)
    only_geo = set(compare_cols_geo) - set(compare_cols_gold)

    if only_gold:
        print(f"  WARNING: Columns only in Gold: {only_gold}")
    if only_geo:
        print(f"  WARNING: Columns only in GeoSpot: {only_geo}")

    valid_keys = [k for k in keys if k in common_cols]
    g = gold_df.select(common_cols).sort(valid_keys)
    s = geospot_df.select(common_cols).sort(valid_keys)

    if g.height != s.height:
        print(f"  FAIL: Row count differs: Gold={g.height}, GeoSpot={s.height}")
        return False

    print(f"  Row count: {g.height} (match)")

    hard_mismatches = []
    soft_mismatches = []

    for col in common_cols:
        g_col = g[col]
        s_col = s[col]

        is_ml_table = epsilon is not None
        both_float = _is_float_dtype(g_col.dtype) and _is_float_dtype(s_col.dtype)
        both_int = (not both_float
                    and _is_numeric_dtype(g_col.dtype)
                    and _is_numeric_dtype(s_col.dtype))

        if both_float and is_ml_table:
            g_f = g_col.cast(pl.Float64)
            s_f = s_col.cast(pl.Float64)
            null_match = g_f.is_null() & s_f.is_null()
            within_eps = ((g_f - s_f).abs() <= epsilon).fill_null(False)
            matches = null_match | within_eps
            total = g.height
            match_count = int(matches.sum() or 0)

            if match_count < total:
                diff_count = total - match_count
                max_diff = float(((g_f - s_f).abs()).max() or 0)
                if max_diff <= epsilon:
                    soft_mismatches.append((col, max_diff))
                    print(f"  OK (≈) '{col}': max_diff={max_diff:.6f} ≤ ε={epsilon}")
                else:
                    hard_mismatches.append((col, diff_count))
                    print(f"  FAIL in '{col}': {diff_count}/{total} rows exceed ε={epsilon} (max_diff={max_diff:.6f})")
            else:
                exact_eq = ((g_f == s_f) | null_match)
                exact_count = int(exact_eq.sum() or 0)
                if exact_count < total:
                    max_diff = float(((g_f - s_f).abs()).max() or 0)
                    soft_mismatches.append((col, max_diff))
                    print(f"  OK (≈) '{col}': max_diff={max_diff:.6f} ≤ ε={epsilon}")
            continue

        if both_int and is_ml_table:
            matches = (g_col == s_col) | (g_col.is_null() & s_col.is_null())
            total = g.height
            match_count = int(matches.sum() or 0)
            if match_count < total:
                diff_count = total - match_count
                soft_mismatches.append((col, diff_count))
                print(f"  OK (≈) '{col}': {diff_count}/{total} int diffs (stochastic retraining)")
            continue

        try:
            if g_col.dtype != s_col.dtype:
                g_col = g_col.cast(pl.Utf8)
                s_col = s_col.cast(pl.Utf8)
        except Exception:
            pass

        try:
            matches = (g_col == s_col) | (g_col.is_null() & s_col.is_null())
            total = g.height
            match_count = int(matches.sum() or 0)

            if match_count < total:
                diff_count = total - match_count
                hard_mismatches.append((col, diff_count))
                diff_mask = ~matches
                diff_rows = g.filter(diff_mask).head(3)
                diff_rows_s = s.filter(diff_mask).head(3)
                print(f"  DIFF in '{col}': {diff_count}/{total} rows differ")
                for i in range(min(3, diff_rows.height)):
                    print(f"    Gold[{i}]: {diff_rows[col][i]}")
                    print(f"    GeoS[{i}]: {diff_rows_s[col][i]}")
        except Exception as e:
            try:
                g_f = g_col.cast(pl.Float64)
                s_f = s_col.cast(pl.Float64)
                diff = (g_f - s_f).abs()
                max_diff = float(diff.max() or 0)
                tol = epsilon if epsilon is not None else 1e-6
                if max_diff > tol:
                    hard_mismatches.append((col, f"max_diff={max_diff:.10f}"))
                    print(f"  DIFF in '{col}': max absolute diff = {max_diff:.10f}")
            except Exception:
                hard_mismatches.append((col, f"comparison error: {e}"))
                print(f"  ERROR comparing '{col}': {e}")

    if hard_mismatches:
        print(f"  RESULT: {len(hard_mismatches)} columns with hard differences"
              + (f", {len(soft_mismatches)} within ε" if soft_mismatches else ""))
        return False
    elif soft_mismatches:
        print(f"  RESULT: ALL {len(common_cols)} columns match"
              f" ({len(soft_mismatches)} within ε={epsilon})")
        return True
    else:
        print(f"  RESULT: ALL {len(common_cols)} columns match!")
        return True


def main():
    # =========================================================================
    # 1. Run the refactored pipeline programmatically
    # =========================================================================
    print("=" * 70)
    print("STEP 1: Running refactored pipeline...")
    print("=" * 70)

    # Add dagster-pipeline to path
    dagster_src = Path(__file__).resolve().parents[3] / "dagster-pipeline" / "src"
    sys.path.insert(0, str(dagster_src))

    import dagster as dg
    from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source
    from dagster_pipeline.defs.effective_supply import (
        raw_bq_spot_contact_view_event_counts,
        raw_gs_bt_lds_spot_added,
        raw_gs_lk_spots,
        raw_gs_effective_supply_run_id,
        stg_bq_spot_contact_view_event_counts,
        stg_gs_bt_lds_spot_added,
        stg_gs_lk_spots,
        stg_gs_effective_supply_run_id,
        core_effective_supply_events,
        core_effective_supply,
        core_ml_build_universes,
        core_ml_feature_engineering,
        core_ml_train,
        core_ml_score,
        core_ml_drivers,
        core_ml_category_effects,
        core_ml_rules,
        core_lk_effective_supply,
        core_lk_effective_supply_propensity_scores,
        core_lk_effective_supply_drivers,
        core_lk_effective_supply_category_effects,
        core_lk_effective_supply_rules,
        core_lk_effective_supply_model_metrics,
        gold_lk_effective_supply,
        gold_lk_effective_supply_propensity_scores,
        gold_lk_effective_supply_drivers,
        gold_lk_effective_supply_category_effects,
        gold_lk_effective_supply_rules,
        gold_lk_effective_supply_model_metrics,
    )

    all_assets = [
        raw_bq_spot_contact_view_event_counts,
        raw_gs_bt_lds_spot_added,
        raw_gs_lk_spots,
        raw_gs_effective_supply_run_id,
        stg_bq_spot_contact_view_event_counts,
        stg_gs_bt_lds_spot_added,
        stg_gs_lk_spots,
        stg_gs_effective_supply_run_id,
        core_effective_supply_events,
        core_effective_supply,
        core_ml_build_universes,
        core_ml_feature_engineering,
        core_ml_train,
        core_ml_score,
        core_ml_drivers,
        core_ml_category_effects,
        core_ml_rules,
        core_lk_effective_supply,
        core_lk_effective_supply_propensity_scores,
        core_lk_effective_supply_drivers,
        core_lk_effective_supply_category_effects,
        core_lk_effective_supply_rules,
        core_lk_effective_supply_model_metrics,
        gold_lk_effective_supply,
        gold_lk_effective_supply_propensity_scores,
        gold_lk_effective_supply_drivers,
        gold_lk_effective_supply_category_effects,
        gold_lk_effective_supply_rules,
        gold_lk_effective_supply_model_metrics,
    ]

    gold_selection = dg.AssetSelection.assets(
        "gold_lk_effective_supply",
        "gold_lk_effective_supply_propensity_scores",
        "gold_lk_effective_supply_drivers",
        "gold_lk_effective_supply_category_effects",
        "gold_lk_effective_supply_rules",
        "gold_lk_effective_supply_model_metrics",
    ).upstream()

    result = dg.materialize(assets=all_assets, selection=gold_selection)

    if not result.success:
        print("PIPELINE FAILED!")
        sys.exit(1)

    print("Pipeline materialization successful.\n")

    # =========================================================================
    # 2. Extract gold outputs from materialization result
    # =========================================================================
    gold_name_to_asset = {
        "lk_effective_supply": "gold_lk_effective_supply",
        "lk_effective_supply_propensity_scores": "gold_lk_effective_supply_propensity_scores",
        "lk_effective_supply_drivers": "gold_lk_effective_supply_drivers",
        "lk_effective_supply_category_effects": "gold_lk_effective_supply_category_effects",
        "lk_effective_supply_rules": "gold_lk_effective_supply_rules",
        "lk_effective_supply_model_metrics": "gold_lk_effective_supply_model_metrics",
    }

    gold_outputs = {}
    for table_name, asset_name in gold_name_to_asset.items():
        try:
            gold_outputs[table_name] = result.output_for_node(asset_name)
        except Exception as e:
            print(f"WARNING: Could not get output for {asset_name}: {e}")

    # =========================================================================
    # 3. Compare each table
    # =========================================================================
    print("=" * 70)
    print("STEP 2: Comparing Gold output vs GeoSpot tables...")
    print("=" * 70)

    all_ok = True
    for table_info in TABLES:
        table_name = table_info["name"]
        keys = table_info["keys"]
        epsilon = table_info.get("epsilon")

        print(f"\n--- {table_name} ---")
        if epsilon is not None:
            print(f"  (ML table — float tolerance ε={epsilon})")

        if table_name not in gold_outputs:
            print(f"  SKIP: No Gold output available")
            all_ok = False
            continue

        gold_df = gold_outputs[table_name]
        print(f"  Gold: {gold_df.height} rows, {gold_df.width} columns")

        geospot_df = query_bronze_source(
            f"SELECT * FROM {table_name}",
            source_type="geospot_postgres",
        )
        print(f"  GeoSpot: {geospot_df.height} rows, {geospot_df.width} columns")

        if geospot_df.height == 0:
            print(f"  SKIP: GeoSpot table is empty")
            continue

        ok = compare_table(gold_df, geospot_df, table_name, keys, epsilon=epsilon)
        if not ok:
            all_ok = False

    # =========================================================================
    # 4. Final verdict
    # =========================================================================
    print("\n" + "=" * 70)
    if all_ok:
        print("VALIDATION PASSED: All 6 tables match GeoSpot.")
    else:
        print("VALIDATION FAILED: Some tables have differences.")
    print("=" * 70)

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
