#!/usr/bin/env python3
"""
Test harness for validating Dagster flow changes against real data.

Uses dg.materialize() to run Bronze → STG → Core → Gold with real database connections.
Validates the Gold output against schema contracts, null thresholds, and flow-specific invariants.

Usage:
    cd dagster-pipeline
    uv run python -m tests.test_harness --flow=amenity_description_consistency
    uv run python -m tests.test_harness --flow=spot_state_transitions --extra-checks='{"check_sstd_unique": true}'
"""
import argparse
import importlib
import json
import sys
from typing import Any

import dagster as dg
import polars as pl

from tests.flow_registry import get_flow_config


def import_all_assets(module_name: str) -> list:
    """
    Dynamically import all assets from a flow module.

    Args:
        module_name: Module path (e.g., "dagster_pipeline.defs.amenity_description_consistency")

    Returns:
        List of Dagster asset objects
    """
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        print(f"ERROR: Could not import module '{module_name}': {e}", file=sys.stderr)
        sys.exit(1)

    # Get all assets from the module's __all__ or by scanning for AssetsDefinition objects
    if hasattr(module, "__all__"):
        assets = [getattr(module, name) for name in module.__all__]
    else:
        # Fallback: scan for Dagster asset/multi-asset objects
        assets = [
            obj for obj in vars(module).values()
            if isinstance(obj, (dg.AssetsDefinition, dg.SourceAsset))
        ]

    if not assets:
        print(f"WARNING: No assets found in module '{module_name}'", file=sys.stderr)

    return assets


def get_schema_contract(schema_module: str, schema_const_name: str) -> list[str]:
    """
    Import and return the schema contract (list of column names) from a module.

    Args:
        schema_module: Module path where the constant is defined
        schema_const_name: Name of the constant (e.g., "_PUBLISH_COLUMNS", "FINAL_COLUMNS")

    Returns:
        List of expected column names
    """
    try:
        module = importlib.import_module(schema_module)
        schema_contract = getattr(module, schema_const_name)
        if isinstance(schema_contract, dict):
            # Handle cases like FINAL_RENAME (dict) — use keys
            return list(schema_contract.keys())
        return list(schema_contract)
    except (ImportError, AttributeError) as e:
        print(f"WARNING: Could not load schema contract from {schema_module}.{schema_const_name}: {e}", file=sys.stderr)
        return []


def materialize_flow(
    flow_name: str,
    gold_asset: str,
    all_assets: list,
) -> tuple[bool, pl.DataFrame | None, str]:
    """
    Materialize a flow's assets up to the gold layer using real data.

    Args:
        flow_name: Name of the flow (for logging)
        gold_asset: Name of the gold asset to materialize
        all_assets: List of all asset objects from the flow

    Returns:
        Tuple of (success: bool, gold_df: DataFrame | None, error_msg: str)
    """
    print(f"\n[MATERIALIZE] Running {flow_name} up to {gold_asset}...")

    try:
        gold_selection = dg.AssetSelection.assets(gold_asset).upstream()
        result = dg.materialize(assets=all_assets, selection=gold_selection)

        if not result.success:
            return False, None, "Materialization failed (result.success == False)"

        gold_df = result.output_for_node(gold_asset)
        if gold_df is None or (hasattr(gold_df, 'height') and gold_df.height == 0):
            return False, None, f"Gold asset '{gold_asset}' returned empty DataFrame"

        print(f"[MATERIALIZE] ✓ Success: {gold_df.height} rows, {gold_df.width} columns")
        return True, gold_df, ""

    except Exception as e:
        return False, None, f"Exception during materialization: {e}"


def validate_schema(
    df: pl.DataFrame,
    expected_columns: list[str],
) -> dict[str, Any]:
    """
    Validate that the DataFrame contains all expected columns.

    Args:
        df: The gold DataFrame
        expected_columns: List of expected column names from schema contract

    Returns:
        Dict with validation result
    """
    if not expected_columns:
        return {"status": "SKIP", "reason": "No schema contract defined"}

    actual_columns = set(df.columns)
    expected_set = set(expected_columns)

    # Allow audit fields (aud_*) to be extra
    actual_non_audit = {c for c in actual_columns if not c.startswith("aud_")}

    missing = expected_set - actual_non_audit
    unexpected = actual_non_audit - expected_set

    if missing or unexpected:
        return {
            "status": "FAIL",
            "expected_columns": len(expected_columns),
            "actual_columns": len(actual_non_audit),
            "missing": list(missing),
            "unexpected": list(unexpected),
        }

    return {
        "status": "PASS",
        "expected_columns": len(expected_columns),
        "actual_columns": len(actual_non_audit),
    }


def validate_nulls(df: pl.DataFrame, max_null_pct: float = 50.0) -> dict[str, Any]:
    """
    Validate that no column has excessive nulls (regression check).

    Args:
        df: The gold DataFrame
        max_null_pct: Maximum allowed null percentage (default 50%)

    Returns:
        Dict with validation result
    """
    null_counts = df.null_count()
    total_rows = df.height
    violations = []

    for col in df.columns:
        null_count = null_counts[col][0]
        null_pct = (null_count / total_rows) * 100 if total_rows > 0 else 0
        if null_pct > max_null_pct:
            violations.append({
                "column": col,
                "null_count": null_count,
                "null_pct": round(null_pct, 2),
            })

    if violations:
        return {
            "status": "FAIL",
            "violations": violations,
            "threshold_pct": max_null_pct,
        }

    return {
        "status": "PASS",
        "threshold_pct": max_null_pct,
    }


def validate_row_count(df: pl.DataFrame, min_rows: int = 1) -> dict[str, Any]:
    """
    Validate that the DataFrame has a reasonable number of rows.

    Args:
        df: The gold DataFrame
        min_rows: Minimum expected rows (default 1)

    Returns:
        Dict with validation result
    """
    row_count = df.height

    if row_count < min_rows:
        return {
            "status": "FAIL",
            "actual_rows": row_count,
            "min_expected": min_rows,
        }

    return {
        "status": "PASS",
        "actual_rows": row_count,
    }


def validate_audit_fields(df: pl.DataFrame) -> dict[str, Any]:
    """
    Validate that audit fields are present and non-null (Gold layer requirement).

    Args:
        df: The gold DataFrame

    Returns:
        Dict with validation result
    """
    audit_fields = ["aud_created_at", "aud_updated_at"]
    present_audit = [f for f in audit_fields if f in df.columns]

    if not present_audit:
        return {"status": "SKIP", "reason": "No audit fields found (not a gold layer?)"}

    missing = [f for f in audit_fields if f not in df.columns]
    null_audit = []

    for field in present_audit:
        null_count = df[field].null_count()
        if null_count > 0:
            null_audit.append({"field": field, "null_count": null_count})

    if missing or null_audit:
        return {
            "status": "FAIL",
            "missing_fields": missing,
            "null_violations": null_audit,
        }

    return {
        "status": "PASS",
        "audit_fields_present": present_audit,
    }


def run_extra_checks(df: pl.DataFrame, checks: dict[str, Any]) -> dict[str, Any]:
    """
    Run flow-specific assertions based on extra_checks JSON.

    Args:
        df: The gold DataFrame
        checks: Dict of check names to parameters

    Returns:
        Dict with results of each check
    """
    results = {}

    # Example flow-specific check for spot_state_transitions
    if checks.get("check_sstd_unique"):
        if "sstd_status_final_id" in df.columns:
            duplicates = df.filter(pl.col("sstd_status_final_id").is_duplicated())
            if duplicates.height > 0:
                results["check_sstd_unique"] = {
                    "status": "FAIL",
                    "duplicate_count": duplicates.height,
                }
            else:
                results["check_sstd_unique"] = {"status": "PASS"}
        else:
            results["check_sstd_unique"] = {
                "status": "SKIP",
                "reason": "Column 'sstd_status_final_id' not found",
            }

    return results


def main():
    parser = argparse.ArgumentParser(description="Test harness for Dagster flows")
    parser.add_argument("--flow", required=True, help="Flow name (from flow_registry)")
    parser.add_argument("--extra-checks", help="JSON string with flow-specific checks")
    args = parser.parse_args()

    # Parse extra checks if provided
    extra_checks = {}
    if args.extra_checks:
        try:
            extra_checks = json.loads(args.extra_checks)
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in --extra-checks: {e}", file=sys.stderr)
            sys.exit(1)

    # Load flow configuration
    try:
        config = get_flow_config(args.flow)
    except KeyError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"═══════════════════════════════════════════════════════")
    print(f"TEST HARNESS: {args.flow}")
    print(f"Description: {config['description']}")
    print(f"Gold asset: {config['gold_asset']}")
    print(f"═══════════════════════════════════════════════════════")

    # Import assets
    all_assets = import_all_assets(config["module"])
    if not all_assets:
        print("ERROR: No assets imported", file=sys.stderr)
        sys.exit(1)

    print(f"[IMPORT] Loaded {len(all_assets)} assets from {config['module']}")

    # Load schema contract
    expected_columns = get_schema_contract(
        config["schema_constant_module"],
        config["schema_constant_name"],
    )
    if expected_columns:
        print(f"[SCHEMA] Expected {len(expected_columns)} columns from {config['schema_constant_name']}")

    # Materialize the flow
    success, gold_df, error_msg = materialize_flow(
        args.flow,
        config["gold_asset"],
        all_assets,
    )

    if not success:
        print(f"\n❌ MATERIALIZATION FAILED: {error_msg}")
        result = {
            "flow": args.flow,
            "status": "FAIL",
            "error": error_msg,
        }
        print(f"\n{json.dumps(result, indent=2)}")
        sys.exit(1)

    # Run validations
    print(f"\n[VALIDATE] Running standard checks...")
    validations = {
        "schema": validate_schema(gold_df, expected_columns),
        "nulls": validate_nulls(gold_df),
        "row_count": validate_row_count(gold_df),
        "audit_fields": validate_audit_fields(gold_df),
    }

    # Run extra checks if provided
    if extra_checks:
        print(f"[VALIDATE] Running {len(extra_checks)} extra checks...")
        validations["extra"] = run_extra_checks(gold_df, extra_checks)

    # Determine overall status
    failed_checks = [
        name for name, result in validations.items()
        if isinstance(result, dict) and result.get("status") == "FAIL"
    ]

    overall_status = "PASS" if not failed_checks else "FAIL"

    # Build result
    result = {
        "flow": args.flow,
        "status": overall_status,
        "gold_asset": config["gold_asset"],
        "rows": gold_df.height,
        "columns": gold_df.width,
        "validations": validations,
        "failed_checks": failed_checks,
    }

    # Print result
    print(f"\n═══════════════════════════════════════════════════════")
    if overall_status == "PASS":
        print(f"✅ ALL CHECKS PASSED")
    else:
        print(f"❌ FAILED CHECKS: {', '.join(failed_checks)}")
    print(f"═══════════════════════════════════════════════════════")
    print(f"\n{json.dumps(result, indent=2)}")

    sys.exit(0 if overall_status == "PASS" else 1)


if __name__ == "__main__":
    main()
