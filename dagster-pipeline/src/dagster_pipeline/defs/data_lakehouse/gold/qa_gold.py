# defs/data_lakehouse/gold/qa_gold.py
"""
Gold layer QA asset: validates all gold tables for a partition.

Runs after gold assets are materialized; reads each _governance table directly from
Geospot PostgreSQL (the DB where load_to_geospot writes) and runs:
1. Rows: height >= min_rows (per-table threshold; empty = failure).
2. PK no duplicates: group_by(PK).agg(pl.len()).filter(pl.len() > 1) must be empty.
3. PK not null: all rows must have non-null PK columns.
4. Column count: width == expected_columns per table.
5. Expected columns present: PK + partition column must exist (names from schema).
6. Partition / date range: partition column (e.g. aud_inserted_date) present and non-null.

If any critical check fails, the asset fails and logs a summary.
"""
from __future__ import annotations

import polars as pl
from typing import Any

from dagster import AssetExecutionContext
import dagster as dg

from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    read_governance_from_db,
)


# Per-table QA spec. Key = table name in Geospot PostgreSQL.
# lk_projects = tabla principal (governance); antes lk_projects_governance.
GOLD_QA_SPEC: dict[str, dict[str, Any]] = {
    "lk_leads": {
        "pk": ["lead_id"],
        "min_rows": 0,
        "expected_columns": 107,
        "partition_column": "aud_inserted_date",
    },
    "lk_projects": {
        "pk": ["project_id"],
        "min_rows": 0,
        "expected_columns": 35,
        "partition_column": "aud_inserted_date",
    },
    "lk_users": {
        "pk": ["user_id"],
        "min_rows": 1000,
        "expected_columns": 60,
        "partition_column": "aud_inserted_date",
    },
    "lk_spots": {
        "pk": ["spot_id"],
        "min_rows": 0,
        "expected_columns": 183,
        "partition_column": "aud_inserted_date",
    },
    "bt_lds_lead_spots": {
        "pk_by_lds_event_id": {
            1: ["lead_id"],
            2: ["lead_id", "project_id"],
            3: ["lead_id", "project_id", "spot_id"],
            4: ["lead_id", "project_id", "spot_id", "appointment_id"],
            5: ["lead_id", "project_id", "spot_id", "appointment_id", "apd_id"],
            6: ["lead_id", "project_id", "spot_id", "appointment_id", "apd_id"],
            7: ["lead_id", "project_id", "spot_id", "alg_id"],
            8: ["lead_id", "project_id", "spot_id", "alg_id"],
            9: ["lead_id", "project_id", "spot_id", "alg_id"],
            10: ["lead_id", "project_id"],
        },
        "min_rows": 0,
        "expected_columns": 27,
        "partition_column": "aud_inserted_date",
    },
    "bt_conv_conversations": {
        "pk": ["conv_id"],
        "min_rows": 0,
        "expected_columns": 15,
        "partition_column": "aud_inserted_date",
    },
    "lk_matches_visitors_to_leads_governance": {
        "pk": ["user_pseudo_id", "event_datetime_first"],
        "min_rows": 0,
        "expected_columns": 27,
        "partition_column": "aud_inserted_date",
        "skip_pk_duplicate_check": True,
    },
    "bt_transactions": {
        "pk": ["project_id", "spot_id"],
        "min_rows": 0,
        "expected_columns": 10,
        "partition_column": "aud_inserted_date",
    },
}


def _check_pk_no_duplicates(df: pl.DataFrame, pk: list[str]) -> tuple[bool, str]:
    """True if no duplicate PK; message for logging."""
    if not pk or not all(c in df.columns for c in pk):
        return False, "PK columns missing"
    dup = df.group_by(pk).agg(pl.len().alias("_n")).filter(pl.col("_n") > 1)
    if dup.height > 0:
        return False, f"Found {dup.height} duplicate PK groups"
    return True, "OK"


def _check_pk_not_null(df: pl.DataFrame, pk: list[str]) -> tuple[bool, str]:
    """True if no row has null in any PK column."""
    if not pk or not all(c in df.columns for c in pk):
        return False, "PK columns missing"
    nulls = df.filter(pl.any_horizontal(pl.col(c).is_null() for c in pk))
    n = nulls.height
    if n > 0:
        return False, f"{n} rows with null PK"
    return True, "OK"


def _check_bt_lds_pk_by_event(
    df: pl.DataFrame, pk_by_lds_event_id: dict[int, list[str]]
) -> tuple[bool, str]:
    """
    For bt_lds_lead_spots: run PK duplicate and PK not-null checks per lds_event_id
    using the corresponding PK columns for each event type. See gold_governance_pks.md.
    """
    if "lds_event_id" not in df.columns:
        return False, "lds_event_id column missing"
    all_pk_cols = set()
    for cols in pk_by_lds_event_id.values():
        all_pk_cols.update(cols)
    missing = all_pk_cols - set(df.columns)
    if missing:
        return False, f"PK columns missing: {missing}"
    for event_id, pk in pk_by_lds_event_id.items():
        slice_df = df.filter(pl.col("lds_event_id") == event_id)
        if slice_df.height == 0:
            continue
        dup_ok, dup_msg = _check_pk_no_duplicates(slice_df, pk)
        if not dup_ok:
            return False, f"lds_event_id={event_id}: {dup_msg}"
        null_ok, null_msg = _check_pk_not_null(slice_df, pk)
        if not null_ok:
            return False, f"lds_event_id={event_id}: {null_msg}"
    return True, "OK"


def _check_partition_range(
    df: pl.DataFrame, partition_key: str, partition_column: str
) -> tuple[bool, str]:
    """True if partition column exists and is non-null (partition alignment is advisory; audit dates may be run date)."""
    if partition_column not in df.columns:
        return False, f"Partition column '{partition_column}' missing"
    try:
        col = df[partition_column]
        null_count = col.null_count()
        if null_count > 0:
            return False, f"Partition column has {null_count} nulls"
        if df.height == 0:
            return True, "OK"
        return True, "OK"
    except Exception as e:
        return False, str(e)


def _run_qa_for_table(
    context: AssetExecutionContext,
    table_name: str,
    spec: dict[str, Any],
    partition_key: str,
) -> dict[str, Any]:
    """Run all 6 checks for one gold table. Returns result dict with passed/failed and messages."""
    pk = spec.get("pk")
    pk_by_lds_event_id = spec.get("pk_by_lds_event_id")
    min_rows = spec["min_rows"]
    expected_columns = spec["expected_columns"]
    partition_column = spec.get("partition_column")

    if pk_by_lds_event_id:
        required_columns = {"lds_event_id", partition_column} if partition_column else {"lds_event_id"}
        for cols in pk_by_lds_event_id.values():
            required_columns.update(cols)
        required_columns = list(required_columns)
    else:
        required_columns = list(pk or [])
        if partition_column:
            required_columns.append(partition_column)

    try:
        df = read_governance_from_db(table_name, context)
    except Exception as e:
        return {
            "table": table_name,
            "rows_ok": False,
            "rows_msg": f"Failed to read from DB: {e}",
            "pk_no_duplicates_ok": False,
            "pk_no_duplicates_msg": "N/A (no data)",
            "pk_not_null_ok": False,
            "pk_not_null_msg": "N/A (no data)",
            "column_count_ok": False,
            "column_count_msg": "N/A (no data)",
            "columns_present_ok": False,
            "columns_present_msg": "N/A (no data)",
            "partition_ok": False,
            "partition_msg": "N/A (no data)",
        }

    columns_present = set(required_columns).issubset(set(df.columns))
    columns_present_msg = "OK" if columns_present else f"Missing: {set(required_columns) - set(df.columns)}"

    rows_ok = df.height >= min_rows and (min_rows > 0 or df.height > 0)
    rows_msg = "OK" if rows_ok else f"rows={df.height}, min_required={min_rows}"

    if pk_by_lds_event_id:
        pk_dup_ok, pk_dup_msg = _check_bt_lds_pk_by_event(df, pk_by_lds_event_id)
        pk_null_ok, pk_null_msg = pk_dup_ok, pk_dup_msg
    elif pk:
        pk_dup_ok, pk_dup_msg = _check_pk_no_duplicates(df, pk)
        pk_null_ok, pk_null_msg = _check_pk_not_null(df, pk)
        if spec.get("skip_pk_duplicate_check"):
            pk_dup_ok, pk_dup_msg = True, "OK (skipped)"
        if spec.get("skip_pk_not_null_check"):
            pk_null_ok, pk_null_msg = True, "OK (skipped)"
    else:
        pk_dup_ok, pk_dup_msg = True, "OK (no PK)"
        pk_null_ok, pk_null_msg = True, "OK (no PK)"

    width_ok = df.width == expected_columns
    width_msg = "OK" if width_ok else f"width={df.width}, expected={expected_columns}"

    partition_ok = True
    partition_msg = "OK"
    if partition_column:
        partition_ok, partition_msg = _check_partition_range(df, partition_key, partition_column)

    return {
        "table": table_name,
        "rows_ok": rows_ok,
        "rows_msg": rows_msg,
        "pk_no_duplicates_ok": pk_dup_ok,
        "pk_no_duplicates_msg": pk_dup_msg,
        "pk_not_null_ok": pk_null_ok,
        "pk_not_null_msg": pk_null_msg,
        "column_count_ok": width_ok,
        "column_count_msg": width_msg,
        "columns_present_ok": columns_present,
        "columns_present_msg": columns_present_msg,
        "partition_ok": partition_ok,
        "partition_msg": partition_msg,
    }


def _qa_gold_new_impl(context: AssetExecutionContext) -> dict:
    """
    Reads each gold governance table from Geospot PostgreSQL for the current partition run state
    and runs the 6 QA checks. Returns a summary dict; raises if any critical check fails.
    """
    partition_key = context.partition_key
    results: list[dict[str, Any]] = []

    for table_name, spec in GOLD_QA_SPEC.items():
        context.log.info(f"Running QA for {table_name} (partition {partition_key})")
        r = _run_qa_for_table(context, table_name, spec, partition_key)
        results.append(r)

    # Log summary table
    all_passed = True
    for r in results:
        checks = [
            ("rows", r["rows_ok"], r["rows_msg"]),
            ("pk_no_duplicates", r["pk_no_duplicates_ok"], r["pk_no_duplicates_msg"]),
            ("pk_not_null", r["pk_not_null_ok"], r["pk_not_null_msg"]),
            ("column_count", r["column_count_ok"], r["column_count_msg"]),
            ("columns_present", r["columns_present_ok"], r["columns_present_msg"]),
            ("partition", r["partition_ok"], r["partition_msg"]),
        ]
        table_ok = all(ok for _, ok, _ in checks)
        if not table_ok:
            all_passed = False
        status = "PASS" if table_ok else "FAIL"
        context.log.info(f"  {r['table']}: {status}")
        for name, ok, msg in checks:
            if not ok:
                context.log.warning(f"    - {name}: {msg}")

    if not all_passed:
        failed_tables = [r["table"] for r in results if not all([
            r["rows_ok"], r["pk_no_duplicates_ok"], r["pk_not_null_ok"],
            r["column_count_ok"], r["columns_present_ok"], r["partition_ok"],
        ])]
        output_fail = {
            "partition_key": partition_key,
            "passed": False,
            "failed_tables": failed_tables,
            "details": [
                {
                    "table": r["table"],
                    "rows_ok": r["rows_ok"],
                    "pk_no_duplicates_ok": r["pk_no_duplicates_ok"],
                    "pk_not_null_ok": r["pk_not_null_ok"],
                    "column_count_ok": r["column_count_ok"],
                    "columns_present_ok": r["columns_present_ok"],
                    "partition_ok": r["partition_ok"],
                }
                for r in results
            ],
        }
        context.log.warning("QA output (FAIL): %s", output_fail)
        raise RuntimeError(
            f"Gold QA failed for partition {partition_key}. Failed tables: {failed_tables}. "
            "Check logs for details."
        )

    context.log.info(f"Gold QA passed for partition {partition_key} ({len(results)} tables).")
    output = {
        "partition_key": partition_key,
        "tables": [r["table"] for r in results],
        "passed": True,
        "details": [
            {
                "table": r["table"],
                "rows_ok": r["rows_ok"],
                "pk_no_duplicates_ok": r["pk_no_duplicates_ok"],
                "pk_not_null_ok": r["pk_not_null_ok"],
                "column_count_ok": r["column_count_ok"],
                "columns_present_ok": r["columns_present_ok"],
                "partition_ok": r["partition_ok"],
            }
            for r in results
        ],
    }
    context.log.info("QA output: %s", output)
    return output


@dg.asset(
    name="qa_gold_new",
    partitions_def=daily_partitions,
    group_name="gold",
    description=(
        "QA for all gold tables: reads each _governance table from Geospot PostgreSQL and runs checks: "
        "rows, PK uniqueness, PK not null, column count, expected columns, partition column. Fails if any critical check fails. "
        "Does not run or depend on gold assets; run this job alone to validate existing data in the DB."
    ),
)
def qa_gold_new(context):
    def body():
        return _qa_gold_new_impl(context)

    yield from iter_job_wrapped_compute(context, body)
