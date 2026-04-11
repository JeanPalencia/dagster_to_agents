"""
Regression and integrity test for bt_lds_lead_spots channel attribution.

Reads the baseline parquet (pre-change) and the current gold output from S3,
then runs 26 checks covering:
  6a. Original column regression
  6b. Structural validation of new fields
  6c. Nullability by event
  6d. Channel coverage
  6e. Cross-reference keys
  6f. Consistency between channel and flags
  6g. Uniqueness

Usage:
    cd dagster-pipeline
    uv run python ../lakehouse-sdk/tests/bt_lds_channel_attribution/test_regression.py
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "dagster-pipeline", "src"))

import polars as pl
import boto3
import io
from datetime import date, timedelta

S3_BUCKET = "dagster-assets-production"
s3 = boto3.client("s3", region_name="us-east-1")
BASELINE_PATH = os.path.join(os.path.dirname(__file__), "baseline.parquet")

CHANNEL_COLS = (
    "lds_channel_attribution_id", "lds_channel_attribution",
    "lds_algorithm_evidence_at", "lds_chatbot_evidence_at",
    "is_recommended_algorithm_id", "is_recommended_algorithm",
    "is_recommended_chatbot_id", "is_recommended_chatbot",
)

ORIGINAL_COLS = (
    "lead_id", "project_id", "spot_id", "appointment_id", "apd_id", "alg_id",
    "appointment_last_date_status_id", "appointment_last_status_at", "apd_status_id", "alg_event",
    "lds_event_id", "lds_event", "lds_event_at",
    "lead_max_type_id", "lead_max_type", "spot_sector_id", "spot_sector",
    "user_industria_role_id", "user_industria_role",
    "lds_cohort_type_id", "lds_cohort_type", "lds_cohort_at",
)

results = []


def check(name: str, passed: bool, detail: str = ""):
    status = "PASS" if passed else "FAIL"
    results.append((name, passed, detail))
    print(f"  [{status}] {name}" + (f" -- {detail}" if detail else ""))


def _find_latest_gold_key() -> str:
    today = date.today()
    for offset in range(30):
        d = today - timedelta(days=offset)
        key = f"gold/bt_lds_lead_spots/{d.year}/{d.month:02d}/data-{d.day:02d}.parquet"
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=key)
            return key
        except Exception:
            continue
    raise FileNotFoundError("No gold parquet found for bt_lds_lead_spots in the last 30 days")


def main():
    print("=" * 70)
    print("REGRESSION TEST: bt_lds_lead_spots channel attribution")
    print("=" * 70)

    # Load baseline
    if not os.path.exists(BASELINE_PATH):
        print(f"ERROR: Baseline not found at {BASELINE_PATH}")
        print("Run capture_baseline.py first.")
        sys.exit(1)

    df_baseline = pl.read_parquet(BASELINE_PATH)
    print(f"\nBaseline: {df_baseline.height:,} rows, {df_baseline.width} columns")

    # Load current gold from S3
    key = _find_latest_gold_key()
    print(f"Current gold: s3://{S3_BUCKET}/{key}")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    df_current = pl.read_parquet(io.BytesIO(obj["Body"].read()))
    print(f"Current: {df_current.height:,} rows, {df_current.width} columns\n")

    # Strip aud columns from current
    aud_cols = [c for c in df_current.columns if c.startswith("aud_")]
    df_current_no_aud = df_current.drop(aud_cols)

    # =====================================================================
    # 6a. REGRESSION OF ORIGINAL COLUMNS
    # =====================================================================
    print("--- 6a. Regression of original columns ---")

    df_current_orig = df_current_no_aud.select([c for c in ORIGINAL_COLS if c in df_current_no_aud.columns])
    df_baseline_orig = df_baseline.select([c for c in ORIGINAL_COLS if c in df_baseline.columns])

    check("6a.1 Schema match",
          list(df_current_orig.columns) == list(df_baseline_orig.columns),
          f"current={list(df_current_orig.columns)}, baseline={list(df_baseline_orig.columns)}")

    check("6a.2 Row count match",
          df_current_orig.height == df_baseline_orig.height,
          f"current={df_current_orig.height:,}, baseline={df_baseline_orig.height:,}")

    # Sort both for deterministic comparison
    sort_cols = ["lead_id", "project_id", "spot_id", "lds_event_id", "lds_event_at",
                 "apd_id", "appointment_id", "alg_id"]
    available_sort = [c for c in sort_cols if c in df_current_orig.columns]

    df_c_sorted = df_current_orig.sort(available_sort, nulls_last=True)
    df_b_sorted = df_baseline_orig.sort(available_sort, nulls_last=True)

    if df_c_sorted.height == df_b_sorted.height:
        mismatches = 0
        for col in ORIGINAL_COLS:
            if col not in df_c_sorted.columns:
                continue
            c_vals = df_c_sorted[col]
            b_vals = df_b_sorted[col]
            diff_mask = c_vals.ne_missing(b_vals)
            col_mismatches = diff_mask.sum()
            if col_mismatches > 0:
                mismatches += col_mismatches
                print(f"    Column {col}: {col_mismatches} mismatches")
        check("6a.3 Values match (all original columns)",
              mismatches == 0,
              f"{mismatches} total mismatches")
    else:
        check("6a.3 Values match (all original columns)", False,
              "Skipped due to row count mismatch")

    # =====================================================================
    # 6b. STRUCTURAL VALIDATION OF NEW FIELDS
    # =====================================================================
    print("\n--- 6b. Structural validation of new fields ---")

    for col in CHANNEL_COLS:
        check(f"6b.1 Column exists: {col}", col in df_current.columns)

    check("6b.2 Total columns = 35",
          df_current_no_aud.width + len(aud_cols) == 35 or df_current.width == 35,
          f"actual={df_current.width}")

    from dagster_pipeline.defs.data_lakehouse.gold.gold_bt_lds_lead_spots_new import BT_LDS_LEAD_SPOTS_COLUMN_ORDER
    check("6b.3 Column order matches BT_LDS_LEAD_SPOTS_COLUMN_ORDER",
          list(df_current.columns) == list(BT_LDS_LEAD_SPOTS_COLUMN_ORDER),
          f"expected {len(BT_LDS_LEAD_SPOTS_COLUMN_ORDER)}, got {len(df_current.columns)}")

    # =====================================================================
    # 6c. NULLABILITY BY EVENT
    # =====================================================================
    print("\n--- 6c. Nullability by event ---")

    has_channel_cols = all(c in df_current.columns for c in CHANNEL_COLS)
    if not has_channel_cols:
        print("    (Channel columns not yet in gold - skipping 6c-6g)")
        print("    Run the modified pipeline first to populate these columns.")
        check("6c-6g Skipped (columns not present)", True, "Run pipeline first")
        print("\n" + "=" * 70)
        passed = sum(1 for _, p, _ in results if p)
        failed = sum(1 for _, p, _ in results if not p)
        print(f"RESULTS: {passed} passed, {failed} failed, {len(results)} total")
        if failed > 0:
            print("\nFAILED CHECKS:")
            for name, p, detail in results:
                if not p:
                    print(f"  - {name}: {detail}")
        print("=" * 70)
        return 0 if failed == 0 else 1

    df_not_3 = df_current.filter(pl.col("lds_event_id") != 3)
    df_evt_3 = df_current.filter(pl.col("lds_event_id") == 3)

    all_null_for_non3 = True
    for col in CHANNEL_COLS:
        if col in df_not_3.columns:
            non_null = df_not_3.filter(pl.col(col).is_not_null()).height
            if non_null > 0:
                all_null_for_non3 = False
                print(f"    {col}: {non_null} non-null values for event != 3")
    check("6c.1 All channel cols null for lds_event_id != 3", all_null_for_non3)

    if df_evt_3.height > 0:
        null_attr_id = df_evt_3.filter(pl.col("lds_channel_attribution_id").is_null()).height
        check("6c.2 lds_channel_attribution_id never null for event 3",
              null_attr_id == 0,
              f"{null_attr_id} null values")

        null_attr = df_evt_3.filter(pl.col("lds_channel_attribution").is_null()).height
        check("6c.3 lds_channel_attribution never null for event 3",
              null_attr == 0,
              f"{null_attr} null values")
    else:
        check("6c.2 lds_channel_attribution_id never null for event 3", True, "No event 3 rows")
        check("6c.3 lds_channel_attribution never null for event 3", True, "No event 3 rows")

    # =====================================================================
    # 6d. CHANNEL COVERAGE
    # =====================================================================
    print("\n--- 6d. Channel coverage ---")

    if df_evt_3.height > 0:
        channel_ids = set(df_evt_3["lds_channel_attribution_id"].unique().to_list())
        check("6d.1 All three channels present (1, 2, 3)",
              {1, 2, 3}.issubset(channel_ids),
              f"found channels: {sorted(channel_ids)}")

        channel_counts = df_evt_3.group_by("lds_channel_attribution_id").len()
        total_from_channels = channel_counts["len"].sum()
        check("6d.2 Channel sum equals total event 3 rows",
              total_from_channels == df_evt_3.height,
              f"sum={total_from_channels}, total={df_evt_3.height}")

        invalid = df_evt_3.filter(~pl.col("lds_channel_attribution_id").is_in([1, 2, 3])).height
        check("6d.3 No invalid channel IDs",
              invalid == 0,
              f"{invalid} invalid")

        print(f"\n    Channel distribution:")
        for row in channel_counts.sort("lds_channel_attribution_id").iter_rows(named=True):
            pct = row["len"] / df_evt_3.height * 100
            print(f"      Channel {row['lds_channel_attribution_id']}: {row['len']:,} ({pct:.1f}%)")
    else:
        check("6d.1 All three channels present", False, "No event 3 rows")
        check("6d.2 Channel sum equals total", False, "No event 3 rows")
        check("6d.3 No invalid channel IDs", True, "No event 3 rows")

    # =====================================================================
    # 6e. CROSS-REFERENCE KEY VALIDATION
    # =====================================================================
    print("\n--- 6e. Cross-reference key validation ---")

    if df_evt_3.height > 0:
        algo_rows = df_evt_3.filter(pl.col("lds_channel_attribution_id") == 1)
        chat_rows = df_evt_3.filter(pl.col("lds_channel_attribution_id") == 2)
        manual_rows = df_evt_3.filter(pl.col("lds_channel_attribution_id") == 3)

        if algo_rows.height > 0:
            null_pid = algo_rows.filter(pl.col("project_id").is_null()).height
            null_sid = algo_rows.filter(pl.col("spot_id").is_null()).height
            check("6e.1 Algorithm: project_id/spot_id not null",
                  null_pid == 0 and null_sid == 0,
                  f"null project_id={null_pid}, null spot_id={null_sid}")

            null_algo_ts = algo_rows.filter(pl.col("lds_algorithm_evidence_at").is_null()).height
            check("6e.4 Algorithm: lds_algorithm_evidence_at not null",
                  null_algo_ts == 0,
                  f"{null_algo_ts} null")
        else:
            check("6e.1 Algorithm: project_id/spot_id not null", True, "No algorithm rows")
            check("6e.4 Algorithm: lds_algorithm_evidence_at not null", True, "No algorithm rows")

        if chat_rows.height > 0:
            null_lid = chat_rows.filter(pl.col("lead_id").is_null()).height
            null_sid = chat_rows.filter(pl.col("spot_id").is_null()).height
            check("6e.2 Chatbot: lead_id/spot_id not null",
                  null_lid == 0 and null_sid == 0,
                  f"null lead_id={null_lid}, null spot_id={null_sid}")

            null_chat_ts = chat_rows.filter(pl.col("lds_chatbot_evidence_at").is_null()).height
            check("6e.5 Chatbot: lds_chatbot_evidence_at not null",
                  null_chat_ts == 0,
                  f"{null_chat_ts} null")
        else:
            check("6e.2 Chatbot: lead_id/spot_id not null", True, "No chatbot rows")
            check("6e.5 Chatbot: lds_chatbot_evidence_at not null", True, "No chatbot rows")

        if manual_rows.height > 0:
            null_pid = manual_rows.filter(pl.col("project_id").is_null()).height
            null_sid = manual_rows.filter(pl.col("spot_id").is_null()).height
            check("6e.3 Manual: project_id/spot_id not null",
                  null_pid == 0 and null_sid == 0,
                  f"null project_id={null_pid}, null spot_id={null_sid}")

            both_null = manual_rows.filter(
                pl.col("lds_algorithm_evidence_at").is_null() &
                pl.col("lds_chatbot_evidence_at").is_null()
            ).height
            check("6e.6 Manual: both evidence timestamps null",
                  both_null == manual_rows.height,
                  f"{manual_rows.height - both_null} have non-null evidence")
        else:
            check("6e.3 Manual: project_id/spot_id not null", True, "No manual rows")
            check("6e.6 Manual: both evidence timestamps null", True, "No manual rows")
    else:
        for i in range(1, 7):
            check(f"6e.{i} (skipped)", True, "No event 3 rows")

    # =====================================================================
    # 6f. CONSISTENCY BETWEEN CHANNEL AND FLAGS
    # =====================================================================
    print("\n--- 6f. Consistency between channel and flags ---")

    if df_evt_3.height > 0:
        algo_not_rec = df_evt_3.filter(
            (pl.col("lds_channel_attribution_id") == 1) &
            (pl.col("is_recommended_algorithm_id") != 1)
        ).height
        check("6f.1 Algorithm channel -> is_recommended_algorithm=1",
              algo_not_rec == 0,
              f"{algo_not_rec} violations")

        id_yes_mismatch = df_evt_3.filter(
            (pl.col("is_recommended_algorithm_id") == 1) &
            (pl.col("is_recommended_algorithm") != "Yes")
        ).height
        id_no_mismatch = df_evt_3.filter(
            (pl.col("is_recommended_algorithm_id") == 0) &
            (pl.col("is_recommended_algorithm") != "No")
        ).height
        check("6f.2 is_recommended_algorithm_id matches text",
              id_yes_mismatch == 0 and id_no_mismatch == 0,
              f"yes_mismatch={id_yes_mismatch}, no_mismatch={id_no_mismatch}")

        cb_yes_mismatch = df_evt_3.filter(
            (pl.col("is_recommended_chatbot_id") == 1) &
            (pl.col("is_recommended_chatbot") != "Yes")
        ).height
        cb_no_mismatch = df_evt_3.filter(
            (pl.col("is_recommended_chatbot_id") == 0) &
            (pl.col("is_recommended_chatbot") != "No")
        ).height
        check("6f.3 is_recommended_chatbot_id matches text",
              cb_yes_mismatch == 0 and cb_no_mismatch == 0,
              f"yes_mismatch={cb_yes_mismatch}, no_mismatch={cb_no_mismatch}")

        invalid_algo = df_evt_3.filter(
            ~pl.col("is_recommended_algorithm_id").is_in([0, 1])
        ).height
        invalid_chat = df_evt_3.filter(
            ~pl.col("is_recommended_chatbot_id").is_in([0, 1])
        ).height
        check("6f.4 Flag values in {0, 1} only",
              invalid_algo == 0 and invalid_chat == 0,
              f"invalid algo={invalid_algo}, chatbot={invalid_chat}")
    else:
        for i in range(1, 5):
            check(f"6f.{i} (skipped)", True, "No event 3 rows")

    # =====================================================================
    # 6g. UNIQUENESS
    # =====================================================================
    print("\n--- 6g. Uniqueness ---")

    if df_evt_3.height > 0:
        unique_combos = df_evt_3.select(["lead_id", "project_id", "spot_id"]).unique().height
        check("6g.1 Unique (lead_id, project_id, spot_id) for event 3",
              unique_combos == df_evt_3.height,
              f"unique={unique_combos}, total={df_evt_3.height}")
    else:
        check("6g.1 Uniqueness", True, "No event 3 rows")

    # =====================================================================
    # SUMMARY
    # =====================================================================
    print("\n" + "=" * 70)
    passed = sum(1 for _, p, _ in results if p)
    failed = sum(1 for _, p, _ in results if not p)
    print(f"RESULTS: {passed} passed, {failed} failed, {len(results)} total")

    if failed > 0:
        print("\nFAILED CHECKS:")
        for name, p, detail in results:
            if not p:
                print(f"  - {name}: {detail}")

    print("=" * 70)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
