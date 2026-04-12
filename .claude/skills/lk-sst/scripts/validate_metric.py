#!/usr/bin/env python3
"""
Validates that ad-hoc metrics from lk_spot_status_history match
the pre-computed dm_spot_historical_metrics table.

Test case: daily public inventory by sector and modality.

Usage:
    cd dagster-pipeline
    uv run python ../.claude/skills/lk-sst/scripts/validate_metric.py

    # Or from repo root:
    cd .claude/skills/lk-sst/scripts
    python validate_metric.py
"""

import sys
import warnings

import boto3
import pandas as pd
import psycopg2

warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")


# =============================================================================
# CONNECTION (AWS SSM — same pattern as existing tests)
# =============================================================================

ssm = boto3.client("ssm", region_name="us-east-1")


def _get_ssm_param(name: str, decrypt: bool = True) -> str:
    return ssm.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]


def get_postgres_connection():
    host = _get_ssm_param("/dagster/lk_conversations_metrics/db_geospot_host", decrypt=False)
    user = _get_ssm_param("/dagster/lk_conversations_metrics/db_geospot_user")
    password = _get_ssm_param("/dagster/lk_conversations_metrics/db_geospot_password")
    return psycopg2.connect(
        host=host, port=5432, user=user, password=password, database="geospot",
    )


# =============================================================================
# QUERIES
# =============================================================================

ADHOC_QUERY = """
WITH date_series AS (
  SELECT generate_series('2025-09-01'::date, CURRENT_DATE, '1 day')::date AS fecha
)
SELECT
  ds.fecha,
  s.spot_sector,
  s.spot_modality,
  COUNT(DISTINCT h.spot_id) AS spots_activos
FROM date_series ds
JOIN lk_spot_status_history h
  ON h.source_sst_created_at::date <= ds.fecha
  AND (h.next_source_sst_created_at::date >= ds.fecha
       OR h.next_source_sst_created_at IS NULL)
JOIN lk_spots s ON h.spot_id = s.spot_id
WHERE h.spot_status_id = 1              -- Public
  AND s.spot_sector_id IS NOT NULL
  AND h.spot_hard_deleted_id = 0         -- No
GROUP BY ds.fecha, s.spot_sector, s.spot_modality
ORDER BY ds.fecha, s.spot_sector, s.spot_modality
"""

DM_QUERY = """
SELECT
  DATE(timeframe_timestamp) AS fecha,
  spot_sector,
  spot_modality,
  SUM(spot_count) AS spots_activos
FROM dm_spot_historical_metrics
WHERE timeframe_id = 1
  AND spot_status = 'Public'
  AND spot_sector IS NOT NULL
  AND DATE(timeframe_timestamp) >= '2025-09-01'
GROUP BY DATE(timeframe_timestamp), spot_sector, spot_modality
ORDER BY fecha, spot_sector, spot_modality
"""


# =============================================================================
# COMPARISON
# =============================================================================

SORT_KEYS = ["fecha", "spot_sector", "spot_modality"]


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizes a DataFrame for comparison."""
    df = df.copy()
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
    df["spot_sector"] = df["spot_sector"].astype(str).str.strip()
    df["spot_modality"] = df["spot_modality"].fillna("").astype(str).str.strip()
    df["spots_activos"] = pd.to_numeric(df["spots_activos"], errors="coerce").fillna(0).astype(int)
    df = df.sort_values(SORT_KEYS, kind="mergesort").reset_index(drop=True)
    return df


def compare(df_adhoc: pd.DataFrame, df_dm: pd.DataFrame):
    """Compares ad-hoc results vs dm_spot_historical_metrics."""
    print(f"\n  Ad-hoc shape:  {df_adhoc.shape}")
    print(f"  DM shape:      {df_dm.shape}")

    if df_adhoc.shape[0] != df_dm.shape[0]:
        print(f"\n  ROW COUNT MISMATCH: ad-hoc={df_adhoc.shape[0]}, dm={df_dm.shape[0]}")

        adhoc_keys = set(df_adhoc[SORT_KEYS].apply(tuple, axis=1))
        dm_keys = set(df_dm[SORT_KEYS].apply(tuple, axis=1))

        only_adhoc = adhoc_keys - dm_keys
        only_dm = dm_keys - adhoc_keys

        if only_adhoc:
            print(f"  Keys only in ad-hoc ({len(only_adhoc)}):")
            for k in sorted(only_adhoc)[:5]:
                print(f"    {k}")

        if only_dm:
            print(f"  Keys only in dm ({len(only_dm)}):")
            for k in sorted(only_dm)[:5]:
                print(f"    {k}")

    merged = df_adhoc.merge(
        df_dm,
        on=SORT_KEYS,
        how="outer",
        suffixes=("_adhoc", "_dm"),
        indicator=True,
    )

    both = merged[merged["_merge"] == "both"]
    mismatches = both[both["spots_activos_adhoc"] != both["spots_activos_dm"]]

    n_total = len(both)
    n_match = n_total - len(mismatches)

    print(f"\n  Rows compared:  {n_total}")
    print(f"  Matches:        {n_match}")
    print(f"  Mismatches:     {len(mismatches)}")

    if len(mismatches) > 0:
        print(f"\n  First 10 mismatches:")
        print(f"  {'fecha':<12} {'sector':<15} {'modality':<12} {'ad-hoc':>8} {'dm':>8} {'diff':>8}")
        print(f"  {'-'*12} {'-'*15} {'-'*12} {'-'*8} {'-'*8} {'-'*8}")

        for _, row in mismatches.head(10).iterrows():
            diff = row["spots_activos_adhoc"] - row["spots_activos_dm"]
            print(
                f"  {str(row['fecha']):<12} "
                f"{str(row['spot_sector']):<15} "
                f"{str(row['spot_modality']):<12} "
                f"{row['spots_activos_adhoc']:>8} "
                f"{row['spots_activos_dm']:>8} "
                f"{diff:>+8}"
            )

    all_match = len(mismatches) == 0 and df_adhoc.shape[0] == df_dm.shape[0]
    return all_match


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 70)
    print("VALIDATION: Ad-hoc metric vs dm_spot_historical_metrics")
    print("Metric: Daily public inventory by sector and modality")
    print("=" * 70)

    conn = get_postgres_connection()

    try:
        print("\n[1/3] Running ad-hoc query (lk_spot_status_history + lk_spots, ID-first)...")
        df_adhoc = pd.read_sql(ADHOC_QUERY, conn)
        print(f"  -> {len(df_adhoc):,} rows")

        print("\n[2/3] Running dm query (dm_spot_historical_metrics)...")
        df_dm = pd.read_sql(DM_QUERY, conn)
        print(f"  -> {len(df_dm):,} rows")
    finally:
        conn.close()

    print("\n[3/3] Comparing results...")
    df_adhoc_norm = normalize(df_adhoc)
    df_dm_norm = normalize(df_dm)

    all_match = compare(df_adhoc_norm, df_dm_norm)

    print("\n" + "=" * 70)
    if all_match:
        print(f"RESULT: PASS -- 100% match ({df_adhoc_norm.shape[0]:,} rows)")
    else:
        print("RESULT: FAIL -- differences found (see details above)")
    print("=" * 70)

    return 0 if all_match else 1


if __name__ == "__main__":
    sys.exit(main())
