"""
Test: consistencia de canal entre q_browse_sessions.sql y funnel_with_channel_v5.sql.

Compara la intersección de (user_pseudo_id, event_date), filtrando browse_sessions
a primera sesión del día para alinear con la granularidad del funnel.

Ejecutar con: uv run python lakehouse-sdk/ad_hoc/conversion_visitante_lead_2026-04-08/test_channel_consistency.py
"""

import json
from pathlib import Path

import boto3
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

SSM_BIGQUERY_CREDENTIALS = "/dagster/vtl/BIGQUERY_CREDENTIALS"
BASE_DIR = Path(__file__).resolve().parent
QUERIES_DIR = BASE_DIR / "../../sql/old"
WINDOW_DAYS = 7


def get_bq_client():
    ssm = boto3.client("ssm", region_name="us-east-1")
    creds_json = ssm.get_parameter(Name=SSM_BIGQUERY_CREDENTIALS, WithDecryption=True)["Parameter"]["Value"]
    creds_info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(creds_info)
    return bigquery.Client(project=creds_info.get("project_id"), credentials=credentials)


def load_and_scope(path: Path, window_days: int = WINDOW_DAYS) -> str:
    sql = path.read_text(encoding="utf-8").rstrip().rstrip(";")
    sql = sql.replace(
        "event_date >= '20250201'",
        f"event_date >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL {window_days} DAY))"
    )
    # For browse_sessions, restrict the 6-month window to WINDOW_DAYS
    sql = sql.replace(
        "DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH)",
        f"DATE_SUB(CURRENT_DATE(), INTERVAL {window_days} DAY)"
    )
    return sql


def compare_column(merged: pd.DataFrame, col_bs: str, col_v5: str, label: str):
    """Compare a column between browse_sessions and v5, return match count."""
    if col_bs not in merged.columns or col_v5 not in merged.columns:
        print(f"  ? SKIP — '{label}' no encontrada")
        return

    mask = merged[col_bs].fillna("__NULL__") == merged[col_v5].fillna("__NULL__")
    match_count = mask.sum()
    total = len(merged)
    pct = match_count / total * 100 if total > 0 else 0

    status = "✓" if pct >= 90 else "⚠" if pct >= 70 else "✗"
    print(f"  {status} {label}: {match_count}/{total} ({pct:.1f}% coincide)")

    if not mask.all():
        diff = merged[~mask][[col_bs, col_v5]].copy()
        diff.columns = [f"browse_sessions", f"v5"]
        transitions = diff.groupby(["browse_sessions", "v5"]).size().reset_index(name="count")
        transitions = transitions.sort_values("count", ascending=False)
        print(f"    Discrepancias ({(~mask).sum()} filas):")
        for _, row in transitions.head(10).iterrows():
            print(f"      BS='{row['browse_sessions']}' vs V5='{row['v5']}': {row['count']}")


def main():
    client = get_bq_client()

    # Load queries
    browse_sql = load_and_scope(BASE_DIR / "q_browse_sessions.sql")
    v5_sql = load_and_scope(QUERIES_DIR / " funnel_with_channel_v5.sql")

    print(f"Ejecutando browse_sessions ({WINDOW_DAYS} días)...")
    bs_df = client.query(browse_sql).to_dataframe()
    print(f"  browse_sessions: {len(bs_df)} sesiones")

    print(f"Ejecutando v5 ({WINDOW_DAYS} días)...")
    v5_df = client.query(v5_sql).to_dataframe()
    print(f"  v5: {len(v5_df)} filas")

    # Filter browse_sessions to first session per (user, date) to align with funnel
    bs_df["session_start"] = pd.to_datetime(bs_df["session_start"])
    bs_first = (
        bs_df.sort_values("session_start")
        .drop_duplicates(subset=["user_pseudo_id", "event_date"], keep="first")
        .copy()
    )
    print(f"  browse_sessions (primera sesión/día): {len(bs_first)} filas")

    # Normalize user column (v5 uses 'user', browse_sessions uses 'user_pseudo_id')
    v5_df = v5_df.rename(columns={"user": "user_pseudo_id"})

    # Normalize event_date to string YYYYMMDD for both
    bs_first["event_date_str"] = bs_first["event_date"].astype(str)
    v5_df["event_date_str"] = v5_df["event_date"].astype(str)
    # v5 event_date is a DATE type, convert to YYYYMMDD string
    if v5_df["event_date_str"].str.contains("-").any():
        v5_df["event_date_str"] = v5_df["event_date_str"].str.replace("-", "")

    # Merge on intersection
    merged = bs_first.merge(
        v5_df,
        left_on=["user_pseudo_id", "event_date_str"],
        right_on=["user_pseudo_id", "event_date_str"],
        how="inner",
        suffixes=("_bs", "_v5"),
    )
    print(f"\n  Intersección: {len(merged)} filas")
    print(f"  BS primera-sesión: {len(bs_first)} | V5: {len(v5_df)} | Intersección: {len(merged)}")
    pct_bs = len(merged) / len(bs_first) * 100 if len(bs_first) > 0 else 0
    pct_v5 = len(merged) / len(v5_df) * 100 if len(v5_df) > 0 else 0
    print(f"  Cobertura: {pct_bs:.1f}% de BS está en V5, {pct_v5:.1f}% de V5 está en BS")

    print(f"\n{'=' * 70}")
    print("COMPARACIÓN DE COLUMNAS EN LA INTERSECCIÓN")
    print("=" * 70)

    # Detect column names (they may have suffixes from merge)
    # Channel
    ch_bs = "channel_bs" if "channel_bs" in merged.columns else "channel"
    ch_v5 = "Channel" if "Channel" in merged.columns else "channel_v5"
    compare_column(merged, ch_bs, ch_v5, "channel")

    # Traffic type
    tt_bs = "traffic_type_bs" if "traffic_type_bs" in merged.columns else "traffic_type"
    tt_v5 = "Traffic_type" if "Traffic_type" in merged.columns else "traffic_type_v5"
    compare_column(merged, tt_bs, tt_v5, "traffic_type")

    # Source
    src_bs = "source_bs" if "source_bs" in merged.columns else "source"
    src_v5 = "source_v5" if "source_v5" in merged.columns else "source"
    compare_column(merged, src_bs, src_v5, "source")

    # Medium
    med_bs = "medium_bs" if "medium_bs" in merged.columns else "medium"
    med_v5 = "medium_v5" if "medium_v5" in merged.columns else "medium"
    compare_column(merged, med_bs, med_v5, "medium")

    # is_scraping vs user_sospechoso
    scr_bs = "is_scraping"
    scr_v5 = "user_sospechoso"
    if scr_bs in merged.columns and scr_v5 in merged.columns:
        compare_column(merged, scr_bs, scr_v5, "is_scraping vs user_sospechoso")

    # Analysis of channel discrepancies
    if ch_bs in merged.columns and ch_v5 in merged.columns:
        diff_mask = merged[ch_bs].fillna("") != merged[ch_v5].fillna("")
        if diff_mask.any():
            diff_rows = merged[diff_mask].copy()
            print(f"\n{'=' * 70}")
            print(f"ANÁLISIS DE DISCREPANCIAS DE CANAL ({diff_mask.sum()} filas)")
            print("=" * 70)

            # Check if discrepancies are due to first_browse_page != first page of day in funnel
            if "first_browse_page" in diff_rows.columns and "page_locations" in diff_rows.columns:
                print("  Muestra de discrepancias (browse_page vs funnel page_locations):")
                for _, row in diff_rows.head(5).iterrows():
                    bp = str(row.get("first_browse_page", ""))[:80]
                    fp = str(row.get("page_locations", ""))[:80]
                    print(f"    BS channel={row[ch_bs]}, V5 channel={row[ch_v5]}")
                    print(f"      BS page: {bp}")
                    print(f"      V5 page: {fp}")
                    print()

    print(f"\n{'=' * 70}")
    print("VALIDACIÓN DE CONSISTENCIA COMPLETADA")
    print("=" * 70)


if __name__ == "__main__":
    main()
