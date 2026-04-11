"""
Test: ejecutar q_browse_sessions.sql y validar resultados.
Compara volúmenes con hallazgos de Fase 1 y muestra distribuciones clave.

Ejecutar con: uv run python lakehouse-sdk/ad_hoc/conversion_visitante_lead_2026-04-08/test_browse_sessions.py
"""

import json
from pathlib import Path

import boto3
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

SSM_BIGQUERY_CREDENTIALS = "/dagster/vtl/BIGQUERY_CREDENTIALS"
QUERY_PATH = Path(__file__).resolve().parent / "q_browse_sessions.sql"


def get_bq_client():
    ssm = boto3.client("ssm", region_name="us-east-1")
    creds_json = ssm.get_parameter(Name=SSM_BIGQUERY_CREDENTIALS, WithDecryption=True)["Parameter"]["Value"]
    creds_info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(creds_info)
    return bigquery.Client(project=creds_info.get("project_id"), credentials=credentials)


def main():
    client = get_bq_client()
    sql = QUERY_PATH.read_text(encoding="utf-8").rstrip().rstrip(";")

    print("Ejecutando q_browse_sessions.sql (ventana completa ~6 meses)...")
    df = client.query(sql).to_dataframe()
    print(f"Filas: {len(df)}, Columnas: {len(df.columns)}")
    print(f"Columnas: {list(df.columns)}\n")

    print("=" * 70)
    print("RESUMEN GENERAL")
    print("=" * 70)

    n_sessions = len(df)
    n_users = df["user_pseudo_id"].nunique()
    n_dates = df["event_date"].nunique()
    print(f"  Sesiones totales: {n_sessions:,}")
    print(f"  Usuarios únicos:  {n_users:,}")
    print(f"  Días cubiertos:   {n_dates}")

    # Last 30 days subset for comparison with Fase 1
    df["event_date_dt"] = pd.to_datetime(df["event_date"].astype(str), format="%Y%m%d", errors="coerce")
    cutoff_30d = df["event_date_dt"].max() - pd.Timedelta(days=30)
    last30 = df[df["event_date_dt"] >= cutoff_30d]

    print(f"\n  --- Últimos 30 días (para comparar con Fase 1) ---")
    print(f"  Sesiones:         {len(last30):,}")
    print(f"  Usuarios únicos:  {last30['user_pseudo_id'].nunique():,}")
    print(f"  Promedio diario sesiones: ~{len(last30) // max(last30['event_date'].nunique(), 1)}")

    # has_session_id coverage
    if "has_session_id" in df.columns:
        pct_session = df["has_session_id"].mean() * 100
        print(f"  ga_session_id disponible: {pct_session:.1f}%")

    # Channel distribution
    print(f"\n{'=' * 70}")
    print("DISTRIBUCIÓN DE CANAL (últimos 30 días)")
    print("=" * 70)
    ch = last30["channel"].value_counts()
    for canal, count in ch.items():
        pct = count / len(last30) * 100
        print(f"  {canal:<25} {count:>6}  ({pct:.1f}%)")

    # Traffic type
    print(f"\n{'=' * 70}")
    print("DISTRIBUCIÓN TRAFFIC_TYPE (últimos 30 días)")
    print("=" * 70)
    tt = last30["traffic_type"].value_counts()
    for t, count in tt.items():
        pct = count / len(last30) * 100
        print(f"  {t:<25} {count:>6}  ({pct:.1f}%)")

    # is_scraping
    print(f"\n{'=' * 70}")
    print("SCRAPING (últimos 30 días)")
    print("=" * 70)
    scr = last30["is_scraping"].value_counts()
    for val, count in scr.items():
        print(f"  is_scraping={val}: {count:>6}  ({count / len(last30) * 100:.1f}%)")

    # Browse page views per session
    print(f"\n{'=' * 70}")
    print("BROWSE PAGE VIEWS POR SESIÓN (últimos 30 días)")
    print("=" * 70)
    bpv = last30["browse_page_views"]
    bins = [(1, 1), (2, 2), (3, 5), (6, 10), (11, 999)]
    for lo, hi in bins:
        label = str(lo) if lo == hi else f"{lo}-{hi}" if hi < 999 else f"{lo}+"
        count = ((bpv >= lo) & (bpv <= hi)).sum()
        pct = count / len(last30) * 100
        print(f"  {label:<10} {count:>6}  ({pct:.1f}%)")

    # Conversion in session
    print(f"\n{'=' * 70}")
    print("CONVERSIÓN EN SESIÓN (últimos 30 días)")
    print("=" * 70)
    if "has_conversion_in_session" in last30.columns:
        conv = last30["has_conversion_in_session"].sum()
        pct_conv = conv / len(last30) * 100
        print(f"  Sesiones con conversión: {conv:>6}  ({pct_conv:.1f}%)")
        print(f"  Sesiones sin conversión: {len(last30) - conv:>6}  ({100 - pct_conv:.1f}%)")

        if conv > 0:
            conv_names = last30[last30["has_conversion_in_session"]]["conversion_event_names"].str.split(", ").explode().value_counts()
            print("  Eventos de conversión:")
            for ev, c in conv_names.head(10).items():
                print(f"    {ev}: {c}")

    # Device
    print(f"\n{'=' * 70}")
    print("DISPOSITIVO (últimos 30 días)")
    print("=" * 70)
    dev = last30["device_category"].value_counts()
    for d, count in dev.items():
        pct = count / len(last30) * 100
        print(f"  {str(d):<15} {count:>6}  ({pct:.1f}%)")

    # Top browse pages
    print(f"\n{'=' * 70}")
    print("TOP 10 FIRST BROWSE PAGES (últimos 30 días)")
    print("=" * 70)
    top_pages = last30["first_browse_page"].value_counts().head(10)
    for page, count in top_pages.items():
        print(f"  {count:>5}  {str(page)[:100]}")

    print(f"\n{'=' * 70}")
    print("VALIDACIÓN COMPLETADA")
    print("=" * 70)


if __name__ == "__main__":
    main()
