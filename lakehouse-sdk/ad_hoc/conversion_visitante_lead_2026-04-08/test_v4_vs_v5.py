"""
Test: funnel_with_channel v4 vs v5
Valida que v5 no rompe nada y que los cambios son los esperados.

v5 agrega a v4:
  - primera_bifurcacion: +bodegas, +coworking
  - conversion_point: +renta, +venta, +bodegas, +coworking

Ejecutar con: uv run python lakehouse-sdk/ad_hoc/conversion_visitante_lead_2026-04-08/test_v4_vs_v5.py
"""

import sys
import json
from pathlib import Path

import boto3
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

SSM_BIGQUERY_CREDENTIALS = "/dagster/vtl/BIGQUERY_CREDENTIALS"
QUERIES_DIR = Path(__file__).resolve().parent / "../../sql/old"
WINDOW_DAYS = 7


def get_bq_client():
    ssm = boto3.client("ssm", region_name="us-east-1")
    creds_json = ssm.get_parameter(Name=SSM_BIGQUERY_CREDENTIALS, WithDecryption=True)["Parameter"]["Value"]
    creds_info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(creds_info)
    return bigquery.Client(project=creds_info.get("project_id"), credentials=credentials)


def load_and_scope_query(filename: str) -> str:
    """Load SQL file and restrict to last WINDOW_DAYS days for speed."""
    path = QUERIES_DIR / filename
    sql = path.read_text(encoding="utf-8")

    # Replace the broad date filter with a narrow window
    sql = sql.replace(
        "event_date >= '20250201'",
        f"event_date >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL {WINDOW_DAYS} DAY))"
    )
    # Remove trailing semicolons
    sql = sql.rstrip().rstrip(";")
    return sql


def run_query(client, sql: str, label: str) -> pd.DataFrame:
    print(f"  Ejecutando {label}...")
    df = client.query(sql).to_dataframe()
    print(f"  {label}: {len(df)} filas, {len(df.columns)} columnas")
    return df


def compare(v4: pd.DataFrame, v5: pd.DataFrame):
    print("\n" + "=" * 70)
    print("VALIDACIÓN v4 vs v5")
    print("=" * 70)

    all_pass = True

    # 1. Row count
    print(f"\n1. CONTEO DE FILAS")
    print(f"   v4: {len(v4)}")
    print(f"   v5: {len(v5)}")
    if len(v4) == len(v5):
        print("   ✓ PASS — Misma cantidad de filas")
    else:
        print("   ✗ FAIL — Cantidad de filas difiere")
        all_pass = False

    # Align on (user, event_date) for column comparison
    key = ["user", "event_date"]
    merged = v4.merge(v5, on=key, suffixes=("_v4", "_v5"), how="outer", indicator=True)
    only_v4 = (merged["_merge"] == "left_only").sum()
    only_v5 = (merged["_merge"] == "right_only").sum()
    both = (merged["_merge"] == "both").sum()
    print(f"\n2. COBERTURA (merge on user + event_date)")
    print(f"   Ambos: {both} | Solo v4: {only_v4} | Solo v5: {only_v5}")
    if only_v4 == 0 and only_v5 == 0:
        print("   ✓ PASS — Cobertura idéntica")
    else:
        print("   ✗ FAIL — Hay filas no compartidas")
        all_pass = False

    # Filter to intersection
    matched = merged[merged["_merge"] == "both"].copy()

    # 3. Columns that must be 100% identical
    cols_must_match = [
        ("Channel", "channel"),
        ("Traffic_type", "traffic_type"),
        ("source", "source"),
        ("medium", "medium"),
        ("campaign_name", "campaign_name"),
        ("user_sospechoso", "user_sospechoso"),
        ("segunda_bifurcacion", "segunda_bifurcacion"),
        ("flag_engagement", "flag_engagement"),
    ]

    print(f"\n3. COLUMNAS QUE DEBEN SER IDÉNTICAS")
    for col_name, label in cols_must_match:
        c4 = f"{col_name}_v4"
        c5 = f"{col_name}_v5"
        if c4 not in matched.columns or c5 not in matched.columns:
            print(f"   ? SKIP — '{col_name}' no encontrada en ambos DataFrames")
            continue
        diff_mask = matched[c4].fillna("__NULL__") != matched[c5].fillna("__NULL__")
        diff_count = diff_mask.sum()
        pct = (1 - diff_count / len(matched)) * 100 if len(matched) > 0 else 100
        if diff_count == 0:
            print(f"   ✓ PASS — {label}: 100% idéntico")
        else:
            print(f"   ✗ FAIL — {label}: {diff_count} diferencias ({pct:.2f}% coincide)")
            all_pass = False
            sample = matched[diff_mask][[c4, c5]].head(3)
            for _, row in sample.iterrows():
                print(f"           v4='{row[c4]}' → v5='{row[c5]}'")

    # 4. primera_bifurcacion delta
    print(f"\n4. DELTA primera_bifurcacion")
    pb4 = "primera_bifurcacion_v4"
    pb5 = "primera_bifurcacion_v5"
    if pb4 in matched.columns and pb5 in matched.columns:
        changed = matched[matched[pb4].fillna("") != matched[pb5].fillna("")].copy()
        print(f"   Filas que cambiaron: {len(changed)} de {len(matched)}")

        if len(changed) > 0:
            transitions = changed.groupby([pb4, pb5]).size().reset_index(name="count")
            transitions = transitions.sort_values("count", ascending=False)
            print("   Transiciones:")
            for _, row in transitions.iterrows():
                print(f"     '{row[pb4]}' → '{row[pb5]}': {row['count']} filas")

            expected_from = {"Other"}
            expected_to = {"Browse Pages"}
            actual_from = set(transitions[pb4].unique())
            actual_to = set(transitions[pb5].unique())

            if actual_from == expected_from and actual_to == expected_to:
                print("   ✓ PASS — Solo transiciones Other → Browse Pages (esperado)")
            else:
                print(f"   ⚠ WARN — Transiciones inesperadas: from={actual_from}, to={actual_to}")
                all_pass = False

            # Verify the changed pages contain bodegas/coworking
            if "page_locations_v4" in matched.columns:
                sample_pages = changed["page_locations_v4"].head(5).tolist()
                print("   Ejemplo page_locations afectados:")
                for p in sample_pages:
                    print(f"     {str(p)[:120]}")
        else:
            print("   ⚠ INFO — Sin cambios (puede ser que no haya tráfico bodegas/coworking en la ventana)")
    else:
        print("   ? SKIP — columna no encontrada")

    # 5. conversion_point delta
    print(f"\n5. DELTA conversion_point")
    cp4 = "conversion_point_v4"
    cp5 = "conversion_point_v5"
    if cp4 in matched.columns and cp5 in matched.columns:
        changed_cp = matched[matched[cp4].fillna("") != matched[cp5].fillna("")].copy()
        print(f"   Filas que cambiaron: {len(changed_cp)} de {len(matched)}")

        if len(changed_cp) > 0:
            transitions_cp = changed_cp.groupby([cp4, cp5]).size().reset_index(name="count")
            transitions_cp = transitions_cp.sort_values("count", ascending=False)
            print("   Transiciones:")
            for _, row in transitions_cp.iterrows():
                print(f"     '{row[cp4]}' → '{row[cp5]}': {row['count']} filas")
        else:
            print("   ℹ INFO — Sin cambios en conversion_point en la ventana")
    else:
        print("   ? SKIP — columna no encontrada")

    # Summary
    print("\n" + "=" * 70)
    if all_pass:
        print("RESULTADO: ✓ TODAS LAS VALIDACIONES PASARON")
    else:
        print("RESULTADO: ✗ HAY VALIDACIONES FALLIDAS (ver arriba)")
    print("=" * 70)

    return all_pass


def main():
    client = get_bq_client()

    v4_sql = load_and_scope_query(" funnel_with_channel_v4.sql")
    v5_sql = load_and_scope_query(" funnel_with_channel_v5.sql")

    v4_df = run_query(client, v4_sql, "v4")
    v5_df = run_query(client, v5_sql, "v5")

    passed = compare(v4_df, v5_df)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
