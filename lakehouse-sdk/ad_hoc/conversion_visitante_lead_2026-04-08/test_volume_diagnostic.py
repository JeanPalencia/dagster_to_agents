"""
Diagnóstico: diferencia de volúmenes entre Fase 1 (7,557 sesiones/30d)
y q_browse_sessions.sql (31,151 sesiones/30d).

Ejecutar con: uv run python lakehouse-sdk/ad_hoc/conversion_visitante_lead_2026-04-08/test_volume_diagnostic.py
"""

import json
from pathlib import Path

import boto3
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

SSM_BIGQUERY_CREDENTIALS = "/dagster/vtl/BIGQUERY_CREDENTIALS"


def get_bq_client():
    ssm = boto3.client("ssm", region_name="us-east-1")
    creds_json = ssm.get_parameter(Name=SSM_BIGQUERY_CREDENTIALS, WithDecryption=True)["Parameter"]["Value"]
    creds_info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(creds_info)
    return bigquery.Client(project=creds_info.get("project_id"), credentials=credentials)


def main():
    client = get_bq_client()

    # Query 1: Conteo simple con regex amplia (como browse_sessions)
    q_full = """
    SELECT
      event_date,
      COUNT(*) AS page_views,
      COUNT(DISTINCT user_pseudo_id) AS users,
      COUNT(DISTINCT CONCAT(
        user_pseudo_id, '-',
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING)
      )) AS sessions
    FROM `analytics_276054961.events_*`
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
      AND _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d', CURRENT_DATE())
      AND event_name = 'page_view'
      AND REGEXP_CONTAINS(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1),
        r'https://spot2\\.mx/(bodegas|coworking|locales-comerciales|naves-industriales|oficinas|renta|terrenos|venta)(/|$|\\?)'
      )
    GROUP BY event_date
    ORDER BY event_date
    """

    print("Conteo diario page_view con regex completa (30 días)...")
    df = client.query(q_full).to_dataframe()

    print(f"\n{'Fecha':<12} {'PV':>7} {'Users':>7} {'Sessions':>9}")
    print("-" * 38)
    for _, row in df.iterrows():
        print(f"{str(row['event_date']):<12} {row['page_views']:>7} {row['users']:>7} {row['sessions']:>9}")

    print(f"\n{'TOTALES':<12} {df['page_views'].sum():>7} {'-':>7} {'-':>9}")
    print(f"Promedio diario: PV={df['page_views'].mean():.0f}, Users={df['users'].mean():.0f}, Sessions={df['sessions'].mean():.0f}")

    # Query 2: Breakdown by first branch to see volume contribution
    q_branch = """
    SELECT
      REGEXP_EXTRACT(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1),
        r'https://spot2\\.mx/([^/?]+)'
      ) AS first_branch,
      COUNT(*) AS page_views,
      COUNT(DISTINCT user_pseudo_id) AS users
    FROM `analytics_276054961.events_*`
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
      AND _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d', CURRENT_DATE())
      AND event_name = 'page_view'
      AND REGEXP_CONTAINS(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1),
        r'https://spot2\\.mx/(bodegas|coworking|locales-comerciales|naves-industriales|oficinas|renta|terrenos|venta)(/|$|\\?)'
      )
    GROUP BY first_branch
    ORDER BY page_views DESC
    """

    print("\n\nVolumen por rama (30 días):")
    df_branch = client.query(q_branch).to_dataframe()
    for _, row in df_branch.iterrows():
        print(f"  {str(row['first_branch']):<25} PV={row['page_views']:>6}  Users={row['users']:>5}")

    # Query 3: Narrow regex (sin renta/venta raíz) - para ver si la Fase 1 excluía estas
    q_narrow = """
    SELECT
      COUNT(*) AS page_views,
      COUNT(DISTINCT user_pseudo_id) AS users,
      COUNT(DISTINCT CONCAT(
        user_pseudo_id, '-',
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING)
      )) AS sessions
    FROM `analytics_276054961.events_*`
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
      AND _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d', CURRENT_DATE())
      AND event_name = 'page_view'
      AND REGEXP_CONTAINS(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1),
        r'https://spot2\\.mx/(bodegas|coworking|locales-comerciales|naves-industriales|oficinas|renta|terrenos|venta)/'
      )
    """

    print("\n\nRegex estrecha (requiere / después de la rama):")
    df_narrow = client.query(q_narrow).to_dataframe()
    print(f"  PV={df_narrow['page_views'].iloc[0]:,}  Users={df_narrow['users'].iloc[0]:,}  Sessions={df_narrow['sessions'].iloc[0]:,}")

    # Query 4: Original Fase 1 regex (without bodegas/coworking)
    q_fase1 = """
    SELECT
      COUNT(*) AS page_views,
      COUNT(DISTINCT user_pseudo_id) AS users,
      COUNT(DISTINCT CONCAT(
        user_pseudo_id, '-',
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING)
      )) AS sessions
    FROM `analytics_276054961.events_*`
    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
      AND _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d', CURRENT_DATE())
      AND event_name = 'page_view'
      AND REGEXP_CONTAINS(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1),
        r'https://spot2\\.mx/(locales-comerciales|naves-industriales|oficinas|terrenos)/'
      )
    """

    print("\nRegex Fase 1 original (sin bodegas/coworking/renta/venta):")
    df_f1 = client.query(q_fase1).to_dataframe()
    print(f"  PV={df_f1['page_views'].iloc[0]:,}  Users={df_f1['users'].iloc[0]:,}  Sessions={df_f1['sessions'].iloc[0]:,}")


if __name__ == "__main__":
    main()
