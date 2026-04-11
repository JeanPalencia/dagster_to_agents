"""
GSC Spots Performance — 2026-03-26

Rendimiento SEO mensual de las páginas /spots en spot2.mx
según Google Search Console (BigQuery export).

Ejecutar con el venv de dagster-pipeline:
    cd lakehouse-sdk
    ../dagster-pipeline/.venv/bin/python ad_hoc/gsc_spots_performance_2026-03-26/query.py

Credenciales: /geospot/bigquery/credentials (spot2-ga4-connector SA,
              con acceso a searchconsole_spot2mx)
"""

import boto3
import json
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

SSM_BIGQUERY_CREDENTIALS = "/geospot/bigquery/credentials"

QUERY = """
SELECT
    url,
    SUM(impressions)   AS impresiones,
    SUM(clicks)        AS clicks,
    AVG(sum_position)  AS avg_posicion,
    EXTRACT(MONTH FROM data_date) AS mes
FROM `spot2-mx-ga4-bq.searchconsole_spot2mx.searchdata_url_impression`
WHERE site_url   = 'sc-domain:spot2.mx'
  AND url LIKE '%/spots%'
  AND search_type = 'WEB'
  AND data_date  >= '2026-01-01'
GROUP BY 1, 5
"""


def get_bigquery_client() -> bigquery.Client:
    ssm = boto3.client("ssm", region_name="us-east-1")
    raw = ssm.get_parameter(Name=SSM_BIGQUERY_CREDENTIALS, WithDecryption=True)
    creds_info = json.loads(raw["Parameter"]["Value"])
    credentials = service_account.Credentials.from_service_account_info(creds_info)
    return bigquery.Client(
        project=creds_info.get("project_id"),
        credentials=credentials,
    )


OUTPUT_DIR = Path(__file__).parent
RAW_PARQUET = OUTPUT_DIR / "raw_gsc_spots.parquet"


def main():
    client = get_bigquery_client()
    print(f"Proyecto GCP: {client.project}")
    print("Ejecutando query...\n")

    df = client.query(QUERY).to_dataframe()
    print(f"Total de registros: {len(df)}")

    df.to_parquet(RAW_PARQUET, index=False)
    print(f"Guardado en: {RAW_PARQUET}\n")

    pd.set_option("display.max_colwidth", 120)
    pd.set_option("display.width", 200)
    print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
