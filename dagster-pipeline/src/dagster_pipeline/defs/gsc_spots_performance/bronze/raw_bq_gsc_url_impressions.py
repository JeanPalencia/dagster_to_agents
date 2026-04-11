"""
Bronze External: URL impression data from Google Search Console (BigQuery).

Extracts aggregated SEO metrics (impressions, clicks, avg position) for
/spots pages on spot2.mx, grouped by URL, month and year.
Rolling window: 6 months back from the 1st of the current month.

Uses the spot2-ga4-connector service account (SSM /geospot/bigquery/credentials)
which has access to the searchconsole_spot2mx dataset. This differs from the
default BigQuery credentials used by query_bronze_source.

Has a corresponding STG asset: stg_bq_gsc_url_impressions.
"""
import json

import boto3
import dagster as dg
import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account


SSM_BIGQUERY_CREDENTIALS = "/geospot/bigquery/credentials"

_GSC_URL_IMPRESSIONS_SQL = """
SELECT
    url,
    SUM(impressions)   AS impressions,
    SUM(clicks)        AS clicks,
    AVG(sum_position)  AS avg_position,
    EXTRACT(MONTH FROM data_date) AS month,
    EXTRACT(YEAR FROM data_date)  AS year
FROM `spot2-mx-ga4-bq.searchconsole_spot2mx.searchdata_url_impression`
WHERE site_url   = 'sc-domain:spot2.mx'
  AND url LIKE '%/spots%'
  AND search_type = 'WEB'
  AND data_date  >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH)
GROUP BY 1, 5, 6
"""


def _query_gsc_bigquery(query: str, context: dg.AssetExecutionContext) -> pl.DataFrame:
    ssm = boto3.client("ssm", region_name="us-east-1")
    raw = ssm.get_parameter(Name=SSM_BIGQUERY_CREDENTIALS, WithDecryption=True)
    creds_info = json.loads(raw["Parameter"]["Value"])
    credentials = service_account.Credentials.from_service_account_info(creds_info)
    client = bigquery.Client(
        project=creds_info.get("project_id"),
        credentials=credentials,
    )
    context.log.info(f"Executing BigQuery GSC query on project: {client.project}")
    df_pd = client.query(query).result().to_dataframe()
    return pl.from_pandas(df_pd)


@dg.asset(
    group_name="gsp_bronze",
    description="Bronze: GSC URL impressions for /spots pages from BigQuery (6-month window).",
)
def raw_bq_gsc_url_impressions(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Extracts GSC URL-level metrics from BigQuery."""
    df = _query_gsc_bigquery(_GSC_URL_IMPRESSIONS_SQL, context)
    context.log.info(f"raw_bq_gsc_url_impressions: {df.height:,} rows, {df.width} columns")
    return df
