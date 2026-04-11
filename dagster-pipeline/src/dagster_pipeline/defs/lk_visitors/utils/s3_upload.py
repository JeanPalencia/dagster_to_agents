"""Functions for uploading data to S3 and triggering table replacement."""

from datetime import datetime
from io import BytesIO
from pathlib import Path

import boto3
import requests
import pandas as pd


def _get_s3_client():
    """Create a fresh S3 client."""
    return boto3.client("s3", region_name="us-east-1")


def _get_ssm_client():
    """Create a fresh SSM client."""
    return boto3.client("ssm", region_name="us-east-1")


# S3 Configuration
S3_BUCKET_NAME = "dagster-assets-production"
S3_KEY_CSV = "lk_visitors/lk_visitors.csv" # Don't use anymore
S3_KEY_PARQUET = "lk_visitors/lk_visitors.parquet"

# Geospot API Configuration
GEOSPOT_API_URL = "https://geospot.spot2.mx/data-lake-house/dagster/"
GEOSPOT_API_KEY_PARAM = "/dagster/API_GEOSPOT_LAKEHOUSE_KEY"
TABLE_NAME = "lk_visitors_bck"

# Upsert configuration for lk_visitors_bck (legacy pipeline writes to backup table)
CONFLICT_COLUMNS = ["vis_id"]
UPDATE_COLUMNS = [
    "vis_is_scraping",
    "aud_updated_date",
    "aud_updated_at",
]


def get_geospot_api_key() -> str:
    """Get Geospot API key from SSM Parameter Store."""
    ssm = _get_ssm_client()
    response = ssm.get_parameter(Name=GEOSPOT_API_KEY_PARAM, WithDecryption=True)
    return response["Parameter"]["Value"]


def save_to_local(df: pd.DataFrame, output_dir: str = "output") -> str:
    """Save DataFrame to local CSV for testing."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    filename = f"lk_visitors_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv"
    filepath = output_path / filename
    
    df.to_csv(filepath, index=False, encoding="utf-8")
    return str(filepath)


def upload_dataframe_to_s3(
    df: pd.DataFrame,
    output_format: str = "csv",
) -> tuple[str, str]:
    """Upload a DataFrame to S3 as CSV or Parquet. Returns (s3_uri, s3_key)."""
    s3 = _get_s3_client()
    df_export = df.copy()
    for col in ["user_pseudo_id", "vis_id"]:
        if col in df_export.columns:
            df_export[col] = df_export[col].astype(str)

    buffer = BytesIO()
    if output_format == "parquet":
        s3_key = S3_KEY_PARQUET
        df_export.to_parquet(buffer, index=False)
        content = buffer.getvalue()
        content_type = "application/vnd.apache.parquet"
    else:
        s3_key = S3_KEY_CSV
        df_export.to_csv(buffer, index=False, encoding="utf-8")
        content = buffer.getvalue()
        content_type = "text/csv"

    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=content,
        ContentType=content_type,
    )
    return f"s3://{S3_BUCKET_NAME}/{s3_key}", s3_key


def trigger_table_replacement(
    s3_key: str = S3_KEY_CSV,
    table_name: str = TABLE_NAME,
    mode: str = "replace",
) -> dict:
    """Call Geospot API to replace or upsert table with S3 content.

    When mode='replace', sends only bucket, s3_key, table_name, mode.
    When mode='upsert', also sends conflict_columns and update_columns.
    """
    api_key = get_geospot_api_key()

    headers = {
        "Authorization": f"Api-Key {api_key}",
        "Content-Type": "application/json",
    }

    payload: dict = {
        "bucket_name": S3_BUCKET_NAME,
        "s3_key": s3_key,
        "table_name": table_name,
        "mode": mode,
    }

    if mode == "upsert":
        payload["conflict_columns"] = CONFLICT_COLUMNS
        payload["update_columns"] = UPDATE_COLUMNS

    response = requests.post(
        GEOSPOT_API_URL,
        headers=headers,
        json=payload,
        timeout=120,
    )

    response.raise_for_status()
    return response.json()
