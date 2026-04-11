"""Functions for uploading data to S3 and triggering table replacement."""

# Dtype for user_pseudo_id when reading CSVs - prevents pandas from inferring float
# (GA4 IDs like "989107397.1771268336" lose precision as float64)
CSV_DTYPES_USER_PSEUDO_ID = {"user_pseudo_id": "string"}

from datetime import datetime
from io import BytesIO
from pathlib import Path

import boto3
import requests
import pandas as pd


def read_temp_matches_csv(path: str | Path) -> pd.DataFrame:
    """Read temp_matches CSV with user_pseudo_id as string (avoids float truncation)."""
    return pd.read_csv(path, dtype=CSV_DTYPES_USER_PSEUDO_ID)


def save_to_local(
    df: pd.DataFrame,
    output_dir: str = "output",
    output_format: str = "csv",
) -> str:
    """Save DataFrame to local file (CSV or Parquet). Returns path."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    ext = "parquet" if output_format == "parquet" else "csv"
    path = out / f"temp_matches_users_to_leads_{ts}.{ext}"
    df_export = df.copy()
    if "user_pseudo_id" in df_export.columns:
        df_export["user_pseudo_id"] = df_export["user_pseudo_id"].astype("string")
    if output_format == "parquet":
        df_export.to_parquet(path, index=False)
    else:
        df_export.to_csv(path, index=False, encoding="utf-8")
    return str(path)


def _get_s3_client():
    """Create a fresh S3 client (avoids token expiration issues)."""
    return boto3.client("s3", region_name="us-east-1")


def _get_ssm_client():
    """Create a fresh SSM client (avoids token expiration issues)."""
    return boto3.client("ssm", region_name="us-east-1")

# S3 Configuration (hardcoded - not secrets)
S3_BUCKET_NAME = "dagster-assets-production"
S3_KEY = "temp_matches_users_to_leads/temp_matches_users_to_leads_campaign_name.csv"
S3_BACKUP_PREFIX = "temp_matches_users_to_leads/backups"

# Geospot API Configuration
GEOSPOT_API_URL = "https://geospot.spot2.mx/data-lake-house/dagster/"
GEOSPOT_API_KEY_PARAM = "/dagster/API_GEOSPOT_LAKEHOUSE_KEY"
TABLE_NAME = "temp_matches_users_to_leads"


def get_geospot_api_key() -> str:
    """Get Geospot API key from SSM Parameter Store."""
    ssm = _get_ssm_client()
    response = ssm.get_parameter(Name=GEOSPOT_API_KEY_PARAM, WithDecryption=True)
    return response["Parameter"]["Value"]


def upload_dataframe_to_s3(
    df: pd.DataFrame, 
    s3_key: str = S3_KEY,
    create_backup: bool = True,
) -> tuple[str, str | None]:
    """Upload a DataFrame to S3 as CSV.
    
    Args:
        df: DataFrame to upload
        s3_key: S3 key (path) for the main file
        create_backup: If True, also creates a dated backup copy
        
    Returns:
        Tuple of (main S3 URI, backup S3 URI or None)
    """
    s3 = _get_s3_client()
    
    df_export = df.copy()
    if "user_pseudo_id" in df_export.columns:
        df_export["user_pseudo_id"] = df_export["user_pseudo_id"].astype("string")
    
    buffer = BytesIO()
    df_export.to_csv(buffer, index=False, encoding="utf-8")
    csv_content = buffer.getvalue()
    
    # Upload main file
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=csv_content,
        ContentType="text/csv",
    )
    main_uri = f"s3://{S3_BUCKET_NAME}/{s3_key}"
    
    # Upload backup with date
    backup_uri = None
    if create_backup:
        today = datetime.now().strftime("%Y-%m-%d")
        backup_key = f"{S3_BACKUP_PREFIX}/{today}.csv"
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=backup_key,
            Body=csv_content,
            ContentType="text/csv",
        )
        backup_uri = f"s3://{S3_BUCKET_NAME}/{backup_key}"
    
    return main_uri, backup_uri


def trigger_table_replacement(
    s3_key: str = S3_KEY,
    table_name: str = TABLE_NAME,
    mode: str = "replace",
) -> dict:
    """Call Geospot API to replace table with S3 CSV content.
    
    Args:
        s3_key: S3 key of the CSV file
        table_name: Target table name in the database
        mode: Replacement mode ('replace' or 'append')
        
    Returns:
        API response as dict
    """
    api_key = get_geospot_api_key()
    
    headers = {
        "Authorization": f"Api-Key {api_key}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "bucket_name": S3_BUCKET_NAME,
        "s3_key": s3_key,
        "table_name": table_name,
        "mode": mode,
    }
    
    response = requests.post(
        GEOSPOT_API_URL,
        headers=headers,
        json=payload,
        timeout=120,
    )
    
    response.raise_for_status()
    return response.json()


def upload_and_replace_table(df: pd.DataFrame) -> dict:
    """Upload DataFrame to S3 and trigger table replacement.
    
    This is the main function to call for production deployments.
    
    Args:
        df: DataFrame to upload and use for table replacement
        
    Returns:
        API response from table replacement
    """
    # Upload to S3 (main + backup)
    main_uri, backup_uri = upload_dataframe_to_s3(df)
    print(f"Uploaded DataFrame to {main_uri}")
    if backup_uri:
        print(f"Backup created at {backup_uri}")
    
    # Trigger table replacement
    result = trigger_table_replacement()
    print(f"Table replacement triggered: {result}")
    
    return result
