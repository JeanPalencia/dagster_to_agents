"""S3 upload and Geospot API utilities."""

import json
import re
from io import BytesIO
import boto3
import requests
import pandas as pd

from dagster_pipeline.defs.conversation_analysis.utils.database import get_ssm_parameter


# Constants
GEOSPOT_API_URL = "https://geospot.spot2.mx/data-lake-house/dagster/"
GEOSPOT_API_KEY_PARAM = "/dagster/API_GEOSPOT_LAKEHOUSE_KEY"
S3_BUCKET_NAME = "dagster-assets-production"


def sanitize_text(text: str) -> str:
    """
    Sanitize text to ensure it's safe for CSV and JSONB.
    Removes or replaces problematic characters.
    """
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text)
    
    # Replace problematic whitespace characters
    text = text.replace('\r\n', ' ')
    text = text.replace('\r', ' ')
    text = text.replace('\n', ' ')
    text = text.replace('\t', ' ')
    
    # Remove null bytes and other control characters (except normal space)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    
    # Normalize multiple spaces to single space
    text = re.sub(r' +', ' ', text)
    
    return text.strip()


def sanitize_json_string(json_str) -> str:
    """
    Sanitize a JSON string or list/dict to ensure it's valid for JSONB.
    Parses, cleans values, and re-serializes.
    """
    # Handle None and empty values
    if json_str is None:
        return '[]'
    
    # Handle lists/dicts directly (already parsed)
    if isinstance(json_str, (list, dict)):
        data = json_str
    elif isinstance(json_str, str):
        if json_str == '':
            return '[]'
        try:
            data = json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return '[]'
    else:
        # Try pd.isna for scalar values
        try:
            if pd.isna(json_str):
                return '[]'
        except (ValueError, TypeError):
            pass
        return '[]'
    
    # Recursively sanitize string values
    def clean_value(obj):
        if isinstance(obj, str):
            return sanitize_text(obj)
        elif isinstance(obj, dict):
            return {k: clean_value(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_value(item) for item in obj]
        else:
            return obj
    
    try:
        cleaned_data = clean_value(data)
        # Re-serialize with ensure_ascii=False to handle unicode properly
        return json.dumps(cleaned_data, ensure_ascii=False)
    except (TypeError, ValueError):
        # If cleaning fails, try to serialize original data
        try:
            return json.dumps(data, ensure_ascii=False)
        except:
            return '[]'


def prepare_dataframe_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame for CSV export, sanitizing JSONB and text columns.
    """
    df_clean = df.copy()
    
    # Columns that are JSONB
    jsonb_columns = ['conv_messages', 'conv_variables']
    
    # Columns that are text
    text_columns = ['conv_text']
    
    # Sanitize JSONB columns
    for col in jsonb_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(sanitize_json_string)
    
    # Sanitize text columns
    for col in text_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(sanitize_text)
    
    return df_clean


def get_geospot_api_key() -> str:
    """Get Geospot API key from SSM Parameter Store."""
    return get_ssm_parameter(GEOSPOT_API_KEY_PARAM)


def trigger_table_replacement(
    s3_key: str = None,
    table_name: str = "bt_conv_conversations_bck",
    mode: str = "replace"
) -> dict:
    """
    Trigger table replacement via Geospot API.
    
    Args:
        s3_key: S3 key of the uploaded file
        table_name: Target table name
        mode: 'replace' or 'append'
    
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


def upload_dataframe_to_s3(
    df: pd.DataFrame,
    test_mode: bool = False,
    bucket_name: str = S3_BUCKET_NAME
) -> tuple[str, str]:
    """
    Upload DataFrame to S3 as CSV.
    
    Args:
        df: DataFrame to upload
        test_mode: If True, saves with _test suffix
        bucket_name: S3 bucket name
    
    Returns:
        Tuple of (main_uri, s3_key)
    """
    s3_client = boto3.client('s3', region_name='us-east-1')
    
    # Generate filename
    if test_mode:
        filename = 'df_conversations_metrics_test.csv'
    else:
        filename = 'df_conversations_metrics.csv'
    s3_key = f'conversation_analysis/{filename}'
    
    # Sanitize DataFrame for CSV/JSONB compatibility
    df_clean = prepare_dataframe_for_csv(df)
    
    # Convert to CSV and upload
    buffer = BytesIO()
    df_clean.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType='text/csv'
    )
    
    file_size = len(buffer.getvalue()) / (1024 * 1024)
    s3_url = f's3://{bucket_name}/{s3_key}'
    
    return s3_url, s3_key
