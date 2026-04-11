"""Database connection utilities using AWS SSM Parameter Store for credentials."""

import json
import boto3
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account


# SSM Parameter paths - Prod DB (MySQL)
SSM_PROD_DB_HOST = "/dagster/vtl/PROD_DB_HOST"
SSM_PROD_DB_PORT = "/dagster/vtl/PROD_DB_PORT"
SSM_PROD_DB_DATABASE = "/dagster/vtl/PROD_DB_DATABASE"
SSM_PROD_DB_USERNAME = "/dagster/vtl/PROD_DB_USERNAME"
SSM_PROD_DB_PASSWORD = "/dagster/vtl/PROD_DB_PASSWORD"

# SSM Parameter paths - Geospot DB (PostgreSQL)
SSM_GEOSPOT_HOST = "/dagster/lk_visitors/GEOSPOT_HOST"
SSM_GEOSPOT_PORT = "/dagster/lk_visitors/GEOSPOT_PORT"
SSM_GEOSPOT_DATABASE = "/dagster/lk_visitors/GEOSPOT_DATABASE"
SSM_GEOSPOT_USERNAME = "/dagster/lk_visitors/GEOSPOT_USERNAME"
SSM_GEOSPOT_PASSWORD = "/dagster/lk_visitors/GEOSPOT_PASSWORD"

# SSM Parameter paths - BigQuery
SSM_BIGQUERY_CREDENTIALS = "/dagster/vtl/BIGQUERY_CREDENTIALS"


class BigQueryCredentialsError(Exception):
    """Raised when BigQuery credentials are not found or invalid."""
    pass


def _get_ssm_client():
    """Create a fresh SSM client."""
    return boto3.client("ssm", region_name="us-east-1")


def get_ssm_parameter(name: str, decrypt: bool = True) -> str:
    """Get a parameter value from AWS SSM Parameter Store."""
    ssm = _get_ssm_client()
    response = ssm.get_parameter(Name=name, WithDecryption=decrypt)
    return response["Parameter"]["Value"]


def get_prod_db_credentials() -> dict:
    """Get Prod DB (MySQL) credentials from SSM."""
    return {
        "host": get_ssm_parameter(SSM_PROD_DB_HOST, decrypt=False),
        "port": get_ssm_parameter(SSM_PROD_DB_PORT, decrypt=False),
        "database": get_ssm_parameter(SSM_PROD_DB_DATABASE),
        "username": get_ssm_parameter(SSM_PROD_DB_USERNAME),
        "password": get_ssm_parameter(SSM_PROD_DB_PASSWORD),
    }


def get_geospot_credentials() -> dict:
    """Get Geospot database credentials from SSM."""
    return {
        "host": get_ssm_parameter(SSM_GEOSPOT_HOST),
        "port": get_ssm_parameter(SSM_GEOSPOT_PORT),
        "database": get_ssm_parameter(SSM_GEOSPOT_DATABASE),
        "username": get_ssm_parameter(SSM_GEOSPOT_USERNAME),
        "password": get_ssm_parameter(SSM_GEOSPOT_PASSWORD),
    }


def query_bigquery(query: str, project_id: str = None) -> pd.DataFrame:
    """Execute a query on BigQuery and return results as DataFrame."""
    try:
        credentials_json = get_ssm_parameter(SSM_BIGQUERY_CREDENTIALS)
        credentials_info = json.loads(credentials_json)
    except Exception as e:
        raise BigQueryCredentialsError(f"Failed to load BigQuery credentials from SSM: {e}")
    
    if project_id is None:
        project_id = credentials_info.get("project_id")
    
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = bigquery.Client(project=project_id, credentials=credentials)
    return client.query(query).to_dataframe()


def get_engine(database_name: str):
    """Get SQLAlchemy engine for the specified database."""
    match database_name:
        case "geospot":
            creds = get_geospot_credentials()
            engine_url = (
                f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
                f"@{creds['host']}:{creds['port']}/{creds['database']}"
            )
            return create_engine(engine_url)

        case "prod":
            creds = get_prod_db_credentials()
            engine_url = (
                f"mysql+mysqlconnector://{creds['username']}:{creds['password']}"
                f"@{creds['host']}:{creds['port']}/{creds['database']}"
            )
            return create_engine(engine_url)

        case _:
            raise ValueError(f"Database '{database_name}' not supported. Use: 'geospot', 'prod'")
