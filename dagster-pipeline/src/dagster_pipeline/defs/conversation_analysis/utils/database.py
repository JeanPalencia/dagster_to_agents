"""Database connection utilities using AWS SSM Parameter Store for credentials."""

import os
import boto3
import pandas as pd
from sqlalchemy import create_engine


# SSM Parameter paths - Chatbot (Postgres)
SSM_CHATBOT_HOST = "/dagster/lk_conversations_metrics/db_chatbot_host"
SSM_CHATBOT_PORT = "/dagster/lk_conversations_metrics/db_chatbot_port"
SSM_CHATBOT_DATABASE = "/dagster/lk_conversations_metrics/db_chatbot_postgres_name"
SSM_CHATBOT_USERNAME = "/dagster/lk_conversations_metrics/db_chatbot_user"
SSM_CHATBOT_PASSWORD = "/dagster/lk_conversations_metrics/db_chatbot_password"

# SSM Parameter paths - Prod DB (MySQL)
SSM_PROD_DB_HOST = "/dagster/lk_conversations_metrics/db_spot_service_host"
SSM_PROD_DB_PORT = "/dagster/lk_conversations_metrics/db_spot_service_port"
SSM_PROD_DB_DATABASE = "/dagster/lk_conversations_metrics/db_spot_service_name"
SSM_PROD_DB_USERNAME = "/dagster/lk_conversations_metrics/db_spot_service_user"
SSM_PROD_DB_PASSWORD = "/dagster/lk_conversations_metrics/db_spot_service_password"

# SSM Parameter paths - Geospot Lakehouse (Postgres)
SSM_GEOSPOT_HOST = "/dagster/lk_conversations_metrics/db_geospot_host"
SSM_GEOSPOT_USERNAME = "/dagster/lk_conversations_metrics/db_geospot_user"
SSM_GEOSPOT_PASSWORD = "/dagster/lk_conversations_metrics/db_geospot_password"
GEOSPOT_DATABASE = "geospot"
GEOSPOT_PORT = "5432"

# SSM Parameter paths - Spot Chatbot (Postgres) - conversation_events
SSM_SPOT_CHATBOT_HOST = "/dagster/lk_conversations_metrics/spot_chatbot_host"
SSM_SPOT_CHATBOT_PORT = "/dagster/lk_conversations_metrics/spot_chatbot_port"
SSM_SPOT_CHATBOT_DATABASE = "/dagster/lk_conversations_metrics/spot_chatbot_db"
SSM_SPOT_CHATBOT_USERNAME = "/dagster/lk_conversations_metrics/spot_chatbot_user"
SSM_SPOT_CHATBOT_PASSWORD = "/dagster/lk_conversations_metrics/spot_chatbot_password"

# SSM Parameter paths - OpenAI
SSM_OPENAI_API_KEY = "/dagster/lk_conversations_metrics/api_openai_key"

# SSM Parameter paths - LangSmith (tracing para DSPy evaluator)
SSM_LANGCHAIN_API_KEY = "/dagster/lk_conversations_metrics/langchain_api_key"


def _get_ssm_client():
    """Create a fresh SSM client (avoids token expiration issues)."""
    return boto3.client("ssm", region_name="us-east-1")


def get_ssm_parameter(name: str, decrypt: bool = True) -> str:
    """Get a parameter value from AWS SSM Parameter Store."""
    ssm = _get_ssm_client()
    response = ssm.get_parameter(Name=name, WithDecryption=decrypt)
    return response["Parameter"]["Value"]


def ensure_langsmith_env_from_ssm() -> None:
    """
    Set LangSmith env vars from SSM when LANGCHAIN_API_KEY is not already set.
    Uses SSM path /dagster/lk_conversations_metrics/langchain_api_key and sets
    LANGCHAIN_TRACING_V2=true, LANGCHAIN_PROJECT=dspy-evaluator.
    If already set (e.g. from .env locally), does nothing. On SSM failure, no-op.
    """
    if os.environ.get("LANGCHAIN_API_KEY"):
        return
    try:
        api_key = get_ssm_parameter(SSM_LANGCHAIN_API_KEY)
        if api_key:
            os.environ["LANGCHAIN_API_KEY"] = api_key
            os.environ["LANGCHAIN_TRACING_V2"] = "true"
            os.environ["LANGCHAIN_PROJECT"] = "dspy-evaluator"
    except Exception:
        pass


def get_chatbot_credentials() -> dict:
    """Get Chatbot database credentials from SSM."""
    return {
        "host": get_ssm_parameter(SSM_CHATBOT_HOST, decrypt=False),
        "port": get_ssm_parameter(SSM_CHATBOT_PORT, decrypt=False),
        "database": get_ssm_parameter(SSM_CHATBOT_DATABASE, decrypt=False),
        "username": get_ssm_parameter(SSM_CHATBOT_USERNAME),
        "password": get_ssm_parameter(SSM_CHATBOT_PASSWORD),
    }


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
    """Get Geospot Lakehouse (PostgreSQL) credentials from SSM."""
    return {
        "host": get_ssm_parameter(SSM_GEOSPOT_HOST, decrypt=False),
        "port": GEOSPOT_PORT,
        "database": GEOSPOT_DATABASE,
        "username": get_ssm_parameter(SSM_GEOSPOT_USERNAME),
        "password": get_ssm_parameter(SSM_GEOSPOT_PASSWORD),
    }


def get_spot_chatbot_credentials() -> dict:
    """Get Spot Chatbot (PostgreSQL) credentials from SSM. Used for conversation_events."""
    return {
        "host": get_ssm_parameter(SSM_SPOT_CHATBOT_HOST, decrypt=False),
        "port": get_ssm_parameter(SSM_SPOT_CHATBOT_PORT, decrypt=False),
        "database": get_ssm_parameter(SSM_SPOT_CHATBOT_DATABASE, decrypt=False),
        "username": get_ssm_parameter(SSM_SPOT_CHATBOT_USERNAME),
        "password": get_ssm_parameter(SSM_SPOT_CHATBOT_PASSWORD),
    }


def get_engine(database_name: str):
    """
    Get SQLAlchemy engine for the specified database.
    
    Args:
        database_name: One of 'chatbot', 'prod', 'geospot', or 'spot_chatbot'
    
    Returns:
        SQLAlchemy Engine instance
    
    Raises:
        ValueError: If database_name is not supported
    """
    if database_name == "chatbot":
        creds = get_chatbot_credentials()
        engine_url = (
            f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
            f"@{creds['host']}:{creds['port']}/{creds['database']}"
        )
        return create_engine(engine_url)

    elif database_name == "prod":
        creds = get_prod_db_credentials()
        engine_url = (
            f"mysql+mysqlconnector://{creds['username']}:{creds['password']}"
            f"@{creds['host']}:{creds['port']}/{creds['database']}"
        )
        return create_engine(engine_url)

    elif database_name == "geospot":
        creds = get_geospot_credentials()
        engine_url = (
            f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
            f"@{creds['host']}:{creds['port']}/{creds['database']}"
        )
        return create_engine(engine_url)

    elif database_name == "spot_chatbot":
        creds = get_spot_chatbot_credentials()
        engine_url = (
            f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
            f"@{creds['host']}:{creds['port']}/{creds['database']}"
        )
        return create_engine(engine_url)

    else:
        raise ValueError(
            f"Database '{database_name}' is not supported. "
            f"Supported databases: 'chatbot', 'prod', 'geospot', 'spot_chatbot'"
        )


def query_postgres(query: str) -> pd.DataFrame:
    """Execute a query on the Chatbot PostgreSQL database."""
    engine = get_engine("chatbot")
    with engine.connect() as connection:
        return pd.read_sql(query, connection)


def query_mysql(query: str) -> pd.DataFrame:
    """Execute a query on the Prod MySQL database."""
    engine = get_engine("prod")
    with engine.connect() as connection:
        return pd.read_sql(query, connection)


def query_geospot(query: str) -> pd.DataFrame:
    """Execute a query on the Geospot Lakehouse PostgreSQL database."""
    engine = get_engine("geospot")
    with engine.connect() as connection:
        return pd.read_sql(query, connection)


def query_spot_chatbot(query: str) -> pd.DataFrame:
    """Execute a query on the Spot Chatbot PostgreSQL database (conversation_events)."""
    engine = get_engine("spot_chatbot")
    with engine.connect() as connection:
        return pd.read_sql(query, connection)


def get_openai_api_key() -> str:
    """Get OpenAI API key from SSM Parameter Store."""
    return get_ssm_parameter(SSM_OPENAI_API_KEY)


# Alias for backwards compatibility
query_staging = query_spot_chatbot
