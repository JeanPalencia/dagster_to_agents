# defs/data_lakehouse/shared.py
import os
import dagster as dg
import boto3
import json
import mysql.connector
import pandas as pd
import polars as pl
import requests
import io
import time
from zoneinfo import ZoneInfo
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Literal, Dict, List, Optional
from sqlalchemy import create_engine, text as sa_text
from google.cloud import bigquery
from google.oauth2 import service_account
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError

# S3: longer timeouts for large parquet reads (avoid Read timeout after 48+ min runs)
_s3_config = BotocoreConfig(
    connect_timeout=120,
    read_timeout=300,
    retries={"max_attempts": 4, "mode": "adaptive"},
)

# AWS Clients
ssm = boto3.client("ssm", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1", config=_s3_config)

# Configuration
ANALYSIS_DB_ENDPOINT = "spot2-production-analysis.c4x5kddqib5v.us-east-1.rds.amazonaws.com"
S3_BUCKET = "dagster-assets-production"

# Geospot API Configuration
GEOSPOT_LAKEHOUSE_ENDPOINT = "https://geospot.spot2.mx/data-lake-house/dagster/"
GEOSPOT_API_KEY_PARAM = "/dagster/API_GEOSPOT_LAKEHOUSE_KEY"

# Daily partitions - ALL layers must use this same instance
daily_partitions = dg.DailyPartitionsDefinition(
    start_date=(date.today() - relativedelta(months=3)).strftime("%Y-%m-%d"),
    timezone="America/Mexico_City",
)


def calendar_today_mx_iso() -> str:
    """Current calendar date in America/Mexico_City (YYYY-MM-DD)."""
    return datetime.now(ZoneInfo("America/Mexico_City")).date().isoformat()


def resolve_stale_reference_date(context: dg.AssetExecutionContext) -> str:
    """
    Reference date YYYY-MM-DD for SQL windows, stale checks on S3, and logs.

    Precedence: run tag dagster/partition (partitioned jobs materializing non-partitioned assets)
    → asset partition_key → today in America/Mexico_City.
    """
    run = getattr(context, "run", None)
    if run is not None and getattr(run, "tags", None):
        pk = run.tags.get("dagster/partition")
        if pk:
            return pk
    try:
        return context.partition_key
    except Exception:
        pass
    return calendar_today_mx_iso()


# ============ Type Mapping Dictionaries for Schema Consistency ============

# PostgreSQL type OIDs to Polars types
# Reference: https://www.postgresql.org/docs/current/datatype.html
# OIDs from psycopg2.extensions.* and common PostgreSQL type codes
POSTGRES_TO_POLARS: Dict[str, pl.DataType] = {
    # Integer types
    "int2": pl.Int16,
    "int4": pl.Int32,
    "int8": pl.Int64,
    "smallint": pl.Int16,
    "integer": pl.Int32,
    "bigint": pl.Int64,
    "serial": pl.Int32,
    "bigserial": pl.Int64,
    # Floating point types
    "float4": pl.Float32,
    "float8": pl.Float64,
    "real": pl.Float32,
    "double precision": pl.Float64,
    "numeric": pl.Float64,
    "decimal": pl.Float64,
    # Boolean
    "bool": pl.Boolean,
    "boolean": pl.Boolean,
    # String types
    "text": pl.Utf8,
    "varchar": pl.Utf8,
    "char": pl.Utf8,
    "character varying": pl.Utf8,
    "character": pl.Utf8,
    "name": pl.Utf8,
    "uuid": pl.Utf8,
    # Date/Time types
    "date": pl.Date,
    "timestamp": pl.Datetime,
    "timestamptz": pl.Datetime,
    "timestamp without time zone": pl.Datetime,
    "timestamp with time zone": pl.Datetime,
    "time": pl.Time,
    "timetz": pl.Time,
    "time without time zone": pl.Time,
    "time with time zone": pl.Time,
    "interval": pl.Utf8,  # Interval as string
    # JSON types (as string for compatibility)
    "json": pl.Utf8,
    "jsonb": pl.Utf8,
    # Binary types
    "bytea": pl.Binary,
    # Array types (as string for CSV compatibility)
    "_int4": pl.Utf8,  # integer[]
    "_int8": pl.Utf8,  # bigint[]
    "_text": pl.Utf8,  # text[]
    "_varchar": pl.Utf8,  # varchar[]
}

# BigQuery type names to Polars types
# Reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
BIGQUERY_TO_POLARS: Dict[str, pl.DataType] = {
    # Integer types
    "INT64": pl.Int64,
    "INTEGER": pl.Int64,
    "INT": pl.Int64,
    "SMALLINT": pl.Int64,
    "BIGINT": pl.Int64,
    "TINYINT": pl.Int64,
    "BYTEINT": pl.Int64,
    # Floating point types
    "FLOAT64": pl.Float64,
    "FLOAT": pl.Float64,
    "NUMERIC": pl.Float64,
    "BIGNUMERIC": pl.Float64,
    "DECIMAL": pl.Float64,
    "BIGDECIMAL": pl.Float64,
    # Boolean
    "BOOL": pl.Boolean,
    "BOOLEAN": pl.Boolean,
    # String types
    "STRING": pl.Utf8,
    "BYTES": pl.Binary,
    # Date/Time types
    "DATE": pl.Date,
    "DATETIME": pl.Datetime,
    "TIME": pl.Time,
    "TIMESTAMP": pl.Datetime,
    # Complex types (as string for compatibility)
    "ARRAY": pl.Utf8,
    "STRUCT": pl.Utf8,
    "JSON": pl.Utf8,
    "GEOGRAPHY": pl.Utf8,
}


def _apply_polars_schema_overrides(
    df: pl.DataFrame,
    type_mapping: Dict[str, pl.DataType],
    source_schema: Dict[str, str],
    context: Optional[dg.AssetExecutionContext] = None,
) -> pl.DataFrame:
    """
    Apply schema overrides to a Polars DataFrame based on source database types.
    
    Args:
        df: Input Polars DataFrame
        type_mapping: Dictionary mapping source types to Polars types
        source_schema: Dictionary mapping column names to source type names
        context: Dagster context for logging
    
    Returns:
        DataFrame with corrected column types
    """
    cast_expressions = []
    casts_applied = []
    
    for col_name, source_type in source_schema.items():
        if col_name not in df.columns:
            continue
        
        # Normalize type name (lowercase, strip parameters like varchar(255))
        normalized_type = source_type.lower().split("(")[0].strip()
        
        if normalized_type in type_mapping:
            target_type = type_mapping[normalized_type]
            current_type = df.schema.get(col_name)
            
            # Only cast if types differ
            if current_type != target_type:
                try:
                    cast_expressions.append(
                        pl.col(col_name).cast(target_type).alias(col_name)
                    )
                    casts_applied.append(f"{col_name}: {current_type} → {target_type}")
                except Exception:
                    # Skip if cast fails (e.g., incompatible types)
                    pass
    
    if cast_expressions:
        df = df.with_columns(cast_expressions)
        if context and casts_applied:
            context.log.info(f"Schema overrides applied: {len(casts_applied)} columns cast")
    
    return df


# ============ Multi-Source Database Helpers for Bronze Layer ============

BronzeSourceType = Literal["mysql_prod", "bigquery", "geospot_postgres", "chatbot_postgres", "staging_postgres"]


def query_bronze_source(
    query: str,
    source_type: BronzeSourceType = "mysql_prod",
    context: Optional[dg.AssetExecutionContext] = None,
) -> pl.DataFrame:
    """
    Wrapper function to query different data sources for Bronze layer assets.
    
    This is the main function that Bronze assets should use. It routes to the
    appropriate source-specific function based on source_type.
    
    Args:
        query: SQL query string to execute
        source_type: Type of data source (default: "mysql_prod")
                   Options:
                   - "mysql_prod": MySQL Production database (uses optimized function)
                   - "bigquery": Google BigQuery
                   - "geospot_postgres": Geospot PostgreSQL database
                   - "chatbot_postgres": Chatbot PostgreSQL database (spot2_chatbot)
        context: Dagster context for logging
    
    Returns:
        Polars DataFrame with query results
    
    Example:
        # MySQL Production (default)
        df = query_bronze_source("SELECT * FROM clients", context=context)
        
        # BigQuery
        df = query_bronze_source("SELECT * FROM dataset.table", source_type="bigquery", context=context)
        
        # Geospot PostgreSQL
        df = query_bronze_source("SELECT * FROM lk_visitors", source_type="geospot_postgres", context=context)
        
        # Chatbot PostgreSQL
        df = query_bronze_source("SELECT * FROM messages", source_type="chatbot_postgres", context=context)
    """
    if source_type == "mysql_prod":
        return _query_mysql_prod_to_polars(query, context)
    elif source_type == "bigquery":
        return _query_bigquery_to_polars(query, context)
    elif source_type == "geospot_postgres":
        return _query_geospot_postgres_to_polars(query, context)
    elif source_type == "chatbot_postgres":
        return _query_chatbot_postgres_to_polars(query, context)
    elif source_type == "staging_postgres":
        return _query_staging_postgres_to_polars(query, context)
    else:
        raise ValueError(
            f"Unknown source_type '{source_type}'. "
            f"Supported types: 'mysql_prod', 'bigquery', 'geospot_postgres', 'chatbot_postgres', 'staging_postgres'"
        )


def _query_mysql_prod_to_polars(
    query: str,
    context: Optional[dg.AssetExecutionContext] = None,
) -> pl.DataFrame:
    """
    Query MySQL Production database and return Polars DataFrame.
    Uses the optimized query_mysql_to_polars function with schema overrides.
    """
    # Use the existing optimized function (already handles schema overrides)
    return query_mysql_to_polars(query, context=context)


def _query_bigquery_to_polars(
    query: str,
    context: Optional[dg.AssetExecutionContext] = None,
) -> pl.DataFrame:
    """
    Query BigQuery and return Polars DataFrame.
    Credentials are retrieved from SSM Parameter Store.
    Applies schema overrides to ensure consistent type mapping.
    """
    try:
        # Get BigQuery credentials from SSM
        credentials_json = ssm.get_parameter(
            Name="/dagster/vtl/BIGQUERY_CREDENTIALS",
            WithDecryption=True
        )["Parameter"]["Value"]
        credentials_info = json.loads(credentials_json)
        
        # Create BigQuery client
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        project_id = credentials_info.get("project_id")
        client = bigquery.Client(project=project_id, credentials=credentials)
        
        if context:
            context.log.info(f"Executing BigQuery query on project: {project_id}")
        
        # Execute query and get result with schema
        query_job = client.query(query)
        result = query_job.result()
        
        # Extract schema mapping from BigQuery result
        source_schema: Dict[str, str] = {}
        for field in result.schema:
            source_schema[field.name] = field.field_type
        
        # Convert to pandas, then to Polars
        df_pd = result.to_dataframe()
        df = pl.from_pandas(df_pd)
        
        # Apply schema overrides based on BigQuery types
        df = _apply_polars_schema_overrides(
            df, BIGQUERY_TO_POLARS, source_schema, context
        )
        
        if context:
            context.log.info(
                f"✅ BigQuery query completed: {df.height:,} rows, {df.width} columns "
                f"(project: {project_id})"
            )
        
        return df
    except Exception as e:
        if context:
            context.log.error(f"BigQuery query failed: {str(e)}")
        raise


def _query_geospot_postgres_to_polars(
    query: str,
    context: Optional[dg.AssetExecutionContext] = None,
) -> pl.DataFrame:
    """
    Query Geospot PostgreSQL database and return Polars DataFrame.
    Credentials are retrieved from SSM Parameter Store.
    Applies schema overrides to ensure consistent type mapping.
    """
    try:
        # Get Geospot credentials from SSM
        host = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/db_geospot_host",
            WithDecryption=False
        )["Parameter"]["Value"]
        username = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/db_geospot_user",
            WithDecryption=True
        )["Parameter"]["Value"]
        password = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/db_geospot_password",
            WithDecryption=True
        )["Parameter"]["Value"]
        database = "geospot"
        port = "5432"
        
        # Create SQLAlchemy engine
        engine_url = (
            f"postgresql+psycopg2://{username}:{password}"
            f"@{host}:{port}/{database}"
        )
        engine = create_engine(engine_url)
        
        if context:
            context.log.info(f"Executing PostgreSQL query on Geospot: {host}/{database}")
        
        # Execute query with connection to get cursor description for types
        with engine.connect() as connection:
            # Get the result proxy to access column types
            result_proxy = connection.execute(sa_text(query))
            
            # Extract schema from cursor description
            source_schema: Dict[str, str] = {}
            if result_proxy.cursor and result_proxy.cursor.description:
                for col_desc in result_proxy.cursor.description:
                    col_name = col_desc[0]
                    # Get type name from psycopg2 type code
                    type_code = col_desc[1]
                    # Map PostgreSQL type OID to type name
                    type_name = _get_postgres_type_name(type_code)
                    if type_name:
                        source_schema[col_name] = type_name
            
            # Fetch all rows
            rows = result_proxy.fetchall()
            columns = result_proxy.keys()
        
        # Convert to pandas DataFrame
        df_pd = pd.DataFrame(rows, columns=columns)
        df = pl.from_pandas(df_pd)
        
        # Apply schema overrides based on PostgreSQL types
        if source_schema:
            df = _apply_polars_schema_overrides(
                df, POSTGRES_TO_POLARS, source_schema, context
            )
        
        engine.dispose()
        
        if context:
            context.log.info(
                f"✅ Geospot PostgreSQL query completed: {df.height:,} rows, {df.width} columns "
                f"(host: {host}/{database})"
            )
        
        return df
    except Exception as e:
        if context:
            context.log.error(f"Geospot PostgreSQL query failed: {str(e)}")
        raise


def _get_postgres_type_name(type_code: int) -> Optional[str]:
    """
    Map PostgreSQL type OID to type name.
    
    Common PostgreSQL type OIDs:
    https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
    """
    # Common PostgreSQL type OIDs
    PG_TYPE_OID_MAP = {
        16: "bool",
        17: "bytea",
        18: "char",
        19: "name",
        20: "int8",
        21: "int2",
        23: "int4",
        25: "text",
        26: "oid",
        114: "json",
        142: "xml",
        700: "float4",
        701: "float8",
        790: "money",
        1042: "char",
        1043: "varchar",
        1082: "date",
        1083: "time",
        1114: "timestamp",
        1184: "timestamptz",
        1186: "interval",
        1266: "timetz",
        1700: "numeric",
        2950: "uuid",
        3802: "jsonb",
        # Array types
        1007: "_int4",
        1016: "_int8",
        1009: "_text",
        1015: "_varchar",
    }
    return PG_TYPE_OID_MAP.get(type_code)


def _query_chatbot_postgres_to_polars(
    query: str,
    context: Optional[dg.AssetExecutionContext] = None,
) -> pl.DataFrame:
    """
    Query Chatbot PostgreSQL database (spot2_chatbot) and return Polars DataFrame.
    Credentials are retrieved from SSM Parameter Store.
    Applies schema overrides to ensure consistent type mapping.
    """
    try:
        # Get Chatbot credentials from SSM
        host = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/db_chatbot_host",
            WithDecryption=False
        )["Parameter"]["Value"]
        port = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/db_chatbot_port",
            WithDecryption=False
        )["Parameter"]["Value"]
        database = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/db_chatbot_postgres_name",
            WithDecryption=False
        )["Parameter"]["Value"]
        username = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/db_chatbot_user",
            WithDecryption=True
        )["Parameter"]["Value"]
        password = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/db_chatbot_password",
            WithDecryption=True
        )["Parameter"]["Value"]
        
        # Create SQLAlchemy engine
        engine_url = (
            f"postgresql+psycopg2://{username}:{password}"
            f"@{host}:{port}/{database}"
        )
        engine = create_engine(engine_url)
        
        if context:
            context.log.info(f"Executing PostgreSQL query on Chatbot: {host}:{port}/{database}")
        
        # Execute query with connection to get cursor description for types
        with engine.connect() as connection:
            # Get the result proxy to access column types
            result_proxy = connection.execute(sa_text(query))
            
            # Extract schema from cursor description
            source_schema: Dict[str, str] = {}
            if result_proxy.cursor and result_proxy.cursor.description:
                for col_desc in result_proxy.cursor.description:
                    col_name = col_desc[0]
                    type_code = col_desc[1]
                    type_name = _get_postgres_type_name(type_code)
                    if type_name:
                        source_schema[col_name] = type_name
            
            # Fetch all rows
            rows = result_proxy.fetchall()
            columns = result_proxy.keys()
        
        # Convert to pandas DataFrame
        df_pd = pd.DataFrame(rows, columns=columns)
        
        # Convert any complex types (JSON, arrays, etc.) to strings before converting to Polars
        # This prevents "cannot mix struct and non-struct" errors
        for col in df_pd.columns:
            if df_pd[col].dtype == 'object':
                # Check if it's a complex type (dict, list, etc.)
                sample = df_pd[col].dropna()
                if len(sample) > 0:
                    first_val = sample.iloc[0]
                    if isinstance(first_val, (dict, list)):
                        # Convert to JSON string
                        df_pd[col] = df_pd[col].apply(
                            lambda x: json.dumps(x) if x is not None and isinstance(x, (dict, list)) else x
                        )
        
        df = pl.from_pandas(df_pd)
        
        # Apply schema overrides based on PostgreSQL types
        if source_schema:
            df = _apply_polars_schema_overrides(
                df, POSTGRES_TO_POLARS, source_schema, context
            )
        
        engine.dispose()
        
        if context:
            context.log.info(
                f"✅ Chatbot PostgreSQL query completed: {df.height:,} rows, {df.width} columns "
                f"(host: {host}/{database})"
            )
        
        return df
    except Exception as e:
        if context:
            context.log.error(f"Chatbot PostgreSQL query failed: {str(e)}")
        raise


def _query_staging_postgres_to_polars(
    query: str,
    context: Optional[dg.AssetExecutionContext] = None,
) -> pl.DataFrame:
    """
    Query Staging / Spot Chatbot PostgreSQL (conversation_events) and return Polars DataFrame.
    Credentials from SSM (same as conversation_analysis: spot_chatbot_*).
    """
    try:
        host = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/spot_chatbot_host",
            WithDecryption=False
        )["Parameter"]["Value"]
        port = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/spot_chatbot_port",
            WithDecryption=False
        )["Parameter"]["Value"]
        database = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/spot_chatbot_db",
            WithDecryption=False
        )["Parameter"]["Value"]
        username = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/spot_chatbot_user",
            WithDecryption=True
        )["Parameter"]["Value"]
        password = ssm.get_parameter(
            Name="/dagster/lk_conversations_metrics/spot_chatbot_password",
            WithDecryption=True
        )["Parameter"]["Value"]
        engine_url = (
            f"postgresql+psycopg2://{username}:{password}"
            f"@{host}:{port}/{database}"
        )
        engine = create_engine(engine_url)
        if context:
            context.log.info(f"Executing PostgreSQL query on Staging (Spot Chatbot): {host}:{port}/{database}")
        with engine.connect() as connection:
            result_proxy = connection.execute(sa_text(query))
            source_schema: Dict[str, str] = {}
            if result_proxy.cursor and result_proxy.cursor.description:
                for col_desc in result_proxy.cursor.description:
                    col_name = col_desc[0]
                    type_code = col_desc[1]
                    type_name = _get_postgres_type_name(type_code)
                    if type_name:
                        source_schema[col_name] = type_name
            rows = result_proxy.fetchall()
            columns = result_proxy.keys()
        df_pd = pd.DataFrame(rows, columns=columns)
        for col in df_pd.columns:
            if df_pd[col].dtype == "object":
                sample = df_pd[col].dropna()
                if len(sample) > 0 and isinstance(sample.iloc[0], (dict, list)):
                    df_pd[col] = df_pd[col].apply(
                        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
                    )
        df = pl.from_pandas(df_pd)
        if source_schema:
            df = _apply_polars_schema_overrides(
                df, POSTGRES_TO_POLARS, source_schema, context
            )
        engine.dispose()
        if context:
            context.log.info(
                f"✅ Staging PostgreSQL query completed: {df.height:,} rows, {df.width} columns "
                f"(host: {host}/{database})"
            )
        return df
    except Exception as e:
        if context:
            context.log.error(f"Staging PostgreSQL query failed: {str(e)}")
        raise


# ============ DB Helpers (Legacy - for backward compatibility) ============

def get_analysis_credentials():
    """Gets analysis database credentials from SSM."""
    username = ssm.get_parameter(
        Name="/dagster/DB_ANALYSIS_USERNAME", WithDecryption=True
    )["Parameter"]["Value"]
    password = ssm.get_parameter(
        Name="/dagster/DB_ANALYSIS_PASSWORD", WithDecryption=True
    )["Parameter"]["Value"]
    return username, password


def get_analysis_connection():
    """Returns a connection to the analysis database."""
    username, password = get_analysis_credentials()
    return mysql.connector.connect(
        host=ANALYSIS_DB_ENDPOINT,
        user=username,
        password=password,
        database="spot2_service",
    )


def get_partition_bounds(partition_key: str) -> tuple[str, str]:
    """Returns (start_date, end_date) for a partition."""
    start = datetime.strptime(partition_key, "%Y-%m-%d")
    end = start + timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def get_mysql_schema_overrides(query: str) -> dict:
    """
    Executes query to get MySQL column types and maps them to Polars types.
    
    Returns:
        Dictionary mapping column names to Polars dtypes for schema_overrides
    """
    from mysql.connector import FieldType
    
    conn = get_analysis_connection()
    cursor = conn.cursor()
    
    try:
        # Execute query to get metadata (LIMIT 0 to avoid data transfer)
        cursor.execute(f"{query} LIMIT 0")
        
        # Fetch all results to clear the cursor (even though LIMIT 0 returns no rows)
        cursor.fetchall()
        
        # MySQL type code to Polars type mapping
        MYSQL_TO_POLARS = {
            FieldType.TINY: pl.Int64,        # TINYINT → Int64 (prevents Boolean inference)
            FieldType.SHORT: pl.Int64,       # SMALLINT
            FieldType.LONG: pl.Int64,        # INT
            FieldType.LONGLONG: pl.Int64,    # BIGINT
            FieldType.FLOAT: pl.Float64,
            FieldType.DOUBLE: pl.Float64,
            FieldType.DECIMAL: pl.Float64,
            FieldType.NEWDECIMAL: pl.Float64,
            FieldType.DATE: pl.Date,
            FieldType.DATETIME: pl.Datetime,
            FieldType.TIMESTAMP: pl.Datetime,
            FieldType.TIME: pl.Time,
            FieldType.STRING: pl.Utf8,        # CHAR, VARCHAR
            FieldType.VAR_STRING: pl.Utf8,    # VARCHAR
            FieldType.BLOB: pl.Utf8,          # TEXT, BLOB (including JSON stored as TEXT)
            FieldType.MEDIUM_BLOB: pl.Utf8,
            FieldType.LONG_BLOB: pl.Utf8,
            FieldType.TINY_BLOB: pl.Utf8,
            FieldType.JSON: pl.Utf8,          # JSON type
        }
        
        schema_overrides = {}
        for col_desc in cursor.description:
            col_name = col_desc[0]
            col_type_code = col_desc[1]
            
            # Map MySQL type to Polars type
            if col_type_code in MYSQL_TO_POLARS:
                schema_overrides[col_name] = MYSQL_TO_POLARS[col_type_code]
        
        return schema_overrides
        
    finally:
        cursor.close()
        conn.close()


def query_mysql_to_polars(
    query: str, 
    params: tuple | None = None,  # Deprecated - keep for compatibility
    context: dg.AssetExecutionContext = None,
) -> pl.DataFrame:
    """
    Executes query in MySQL and returns Polars DataFrame.
    Uses connectorx with schema overrides to prevent type inference errors.
    
    Args:
        query: Complete SQL query (with values included, no placeholders)
        params: DEPRECATED - Not used, kept for compatibility
        context: Dagster context for logging
    
    Returns:
        Polars DataFrame with correct types
    """
    username, password = get_analysis_credentials()
    conn_str = f"mysql://{username}:{password}@{ANALYSIS_DB_ENDPOINT}/spot2_service"
    
    try:
        # Get MySQL schema and map to Polars types
        schema_overrides = get_mysql_schema_overrides(query)
        
        if context:
            context.log.info(f"Using schema overrides for {len(schema_overrides)} columns")
        
        # Use MySQL connection directly with pl.read_database()
        conn = get_analysis_connection()
        
        try:
            df = pl.read_database(query, conn, schema_overrides=schema_overrides)
            
            if context:
                context.log.info(
                    f"Extracted {df.height} rows from MySQL (polars read_database with schema)"
                )
            
            return df
        
        finally:
            conn.close()
        
    except Exception as e:
        # Fallback to pandas if connectorx fails
        if context:
            context.log.warning(
                f"Error with connectorx: {e}. Using pandas fallback."
            )
        
        conn = get_analysis_connection()
        
        if params:
            df_pd = pd.read_sql(query, conn, params=params)
        else:
            df_pd = pd.read_sql(query, conn)
        
        conn.close()
        
        if context:
            context.log.info(f"Extracted {len(df_pd)} rows (pandas fallback)")
        
        return pl.from_pandas(df_pd)


# ============ S3 Helpers ============
def build_bronze_s3_key(
    table_name: str, 
    file_format: str = "parquet",
) -> str:
    """
    Generates S3 key for bronze tables.
    Bronze tables use a single file that gets overwritten daily (no partitioning).
    
    Args:
        table_name: Name of the table (e.g., "raw_s2p_clients")
        file_format: File format - "parquet" or "csv" (default: parquet)
    
    Returns:
        S3 key path: bronze/{table_name}/data.{extension}
    """
    extension = file_format.lower()
    return f"bronze/{table_name}/data.{extension}"


def build_gold_s3_key(
    table_name: str, 
    partition_key: str,
    file_format: str = "parquet",
) -> str:
    """
    Generates S3 key for gold tables in Hive-style format.
    
    Args:
        table_name: Name of the table
        partition_key: Partition date (YYYY-MM-DD)
        file_format: File format - "parquet" or "csv" (default: parquet)
    
    Returns:
        S3 key path with appropriate extension
    """
    dt = datetime.strptime(partition_key, "%Y-%m-%d")
    year = dt.year
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"
    extension = file_format.lower()
    return f"gold/{table_name}/{year}/{month}/data-{day}.{extension}"


def build_gold_s3_key_part(
    table_name: str,
    partition_key: str,
    part_index: int,
    file_format: str = "parquet",
) -> str:
    """S3 key for one chunk of a gold table. part_index is 1-based."""
    dt = datetime.strptime(partition_key, "%Y-%m-%d")
    year = dt.year
    month = f"{dt.month:02d}"
    day = f"{dt.day:02d}"
    ext = file_format.lower()
    return f"gold/{table_name}/{year}/{month}/data-{day}-part-{part_index:05d}.{ext}"


def build_silver_s3_key(
    table_name: str, 
    file_format: str = "parquet",
) -> str:
    """
    Generates S3 key for silver tables.
    Silver tables use a single file that gets overwritten daily (no partitioning).
    
    Args:
        table_name: Name of the table (e.g., "stg_s2p_leads")
        file_format: File format - "parquet" or "csv" (default: parquet)
    
    Returns:
        S3 key path: silver/{table_name}/data.{extension}
    """
    extension = file_format.lower()
    return f"silver/{table_name}/data.{extension}"


def validate_dataframe_auto(
    df: pl.DataFrame,
    context: dg.AssetExecutionContext,
    primary_key_col: str = None,
) -> dict:
    """
    Basic automatic validations with no user configuration.

    Args:
        df: DataFrame to validate
        context: Dagster context for logging
        primary_key_col: Column name that should act as primary key (optional)

    Returns:
        Dict of validation results: {"check_name": (passed: bool, message: str)}
    """
    results = {}

    # 1. Non-empty check
    if df.height == 0:
        results["empty_dataframe"] = (False, "DataFrame is empty")
        context.log.warning("⚠️  Empty DataFrame detected")
    else:
        results["empty_dataframe"] = (True, f"DataFrame has {df.height} rows")

    # 2. Primary key (if specified)
    if primary_key_col and primary_key_col in df.columns:
        null_count = df.filter(pl.col(primary_key_col).is_null()).height
        duplicate_count = df.height - df.unique(subset=[primary_key_col]).height

        if null_count > 0:
            results["pk_nulls"] = (False, f"Primary key '{primary_key_col}' has {null_count} NULL values")
            context.log.warning(f"⚠️  {null_count} NULL values in primary key '{primary_key_col}'")
        else:
            results["pk_nulls"] = (True, f"Primary key '{primary_key_col}' has no NULLs")

        if duplicate_count > 0:
            results["pk_duplicates"] = (False, f"Primary key '{primary_key_col}' has {duplicate_count} duplicates")
            context.log.warning(f"⚠️  {duplicate_count} duplicates in primary key '{primary_key_col}'")
        else:
            results["pk_duplicates"] = (True, f"Primary key '{primary_key_col}' is unique")

    # 3. Columns with many NULLs (>50%)
    null_threshold = 0.5
    for col in df.columns:
        null_pct = df.select(pl.col(col).is_null().sum() / pl.len()).item()
        if null_pct > null_threshold:
            results[f"high_nulls_{col}"] = (
                False,
                f"Column '{col}' has {null_pct*100:.1f}% NULLs (threshold: {null_threshold*100}%)",
            )
            context.log.warning(f"⚠️  Column '{col}' has {null_pct*100:.1f}% NULLs")

    # 4. Basic dtype checks
    for col in df.columns:
        dtype = df.schema[col]
        # Flag numeric columns with unexpected negative values
        if dtype in [pl.Int64, pl.Float64]:
            negative_count = df.filter(pl.col(col) < 0).height
            if negative_count > 0 and col not in ["id", "created_at", "updated_at"]:  # Exclude ids and timestamps
                # Warning only; negatives may be valid
                context.log.info(f"ℹ️  Column '{col}' has {negative_count} negative values")

    # Summary log
    passed = sum(1 for v in results.values() if v[0])
    total = len(results)
    context.log.info(f"✅ Validations: {passed}/{total} passed")
    
    return results


def read_polars_from_s3(
    s3_key: str,
    file_format: str = "parquet",
) -> pl.DataFrame:
    """
    Read a Polars DataFrame from S3.

    Args:
        s3_key: S3 key path (e.g. "bronze/raw_s2p_clients/data.parquet")
        file_format: File format - "parquet" or "csv" (default: parquet)

    Returns:
        Polars DataFrame

    Raises:
        Exception: If the object does not exist or read fails
    """
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        buffer = io.BytesIO(obj["Body"].read())

        if file_format.lower() == "parquet":
            df = pl.read_parquet(buffer)
        else:
            df = pl.read_csv(buffer)

        return df
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"File not found in S3: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        raise Exception(f"Error reading from S3: {str(e)}")


def s3_file_exists(s3_key: str) -> bool:
    """
    Checks if file exists in S3.

    Args:
        s3_key: S3 key path

    Returns:
        True if file exists, False otherwise
    """
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except s3.exceptions.NoSuchKey:
        return False
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return False
        raise Exception(f"Error checking file existence in S3: {e}")
    except Exception as e:
        raise Exception(f"Error checking file existence in S3: {e}")


def get_s3_file_metadata(s3_key: str) -> dict:
    """
    Gets metadata of S3 file (last modified date, size, etc.).
    
    Args:
        s3_key: S3 key path
    
    Returns:
        Dict with metadata: {
            'last_modified': datetime,
            'size': int,
            'exists': bool
        }
    """
    try:
        response = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return {
            'last_modified': response['LastModified'],
            'size': response['ContentLength'],
            'exists': True
        }
    except s3.exceptions.NoSuchKey:
        return {
            'last_modified': None,
            'size': 0,
            'exists': False
        }
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return {
                'last_modified': None,
                'size': 0,
                'exists': False
            }
        raise Exception(f"Error getting file metadata from S3: {e}")
    except Exception as e:
        raise Exception(f"Error getting file metadata from S3: {e}")


def read_bronze_from_s3(
    bronze_table_name: str,
    context: dg.AssetExecutionContext,
) -> tuple[pl.DataFrame, dict]:
    """
    Reads bronze table from S3 with stale data detection.
    
    Silver assets use this to read their upstream bronze tables.
    If the file doesn't exist, raises an error (upstream asset failed).
    If the file exists but is stale (older than partition date), logs warning.
    
    Args:
        bronze_table_name: Name of bronze table (e.g., "raw_s2p_clients_new")
        context: Dagster context for logging and partition key
    
    Returns:
        Tuple of:
        - DataFrame: Data from S3
        - dict: {
            'file_path': str,
            'is_stale': bool,  # True if file date < partition date
            'file_date': str,  # Date of file in S3 (YYYY-MM-DD)
            'warning_message': str | None
          }
    
    Raises:
        ValueError: If file doesn't exist in S3
    """
    s3_key = build_bronze_s3_key(bronze_table_name, file_format="parquet")
    
    # Check if file exists
    if not s3_file_exists(s3_key):
        raise ValueError(
            f"Bronze table {bronze_table_name} not found in S3 at s3://{S3_BUCKET}/{s3_key}. "
            f"Upstream asset may have failed."
        )
    
    # Read file
    df = read_polars_from_s3(s3_key, file_format="parquet")
    
    # Get file metadata to check if stale
    metadata = get_s3_file_metadata(s3_key)
    file_last_modified = metadata['last_modified']
    
    if file_last_modified is None:
        # Should not happen if file exists, but handle gracefully
        context.log.warning(f"⚠️  Could not get metadata for {bronze_table_name}")
        return df, {
            'file_path': s3_key,
            'is_stale': False,
            'file_date': None,
            'warning_message': None
        }
    
    # Compare file date with partition date
    # Data is stale if file_date < partition_date
    # If partition_date is 2026-01-26, file_date should be 2026-01-26 (OK)
    # If file_date is 2026-01-25 or earlier, it's stale
    file_date = file_last_modified.date()
    ref = resolve_stale_reference_date(context)
    partition_date = datetime.strptime(ref, "%Y-%m-%d").date()
    is_stale = file_date < partition_date

    warning_msg = None
    if is_stale:
        warning_msg = (
            f"⚠️  STALE DATA: {bronze_table_name} using data from {file_date} "
            f"(expected {partition_date}, but latest in S3 is {file_date})"
        )
        context.log.warning(warning_msg)

    return df, {
        'file_path': f"s3://{S3_BUCKET}/{s3_key}",
        'is_stale': is_stale,
        'file_date': str(file_date),
        'warning_message': warning_msg
    }


def read_silver_from_s3(
    silver_table_name: str,
    context: dg.AssetExecutionContext,
) -> tuple[pl.DataFrame, dict]:
    """
    Reads silver table from S3 with stale data detection.
    
    Gold assets use this to read their upstream silver tables.
    If the file doesn't exist, raises an error (upstream asset failed).
    If the file exists but is stale (older than partition date), logs warning.
    
    Args:
        silver_table_name: Name of silver table (e.g., "stg_s2p_clients_new")
        context: Dagster context for logging and partition key
    
    Returns:
        Tuple of:
        - DataFrame: Data from S3
        - dict: {
            'file_path': str,
            'is_stale': bool,  # True if file date < partition date
            'file_date': str,  # Date of file in S3 (YYYY-MM-DD)
            'warning_message': str | None
          }
    
    Raises:
        ValueError: If file doesn't exist in S3
    """
    s3_key = build_silver_s3_key(silver_table_name, file_format="parquet")
    
    # Check if file exists
    if not s3_file_exists(s3_key):
        raise ValueError(
            f"Silver table {silver_table_name} not found in S3 at s3://{S3_BUCKET}/{s3_key}. "
            f"Upstream asset may have failed."
        )
    
    # Read file
    df = read_polars_from_s3(s3_key, file_format="parquet")
    
    # Get file metadata to check if stale
    metadata = get_s3_file_metadata(s3_key)
    file_last_modified = metadata['last_modified']
    
    if file_last_modified is None:
        # Should not happen if file exists, but handle gracefully
        context.log.warning(f"⚠️  Could not get metadata for {silver_table_name}")
        return df, {
            'file_path': s3_key,
            'is_stale': False,
            'file_date': None,
            'warning_message': None
        }
    
    # Compare file date with partition date
    # Data is stale if file_date < partition_date
    # If partition_date is 2026-01-26, file_date should be 2026-01-26 (OK)
    # If file_date is 2026-01-25 or earlier, it's stale
    file_date = file_last_modified.date()
    ref = resolve_stale_reference_date(context)
    partition_date = datetime.strptime(ref, "%Y-%m-%d").date()
    is_stale = file_date < partition_date

    warning_msg = None
    if is_stale:
        warning_msg = (
            f"⚠️  STALE DATA: {silver_table_name} using data from {file_date} "
            f"(expected {partition_date}, but latest in S3 is {file_date})"
        )
        context.log.warning(warning_msg)

    return df, {
        'file_path': f"s3://{S3_BUCKET}/{s3_key}",
        'is_stale': is_stale,
        'file_date': str(file_date),
        'warning_message': warning_msg
    }


def read_gold_from_s3(
    gold_table_name: str,
    context: dg.AssetExecutionContext,
) -> tuple[pl.DataFrame, dict]:
    """
    Reads gold table from S3 with stale data detection.
    
    Gold assets use this to read their upstream gold tables.
    If the file doesn't exist, raises an error (upstream asset failed).
    If the file exists but is stale (older than partition date), logs warning.
    
    Args:
        gold_table_name: Name of gold table (e.g., "gold_lk_leads_new")
        context: Dagster context for logging and partition key
    
    Returns:
        Tuple of:
        - DataFrame: Data from S3
        - dict: {
            'file_path': str,
            'is_stale': bool,  # True if file date < partition date
            'file_date': str,  # Date of file in S3 (YYYY-MM-DD)
            'warning_message': str | None
          }
    
    Raises:
        ValueError: If file doesn't exist in S3
    """
    s3_key = build_gold_s3_key(gold_table_name, context.partition_key, file_format="parquet")
    
    # Check if file exists
    if not s3_file_exists(s3_key):
        raise ValueError(
            f"Gold table {gold_table_name} not found in S3 at s3://{S3_BUCKET}/{s3_key}. "
            f"Upstream asset may have failed."
        )
    
    # Read file
    df = read_polars_from_s3(s3_key, file_format="parquet")
    
    # Get file metadata to check if stale
    metadata = get_s3_file_metadata(s3_key)
    file_last_modified = metadata['last_modified']
    
    if file_last_modified is None:
        # Should not happen if file exists, but handle gracefully
        context.log.warning(f"⚠️  Could not get metadata for {gold_table_name}")
        return df, {
            'file_path': s3_key,
            'is_stale': False,
            'file_date': None,
            'warning_message': None
        }
    
    # Compare file date with partition date
    # Data is stale if file_date < partition_date
    # If partition_date is 2026-01-26, file_date should be 2026-01-26 (OK)
    # If file_date is 2026-01-25 or earlier, it's stale
    file_date = file_last_modified.date()
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    is_stale = file_date < partition_date
    
    warning_msg = None
    if is_stale:
        warning_msg = (
            f"⚠️  STALE DATA: {gold_table_name} using data from {file_date} "
            f"(expected {partition_date}, but latest in S3 is {file_date})"
        )
        context.log.warning(warning_msg)
    
    return df, {
        'file_path': f"s3://{S3_BUCKET}/{s3_key}",
        'is_stale': is_stale,
        'file_date': str(file_date),
        'warning_message': warning_msg
    }


def read_governance_from_db(
    table_name: str,
    context: dg.AssetExecutionContext,
    *,
    columns: Optional[List[str]] = None,
    jsonb_columns: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Reads a governance table directly from Geospot PostgreSQL (the DB where load_to_geospot writes).

    Use this for QA checks so we validate the actual data loaded in the DB, not the parquet in S3.

    Args:
        table_name: Governance table name (e.g. "lk_leads", "bt_conv_conversations").
        context: Dagster context for logging.
        columns: Optional list of column names (order preserved). If provided, builds an explicit
                 SELECT (avoids SELECT * pulling array columns that Polars cannot read as strings).
                 Columns in jsonb_columns are cast to text in SQL.
        jsonb_columns: Column names that are JSONB; cast to text in SQL when columns is provided.

    Returns:
        Polars DataFrame with the full table contents.

    Raises:
        Exception: If the query fails (e.g. table missing, connection error).
    """
    jsonb_set = set(jsonb_columns or [])
    if columns:
        # Explicit SELECT: avoids SELECT * pulling array/json columns that break Polars (e.g. lk_projects.project_state_ids).
        select_parts = [
            f'"{c}"::text AS "{c}"' if c in jsonb_set else f'"{c}"'
            for c in columns
        ]
        query = f'SELECT {", ".join(select_parts)} FROM "{table_name}"'
    else:
        query = f'SELECT * FROM "{table_name}"'
    return _query_geospot_postgres_to_polars(query, context=context)


def get_table_info_from_s3(
    layer: str,
    table_name: str,
    file_format: str = "parquet",
) -> dict:
    """
    Get basic info for a table in S3 (row count, columns, etc.).

    Args:
        layer: Layer ("bronze", "silver", "gold")
        table_name: Table name (e.g. "raw_s2p_clients", "stg_s2p_clients")
        file_format: File format - "parquet" or "csv" (default: parquet)

    Returns:
        Dict with: {
            "rows": int,
            "columns": int,
            "column_names": list,
            "s3_path": str,
            "exists": bool
        }
    """
    # Build S3 key from layer
    if layer == "bronze":
        s3_key = build_bronze_s3_key(table_name, file_format=file_format)
    elif layer == "silver":
        s3_key = build_silver_s3_key(table_name, file_format=file_format)
    elif layer == "gold":
        # Gold needs partition_key; use get_table_info_from_s3_gold() instead
        raise ValueError("For gold, use get_table_info_from_s3_gold() with partition_key")
    else:
        raise ValueError(f"Invalid layer '{layer}'. Use 'bronze', 'silver', or 'gold'")
    
    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    
    try:
        df = read_polars_from_s3(s3_key, file_format=file_format)
        return {
            "rows": df.height,
            "columns": df.width,
            "column_names": df.columns,
            "s3_path": s3_path,
            "exists": True,
        }
    except FileNotFoundError:
        return {
            "rows": 0,
            "columns": 0,
            "column_names": [],
            "s3_path": s3_path,
            "exists": False,
        }
    except Exception as e:
        return {
            "rows": 0,
            "columns": 0,
            "column_names": [],
            "s3_path": s3_path,
            "exists": False,
            "error": str(e),
        }


def get_table_info_from_s3_gold(
    table_name: str,
    partition_key: str,
    file_format: str = "parquet",
) -> dict:
    """
    Get basic info for a gold table in S3 (partitioned path).

    Args:
        table_name: Table name (e.g. "lk_projects")
        partition_key: Partition key (e.g. "2024-12-01")
        file_format: File format - "parquet" or "csv" (default: parquet)

    Returns:
        Dict with: {
            "rows": int,
            "columns": int,
            "column_names": list,
            "s3_path": str,
            "exists": bool
        }
    """
    s3_key = build_gold_s3_key(table_name, partition_key, file_format=file_format)
    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    
    try:
        df = read_polars_from_s3(s3_key, file_format=file_format)
        return {
            "rows": df.height,
            "columns": df.width,
            "column_names": df.columns,
            "s3_path": s3_path,
            "exists": True,
            "partition": partition_key,
        }
    except FileNotFoundError:
        return {
            "rows": 0,
            "columns": 0,
            "column_names": [],
            "s3_path": s3_path,
            "exists": False,
            "partition": partition_key,
        }
    except Exception as e:
        return {
            "rows": 0,
            "columns": 0,
            "column_names": [],
            "s3_path": s3_path,
            "exists": False,
            "partition": partition_key,
            "error": str(e),
        }


def _list_to_json(x) -> str | None:
    """Converts a Polars list element to JSON string."""
    if x is None:
        return None
    # Handle both Python list and Polars Series
    if hasattr(x, 'to_list'):
        return json.dumps(x.to_list())
    elif isinstance(x, list):
        return json.dumps(x)
    else:
        return json.dumps(list(x))


def _convert_lists_to_json(df: pl.DataFrame) -> pl.DataFrame:
    """
    Converts List columns to JSON string for CSV compatibility.
    
    Parquet supports native List types, but CSV does not.
    This function serializes List columns as JSON strings.
    """
    list_columns = [
        col for col in df.columns 
        if df.schema[col].base_type() == pl.List
    ]
    
    if not list_columns:
        return df
    
    # Convert each List column to JSON string
    conversions = []
    for col in list_columns:
        conversions.append(
            pl.col(col).map_elements(
                _list_to_json,
                return_dtype=pl.Utf8
            ).alias(col)
        )
    
    return df.with_columns(conversions)


def _normalize_jsonb_cell_for_geospot(val, empty_json: str) -> Optional[str]:
    """
    Produce text Geospot/Postgres can cast to jsonb from Parquet string columns.
    Empty or invalid JSON strings become empty_json (default []); None/NaN -> SQL NULL (None).
    """
    import math

    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return empty_json
        try:
            parsed = json.loads(s)
            return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
        except (json.JSONDecodeError, TypeError, ValueError):
            return empty_json
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
    try:
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        return empty_json


def normalize_jsonb_columns_for_geospot_parquet(
    df: pl.DataFrame,
    columns: List[str],
    *,
    empty_json: str = "[]",
) -> pl.DataFrame:
    """
    Re-serialize JSONB-bound columns as valid JSON UTF-8 text before Parquet upload to Geospot.
    Reduces async COPY failures on '', invalid JSON, or inconsistent string encoding.
    """
    if df.height == 0 or not columns:
        return df
    out = df

    def _make_mapper(ej: str):
        def _mapper(v):
            return _normalize_jsonb_cell_for_geospot(v, ej)

        return _mapper

    for col in columns:
        if col not in out.columns:
            continue
        out = out.with_columns(
            pl.col(col).map_elements(_make_mapper(empty_json), return_dtype=pl.Utf8).alias(col)
        )
    return out


def write_polars_to_s3(
    df: pl.DataFrame,
    s3_key: str,
    context: dg.AssetExecutionContext,
    file_format: str = "parquet",
    geospot_jsonb_columns: Optional[List[str]] = None,
) -> str:
    """
    Writes a Polars DataFrame to S3 in the specified format.

    Args:
        df: Polars DataFrame to write
        s3_key: S3 key path (should match the format extension)
        context: Dagster context for logging
        file_format: File format - "parquet" or "csv" (default: parquet)
        geospot_jsonb_columns: If set, normalize these columns to valid JSON text before write
            (for PostgreSQL jsonb loads from Parquet via Geospot).

    Returns:
        Full S3 path where the file was saved

    Note:
        For CSV format, List columns are automatically converted to JSON strings.
        Parquet preserves native List types for better query performance.
    """
    if df.height == 0:
        context.log.warning(f"⚠️  No data to save at {s3_key}")
        return "No data to save"

    df_out = df
    if geospot_jsonb_columns:
        context.log.info(
            f"   Normalizing JSONB-bound columns for Geospot Parquet: {geospot_jsonb_columns}"
        )
        df_out = normalize_jsonb_columns_for_geospot_parquet(df_out, geospot_jsonb_columns)

    # Log data summary before writing
    context.log.info(
        f"📊 Writing to S3: {df_out.height:,} rows, {df_out.width} columns "
        f"→ s3://{S3_BUCKET}/{s3_key}"
    )

    buffer = io.BytesIO()

    if file_format.lower() == "csv":
        # CSV format: convert List columns to JSON strings
        df_csv = _convert_lists_to_json(df_out)
        csv_content = df_csv.write_csv()
        buffer.write(csv_content.encode("utf-8"))
    else:
        # Parquet format: preserve native types (with snappy compression)
        df_out.write_parquet(buffer, compression="snappy")

    buffer.seek(0)
    file_size_bytes = len(buffer.getvalue())
    file_size_mb = file_size_bytes / (1024 * 1024)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
    )

    full_path = f"s3://{S3_BUCKET}/{s3_key}"
    context.log.info(
        f"✅ Saved to S3: {df_out.height:,} rows, {df_out.width} columns, "
        f"{file_size_mb:.2f} MB ({file_format}) → {full_path}"
    )
    return full_path


# S3 multipart minimum part size (except last part)
_S3_MULTIPART_MIN_BYTES = 5 * 1024 * 1024  # 5 MB


def write_polars_to_s3_csv_chunked(
    df: pl.DataFrame,
    s3_key: str,
    context: dg.AssetExecutionContext,
    chunk_rows: int = 10_000,
) -> str:
    """
    Write a DataFrame to S3 as CSV in chunks/streaming to avoid materializing the full CSV in memory.
    Uses multipart upload (parts >= 5 MB).
    Header is included only in the first part.
    """
    if df.height == 0:
        context.log.warning(f"⚠️  No data to save at {s3_key}")
        return "No data to save"

    df_csv = _convert_lists_to_json(df)
    context.log.info(
        f"📊 Writing to S3 (CSV chunked): {df.height:,} rows, {df.width} columns "
        f"→ s3://{S3_BUCKET}/{s3_key}"
    )

    mpu = s3.create_multipart_upload(Bucket=S3_BUCKET, Key=s3_key)
    upload_id = mpu["UploadId"]
    parts = []
    part_number = 1
    buffer = io.BytesIO()
    total_uploaded = 0

    for start in range(0, df_csv.height, chunk_rows):
        chunk = df_csv.slice(start, chunk_rows)
        # quote_style='non_numeric' keeps commas inside text from breaking backend CSV parsing
        chunk_bytes = chunk.write_csv(
            include_header=(start == 0),
            quote_style="non_numeric",
        ).encode("utf-8")
        buffer.write(chunk_bytes)

        while buffer.tell() >= _S3_MULTIPART_MIN_BYTES:
            buffer.seek(0)
            body = buffer.read(_S3_MULTIPART_MIN_BYTES)
            remainder = buffer.read()
            buffer = io.BytesIO(remainder)
            resp = s3.upload_part(
                Bucket=S3_BUCKET,
                Key=s3_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=body,
            )
            parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})
            part_number += 1
            total_uploaded += len(body)

    if buffer.tell() > 0:
        buffer.seek(0)
        body = buffer.getvalue()
        total_uploaded += len(body)
        resp = s3.upload_part(
            Bucket=S3_BUCKET,
            Key=s3_key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=body,
        )
        parts.append({"PartNumber": part_number, "ETag": resp["ETag"]})

    s3.complete_multipart_upload(
        Bucket=S3_BUCKET,
        Key=s3_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    file_size_mb = total_uploaded / (1024 * 1024)
    full_path = f"s3://{S3_BUCKET}/{s3_key}"
    context.log.info(
        f"✅ Saved to S3 (chunked): {df.height:,} rows, {df.width} columns, "
        f"~{file_size_mb:.2f} MB (csv) → {full_path}"
    )
    return full_path


# ============ Geospot API Helpers ============

def get_geospot_api_key() -> str:
    """Gets Geospot Lakehouse API key from SSM."""
    resp = ssm.get_parameter(
        Name=GEOSPOT_API_KEY_PARAM,
        WithDecryption=True,
    )
    return resp["Parameter"]["Value"]


def _geospot_table_row_count(table_name: str, context: dg.AssetExecutionContext) -> int:
    """Cheap COUNT(*) against Geospot Postgres (same connection as read_governance_from_db)."""
    q = f'SELECT COUNT(*) AS _cnt FROM "{table_name}"'
    df = _query_geospot_postgres_to_polars(q, context=context)
    if df.is_empty():
        return 0
    return int(df["_cnt"][0])


def _resolve_geospot_post_verify_seconds(explicit: Optional[float]) -> Optional[float]:
    """Seconds to wait after API 200 then COUNT(*) — from arg or env GEOSPOT_LOAD_VERIFY_SECONDS."""
    if explicit is not None:
        return explicit if explicit > 0 else None
    raw = os.environ.get("GEOSPOT_LOAD_VERIFY_SECONDS", "").strip()
    if not raw:
        return None
    try:
        v = float(raw)
        return v if v > 0 else None
    except ValueError:
        return None


def _maybe_verify_geospot_load(
    table_name: str,
    context: dg.AssetExecutionContext,
    verify_seconds: Optional[float],
    was_async_response: bool,
) -> None:
    """
    After async Geospot loads, row count may not change until the server finishes COPY.
    Optional sleep + COUNT(*) to surface stuck loads in Dagster logs.
    """
    if not verify_seconds or verify_seconds <= 0:
        return
    if not was_async_response:
        return
    context.log.info(
        f"Geospot: waiting {verify_seconds:.0f}s (GEOSPOT_LOAD_VERIFY_SECONDS / post_async_verify_seconds) "
        f"then COUNT(*) on {table_name}..."
    )
    time.sleep(verify_seconds)
    try:
        n = _geospot_table_row_count(table_name, context)
        context.log.info(f"Geospot post-load verify: {table_name} row_count={n:,}")
    except Exception as e:
        context.log.warning(f"Geospot post-load verify failed (COUNT query): {e}")


def load_to_geospot(
    s3_key: str,
    table_name: str,
    mode: str,
    context: dg.AssetExecutionContext,
    max_retries: int = 3,
    base_delay: float = 2.0,
    timeout: float = 120,
    conflict_columns: Optional[List[str]] = None,
    update_columns: Optional[List[str]] = None,
    post_async_verify_seconds: Optional[float] = None,
) -> str:
    """
    Sends HTTP request to load data from S3 into PostgreSQL via Geospot API.

    Includes retry with exponential backoff to handle transient errors
    (timeouts, 429 Too Many Requests, 5xx server errors).

    Args:
        s3_key: S3 key path (without bucket name)
        table_name: Target table name in PostgreSQL
        mode: Load mode - "replace", "append", or "upsert"
        context: Dagster context for logging
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds for exponential backoff (default: 2.0)
        timeout: Request timeout in seconds (default: 120). Use a higher value for
            large tables (e.g. lk_spots) if the Geospot gateway allows it.
        conflict_columns: For mode="upsert", columns that define the conflict (e.g. PK).
        update_columns: For mode="upsert", columns to update when conflict (rest are left as-is).
        post_async_verify_seconds: After an async (in progress) 200, wait this many seconds and log
            COUNT(*) for the target table. Overrides env when non-None; env GEOSPOT_LOAD_VERIFY_SECONDS
            is used when this is None.

    Returns:
        Success message string

    Raises:
        dg.Failure: If API returns non-200 status after all retries exhausted
    """
    verify_sec = _resolve_geospot_post_verify_seconds(post_async_verify_seconds)

    api_key = get_geospot_api_key()

    payload: Dict = {
        "bucket_name": S3_BUCKET,
        "s3_key": s3_key,
        "table_name": table_name,
        "mode": mode,
    }
    if mode == "upsert" and conflict_columns is not None:
        payload["conflict_columns"] = conflict_columns
    if mode == "upsert" and update_columns is not None:
        payload["update_columns"] = update_columns

    headers = {
        "Authorization": f"Api-Key {api_key}",
        "Content-Type": "application/json",
    }

    context.log.info(
        f"Sending load request to Geospot:\n"
        f"  endpoint: {GEOSPOT_LAKEHOUSE_ENDPOINT}\n"
        f"  payload: {payload}"
    )

    last_error: Exception | None = None
    last_status: int | None = None
    last_body: str = ""

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                GEOSPOT_LAKEHOUSE_ENDPOINT,
                headers=headers,
                json=payload,
                timeout=timeout,
            )

            last_status = resp.status_code
            last_body = resp.text

            # Log full response details
            context.log.info(
                f"Geospot API response (attempt {attempt}/{max_retries}):\n"
                f"  status_code: {resp.status_code}\n"
                f"  body: {resp.text[:500]}"
            )

            if resp.status_code == 200:
                was_async = False
                try:
                    response_data = resp.json()
                    context.log.info(f"Geospot API parsed response: {response_data}")
                    if isinstance(response_data, dict) and "message" in response_data:
                        msg = str(response_data["message"]).lower()
                        if "in progress" in msg or "progress" in msg:
                            was_async = True
                            context.log.warning(
                                "Geospot load is asynchronous: the API accepted the request but the actual "
                                "COPY/INSERT runs on the server. If row counts do not change, the server job "
                                "likely failed — check Geospot/Postgres logs. "
                                "Set env GEOSPOT_LOAD_VERIFY_SECONDS=60 (or pass post_async_verify_seconds) "
                                "to log COUNT(*) after a short wait."
                            )
                except Exception:
                    context.log.info(f"Geospot API raw response: {resp.text}")
                _maybe_verify_geospot_load(table_name, context, verify_sec, was_async)
                return f"Successfully loaded {table_name} to PostgreSQL"

            # Retryable HTTP status codes: 429 (rate limit), 5xx (server error)
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = base_delay * (2 ** (attempt - 1))
                context.log.warning(
                    f"Retryable error {resp.status_code} on attempt {attempt}/{max_retries}. "
                    f"Waiting {delay:.1f}s before retry..."
                )
                time.sleep(delay)
                continue

            # Non-retryable error (4xx except 429): fail immediately
            context.log.error(
                f"Geospot API non-retryable error: status={resp.status_code}, body={resp.text}"
            )
            raise dg.Failure(
                description=f"Geospot load failed with status {resp.status_code}: {resp.text}"
            )

        except requests.exceptions.RequestException as exc:
            last_error = exc
            delay = base_delay * (2 ** (attempt - 1))
            context.log.warning(
                f"Network error on attempt {attempt}/{max_retries}: {exc}. "
                f"Waiting {delay:.1f}s before retry..."
            )
            time.sleep(delay)

    # All retries exhausted
    error_detail = (
        f"status={last_status}, body={last_body}"
        if last_status is not None
        else str(last_error)
    )
    context.log.error(
        f"Geospot API request failed after {max_retries} attempts: {error_detail}"
    )
    raise dg.Failure(
        description=(
            f"Geospot load failed for {table_name} after {max_retries} attempts: "
            f"{error_detail}"
        )
    )
