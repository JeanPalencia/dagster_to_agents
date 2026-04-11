#!/usr/bin/env python3
"""
Script to extract column names and data types from all gold tables (_new versions).
Reads from S3 parquet files or executes transformations directly to get schema.

Supported tables:
- gold_lk_leads_new
- gold_lk_projects_new
- gold_lk_users_new
- gold_lk_spots_new
- gold_bt_lds_lead_spots_new
- gold_bt_conv_conversations_new
- gold_lk_matches_visitors_to_leads_new
- gold_bt_transactions_new
"""
import polars as pl
import boto3
from botocore.exceptions import ClientError
import sys
from pathlib import Path
from datetime import date

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from dagster_pipeline.defs.data_lakehouse.shared import (
    S3_BUCKET, 
    build_gold_s3_key,
    read_silver_from_s3,
    read_bronze_from_s3,
)
from dagster_pipeline.defs.data_lakehouse.gold.gold_lk_leads_new import _transform_lk_leads_new
from dagster_pipeline.defs.data_lakehouse.gold.gold_lk_projects_new import _transform_lk_projects_new
from dagster_pipeline.defs.data_lakehouse.gold.gold_lk_users_new import _transform_lk_users_new
from dagster_pipeline.defs.data_lakehouse.gold.gold_lk_spots_new import _transform_lk_spots_new
from dagster_pipeline.defs.data_lakehouse.gold.gold_bt_conv_conversations_new import _transform_bt_conv_conversations
from dagster_pipeline.defs.data_lakehouse.gold.gold_bt_lds_lead_spots_new import _normalize_silvers_for_lds
from dagster_pipeline.defs.data_lakehouse.gold.gold_bt_transactions_new import _transform_bt_transactions_new
from dagster_pipeline.defs.data_lakehouse.gold.gold_bt_lds_lead_spots import _transform_bt_lds_lead_spots
from dagster_pipeline.defs.data_lakehouse.gold.utils import group_messages_into_conversations, add_audit_fields

s3 = boto3.client("s3")

# Asset name -> S3 path segment (gold assets write with lk_*_new, not gold_lk_*_new)
TABLE_TO_S3_KEY = {
    "gold_lk_leads_new": "lk_leads",
    "gold_lk_projects_new": "lk_projects_new",
    "gold_lk_users_new": "lk_users",
    "gold_lk_spots_new": "lk_spots_new",
    "gold_bt_lds_lead_spots_new": "bt_lds_lead_spots",
    "gold_bt_conv_conversations_new": "gold_bt_conv_conversations_new",
    "gold_lk_matches_visitors_to_leads_new": "lk_matches_visitors_to_leads_new",
    "gold_bt_transactions_new": "bt_transactions",
}

# All gold tables to process
GOLD_TABLES = [
    "gold_lk_leads_new",
    "gold_lk_projects_new",
    "gold_lk_users_new",
    "gold_lk_spots_new",
    "gold_bt_lds_lead_spots_new",
    "gold_bt_conv_conversations_new",
    "gold_lk_matches_visitors_to_leads_new",
    "gold_bt_transactions_new",
]


def read_gold_from_s3(table_name: str, partition_key: str = "2026-01-23", verbose: bool = False) -> pl.DataFrame:
    """Read gold table from S3. Returns None if not found (silently)."""
    s3_table = TABLE_TO_S3_KEY.get(table_name, table_name)
    s3_key = build_gold_s3_key(s3_table, partition_key=partition_key, file_format="parquet")
    
    try:
        # Try to read from S3
        s3_path = f"s3://{S3_BUCKET}/{s3_key}"
        df = pl.read_parquet(s3_path)
        return df
    except Exception as e:
        # Silently return None - the caller will handle the missing table
        # Only show detailed error if verbose mode is enabled (for debugging)
        if verbose:
            import traceback
            print(f"Error reading {table_name} from S3:")
            traceback.print_exc()
        return None


def get_schema_from_transformation(partition_key: str = "2026-01-23"):
    """Get schema by executing transformations directly."""
    print(f"Executing transformations to get schema for partition {partition_key}...")
    
    # Create a mock context (we won't use it for logging)
    class MockLog:
        """Mock logger that ignores all log calls."""
        def info(self, *args, **kwargs):
            pass
        def warning(self, *args, **kwargs):
            pass
        def error(self, *args, **kwargs):
            pass
        def debug(self, *args, **kwargs):
            pass
    
    class MockContext:
        def __init__(self, pk: str):
            self.partition_key = pk
            self.asset_name = ""
            self.log = MockLog()
    
    context = MockContext(partition_key)
    results = {}
    
    try:
        # Read common Silver tables
        print("  Reading common Silver tables...")
        stg_clients, _ = read_silver_from_s3("stg_s2p_clients_new", context)
        stg_projects, _ = read_silver_from_s3("stg_s2p_projects_new", context)
        stg_funnel, _ = read_silver_from_s3("core_project_funnel_new", context)
        
        # Transform gold_lk_leads_new (+ audit fields, same as asset)
        print("  Transforming gold_lk_leads_new...")
        df_leads = _transform_lk_leads_new(stg_clients, stg_projects, stg_funnel)
        results["gold_lk_leads_new"] = add_audit_fields(df_leads, job_name="lk_leads")
        
        # Transform gold_lk_projects_new (+ audit fields)
        print("  Reading stg_s2p_profiles_new...")
        stg_profiles, _ = read_silver_from_s3("stg_s2p_profiles_new", context)
        print("  Transforming gold_lk_projects_new...")
        df_projects = _transform_lk_projects_new(stg_projects, stg_clients, stg_profiles, stg_funnel)
        results["gold_lk_projects_new"] = add_audit_fields(df_projects, job_name="lk_projects")
        
        # Transform gold_lk_users_new (+ audit fields)
        print("  Reading stg_s2p_users_new...")
        stg_users, _ = read_silver_from_s3("stg_s2p_users_new", context)
        print("  Transforming gold_lk_users_new...")
        df_users = _transform_lk_users_new(stg_users, stg_clients, context)
        results["gold_lk_users_new"] = add_audit_fields(df_users, job_name="lk_users_new")
        
        # Transform gold_lk_spots_new (+ audit fields)
        print("  Reading stg_lk_spots_new...")
        stg_lk_spots, _ = read_silver_from_s3("stg_lk_spots_new", context)
        print("  Transforming gold_lk_spots_new...")
        df_spots = _transform_lk_spots_new(stg_lk_spots)
        results["gold_lk_spots_new"] = add_audit_fields(df_spots, job_name="lk_spots")
        
        # Transform gold_bt_lds_lead_spots_new (reuses legacy transform; audit already in transform)
        print("  Reading LDS silver tables (prs, appointments, apd, alg)...")
        stg_prs, _ = read_silver_from_s3("stg_s2p_prs_project_spots_new", context)
        stg_appointments, _ = read_silver_from_s3("stg_s2p_appointments_new", context)
        stg_apd, _ = read_silver_from_s3("stg_s2p_apd_appointment_dates_new", context)
        stg_alg, _ = read_silver_from_s3("stg_s2p_alg_activity_log_projects_new", context)
        print("  Normalizing and transforming gold_bt_lds_lead_spots_new...")
        norm = _normalize_silvers_for_lds(
            stg_clients, stg_projects, stg_prs, stg_appointments, stg_apd, stg_alg, stg_profiles
        )
        df_lds = _transform_bt_lds_lead_spots(*norm)
        results["gold_bt_lds_lead_spots_new"] = df_lds.with_columns(
            pl.lit("gold_bt_lds_lead_spots_new").alias("aud_job")
        )
        
        # Transform gold_bt_conv_conversations_new
        print("  Reading stg_cb_messages_new...")
        stg_cb_messages, _ = read_silver_from_s3("stg_cb_messages_new", context)
        print("  Grouping messages into conversations...")
        df_conversations = group_messages_into_conversations(stg_cb_messages, gap_hours=24)
        
        # Read appointments (optional, may not exist)
        try:
            print("  Reading stg_s2p_appointments_new...")
            stg_appointments, _ = read_silver_from_s3("stg_s2p_appointments_new", context)
        except Exception:
            print("  ⚠️  stg_s2p_appointments_new not available, using empty DataFrame")
            stg_appointments = pl.DataFrame()
        
        print("  Transforming gold_bt_conv_conversations_new...")
        df_conv = _transform_bt_conv_conversations(
            df_conversations, stg_clients, stg_appointments, stg_projects, context
        )
        results["gold_bt_conv_conversations_new"] = add_audit_fields(df_conv, job_name="bt_conv_conversations_new")
        
        # gold_lk_matches_visitors_to_leads_new: silver + audit
        print("  Reading stg_gs_lk_matches_visitors_to_leads_new...")
        stg_matches, _ = read_silver_from_s3("stg_gs_lk_matches_visitors_to_leads_new", context)
        print("  Building gold_lk_matches_visitors_to_leads_new (add audit)...")
        results["gold_lk_matches_visitors_to_leads_new"] = add_audit_fields(
            stg_matches, job_name="lk_matches_visitors_to_leads_new"
        )
        
        # gold_bt_transactions_new: gold projects + gold spots + silver prs (stg_prs already read above), transform + audit
        print("  Building gold_bt_transactions_new...")
        df_bt = _transform_bt_transactions_new(
            results["gold_lk_projects_new"], stg_prs, results["gold_lk_spots_new"]
        )
        results["gold_bt_transactions_new"] = add_audit_fields(df_bt, job_name="bt_transactions_new")
        
        return results
    except Exception as e:
        print(f"Error executing transformations: {e}")
        import traceback
        traceback.print_exc()
        return {}


def get_schema_markdown(df: pl.DataFrame, table_name: str) -> str:
    """Generate markdown list of columns with data types."""
    schema = df.schema
    lines = [f"## {table_name}\n"]
    
    for col_name, dtype in schema.items():
        # Convert Polars dtype to readable format
        dtype_str = str(dtype)
        # Simplify dtype names
        dtype_str = dtype_str.replace("Int64", "Integer")
        dtype_str = dtype_str.replace("Int32", "Integer")
        dtype_str = dtype_str.replace("Float64", "Float")
        dtype_str = dtype_str.replace("Float32", "Float")
        dtype_str = dtype_str.replace("Utf8", "String")
        dtype_str = dtype_str.replace("Boolean", "Boolean")
        dtype_str = dtype_str.replace("Date", "Date")
        dtype_str = dtype_str.replace("Datetime", "DateTime")
        dtype_str = dtype_str.replace("List", "List")
        
        lines.append(f"* {col_name} - {dtype_str}")
    
    return "\n".join(lines)


def main():
    partition_key = "2026-01-23"  # Default partition, can be made configurable
    
    print(f"📊 Getting schemas for all Gold tables (partition: {partition_key})\n")
    
    # Try to read all tables from S3 first
    results = {}
    missing_tables = []
    
    for table_name in GOLD_TABLES:
        print(f"Reading {table_name} from S3...", end=" ")
        df = read_gold_from_s3(table_name, partition_key, verbose=False)
        if df is not None:
            results[table_name] = df
            print(f"✅ Loaded {len(df.columns)} columns, {df.height} rows")
        else:
            missing_tables.append(table_name)
            print("⚠️  Not found (will use transformations)")
    
    # If any tables are missing, try to get schema from transformations
    if missing_tables:
        print(f"\n⚠️  Could not read {len(missing_tables)} table(s) from S3. Executing transformations to get schema...")
        transformation_results = get_schema_from_transformation(partition_key)
        results.update(transformation_results)
    
    if not results:
        print("❌ Could not get schema for any tables. Please materialize the tables first or check errors above.")
        return
    
    # Generate markdown for each table
    markdown_parts = []
    print("\n" + "=" * 80)
    
    for table_name in GOLD_TABLES:
        if table_name in results:
            df = results[table_name]
            print(f"\n✅ {table_name}: {len(df.columns)} columns, {df.height} rows")
            md = get_schema_markdown(df, table_name)
            markdown_parts.append(md)
            print(md)
        else:
            print(f"\n❌ {table_name}: Could not get schema")
    
    print("\n" + "=" * 80)
    
    # Save to file
    output_file = project_root / "GOLD_TABLES_SCHEMA.md"
    with open(output_file, "w") as f:
        f.write("# Gold Tables Schema\n\n")
        f.write(f"Partition: {partition_key}\n\n")
        f.write("=" * 80 + "\n\n")
        f.write("\n\n".join(markdown_parts))
    
    print(f"\n✅ Schema saved to: {output_file}")


if __name__ == "__main__":
    main()
