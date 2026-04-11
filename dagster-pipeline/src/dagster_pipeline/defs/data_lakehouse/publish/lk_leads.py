# defs/data_lakehouse/publish/lk_leads.py
"""
Publish layer: Load gold_lk_leads to external destinations.

This layer handles the "L" (Load) of ELT:
- lk_leads_to_s3: Writes to S3 as CSV/Parquet
- lk_leads_to_geospot: Loads to PostgreSQL via Geospot API
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_gold_s3_key,
    write_polars_to_s3,
    load_to_geospot,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


# File format for Geospot compatibility
FILE_FORMAT = "csv"  # "parquet" or "csv" - Geospot currently only supports CSV

# Boolean columns that must be written as 0/1 in CSV so PostgreSQL COPY accepts them.
# Polars writes "true"/"false" which some loaders store as NULL for BOOLEAN columns.
BOOLEAN_COLS_CSV = (
    "lead_l0", "lead_l1", "lead_l2", "lead_l3", "lead_l4",
    "lead_supply", "lead_s0", "lead_s1", "lead_s3",
    "lead_agent_by_api",
)


@dg.asset(
    partitions_def=daily_partitions,
    group_name="publish",
    description="Publish: saves gold_lk_leads as CSV to S3.",
)
def lk_leads_to_s3(
    context: dg.AssetExecutionContext,
    gold_lk_leads: pl.DataFrame,
) -> str:
    """Writes DataFrame to S3 and returns the S3 key."""
    def body() -> str:
        partition_key = context.partition_key  # e.g. "2025-12-17"

        # Builds: data_lakehouse/gold/lk_leads/year=2025/month=12/day=17/data.csv
        s3_key = build_gold_s3_key("lk_leads", partition_key, file_format=FILE_FORMAT)

        # Cast boolean columns to 0/1 for CSV so GeoSpot/PostgreSQL COPY populates BOOLEAN correctly.
        # Without this, Polars writes "true"/"false" and the loader may leave columns NULL.
        cols_to_cast = [c for c in BOOLEAN_COLS_CSV if c in gold_lk_leads.columns and gold_lk_leads.schema[c] == pl.Boolean]
        df_for_csv = gold_lk_leads.with_columns([pl.col(c).cast(pl.Int8) for c in cols_to_cast]) if cols_to_cast else gold_lk_leads

        write_polars_to_s3(df_for_csv, s3_key, context, file_format=FILE_FORMAT)

        # Add metadata only when running in Dagster runtime (not direct invocation)
        try:
            context.add_output_metadata({
                "s3_key": s3_key,
                "rows": gold_lk_leads.height,
                "format": FILE_FORMAT,
                "partition": partition_key,
            })
        except Exception:
            context.log.info(f"Metadata: s3_key={s3_key}, rows={gold_lk_leads.height}")

        return s3_key

    yield from iter_job_wrapped_compute(context, body)


@dg.asset(
    partitions_def=daily_partitions,
    group_name="publish",
    description="Publish: loads lk_leads from S3 into PostgreSQL via Geospot API.",
    deps=["lk_leads_to_s3"],  # Dependency without loading the value
    op_tags={"geospot_api": "write"},
)
def lk_leads_to_geospot(
    context: dg.AssetExecutionContext,
    # No longer receives lk_leads_to_s3 as parameter
) -> str:
    """Sends HTTP request to load S3 data into PostgreSQL."""
    def body() -> str:
        partition_key = context.partition_key

        # Reconstruct S3 key (deterministic based on table + partition)
        s3_key = build_gold_s3_key("lk_leads", partition_key, file_format=FILE_FORMAT)

        result = load_to_geospot(
            s3_key=s3_key,
            table_name="lk_leads_v2",  # Target table in PostgreSQL (v2)
            mode="replace",            # or "append"
            context=context,
        )

        # Add metadata only when running in Dagster runtime (not direct invocation)
        try:
            context.add_output_metadata({
                "table_name": "lk_leads_v2",
                "s3_key": s3_key,
                "mode": "replace",
                "partition": partition_key,
            })
        except Exception:
            context.log.info(f"Metadata: table=lk_leads_v2, s3_key={s3_key}")

        return result

    yield from iter_job_wrapped_compute(context, body)
