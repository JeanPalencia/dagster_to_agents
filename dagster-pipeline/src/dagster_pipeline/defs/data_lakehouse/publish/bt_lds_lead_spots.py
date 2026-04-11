# defs/data_lakehouse/publish/bt_lds_lead_spots.py
"""
Publish layer: Load bt_lds_lead_spots_bck (legacy gold) to external destinations.

This layer handles the "L" (Load) of ELT for the legacy pipeline (lakehouse_daily_job):
- bt_lds_lead_spots_to_s3: Writes to S3 as CSV (path bt_lds_lead_spots_bck)
- bt_lds_lead_spots_to_geospot: Loads to PostgreSQL table bt_lds_lead_spots_bck via Geospot API
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


@dg.asset(
    partitions_def=daily_partitions,
    group_name="publish",
    description="Publish: saves bt_lds_lead_spots_bck (legacy) as CSV to S3.",
)
def bt_lds_lead_spots_to_s3(
    context: dg.AssetExecutionContext,
    bt_lds_lead_spots_bck: pl.DataFrame,
) -> str:
    """Writes DataFrame to S3 and returns the S3 key."""
    def body() -> str:
        partition_key = context.partition_key  # e.g. "2025-12-17"

        # Builds: data_lakehouse/gold/bt_lds_lead_spots_bck/year=.../data.csv
        s3_key = build_gold_s3_key("bt_lds_lead_spots_bck", partition_key, file_format=FILE_FORMAT)

        write_polars_to_s3(bt_lds_lead_spots_bck, s3_key, context, file_format=FILE_FORMAT)

        # Add metadata only when running in Dagster runtime (not direct invocation)
        try:
            context.add_output_metadata({
                "s3_key": s3_key,
                "rows": bt_lds_lead_spots_bck.height,
                "format": FILE_FORMAT,
                "partition": partition_key,
            })
        except Exception:
            context.log.info(f"Metadata: s3_key={s3_key}, rows={bt_lds_lead_spots_bck.height}")

        return s3_key

    yield from iter_job_wrapped_compute(context, body)


@dg.asset(
    partitions_def=daily_partitions,
    group_name="publish",
    description="Publish: loads bt_lds_lead_spots_bck (legacy) from S3 into PostgreSQL table bt_lds_lead_spots_bck via Geospot API.",
    deps=["bt_lds_lead_spots_to_s3"],  # Dependency without loading the value
    op_tags={"geospot_api": "write"},
)
def bt_lds_lead_spots_to_geospot(
    context: dg.AssetExecutionContext,
    # No longer receives bt_lds_lead_spots_to_s3 as parameter
) -> str:
    """Sends HTTP request to load S3 data into PostgreSQL (table bt_lds_lead_spots_bck)."""
    def body() -> str:
        partition_key = context.partition_key

        # Reconstruct S3 key (deterministic based on table + partition)
        s3_key = build_gold_s3_key("bt_lds_lead_spots_bck", partition_key, file_format=FILE_FORMAT)

        result = load_to_geospot(
            s3_key=s3_key,
            table_name="bt_lds_lead_spots_bck",  # Legacy table (renamed from bt_lds_lead_spots)
            mode="replace",                       # or "append"
            context=context,
        )

        # Add metadata only when running in Dagster runtime (not direct invocation)
        try:
            context.add_output_metadata({
                "table_name": "bt_lds_lead_spots_bck",
                "s3_key": s3_key,
                "mode": "replace",
                "partition": partition_key,
            })
        except Exception:
            context.log.info(f"Metadata: table=bt_lds_lead_spots_bck, s3_key={s3_key}")

        return result

    yield from iter_job_wrapped_compute(context, body)
