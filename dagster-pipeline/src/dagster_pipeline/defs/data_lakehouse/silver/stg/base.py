# defs/data_lakehouse/silver/stg/base.py
"""
Silver STG base: single public API.

make_silver_stg_asset(bronze_asset_name, transform_fn=None, ...): builds a Silver STG asset that
reads bronze from S3, runs transform_fn (or identity if None), validates, and writes to S3.
Convention: raw_* -> stg_* for default silver_asset_name.
"""
from typing import Callable, Dict, List, Optional

import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_silver_s3_key,
    write_polars_to_s3,
    read_bronze_from_s3,
    resolve_stale_reference_date,
)
from dagster_pipeline.defs.data_lakehouse.notifications import send_stale_data_notification
from dagster_pipeline.defs.data_lakehouse.silver.utils import (
    validate_and_clean_silver,
    validate_input_bronze,
    validate_output_silver,
)


def _default_silver_asset_name(bronze_asset_name: str) -> str:
    """raw_* -> stg_*"""
    if bronze_asset_name.startswith("raw_"):
        return "stg_" + bronze_asset_name[4:]
    return bronze_asset_name


def _run_silver_stg(
    context: dg.AssetExecutionContext,
    bronze_asset_name: str,
    silver_asset_name: str,
    transform_fn: Callable[[pl.DataFrame], pl.DataFrame],
    allow_row_loss: bool,
) -> pl.DataFrame:
    """Common flow: read bronze from S3, validate, transform, validate, clean, write, notify."""
    df_raw, bronze_metadata = read_bronze_from_s3(bronze_asset_name, context)

    stale_tables = []
    ref_date = resolve_stale_reference_date(context)
    if bronze_metadata["is_stale"]:
        stale_tables.append({
            "table_name": bronze_asset_name,
            "expected_date": ref_date,
            "available_date": bronze_metadata["file_date"],
            "layer": "bronze",
            "file_path": bronze_metadata["file_path"],
        })

    input_validation = validate_input_bronze(df_raw, context)
    if input_validation["is_empty"]:
        context.log.warning("⚠️  Empty input - returning empty DataFrame")
        return df_raw

    df_out = transform_fn(df_raw)
    context.log.info(
        f"📥 {silver_asset_name}: {df_out.height:,} rows, {df_out.width} columns (ref_date={ref_date})"
    )

    output_validation = validate_output_silver(
        df_out, df_raw, context, allow_row_loss=allow_row_loss
    )
    if not output_validation["validation_passed"]:
        context.log.error(
            f"❌ Validation failed: {output_validation['rows_lost']:,} rows lost. "
            f"Check transformations."
        )

    df_out = validate_and_clean_silver(df_out, context)

    s3_key = build_silver_s3_key(silver_asset_name, file_format="parquet")
    write_polars_to_s3(df_out, s3_key, context, file_format="parquet")

    if stale_tables:
        send_stale_data_notification(
            context,
            stale_tables,
            current_asset_name=silver_asset_name,
            current_layer="silver",
        )

    return df_out


def make_silver_stg_asset(
    bronze_asset_name: str,
    transform_fn: Optional[Callable[[pl.DataFrame], pl.DataFrame]] = None,
    *,
    silver_asset_name: Optional[str] = None,
    description: str = "",
    allow_row_loss: bool = False,
    deps: Optional[List[str]] = None,
    partitioned: bool = True,
    tags: Optional[Dict[str, str]] = None,
) -> dg.AssetsDefinition:
    """
    Build a Silver STG asset: read bronze from S3, transform, validate, write to S3.

    Args:
        bronze_asset_name: Name of the bronze asset (e.g. "raw_s2p_clients_new").
        transform_fn: Function (df: pl.DataFrame) -> pl.DataFrame. If None, uses identity (passthrough).
        silver_asset_name: Default: "stg_" + bronze_asset_name[4:] (raw_* -> stg_*).
        description: Asset description for Dagster UI.
        allow_row_loss: Passed to validate_output_silver (default False).
        deps: Optional list of asset names to run before this asset (e.g. ["cleanup_storage"]).
        partitioned: If False, asset has no daily_partitions (bt_conv shared S2P/chatbot chain).
        tags: Optional Dagster asset tags (e.g. lk_spots multiprocess concurrency pools).
    """
    if transform_fn is None:
        transform_fn = lambda df: df
    name = silver_asset_name if silver_asset_name is not None else _default_silver_asset_name(bronze_asset_name)
    desc = description or f"Silver STG: transformation from {bronze_asset_name}."

    def _asset(context):
        from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute

        def body():
            return _run_silver_stg(
                context,
                bronze_asset_name=bronze_asset_name,
                silver_asset_name=name,
                transform_fn=transform_fn,
                allow_row_loss=allow_row_loss,
            )

        yield from iter_job_wrapped_compute(context, body)

    _asset.__name__ = name
    asset_kwargs: dict = {
        "name": name,
        "group_name": "silver",
        "retry_policy": dg.RetryPolicy(max_retries=2),
        "description": desc,
        "io_manager_key": "s3_silver",
    }
    if partitioned:
        asset_kwargs["partitions_def"] = daily_partitions
    if deps:
        asset_kwargs["deps"] = deps
    if tags:
        asset_kwargs["tags"] = tags
    return dg.asset(**asset_kwargs)(_asset)
