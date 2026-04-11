# defs/effective_supply/publish/lk_effective_supply.py
"""
Publish layer: Load gold_lk_effective_supply to external destinations.

This layer handles the "L" (Load) of ELT:
- lk_effective_supply_to_s3: Writes to S3 as CSV
- lk_effective_supply_to_geospot: Loads to PostgreSQL via GeoSpot API
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.effective_supply.shared import (
    write_polars_to_s3,
    load_to_geospot,
)


# File format for GeoSpot compatibility
FILE_FORMAT = "csv"  # GeoSpot currently only supports CSV


@dg.asset(
    group_name="effective_supply_publish",
    description="Publish: saves gold_lk_effective_supply as CSV to S3.",
)
def lk_effective_supply_to_s3(
    context: dg.AssetExecutionContext,
    gold_lk_effective_supply: pl.DataFrame,
) -> str:
    """Writes DataFrame to S3 and returns the S3 key."""
    s3_key = f"effective_supply/gold/lk_effective_supply/data.{FILE_FORMAT}"
    
    write_polars_to_s3(
        gold_lk_effective_supply,
        s3_key,
        context,
        file_format=FILE_FORMAT,
    )
    
    # Add metadata
    try:
        context.add_output_metadata({
            "s3_key": s3_key,
            "rows": gold_lk_effective_supply.height,
            "columns": gold_lk_effective_supply.width,
            "format": FILE_FORMAT,
        })
    except Exception:
        context.log.info(
            f"Metadata: s3_key={s3_key}, rows={gold_lk_effective_supply.height}"
        )
    
    return s3_key


@dg.asset(
    group_name="effective_supply_publish",
    description="Publish: loads lk_effective_supply from S3 into PostgreSQL via GeoSpot API.",
    deps=["lk_effective_supply_to_s3"],
    op_tags={"geospot_api": "write"},
)
def lk_effective_supply_to_geospot(
    context: dg.AssetExecutionContext,
) -> str:
    """Sends HTTP request to load S3 data into PostgreSQL."""
    s3_key = f"effective_supply/gold/lk_effective_supply/data.{FILE_FORMAT}"
    
    result = load_to_geospot(
        s3_key=s3_key,
        table_name="lk_effective_supply",
        mode="append",
        context=context,
    )
    
    # Add metadata
    try:
        context.add_output_metadata({
            "table_name": "lk_effective_supply",
            "s3_key": s3_key,
            "mode": "replace",
        })
    except Exception:
        context.log.info(f"Metadata: table=lk_effective_supply, s3_key={s3_key}")
    
    return result
