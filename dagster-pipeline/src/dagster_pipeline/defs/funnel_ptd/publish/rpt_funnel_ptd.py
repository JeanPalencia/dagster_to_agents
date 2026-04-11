# defs/funnel_ptd/publish/rpt_funnel_ptd.py
"""
Publish layer: Load gold_fptd_period_to_date to external destinations.

This layer handles the "L" (Load) of ELT:
- rpt_funnel_ptd_to_s3: Writes to S3 as CSV
- rpt_funnel_ptd_to_geospot: Loads to PostgreSQL via GeoSpot API (replace mode)
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.funnel_ptd.shared import (
    write_polars_to_s3,
    load_to_geospot,
)


FILE_FORMAT = "csv"


@dg.asset(
    group_name="funnel_ptd_publish",
    description="Publish: saves gold_fptd_period_to_date as CSV to S3.",
)
def rpt_funnel_ptd_to_s3(
    context: dg.AssetExecutionContext,
    gold_fptd_period_to_date: pl.DataFrame,
) -> str:
    """Writes DataFrame to S3 and returns the S3 key."""
    s3_key = f"funnel_ptd/gold/rpt_funnel_ptd/data.{FILE_FORMAT}"

    write_polars_to_s3(
        gold_fptd_period_to_date,
        s3_key,
        context,
        file_format=FILE_FORMAT,
    )

    try:
        context.add_output_metadata({
            "s3_key": s3_key,
            "rows": gold_fptd_period_to_date.height,
            "columns": gold_fptd_period_to_date.width,
            "format": FILE_FORMAT,
        })
    except Exception:
        context.log.info(
            f"Metadata: s3_key={s3_key}, rows={gold_fptd_period_to_date.height}"
        )

    return s3_key


@dg.asset(
    group_name="funnel_ptd_publish",
    description="Publish: loads rpt_funnel_ptd from S3 into PostgreSQL via GeoSpot API.",
    deps=["rpt_funnel_ptd_to_s3"],
    op_tags={"geospot_api": "write"},
)
def rpt_funnel_ptd_to_geospot(
    context: dg.AssetExecutionContext,
) -> str:
    """Sends HTTP request to load S3 data into PostgreSQL."""
    s3_key = f"funnel_ptd/gold/rpt_funnel_ptd/data.{FILE_FORMAT}"

    result = load_to_geospot(
        s3_key=s3_key,
        table_name="rpt_funnel_ptd",
        mode="replace",
        context=context,
    )

    try:
        context.add_output_metadata({
            "table_name": "rpt_funnel_ptd",
            "s3_key": s3_key,
            "mode": "replace",
        })
    except Exception:
        context.log.info(f"Metadata: table=rpt_funnel_ptd, s3_key={s3_key}")

    return result
