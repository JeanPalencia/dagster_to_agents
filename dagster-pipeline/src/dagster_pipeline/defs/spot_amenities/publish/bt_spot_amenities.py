"""
Publish layer: Save gold_bt_spot_amenities to S3 and load into GeoSpot.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_amenities.shared import (
    write_polars_to_s3,
    load_to_geospot,
)


FILE_FORMAT = "csv"
S3_KEY = f"spot_amenities/gold/bt_spot_amenities/data.{FILE_FORMAT}"


@dg.asset(
    group_name="spa_publish",
    description="Publish: saves gold_bt_spot_amenities as CSV to S3.",
)
def bt_spot_amenities_to_s3(
    context: dg.AssetExecutionContext,
    gold_bt_spot_amenities: pl.DataFrame,
) -> str:
    """Writes DataFrame to S3 and returns the S3 key."""
    write_polars_to_s3(gold_bt_spot_amenities, S3_KEY, context, file_format=FILE_FORMAT)

    try:
        context.add_output_metadata({
            "s3_key": S3_KEY,
            "rows": gold_bt_spot_amenities.height,
            "columns": gold_bt_spot_amenities.width,
            "format": FILE_FORMAT,
        })
    except Exception:
        context.log.info(f"Metadata: s3_key={S3_KEY}, rows={gold_bt_spot_amenities.height}")

    return S3_KEY


@dg.asset(
    group_name="spa_publish",
    description="Publish: loads bt_spot_amenities from S3 into GeoSpot PostgreSQL via API.",
    deps=["bt_spot_amenities_to_s3"],
    op_tags={"geospot_api": "write"},
)
def bt_spot_amenities_to_geospot(
    context: dg.AssetExecutionContext,
) -> str:
    """Sends HTTP request to load S3 data into PostgreSQL."""
    result = load_to_geospot(
        s3_key=S3_KEY,
        table_name="bt_spot_amenities",
        mode="replace",
        context=context,
    )

    try:
        context.add_output_metadata({
            "table_name": "bt_spot_amenities",
            "s3_key": S3_KEY,
            "mode": "replace",
        })
    except Exception:
        context.log.info(f"Metadata: table=bt_spot_amenities, s3_key={S3_KEY}")

    return result
