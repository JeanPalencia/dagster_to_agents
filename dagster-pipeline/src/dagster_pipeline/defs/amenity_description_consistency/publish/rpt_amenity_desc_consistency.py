"""
Publish layer: Save gold_amenity_desc_consistency to S3 and load into GeoSpot.

- rpt_amenity_desc_consistency_to_s3: Writes CSV to S3
- rpt_amenity_desc_consistency_to_geospot: Loads to PostgreSQL via GeoSpot API (replace mode)
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.amenity_description_consistency.shared import (
    write_polars_to_s3,
    load_to_geospot,
)


FILE_FORMAT = "csv"
TABLE_NAME = "dagster_agent_rpt_amenity_description_consistency"
S3_KEY = f"dagster_agent_amenity_description_consistency/gold/{TABLE_NAME}/data.{FILE_FORMAT}"


@dg.asset(
    group_name="adc_publish",
    description="Publish: saves gold_amenity_desc_consistency as CSV to S3.",
)
def rpt_amenity_desc_consistency_to_s3(
    context: dg.AssetExecutionContext,
    gold_amenity_desc_consistency: pl.DataFrame,
) -> str:
    """Writes DataFrame to S3 and returns the S3 key."""
    # Format adc_mention_rate to always show exactly 3 decimals in CSV
    df_formatted = gold_amenity_desc_consistency.with_columns(
        pl.col("adc_mention_rate").map_elements(
            lambda x: f"{x:.3f}",
            return_dtype=pl.String
        )
    )

    write_polars_to_s3(
        df_formatted, S3_KEY, context, file_format=FILE_FORMAT,
    )

    try:
        context.add_output_metadata({
            "s3_key": S3_KEY,
            "rows": df_formatted.height,
            "columns": df_formatted.width,
            "format": FILE_FORMAT,
        })
    except Exception:
        context.log.info(
            f"Metadata: s3_key={S3_KEY}, rows={df_formatted.height}"
        )

    return S3_KEY


@dg.asset(
    group_name="adc_publish",
    description=(
        "Publish: loads rpt_amenity_description_consistency from S3 "
        "into PostgreSQL via GeoSpot API."
    ),
    deps=["rpt_amenity_desc_consistency_to_s3"],
    op_tags={"geospot_api": "write"},
)
def rpt_amenity_desc_consistency_to_geospot(
    context: dg.AssetExecutionContext,
) -> str:
    """Sends HTTP request to load S3 data into PostgreSQL."""
    result = load_to_geospot(
        s3_key=S3_KEY,
        table_name=TABLE_NAME,
        mode="replace",
        context=context,
    )

    try:
        context.add_output_metadata({
            "table_name": TABLE_NAME,
            "s3_key": S3_KEY,
            "mode": "replace",
        })
    except Exception:
        context.log.info(
            f"Metadata: table={TABLE_NAME}, s3_key={S3_KEY}"
        )

    return result
