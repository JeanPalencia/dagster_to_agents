# defs/spot_state_transitions/publish/lk_spot_status_history.py
"""
Publish layer: Save gold_lk_spot_status_history to S3 as CSV.

GeoSpot publish asset will be added once the upsert endpoint is available.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_state_transitions.shared import write_polars_to_s3


FILE_FORMAT = "csv"


@dg.asset(
    group_name="sst_publish",
    description="Publish: saves gold_lk_spot_status_history as CSV to S3.",
)
def lk_spot_status_history_to_s3(
    context: dg.AssetExecutionContext,
    gold_lk_spot_status_history: pl.DataFrame,
) -> str:
    """Writes DataFrame to S3 and returns the S3 key."""
    s3_key = f"spot_state_transitions/gold/lk_spot_status_history/data.{FILE_FORMAT}"

    write_polars_to_s3(
        gold_lk_spot_status_history,
        s3_key,
        context,
        file_format=FILE_FORMAT,
    )

    try:
        context.add_output_metadata({
            "s3_key": s3_key,
            "rows": gold_lk_spot_status_history.height,
            "columns": gold_lk_spot_status_history.width,
            "format": FILE_FORMAT,
        })
    except Exception:
        context.log.info(
            f"Metadata: s3_key={s3_key}, rows={gold_lk_spot_status_history.height}"
        )

    return s3_key
