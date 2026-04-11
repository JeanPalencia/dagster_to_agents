"""Assets for the LK Visitors pipeline."""

from dagster import asset, AssetExecutionContext

from dagster_pipeline.defs.lk_visitors.config import LKVisitorsConfig
from dagster_pipeline.defs.lk_visitors.main import (
    get_data,
    prepare_funnel_for_output,
)
from dagster_pipeline.defs.lk_visitors.utils.s3_upload import (
    S3_KEY_PARQUET,
    save_to_local,
    upload_dataframe_to_s3,
    trigger_table_replacement,
)


@asset
def lk_funnel_with_channel():
    """Fetch funnel_with_channel from BigQuery."""
    return get_data("funnel_with_channel")


@asset
def lk_output(context: AssetExecutionContext, config: LKVisitorsConfig, lk_funnel_with_channel):
    """Format output, save locally and/or upload to S3."""
    result = prepare_funnel_for_output(lk_funnel_with_channel)
    context.log.info(f"Formatted output: {len(result)} rows")

    # Save locally for testing
    if config.local_output:
        filepath = save_to_local(result, config.output_dir)
        context.log.info(f"Saved to local: {filepath}")

    # Upload to S3
    s3_key = S3_KEY_PARQUET
    if config.upload_to_s3:
        context.log.info(f"Uploading to S3 as {config.output_format}...")
        s3_uri, s3_key = upload_dataframe_to_s3(result, output_format=config.output_format)
        context.log.info(f"Uploaded to {s3_uri}")

    # Trigger table replacement (disabled by default)
    if config.trigger_table_replacement:
        context.log.info(f"Triggering table load (mode={config.table_load_mode})...")
        api_result = trigger_table_replacement(
            s3_key=s3_key,
            mode=config.table_load_mode,
        )
        context.log.info(f"Table load result: {api_result}")

    return result
