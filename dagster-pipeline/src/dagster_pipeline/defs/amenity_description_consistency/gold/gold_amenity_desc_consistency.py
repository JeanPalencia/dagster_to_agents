"""
Gold layer: amenity_desc_consistency.

Drops source-derived columns (spot_type, spot_status_full, spot_description)
that are available via JOIN on lk_spots, keeping only spot_id + adc_* fields.
Then adds audit fields for tracking inserts/updates.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields

_PUBLISH_COLUMNS = [
    "spot_id",
    "adc_tagged_amenities",
    "adc_mentioned_amenities",
    "adc_omitted_amenities",
    "adc_total_tagged",
    "adc_total_mentioned",
    "adc_total_omitted",
    "adc_mention_rate",
    "adc_category_id",
    "adc_category",
]


@dg.asset(
    group_name="adc_gold",
    description="Gold: amenity-description consistency with audit fields.",
)
def gold_amenity_desc_consistency(
    context: dg.AssetExecutionContext,
    core_amenity_desc_consistency: pl.DataFrame,
) -> pl.DataFrame:
    df = core_amenity_desc_consistency.select(_PUBLISH_COLUMNS)
    df = add_audit_fields(df, job_name="amenity_desc_consistency")
    context.log.info(
        f"gold_amenity_desc_consistency: {df.height:,} rows, {df.width} columns"
    )
    return df
