# defs/effective_supply/publish/lk_effective_supply_propensity_scores.py
"""
Publish layer: Load gold_lk_effective_supply_propensity_scores to S3 and PostgreSQL.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.effective_supply.shared import (
    write_polars_to_s3,
    load_to_geospot,
)

FILE_FORMAT = "csv"


@dg.asset(
    group_name="effective_supply_publish",
    description="Publish: saves propensity scores as CSV to S3.",
)
def lk_effective_supply_propensity_scores_to_s3(
    context: dg.AssetExecutionContext,
    gold_lk_effective_supply_propensity_scores: pl.DataFrame,
) -> str:
    """Writes propensity scores DataFrame to S3 and returns the S3 key."""
    s3_key = f"effective_supply/gold/lk_effective_supply_propensity_scores/data.{FILE_FORMAT}"

    write_polars_to_s3(
        gold_lk_effective_supply_propensity_scores,
        s3_key,
        context,
        file_format=FILE_FORMAT,
    )

    try:
        context.add_output_metadata({
            "s3_key": s3_key,
            "rows": gold_lk_effective_supply_propensity_scores.height,
            "columns": gold_lk_effective_supply_propensity_scores.width,
            "format": FILE_FORMAT,
        })
    except Exception:
        context.log.info(
            f"Metadata: s3_key={s3_key}, "
            f"rows={gold_lk_effective_supply_propensity_scores.height}"
        )

    return s3_key


@dg.asset(
    group_name="effective_supply_publish",
    description="Publish: loads propensity scores from S3 into PostgreSQL.",
    deps=["lk_effective_supply_propensity_scores_to_s3"],
    op_tags={"geospot_api": "write"},
)
def lk_effective_supply_propensity_scores_to_geospot(
    context: dg.AssetExecutionContext,
) -> str:
    """Sends HTTP request to load S3 data into PostgreSQL."""
    s3_key = f"effective_supply/gold/lk_effective_supply_propensity_scores/data.{FILE_FORMAT}"

    result = load_to_geospot(
        s3_key=s3_key,
        table_name="lk_effective_supply_propensity_scores",
        mode="append",
        context=context,
    )

    try:
        context.add_output_metadata({
            "table_name": "lk_effective_supply_propensity_scores",
            "s3_key": s3_key,
            "mode": "replace",
        })
    except Exception:
        context.log.info(
            f"Metadata: table=lk_effective_supply_propensity_scores, s3_key={s3_key}"
        )

    return result
