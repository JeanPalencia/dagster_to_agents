"""
Publish layer: Save gold_rpt_gsc_spots_performance to S3 and load into GeoSpot.

Uses Parquet format. Column types are cast to match the PostgreSQL DDL
exactly -- the GeoSpot API requires strict type alignment between the
Parquet schema and the target table.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.gsc_spots_performance.shared import (
    write_polars_to_s3,
    load_to_geospot,
)


FILE_FORMAT = "parquet"
S3_KEY = f"gsc_spots_performance/gold/rpt_gsc_spots_performance/data.{FILE_FORMAT}"

_PG_TYPE_OVERRIDES: dict[str, pl.DataType] = {
    "spot_id":     pl.Int32,
    "impressions": pl.Int32,
    "clicks":      pl.Int32,
    "month":       pl.Int32,
    "year":        pl.Int32,
}


@dg.asset(
    group_name="gsp_publish",
    description="Publish: saves gold_rpt_gsc_spots_performance as Parquet to S3.",
)
def rpt_gsc_spots_performance_to_s3(
    context: dg.AssetExecutionContext,
    gold_rpt_gsc_spots_performance: pl.DataFrame,
) -> str:
    """Writes DataFrame to S3 as Parquet with types matching the PostgreSQL DDL."""
    cast_exprs = [
        pl.col(col).cast(dtype)
        for col, dtype in _PG_TYPE_OVERRIDES.items()
        if col in gold_rpt_gsc_spots_performance.columns
    ]
    df_out = gold_rpt_gsc_spots_performance.with_columns(cast_exprs) if cast_exprs else gold_rpt_gsc_spots_performance

    write_polars_to_s3(df_out, S3_KEY, context, file_format=FILE_FORMAT)

    try:
        context.add_output_metadata({
            "s3_key": S3_KEY,
            "rows": df_out.height,
            "columns": df_out.width,
            "format": FILE_FORMAT,
        })
    except Exception:
        context.log.info(f"Metadata: s3_key={S3_KEY}, rows={df_out.height}")

    return S3_KEY


@dg.asset(
    group_name="gsp_publish",
    description="Publish: loads rpt_gsc_spots_performance from S3 into GeoSpot PostgreSQL via API.",
    deps=["rpt_gsc_spots_performance_to_s3"],
    op_tags={"geospot_api": "write"},
)
def rpt_gsc_spots_performance_to_geospot(
    context: dg.AssetExecutionContext,
) -> str:
    """Sends HTTP request to load S3 data into PostgreSQL."""
    result = load_to_geospot(
        s3_key=S3_KEY,
        table_name="rpt_gsc_spots_performance",
        mode="replace",
        context=context,
    )

    try:
        context.add_output_metadata({
            "table_name": "rpt_gsc_spots_performance",
            "s3_key": S3_KEY,
            "mode": "replace",
        })
    except Exception:
        context.log.info(f"Metadata: table=rpt_gsc_spots_performance, s3_key={S3_KEY}")

    return result
