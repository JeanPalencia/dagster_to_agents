"""
Publish layer: Save gold_rpt_browse_visitors to S3 and load into GeoSpot.

Uses Parquet format (not CSV) to avoid GeoSpot file-size limits on large tables.
Column types are cast to match the PostgreSQL DDL exactly — the GeoSpot API
requires strict type alignment between the Parquet schema and the target table.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.browse_visitors.shared import (
    write_polars_to_s3,
    load_to_geospot,
)


FILE_FORMAT = "parquet"
S3_KEY = f"browse_visitors/gold/rpt_browse_visitors/data.{FILE_FORMAT}"

# Polars aggregation types that don't match the PostgreSQL DDL directly.
# UInt32 (from len/n_unique) → Int32 (INTEGER), Int64 (from sum) → Int32.
_PG_TYPE_OVERRIDES: dict[str, pl.DataType] = {
    "total_sessions": pl.Int32,
    "total_page_views": pl.Int32,
    "days_active": pl.Int32,
}


@dg.asset(
    group_name="bv_publish",
    description="Publish: saves gold_rpt_browse_visitors as Parquet to S3.",
)
def rpt_browse_visitors_to_s3(
    context: dg.AssetExecutionContext,
    gold_rpt_browse_visitors: pl.DataFrame,
) -> str:
    """Writes DataFrame to S3 as Parquet with types matching the PostgreSQL DDL."""
    cast_exprs = [
        pl.col(col).cast(dtype)
        for col, dtype in _PG_TYPE_OVERRIDES.items()
        if col in gold_rpt_browse_visitors.columns
    ]
    df_out = gold_rpt_browse_visitors.with_columns(cast_exprs) if cast_exprs else gold_rpt_browse_visitors

    write_polars_to_s3(df_out, S3_KEY, context, file_format=FILE_FORMAT)

    try:
        context.add_output_metadata({
            "s3_key": S3_KEY,
            "rows": gold_rpt_browse_visitors.height,
            "columns": gold_rpt_browse_visitors.width,
            "format": FILE_FORMAT,
        })
    except Exception:
        context.log.info(f"Metadata: s3_key={S3_KEY}, rows={gold_rpt_browse_visitors.height}")

    return S3_KEY


@dg.asset(
    group_name="bv_publish",
    description="Publish: loads rpt_browse_visitors from S3 into GeoSpot PostgreSQL via API.",
    deps=["rpt_browse_visitors_to_s3"],
    op_tags={"geospot_api": "write"},
)
def rpt_browse_visitors_to_geospot(
    context: dg.AssetExecutionContext,
) -> str:
    """Sends HTTP request to load S3 data into PostgreSQL."""
    result = load_to_geospot(
        s3_key=S3_KEY,
        table_name="rpt_browse_visitors",
        mode="replace",
        context=context,
    )

    try:
        context.add_output_metadata({
            "table_name": "rpt_browse_visitors",
            "s3_key": S3_KEY,
            "mode": "replace",
        })
    except Exception:
        context.log.info(f"Metadata: table=rpt_browse_visitors, s3_key={S3_KEY}")

    return result
