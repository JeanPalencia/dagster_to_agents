# defs/data_lakehouse/silver/stg/stg_cb_conversation_events_new.py
"""
Silver STG: Normalized conversation_events (spot_confirmation only).

Input: raw_cb_conversation_events_new (conversation_id, client_id, spot_id, event_data, created_at)
Output: Cleaned DataFrame with:
  - Backfill records filtered out (spot_id IS NOT NULL AND client_id IS NOT NULL)
  - client_id renamed to lead_id (alignment with Gold layer convention)
  - Deduplicated by (lead_id, spot_id) keeping the oldest created_at
"""
import polars as pl
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset


def _transform_cb_conversation_events(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame(schema={
            "lead_id": pl.Int64,
            "spot_id": pl.Int64,
            "conversation_id": pl.Int64,
            "created_at": pl.Datetime,
        })

    df_out = df.filter(
        pl.col("spot_id").is_not_null() & pl.col("client_id").is_not_null()
    )

    if "client_id" in df_out.columns:
        df_out = df_out.rename({"client_id": "lead_id"})

    for c in ("lead_id", "spot_id"):
        if c in df_out.columns:
            df_out = df_out.with_columns(pl.col(c).cast(pl.Int64))

    if "created_at" in df_out.columns:
        if df_out["created_at"].dtype == pl.Utf8:
            df_out = df_out.with_columns(
                pl.col("created_at").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f", strict=False)
            )

    df_out = (
        df_out
        .sort("created_at", nulls_last=True)
        .group_by(["lead_id", "spot_id"])
        .first()
    )

    cols_to_keep = ["lead_id", "spot_id", "conversation_id", "created_at"]
    available = [c for c in cols_to_keep if c in df_out.columns]
    return df_out.select(available)


stg_cb_conversation_events_new = make_silver_stg_asset(
    "raw_cb_conversation_events_new",
    _transform_cb_conversation_events,
    silver_asset_name="stg_cb_conversation_events_new",
    description=(
        "Silver STG: conversation_events (spot_confirmation) normalized. "
        "Filters backfill records, renames client_id->lead_id, "
        "deduplicates by (lead_id, spot_id) keeping oldest created_at."
    ),
    allow_row_loss=True,
)
