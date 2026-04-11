# defs/data_lakehouse/silver/stg/stg_s2p_model_has_roles_new.py
"""Silver STG: MAX(role_id) per model_id (matches the mhr subquery in stg_lk_spots.sql)."""
import polars as pl

from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset
from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_SILVER


def _aggregate_max_role_per_model(df: pl.DataFrame) -> pl.DataFrame:
    """SELECT model_id, MAX(role_id) AS max_role FROM model_has_roles GROUP BY model_id."""
    empty = pl.DataFrame(schema={"model_id": pl.Int64, "max_role": pl.Int64})
    if df.is_empty():
        return empty
    rename_map = {}
    for old, new in (
        ("Model_id", "model_id"),
        ("modelId", "model_id"),
        ("ROLE_ID", "role_id"),
        ("Role_id", "role_id"),
    ):
        if old in df.columns and new not in df.columns:
            rename_map[old] = new
    if rename_map:
        df = df.rename(rename_map)
    if "model_id" not in df.columns or "role_id" not in df.columns:
        return empty
    mid = pl.col("model_id").cast(pl.Int64, strict=False)
    rid = pl.col("role_id").cast(pl.Int64, strict=False)
    df2 = df.with_columns([mid.alias("model_id"), rid.alias("role_id")]).drop_nulls("model_id")
    if df2.is_empty():
        return empty
    return df2.group_by("model_id").agg(pl.col("role_id").max().alias("max_role"))


stg_s2p_model_has_roles_new = make_silver_stg_asset(
    "raw_s2p_model_has_roles_new",
    transform_fn=_aggregate_max_role_per_model,
    description=(
        "Silver STG: one row per model_id with max_role = MAX(role_id) from model_has_roles. "
        "Feeds stg_lk_spots_new (user_max_role_id, user_max_role)."
    ),
    allow_row_loss=True,
    deps=["raw_s2p_model_has_roles_new"],
    tags=TAGS_LK_SPOTS_SILVER,
)
