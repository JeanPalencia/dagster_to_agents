# defs/data_lakehouse/silver/stg/stg_gs_recommendation_chatbot_exploded_new.py
"""
Silver STG: Explodes recommendation_chatbot JSON arrays into individual rows.

Input: raw_gs_recommendation_chatbot_new (id, spots_suggested, white_list, black_list, ...)
Output: One row per (id, spot_id, list_type) with updated_at.

The 'id' here is the recommendation_chatbot record ID linked to a conversation.
In Gold, this is joined to leads via client_id (from conversation_events).
"""
import json
import polars as pl
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset


def _parse_json_array(val) -> list:
    if val is None:
        return []
    if isinstance(val, str):
        val = val.strip()
        if not val or val in ("[]", "null", "None"):
            return []
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                return [int(x) for x in parsed if x is not None]
        except (json.JSONDecodeError, ValueError):
            pass
        return []
    if isinstance(val, list):
        return [int(x) for x in val if x is not None]
    return []


def _transform_recommendation_chatbot_exploded(df: pl.DataFrame) -> pl.DataFrame:
    json_cols = ["white_list", "spots_suggested", "black_list"]
    available_json = [c for c in json_cols if c in df.columns]

    id_col = "id" if "id" in df.columns else None
    if id_col is None or not available_json:
        return pl.DataFrame(schema={
            "recommendation_chatbot_id": pl.Int64,
            "spot_id": pl.Int64,
            "list_type": pl.Utf8,
            "updated_at": pl.Datetime,
        })

    rows = []
    for row in df.iter_rows(named=True):
        rec_id = row.get("id")
        upd = row.get("updated_at")
        if rec_id is None:
            continue
        for col_name in available_json:
            ids = _parse_json_array(row.get(col_name))
            for sid in ids:
                rows.append({
                    "recommendation_chatbot_id": int(rec_id),
                    "spot_id": int(sid),
                    "list_type": col_name,
                    "updated_at": upd,
                })

    if not rows:
        return pl.DataFrame(schema={
            "recommendation_chatbot_id": pl.Int64,
            "spot_id": pl.Int64,
            "list_type": pl.Utf8,
            "updated_at": pl.Datetime,
        })

    return pl.DataFrame(rows).cast({
        "recommendation_chatbot_id": pl.Int64,
        "spot_id": pl.Int64,
    })


stg_gs_recommendation_chatbot_exploded_new = make_silver_stg_asset(
    "raw_gs_recommendation_chatbot_new",
    _transform_recommendation_chatbot_exploded,
    silver_asset_name="stg_gs_recommendation_chatbot_exploded_new",
    description=(
        "Silver STG: Explodes recommendation_chatbot JSON arrays "
        "(white_list, spots_suggested, black_list) into individual "
        "(recommendation_chatbot_id, spot_id, list_type) rows."
    ),
    allow_row_loss=True,
)
