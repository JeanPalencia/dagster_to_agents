# defs/data_lakehouse/gold/conv_variables_flow.py
"""
conv_variables flow: base metrics + experiment variables + funnel_events + seniority.

AI tags (OpenAI) and DSPy evaluator have been removed.
Reused implementation is imported only via `gold.bt_conv_variables.vendor`.
"""
from __future__ import annotations

import json
import time
from typing import Any, Mapping, Optional

import pandas as pd

from dagster_pipeline.defs.data_lakehouse.gold.bt_conv_variables.config import ConvVariablesConfig
from dagster_pipeline.defs.data_lakehouse.gold.bt_conv_variables.vendor import (
    create_conv_variables,
    get_bot_graph_variant,
    get_experiment_variables,
    get_follow_up_success,
    get_funnel_events_variables,
    get_seniority_variable,
    get_unification_mgs_variant,
    _get_events_in_conversation_window,
)


def run_conv_variables_flow(
    df: pd.DataFrame,
    df_events: pd.DataFrame,
    config: ConvVariablesConfig,
    context,
    *,
    ai_window_start_dt=None,
    ai_window_end_dt=None,
    df_geospot: pd.DataFrame | None = None,
    seniority_by_conversation_id: Optional[Mapping[Any, Any]] = None,
) -> pd.DataFrame:
    """
    Build conv_variables: base metrics + tags + experiment variables + funnel_events + seniority.

    df must already have _to_process. If df_geospot is passed (output of _compute_incremental_mask),
    non-_to_process rows are initialized from df_geospot. Otherwise, _initial_conv_variables
    column is used if present.
    """
    context.log.info("📊 Creating conversation variables...")
    start_time = time.time()

    df = df.copy()
    df_events = df_events.copy()
    df_events["created_at_local"] = pd.to_datetime(df_events["created_at_local"], errors="coerce")
    events_by_conversation = df_events.groupby("conversation_id")

    # Legacy compatibility: create_conv_variables and _get_events use msg['id'] as conversation_id
    def _norm_messages_for_events(msgs):
        if isinstance(msgs, str):
            try:
                msgs = json.loads(msgs)
            except Exception:
                return msgs
        if not isinstance(msgs, list):
            return msgs
        return [dict(m, id=m.get("conversation_id") or m.get("id")) for m in msgs if isinstance(m, dict)]

    df["_messages_for_events"] = df["messages"].apply(_norm_messages_for_events)

    def _row_with_msg_id(row):
        r = row.copy()
        r["messages"] = r["_messages_for_events"]
        return r

    # Initialize conv_variables: unchanged rows from df_geospot or _initial_conv_variables
    df["conv_variables"] = None
    if df_geospot is not None and not df_geospot.empty and "_to_process" in df.columns:
        geo = df_geospot.copy()
        geo["_cid_key"] = geo["conv_id"].astype(str).str.strip()
        geo_by_cid = geo.drop_duplicates("_cid_key", keep="first").set_index("_cid_key")
        for idx in df.index:
            if not df.at[idx, "_to_process"]:
                cid = df.at[idx, "conversation_id"]
                cid_key = str(cid).strip() if cid is not None else ""
                if cid_key and cid_key in geo_by_cid.index:
                    raw = geo_by_cid.at[cid_key, "conv_variables"]
                    try:
                        lst = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, list) else [])
                    except (TypeError, json.JSONDecodeError):
                        lst = []
                    df.at[idx, "conv_variables"] = lst if isinstance(lst, list) else []
    elif "_to_process" in df.columns and "_initial_conv_variables" in df.columns:
        for idx in df.index:
            if not df.at[idx, "_to_process"]:
                raw = df.at[idx, "_initial_conv_variables"]
                try:
                    lst = json.loads(raw) if isinstance(raw, str) and (raw or "").strip() else (raw if isinstance(raw, list) else [])
                except (TypeError, json.JSONDecodeError):
                    lst = []
                df.at[idx, "conv_variables"] = lst if isinstance(lst, list) else []

    # STEP 1: Base variables (metrics + tags) for rows to process
    context.log.info("📝 Creating base variables (metrics + tags)...")
    for idx in df.index:
        if "_to_process" in df.columns and not df.at[idx, "_to_process"]:
            continue
        df.at[idx, "conv_variables"] = create_conv_variables(
            _row_with_msg_id(df.loc[idx]), enable_ai_tags=False
        )
    context.log.info(f"✅ Base variables created in {time.time() - start_time:.2f}s")

    # STEP 1.5: Experiment variables + funnel_events for rows to process
    context.log.info("🧪 Adding experiment variables and funnel_events...")
    exp_count = 0
    for idx in df.index:
        if "_to_process" in df.columns and not df.at[idx, "_to_process"]:
            continue
        row = df.loc[idx]
        events_in_window = _get_events_in_conversation_window(
            _row_with_msg_id(row),
            events_by_conversation,
            row.get("conversation_start"),
            row.get("conversation_end"),
        )
        exp_vars = get_experiment_variables(
            events_in_window,
            row.get("conversation_start"),
            row.get("conversation_end"),
        )
        if exp_vars:
            df.at[idx, "conv_variables"] = df.at[idx, "conv_variables"] + exp_vars
            exp_count += 1
        follow_up_var = get_follow_up_success(row, events_in_window)
        if follow_up_var is not None:
            df.at[idx, "conv_variables"] = df.at[idx, "conv_variables"] + [follow_up_var]
        unification_var = get_unification_mgs_variant(events_in_window)
        if unification_var is not None:
            df.at[idx, "conv_variables"] = df.at[idx, "conv_variables"] + [unification_var]
        bot_graph_var = get_bot_graph_variant(events_in_window)
        if bot_graph_var is not None:
            df.at[idx, "conv_variables"] = df.at[idx, "conv_variables"] + [bot_graph_var]
        funnel_events_vars = get_funnel_events_variables(events_in_window, row)
        if funnel_events_vars:
            df.at[idx, "conv_variables"] = df.at[idx, "conv_variables"] + funnel_events_vars
    context.log.info(f"✅ Experiment variables and funnel_events added for {exp_count} conversations")

    # STEP 1.6: Seniority level
    if seniority_by_conversation_id:
        seg_count = 0
        for idx in df.index:
            vars_list = df.at[idx, "conv_variables"]
            if not isinstance(vars_list, list):
                continue
            cid = df.at[idx, "conversation_id"]
            level = seniority_by_conversation_id.get(cid)
            var = get_seniority_variable(level)
            if var is not None:
                df.at[idx, "conv_variables"] = vars_list + [var]
                seg_count += 1
        context.log.info(f"✅ Segment seniority_level added for {seg_count} conversations")
    else:
        context.log.info("🔇 seniority_by_conversation_id not provided - skipping segment.seniority_level")

    # STEP 2: Serialize to JSON
    context.log.info("📦 Serializing conv_variables to JSON...")

    def _ensure_json(val):
        if isinstance(val, list):
            return json.dumps(val, ensure_ascii=False)
        if isinstance(val, str) and (val or "").strip():
            return val
        return "[]"

    df["conv_variables"] = df["conv_variables"].apply(_ensure_json)

    context.log.info(f"✅ conv_variables flow completed in {time.time() - start_time:.2f}s")
    return df
