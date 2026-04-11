"""
funnel_events: category of conv_variables derived from event sequence and unification_mgs.

Variables: spot_link_to_spot_conf, spot_conf_to_request_visit, visit_request_to_schedule,
reco_index, schedule_form_index, profiling_form_index.
"""

import json
from typing import Any, Dict, List, Optional

import pandas as pd


def _parse_event_data(event_data: Any) -> dict:
    if event_data is None or (isinstance(event_data, float) and pd.isna(event_data)):
        return {}
    if isinstance(event_data, dict):
        return event_data
    if isinstance(event_data, str):
        try:
            return json.loads(event_data)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _get_ordered_event_types(events_in_window: pd.DataFrame) -> List[str]:
    """Return event_type list in chronological order (no consecutive duplicates)."""
    if events_in_window is None or events_in_window.empty:
        return []
    df = events_in_window.copy()
    if "_ts" not in df.columns:
        df["_ts"] = pd.to_datetime(df["created_at_local"], errors="coerce")
    df = df.dropna(subset=["_ts"]).sort_values("_ts").reset_index(drop=True)
    if df.empty:
        return []
    types = df["event_type"].astype(str).str.strip().tolist()
    out = []
    for t in types:
        if t and (not out or out[-1] != t):
            out.append(t)
    return out


def _get_unification_mgs_value(events_in_window: pd.DataFrame) -> Optional[str]:
    """'control' | 'combined' from first spot_link_received event_data.first_interaction_variant; else None."""
    if events_in_window is None or events_in_window.empty:
        return None
    df = events_in_window.copy()
    df["_event_type"] = df["event_type"].astype(str).str.strip()
    match = df[df["_event_type"] == "spot_link_received"]
    if match.empty:
        return None
    first = match.iloc[0]
    data = _parse_event_data(first.get("event_data"))
    v = data.get("first_interaction_variant")
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    v = str(v).strip().lower()
    if v not in ("control", "combined"):
        return None
    return v


def _has_after(seq: List[str], after_type: str, then_type: str) -> bool:
    """True if after_type appears and then then_type appears later in seq."""
    try:
        i = seq.index(after_type)
        return then_type in seq[i + 1:]
    except (ValueError, IndexError):
        return False


def _has_event(seq: List[str], event_type: str) -> bool:
    return event_type in seq


def get_funnel_events_variables(
    events_in_window: pd.DataFrame,
    row: pd.Series,
) -> List[Dict[str, Any]]:
    """
    Build funnel_events variables from event sequence and row (scheduled_visit_count).

    - spot_link_to_spot_conf: 1 if (spot_link_received then spot_confirmation) or unification_mgs=='combined'; 0 if spot_link but no spot_conf and not combined; None if no spot_link and not combined.
    - spot_conf_to_request_visit: 1 if (spot_conf or combined) then explicit_visit_request; 0 if (spot_conf or combined) but no explicit_visit_request; None if no spot_conf and not combined.
    - visit_request_to_schedule: 1 if explicit_visit_request and (visit_scheduled or scheduled_visit>0); 0 if explicit_visit_request but no visit; None if no explicit_visit_request.
    - reco_index: 1 if recommendations_sent then recommendation_scheduling; 0 else; None if no recommendations_sent.
    - schedule_form_index: 1 if scheduling_form_sent then scheduling_form_completed; 0 else; None if no scheduling_form_sent.
    - profiling_form_index: 1 if profiling_form_sent then profiling_form_completed; 0 else; None if no profiling_form_sent.
    """
    seq = _get_ordered_event_types(events_in_window)
    combined = _get_unification_mgs_value(events_in_window) == "combined"
    scheduled_visit_count = row.get("scheduled_visit_count", 0) or 0
    if pd.isna(scheduled_visit_count):
        scheduled_visit_count = 0
    scheduled_visit_count = int(scheduled_visit_count)

    variables = []

    # spot_link_to_spot_conf
    has_spot_link = _has_event(seq, "spot_link_received")
    has_spot_conf_after_link = _has_after(seq, "spot_link_received", "spot_confirmation")
    if has_spot_link or combined:
        val = 1 if (has_spot_conf_after_link or combined) else 0
        variables.append({
            "conv_variable_category": "funnel_events",
            "conv_variable_name": "spot_link_to_spot_conf",
            "conv_variable_value": val,
        })
    # else: None -> do not add

    # spot_conf_to_request_visit: (spot_conf or combined) then explicit_visit_request
    has_spot_conf = _has_event(seq, "spot_confirmation")
    has_explicit_after_conf = _has_after(seq, "spot_confirmation", "explicit_visit_request") or (
        combined and _has_event(seq, "explicit_visit_request")
    )
    if has_spot_conf or combined:
        val = 1 if has_explicit_after_conf else 0
        variables.append({
            "conv_variable_category": "funnel_events",
            "conv_variable_name": "spot_conf_to_request_visit",
            "conv_variable_value": val,
        })

    # visit_request_to_schedule
    has_explicit = _has_event(seq, "explicit_visit_request")
    has_visit_after = _has_after(seq, "explicit_visit_request", "visit_scheduled")
    if has_explicit:
        val = 1 if (has_visit_after or scheduled_visit_count > 0) else 0
        variables.append({
            "conv_variable_category": "funnel_events",
            "conv_variable_name": "visit_request_to_schedule",
            "conv_variable_value": val,
        })

    # reco_index
    has_reco_sent = _has_event(seq, "recommendations_sent")
    if has_reco_sent:
        val = 1 if _has_after(seq, "recommendations_sent", "recommendation_scheduling") else 0
        variables.append({
            "conv_variable_category": "funnel_events",
            "conv_variable_name": "reco_index",
            "conv_variable_value": val,
        })

    # schedule_form_index
    has_sched_form = _has_event(seq, "scheduling_form_sent")
    if has_sched_form:
        val = 1 if _has_after(seq, "scheduling_form_sent", "scheduling_form_completed") else 0
        variables.append({
            "conv_variable_category": "funnel_events",
            "conv_variable_name": "schedule_form_index",
            "conv_variable_value": val,
        })

    # profiling_form_index
    has_prof_form = _has_event(seq, "profiling_form_sent")
    if has_prof_form:
        val = 1 if _has_after(seq, "profiling_form_sent", "profiling_form_completed") else 0
        variables.append({
            "conv_variable_category": "funnel_events",
            "conv_variable_name": "profiling_form_index",
            "conv_variable_value": val,
        })

    return variables
