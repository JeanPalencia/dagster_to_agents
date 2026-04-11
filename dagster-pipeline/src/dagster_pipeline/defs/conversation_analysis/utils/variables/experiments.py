"""
Experiment variables: test_schedule (AB variant), test_schedule_result, unification_mgs.

- test_schedule / test_schedule_result: see get_experiment_variables.
- unification_mgs: variant from first event_type=spot_link_received, event_data["first_interaction_variant"].
  conv_variable_value is "control" or "combined"; if the event is missing, the variable
  is not added (None).

All events must fall within the conversation time window [conversation_start, conversation_end].
"""

import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional

# AB test started this date (conversations started on or after this date get experiment vars)
EXPERIMENT_START_DATE = datetime(2026, 1, 28).date()


def _parse_timestamp(ts) -> Optional[datetime]:
    """Parse timestamp to datetime for comparison."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts
    if hasattr(ts, "to_pydatetime"):  # pandas Timestamp
        return ts.to_pydatetime()
    if isinstance(ts, str):
        for fmt in ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S"]:
            try:
                return datetime.strptime(ts[:26] if len(ts) > 26 else ts, fmt)
            except (ValueError, TypeError):
                continue
    return None


def _parse_event_data(event_data: Any) -> dict:
    """Parse event_data (may be dict from JSONB or string)."""
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


def get_experiment_variables(
    events_in_window: pd.DataFrame,
    conversation_start,
    conversation_end,
) -> List[Dict[str, Any]]:
    """
    Compute experiment variables from events that fall within the conversation window.

    Rules:
    - Only applies when conversation_start >= 2026-01-28 (AB test start).
    - test_schedule: ab_test_variant from the first explicit_visit_request; if that event
      is missing or has no variant, from the first visit_scheduled event_data.
    - test_schedule_result:
      When variant from explicit_visit_request: 0/1/2/3 from events **after** that timestamp.
      When variant from visit_scheduled only: 1 (no form before visit) or 2 (form before visit).

    test_schedule_result (when variant from explicit_visit_request):
      - 0: no scheduling_form_sent and no visit_scheduled (after explicit)
      - 1: no scheduling_form_sent, has visit_scheduled
      - 2: has scheduling_form_sent and visit_scheduled
      - 3: has scheduling_form_sent, no visit_scheduled

    Args:
        events_in_window: DataFrame with event_type, event_data, created_at_local,
                         already filtered to this conversation's ids and time window.
        conversation_start: Start of conversation (datetime or str).
        conversation_end: End of conversation (datetime or str).

    Returns:
        List of variable dicts (0 or 2 elements) for conv_variables.
    """
    if events_in_window is None or events_in_window.empty:
        return []

    # Check experiment start: only conversations that started on or after 2026-01-28
    conv_start_dt = _parse_timestamp(conversation_start)
    if conv_start_dt is None:
        return []
    try:
        conv_start_date = conv_start_dt.date()
    except AttributeError:
        conv_start_date = conv_start_dt
    if conv_start_date < EXPERIMENT_START_DATE:
        return []

    # Work on a copy; ensure created_at_local is comparable
    df = events_in_window.copy()
    df["_ts"] = df["created_at_local"].apply(_parse_timestamp)
    df = df.dropna(subset=["_ts"]).sort_values("_ts").reset_index(drop=True)

    if df.empty:
        return []

    variant = None
    result = None
    from_explicit = False

    # 1) Try variant from first explicit_visit_request
    explicit = df[df["event_type"] == "explicit_visit_request"]
    if not explicit.empty:
        first_explicit = explicit.iloc[0]
        explicit_ts = first_explicit["_ts"]
        event_data = _parse_event_data(first_explicit.get("event_data"))
        v = event_data.get("ab_test_variant")
        if v is not None and (not isinstance(v, str) or v.strip()):
            variant = str(v).strip()
            from_explicit = True
            after = df[df["_ts"] > explicit_ts]
            has_form = (after["event_type"] == "scheduling_form_sent").any()
            has_visit = (after["event_type"] == "visit_scheduled").any()
            if not has_form and not has_visit:
                result = 0
            elif not has_form and has_visit:
                result = 1
            elif has_form and has_visit:
                result = 2
            else:
                result = 3

    # 2) If no variant from explicit, try first visit_scheduled
    if variant is None:
        visits = df[df["event_type"] == "visit_scheduled"]
        if not visits.empty:
            first_visit = visits.iloc[0]
            visit_ts = first_visit["_ts"]
            event_data = _parse_event_data(first_visit.get("event_data"))
            v = event_data.get("ab_test_variant")
            if v is not None and (not isinstance(v, str) or v.strip()):
                variant = str(v).strip()
                # Result: 1 = no form before visit, 2 = form before visit
                before_visit = df[df["_ts"] < visit_ts]
                has_form_before = (before_visit["event_type"] == "scheduling_form_sent").any()
                result = 2 if has_form_before else 1

    if variant is None or result is None:
        return []

    return [
        {
            "conv_variable_category": "experiments",
            "conv_variable_name": "test_schedule",
            "conv_variable_value": variant,
        },
        {
            "conv_variable_category": "experiments",
            "conv_variable_name": "test_schedule_result",
            "conv_variable_value": result,
        },
    ]


# Event type that carries the unification_mgs experiment variant; variant is in event_data["first_interaction_variant"]
UNIFICATION_MGS_EVENT_TYPE = "spot_link_received"
UNIFICATION_MGS_VARIANTS = ("control", "combined")


def get_unification_mgs_variant(events_in_window: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Get unification_mgs experiment variable from the first spot_link_received event.

    Looks for the first event with event_type=spot_link_received in the window.
    Variant is read from event_data (key "first_interaction_variant") and must be "control" or "combined"
    (case-insensitive). If the event is missing or variant is invalid, returns None
    (do not add the variable).

    Returns:
        Dict with conv_variable_category, conv_variable_name "unification_mgs",
        conv_variable_value "control" or "combined"; or None.
    """
    if events_in_window is None or events_in_window.empty:
        return None
    df = events_in_window.copy()
    df["_event_type"] = df["event_type"].astype(str).str.strip()
    match = df[df["_event_type"] == UNIFICATION_MGS_EVENT_TYPE]
    if match.empty:
        return None
    first = match.iloc[0]
    event_data = _parse_event_data(first.get("event_data"))
    v = event_data.get("first_interaction_variant")
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    v = str(v).strip().lower()
    if v not in UNIFICATION_MGS_VARIANTS:
        return None
    return {
        "conv_variable_category": "experiments",
        "conv_variable_name": "unification_mgs",
        "conv_variable_value": v,
    }


# Event type priority for bot_graph: first event (by time) of highest-priority type wins
BOT_GRAPH_EVENT_PRIORITY = ("conversation_start", "spot_link_received", "spot_confirmation")
BOT_GRAPH_VARIANTS = ("monolith", "control")


def get_bot_graph_variant(events_in_window: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Get bot_graph experiment variable from event_data["ab_variants"]["monolith_graph"].

    Iterates over events in priority order (conversation_start > spot_link_received >
    spot_confirmation > any other), by time within each type, until finding an event
    that has ab_variants["monolith_graph"] in ("monolith", "control"). Returns that
    value. If none have it, the variable is not added (returns None).

    No date filter: if the info is missing, the variable is not added.

    Returns:
        Dict with conv_variable_category "experiments", conv_variable_name "bot_graph",
        conv_variable_value "monolith" or "control"; or None.
    """
    if events_in_window is None or events_in_window.empty:
        return None
    df = events_in_window.copy()
    if "_ts" not in df.columns:
        df["_ts"] = df["created_at_local"].apply(_parse_timestamp)
    df = df.dropna(subset=["_ts"]).sort_values("_ts").reset_index(drop=True)
    if df.empty:
        return None
    df["_event_type"] = df["event_type"].astype(str).str.strip()

    # Build ordered list: first all conversation_start (by time), then spot_link_received, then spot_confirmation, then rest
    ordered_rows = []
    for preferred in BOT_GRAPH_EVENT_PRIORITY:
        match = df[df["_event_type"] == preferred].sort_values("_ts")
        for _, r in match.iterrows():
            ordered_rows.append(r)
    rest = df[~df["_event_type"].isin(BOT_GRAPH_EVENT_PRIORITY)].sort_values("_ts")
    for _, r in rest.iterrows():
        ordered_rows.append(r)
    if not ordered_rows:
        ordered_rows = [df.iloc[i] for i in range(len(df))]

    for chosen in ordered_rows:
        event_data = _parse_event_data(chosen.get("event_data"))
        ab_variants = event_data.get("ab_variants")
        if not isinstance(ab_variants, dict):
            continue
        v = ab_variants.get("monolith_graph")
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        v = str(v).strip().lower()
        if v not in BOT_GRAPH_VARIANTS:
            continue
        return {
            "conv_variable_category": "experiments",
            "conv_variable_name": "bot_graph",
            "conv_variable_value": v,
        }
    return None
