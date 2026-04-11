# defs/data_lakehouse/gold/bt_conv_message_event_type_parity.py
"""
Align per-message event_type with historical bt_conv_conversations in Geospot.

Temporal enrichment (_enrich_messages_with_events) may assign newer staging event types;
legacy exports often show scheduling_form_sent / link_followup_reminder.
"""
from __future__ import annotations

from typing import Any, Dict, List

# governance/staging label -> label seen in legacy (QA 3c)
LEGACY_PARITY_EVENT_TYPE_MAP: Dict[str, str] = {
    "explicit_visit_request": "scheduling_form_sent",
    "recommendation_scheduling": "scheduling_form_sent",
    "recommendations_sent": "scheduling_form_sent",
    "scheduling_reminder": "link_followup_reminder",
}


def apply_legacy_event_type_labels_to_messages(messages: Any) -> Any:
    """
    Walk a list of dicts (messages) and rewrite event_type when a mapping exists.
    Returns the input unchanged if it is not a list of dicts.
    """
    if not isinstance(messages, list):
        return messages
    for m in messages:
        if not isinstance(m, dict):
            continue
        et = m.get("event_type")
        if not et or not isinstance(et, str):
            continue
        et_stripped = et.strip()
        if et_stripped in LEGACY_PARITY_EVENT_TYPE_MAP:
            m["event_type"] = LEGACY_PARITY_EVENT_TYPE_MAP[et_stripped]
    return messages
