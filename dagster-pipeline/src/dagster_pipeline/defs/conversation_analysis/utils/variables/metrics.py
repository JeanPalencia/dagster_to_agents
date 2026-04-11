"""Metric variables for conversation analysis."""

import json
import re
import numpy as np
import pandas as pd
from typing import Any


def parse_messages(messages: Any) -> list:
    """Parse messages from JSON string or list."""
    if isinstance(messages, str):
        try:
            messages = json.loads(messages)
        except:
            messages = []
    if not isinstance(messages, list):
        messages = []
    return messages


def get_messages_count(row: pd.Series) -> dict:
    """
    Count total messages in conversation.
    
    Args:
        row: DataFrame row with 'messages' column
    
    Returns:
        Variable dict with messages_count
    """
    messages = parse_messages(row.get('messages', []))
    return {
        'conv_variable_category': 'metric',
        'conv_variable_name': 'messages_count',
        'conv_variable_value': len(messages)
    }


def get_user_messages_count(row: pd.Series) -> dict:
    """
    Count user messages in conversation.
    
    Args:
        row: DataFrame row with 'messages' column
    
    Returns:
        Variable dict with user_messages_count
    """
    messages = parse_messages(row.get('messages', []))
    user_messages = [m for m in messages if isinstance(m, dict) and m.get("type") == "user"]
    return {
        'conv_variable_category': 'metric',
        'conv_variable_name': 'user_messages_count',
        'conv_variable_value': len(user_messages)
    }


# Regex to match links containing https://spot2.mx (captures full URL until whitespace or end)
SPOT2_LINK_PATTERN = re.compile(r'https://spot2\.mx\S*', re.IGNORECASE)
# Regex to extract spot ID from user messages: https://spot2.mx/spots/<id>
SPOT2_SPOTS_ID_PATTERN = re.compile(r'https://spot2\.mx/spots/(\d+)', re.IGNORECASE)

# Template name for assistant carousel of suggested spots (spot ID = last segment of slug after /)
CAROUSEL_TEMPLATE_NAME = "send_suggested_spots_carousel_five_cards_batch_2_v2"


def _get_payload_from_message(m: dict) -> dict | None:
    """Get the message payload that may contain template (full payload or inside content/message_body)."""
    if not isinstance(m, dict):
        return None
    if m.get("template"):
        return m
    for key in ("content", "message_body", "body"):
        raw = m.get(key)
        if raw is None:
            continue
        if isinstance(raw, dict) and raw.get("template"):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and parsed.get("template"):
                    return parsed
            except (TypeError, json.JSONDecodeError):
                pass
    return None


def _extract_spot_ids_from_carousel_payload(payload: dict) -> set:
    """Extract spot IDs from carousel template (url button parameters: slug/SPOT_ID)."""
    ids = set()
    template = payload.get("template") if isinstance(payload, dict) else None
    if not isinstance(template, dict):
        return ids
    if template.get("name") != CAROUSEL_TEMPLATE_NAME:
        return ids
    components = template.get("components") or []
    for comp in components:
        if comp.get("type") != "carousel":
            continue
        cards = comp.get("cards") or []
        for card in cards:
            for c in card.get("components") or []:
                if c.get("type") == "button" and c.get("sub_type") == "url":
                    for param in c.get("parameters") or []:
                        text = param.get("text") if isinstance(param, dict) else None
                        if isinstance(text, str) and "/" in text:
                            part = text.strip().split("/")[-1]
                            if part.isdigit():
                                ids.add(int(part))
    return ids


def _spot_ids_from_assistant_carousel(messages: list) -> set:
    """Collect all spot IDs from assistant messages with send_suggested_spots carousel template."""
    ids = set()
    for m in messages:
        if not isinstance(m, dict) or m.get("type") != "assistant":
            continue
        payload = _get_payload_from_message(m)
        if payload:
            ids |= _extract_spot_ids_from_carousel_payload(payload)
    return ids


def _spot_ids_from_user_links(messages: list) -> set:
    """Extract spot IDs from user messages (https://spot2.mx/spots/<id>)."""
    ids = set()
    for m in messages:
        if not isinstance(m, dict) or m.get("type") != "user":
            continue
        body = m.get("message_body") or m.get("body") or ""
        if not isinstance(body, str):
            continue
        for match in SPOT2_SPOTS_ID_PATTERN.findall(body):
            ids.add(int(match))
    return ids


def get_reco_interact(row: pd.Series) -> dict:
    """
    Count of recommended spots (from assistant carousel template send_suggested_spots_...) that
    the user interacted with (sent a link https://spot2.mx/spots/<id>). Uses only conv_messages:
    - Recommended IDs: from assistant messages with template send_suggested_spots_carousel_five_cards_batch_2_v2
      (spot ID = last segment of url slug in each card, e.g. ".../95932" -> 95932).
    - User interacted: spot IDs extracted from user messages with https://spot2.mx/spots/<id>.
    """
    messages = parse_messages(row.get("messages", []))
    recommended = _spot_ids_from_assistant_carousel(messages)
    sent = _spot_ids_from_user_links(messages)
    value = len(recommended & sent)
    return {
        "conv_variable_category": "metric",
        "conv_variable_name": "reco_interact",
        "conv_variable_value": value,
    }


def get_user_spot2_links_count(row: pd.Series) -> dict:
    """
    Count distinct links sent by the user that contain https://spot2.mx.
    
    Scans message_body of user messages in conv_messages and extracts all URLs
    matching https://spot2.mx (with any path/query). Returns the number of
    distinct URLs per conversation.
    
    Args:
        row: DataFrame row with 'messages' column (each message has message_body, type)
    
    Returns:
        Variable dict with user_spot2_links_count (int)
    """
    messages = parse_messages(row.get('messages', []))
    user_messages = [m for m in messages if isinstance(m, dict) and m.get("type") == "user"]
    seen_urls = set()
    for m in user_messages:
        body = m.get("message_body") or m.get("body") or ""
        if not isinstance(body, str):
            continue
        for match in SPOT2_LINK_PATTERN.findall(body):
            # Normalize: strip trailing punctuation that might have been captured
            url = match.rstrip(".,;:)")
            seen_urls.add(url)
    return {
        'conv_variable_category': 'metric',
        'conv_variable_name': 'user_spot2_links_count',
        'conv_variable_value': len(seen_urls)
    }


def get_total_time_conversation(row: pd.Series) -> dict:
    """
    Calculate total conversation time in minutes.
    
    Args:
        row: DataFrame row with 'conversation_start' and 'conversation_end' columns
    
    Returns:
        Variable dict with total_time_conversation (minutes)
    """
    start = row.get("conversation_start")
    end = row.get("conversation_end")
    total_time = None
    
    if pd.notnull(start) and pd.notnull(end):
        try:
            start_ts = pd.to_datetime(start)
            end_ts = pd.to_datetime(end)
            total_time = (end_ts - start_ts).total_seconds() / 60.0
        except:
            pass
    
    return {
        'conv_variable_category': 'metric',
        'conv_variable_name': 'total_time_conversation',
        'conv_variable_value': total_time
    }


def get_user_average_response_time(row: pd.Series) -> dict:
    """
    Calculate average user response time in minutes.
    
    Measures time between assistant message and user's next response.
    
    Args:
        row: DataFrame row with 'messages' column
    
    Returns:
        Variable dict with user_average_time_respond_minutes
    """
    messages = parse_messages(row.get('messages', []))
    response_times = []
    prev_assistant_time = None
    
    for m in messages:
        if not isinstance(m, dict):
            continue
        m_type = m.get('type')
        m_time = m.get('created_at_utc') or m.get('created_at')
        
        try:
            m_time = pd.to_datetime(m_time)
        except:
            m_time = None
        
        if m_type == 'assistant' and m_time is not None:
            prev_assistant_time = m_time
        elif m_type == 'user' and m_time is not None and prev_assistant_time is not None:
            delta = (m_time - prev_assistant_time).total_seconds() / 60.0
            if delta >= 0:
                response_times.append(delta)
            prev_assistant_time = None
    
    avg_response = np.mean(response_times) if len(response_times) > 0 else None
    
    return {
        'conv_variable_category': 'metric',
        'conv_variable_name': 'user_average_time_respond_minutes',
        'conv_variable_value': avg_response
    }


def _build_funnel_path(messages: list, field_name: str) -> str:
    """
    Helper to build unique funnel path from a specific field in messages.
    
    Args:
        messages: List of message dicts
        field_name: Field to extract ('event_type' or 'event_section')
    
    Returns:
        Funnel path string with " > " delimiter, or None if empty
    """
    # Sort messages by timestamp
    def get_timestamp(m):
        if not isinstance(m, dict):
            return ""
        return m.get('created_at_utc') or m.get('created_at') or ""
    
    sorted_messages = sorted(messages, key=get_timestamp)
    
    # Extract field values in order
    values = []
    for m in sorted_messages:
        if isinstance(m, dict):
            val = m.get(field_name)
            if val and isinstance(val, str):
                values.append(val)
    
    # Remove consecutive duplicates
    unique_values = []
    for val in values:
        if not unique_values or unique_values[-1] != val:
            unique_values.append(val)
    
    # Join with " > " delimiter (safe for JSON, readable)
    return " > ".join(unique_values) if unique_values else None


def get_funnel_type(row: pd.Series) -> dict:
    """
    Get funnel_type path from pre-calculated column.
    
    The funnel_type is now calculated directly from events table in the pipeline,
    ordered chronologically by conversation_id. This ensures precision and accuracy.
    
    Args:
        row: DataFrame row with 'funnel_type' column (pre-calculated from events)
    
    Returns:
        Variable dict with funnel_type path string
    """
    funnel_path = row.get('funnel_type')
    
    # Handle None, NaN, or empty string
    if pd.isna(funnel_path) or not funnel_path:
        funnel_path = None
    
    return {
        'conv_variable_category': 'funnel_metric',
        'conv_variable_name': 'funnel_type',
        'conv_variable_value': funnel_path
    }


def get_funnel_section(row: pd.Series) -> dict:
    """
    Get funnel_section path from pre-calculated column.
    
    The funnel_section is now calculated directly from events table in the pipeline,
    ordered chronologically by conversation_id. This ensures precision and accuracy.
    
    Args:
        row: DataFrame row with 'funnel_section' column (pre-calculated from events)
    
    Returns:
        Variable dict with funnel_section path string
    """
    funnel_path = row.get('funnel_section')
    
    # Handle None, NaN, or empty string
    if pd.isna(funnel_path) or not funnel_path:
        funnel_path = None
    
    return {
        'conv_variable_category': 'funnel_metric',
        'conv_variable_name': 'funnel_section',
        'conv_variable_value': funnel_path
    }

