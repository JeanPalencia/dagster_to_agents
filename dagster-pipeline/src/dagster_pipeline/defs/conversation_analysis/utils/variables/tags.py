"""Tag variables for conversation analysis."""

import json
import pandas as pd
from typing import Any, Optional


def _parse_timestamp(ts) -> Optional[pd.Timestamp]:
    """Parse timestamp for event comparison (same logic as experiments)."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    if isinstance(ts, pd.Timestamp):
        return ts
    try:
        return pd.to_datetime(ts)
    except (ValueError, TypeError):
        return None


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


def get_active_user(row: pd.Series) -> dict:
    """
    Detect if user is active (sent more than 1 message).
    
    Args:
        row: DataFrame row with 'messages' column
    
    Returns:
        Variable dict with active_user (1 or 0)
    """
    messages = parse_messages(row.get('messages', []))
    user_messages = [m for m in messages if isinstance(m, dict) and m.get('type') == 'user']
    is_active = len(user_messages) > 1
    
    return {
        'conv_variable_category': 'tag',
        'conv_variable_name': 'active_user',
        'conv_variable_value': 1 if is_active else 0
    }


def get_engaged_user(row: pd.Series) -> dict:
    """
    Detect if user is engaged (sent more than 2 messages).
    
    Args:
        row: DataFrame row with 'messages' column
    
    Returns:
        Variable dict with engaged_user (1 or 0)
    """
    messages = parse_messages(row.get('messages', []))
    user_messages = [m for m in messages if isinstance(m, dict) and m.get('type') == 'user']
    is_engaged = len(user_messages) > 2
    
    return {
        'conv_variable_category': 'tag',
        'conv_variable_name': 'engaged_user',
        'conv_variable_value': 1 if is_engaged else 0
    }


def get_visit_intention(row: pd.Series) -> dict:
    """
    Detect visit intention in conversation.
    
    Visit intention is detected when:
    1. Assistant sends phrases indicating visit scheduling availability
    2. User mentions "agendar visita" or "agendar cita"
    
    Args:
        row: DataFrame row with 'conversation_text' and 'messages' columns
    
    Returns:
        Variable dict with visit_intention (1 or 0)
    """
    # A) Frases del ASISTENTE que indican intención de visita
    assistant_intent_phrases = [
        'Podemos solicitar una visita a partir de',
        'Indícanos qué día y hora te conviene y lo gestionamos',
        'Por favor, indícame qué día y hora te conviene para solicitar la visita',
        'Estoy aquí para ayudarte a coordinarla',
        'He encontrado esta fecha válida:',
        'Resumen de agendamiento',
        'Tu solicitud de visita ha sido registrada',
        '¡Entendido! La fecha más cercana'
    ]
    conversation_text = str(row.get('conversation_text', ''))
    has_assistant_intent = any(phrase in conversation_text for phrase in assistant_intent_phrases)
    
    # B) Frases del USUARIO que indican intención de visita (case insensitive)
    user_intent_phrases = ['agendar visita', 'agendar cita']
    messages = parse_messages(row.get('messages', []))
    has_user_intent = False
    
    for msg in messages:
        if isinstance(msg, dict) and msg.get('type') == 'user':
            msg_body = str(msg.get('message_body', '')).lower()
            if any(phrase in msg_body for phrase in user_intent_phrases):
                has_user_intent = True
                break
    
    # visit_intention = 1 si cualquiera de los dos
    has_intent = has_assistant_intent or has_user_intent
    
    return {
        'conv_variable_category': 'tag',
        'conv_variable_name': 'visit_intention',
        'conv_variable_value': 1 if has_intent else 0
    }


def get_scheduled_visit(row: pd.Series) -> dict:
    """
    Count of visits scheduled during the conversation date range.
    
    This value is pre-computed in conv_with_projects asset by looking at
    calendar_appointments where visit_created_at is between conv_start and conv_end.
    
    Args:
        row: DataFrame row with 'scheduled_visit_count' column
    
    Returns:
        Variable dict with scheduled_visit count
    """
    count = row.get('scheduled_visit_count', 0)
    # Handle NaN or None
    if pd.isna(count):
        count = 0
    
    return {
        'conv_variable_category': 'tag',
        'conv_variable_name': 'scheduled_visit',
        'conv_variable_value': int(count)
    }


def get_created_project(row: pd.Series) -> dict:
    """
    Count of projects created during the conversation date range.
    
    This value is pre-computed in conv_with_projects asset by looking at
    project_requirements where project_created_at is between conv_start and conv_end.
    
    Args:
        row: DataFrame row with 'created_project_count' column
    
    Returns:
        Variable dict with created_project count
    """
    count = row.get('created_project_count', 0)
    # Handle NaN or None
    if pd.isna(count):
        count = 0
    
    return {
        'conv_variable_category': 'tag',
        'conv_variable_name': 'created_project',
        'conv_variable_value': int(count)
    }


def get_follow_up_success(row: pd.Series, events_in_window: pd.DataFrame):
    """
    follow_up_success = 1 if visit_scheduled after scheduling_reminder, 0 if scheduling_reminder but no visit_scheduled after.
    Returns None (do not add variable) when there is no scheduling_reminder event.

    Category: experiments.
    """
    if events_in_window is None or events_in_window.empty:
        return None
    df = events_in_window.copy()
    df["_ts"] = df["created_at_local"].apply(_parse_timestamp)
    df = df.dropna(subset=["_ts"]).sort_values("_ts").reset_index(drop=True)
    if df.empty:
        return None
    # Normalize event_type for comparison (strip whitespace; exact spelling)
    df["_event_type"] = df["event_type"].astype(str).str.strip()
    reminder = df[df["_event_type"] == "scheduling_reminder"]
    if reminder.empty:
        return None  # no scheduling_reminder → do not add variable (leave empty)
    reminder_ts = reminder.iloc[0]["_ts"]
    after = df[df["_ts"] > reminder_ts]
    has_visit = (after["_event_type"] == "visit_scheduled").any()
    return {
        "conv_variable_category": "experiments",
        "conv_variable_name": "follow_up_success",
        "conv_variable_value": 1 if has_visit else 0,
    }

