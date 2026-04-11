"""Conversation processing utilities."""

import json
import pandas as pd

from dagster_pipeline.defs.conversation_analysis.utils.phone_formats import get_canonical_phone


def group_conversations(df_conversations: pd.DataFrame, gap_hours: int = 72) -> pd.DataFrame:
    """
    Group messages into conversations based on time gaps.
    Default 72h: a new conversation segment starts when there are more than 72 hours
    without messages. Uses canonical phone (get_canonical_phone) for groupby.
    """
    conversations_data = []
    df = df_conversations.copy()
    df["_phone_canonical"] = df["phone_number"].apply(get_canonical_phone)

    for phone_canonical, group in df.groupby("_phone_canonical"):
        # Sort messages
        group_copy = group.copy()
        group_copy['type_priority'] = group_copy['type'].map({'user': 0, 'assistant': 1})
        group_sorted = group_copy.sort_values(
            ['created_at_utc', 'type_priority', 'id']
        ).drop('type_priority', axis=1)
        group_sorted['created_at_utc'] = pd.to_datetime(group_sorted['created_at_utc'])

        # Detect gaps of more than gap_hours between consecutive messages
        group_sorted['time_diff'] = group_sorted['created_at_utc'].diff().dt.total_seconds().div(3600)
        group_sorted['conversation_id'] = (group_sorted['time_diff'] > gap_hours).cumsum()

        # Create a conversation for each detected segment
        for conv_id, conv_group in group_sorted.groupby('conversation_id'):
            conversation_lines = []
            for _, row in conv_group.iterrows():
                time = row['created_at_utc'].strftime('%Y-%m-%d %H:%M:%S')
                message_body = row.get('message_body', '') or ''
                conversation_lines.append(f"[{time}] {row['type']}: {message_body}")

            # Get last message of this conversation
            last_message = conv_group.iloc[-1]
            ultimo_mensaje = last_message['message_body']
            tipo_ultimo_mensaje = last_message['type']
            timestamp_ultimo_mensaje = last_message['created_at_utc'].strftime('%Y-%m-%d %H:%M:%S')
            
            # conversation_id = canonical phone + date (YYYYMMDD only). Same number in different
            # formats (+52, 52, etc.) is grouped via _phone_canonical so we get one row per (phone, day).
            conv_start = conv_group['created_at_utc'].min()
            conv_start_str = conv_start.strftime('%Y%m%d')
            new_conversation_id = f"{phone_canonical}_{conv_start_str}"

            # Convert messages to JSON string (handles Timestamps properly)
            messages_list = []
            for _, msg_row in conv_group.iterrows():
                messages_list.append({
                    'id': msg_row.get('id'),
                    'phone_number': msg_row.get('phone_number'),
                    'message_body': msg_row.get('message_body'),
                    'type': msg_row.get('type'),
                    'created_at_utc': str(msg_row.get('created_at_utc')),
                })
            
            # Use first occurrence of phone_number in group for output (original format)
            phone_display = conv_group['phone_number'].iloc[0]
            conversations_data.append({
                'phone_number': phone_display,
                'conversation_id': new_conversation_id,
                'conversation_start': conv_start,
                'conversation_end': conv_group['created_at_utc'].max(),
                'conversation_text': '\n'.join(conversation_lines),
                'message_count': len(conv_group),
                'messages': json.dumps(messages_list),  # Already JSON string
                'ultimo_mensaje': ultimo_mensaje,
                'tipo_ultimo_mensaje': tipo_ultimo_mensaje,
                'timestamp_ultimo_mensaje': timestamp_ultimo_mensaje
            })

    return pd.DataFrame(conversations_data)


def create_conv_variables(row: pd.Series, enable_ai_tags: bool = False) -> list:
    """
    Create conversation variables for a single conversation row.
    
    Orchestrates calls to individual variable functions from:
    - variables.metrics: Numeric metrics (counts, times)
    - variables.tags: Classification tags (visit_intention, etc.)
    
    NOTE: AI tags are now processed separately in batch via process_ai_tags_batch()
    for optimal performance. The enable_ai_tags parameter is deprecated but kept
    for backwards compatibility.
    
    Args:
        row: DataFrame row with conversation data
        enable_ai_tags: DEPRECATED - AI tags are now processed in batch in assets.py
    
    Returns:
        List of variable dictionaries (without AI tags - those are added separately)
    """
    from dagster_pipeline.defs.conversation_analysis.utils.variables import (
        get_messages_count,
        get_user_messages_count,
        get_total_time_conversation,
        get_user_average_response_time,
        get_funnel_type,
        get_funnel_section,
        get_user_spot2_links_count,
        get_active_user,
        get_engaged_user,
        get_visit_intention,
        get_scheduled_visit,
    )
    
    variables = []
    
    # === METRICS ===
    variables.append(get_messages_count(row))
    variables.append(get_user_messages_count(row))
    variables.append(get_user_spot2_links_count(row))
    variables.append(get_total_time_conversation(row))
    variables.append(get_user_average_response_time(row))
    
    # === FUNNEL METRICS ===
    variables.append(get_funnel_type(row))
    variables.append(get_funnel_section(row))
    
    # === TAGS ===
    variables.append(get_active_user(row))
    variables.append(get_engaged_user(row))
    variables.append(get_visit_intention(row))
    variables.append(get_scheduled_visit(row))
    
    # NOTE: AI tags are now processed in batch in conv_with_variables asset
    # for 20x faster performance using async concurrency

    # === AI TAGS (OpenAI) ===
    if enable_ai_tags:
        from dagster_pipeline.defs.conversation_analysis.utils.variables import get_ai_tags
        ai_tags = get_ai_tags(row)
        variables.extend(ai_tags)

    return variables

