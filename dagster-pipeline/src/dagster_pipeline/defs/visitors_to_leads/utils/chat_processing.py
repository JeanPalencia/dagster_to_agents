"""Utilities for processing chat conversations."""
import pandas as pd
from typing import Optional

from dagster_pipeline.defs.visitors_to_leads.utils.url_processing import (
    extract_urls_from_text,
    extract_spot_id_robust,
    extract_all_spot_ids_from_user_messages,
)


EMPTY_EXTRACTION_RESULT = {
    'first_user_message_timestamp': None,
    'first_user_message_text': None,
    'first_link_with_spot_id': None,
    'spot_id': None,
    'link_in_first_message': False,
    'all_spot_ids': []
}


def get_first_user_message_and_link(conversation: dict) -> dict:
    """Extract first user message timestamp and first link with valid spot_id from conversation."""
    messages = conversation.get('messages', [])
    if not messages:
        return EMPTY_EXTRACTION_RESULT.copy()
    
    sorted_messages = sorted(messages, key=lambda x: x.get('created_at_utc', ''))
    
    # Find first user message
    first_user_message = None
    first_user_message_idx = None
    for idx, msg in enumerate(sorted_messages):
        if msg.get('type') == 'user':
            first_user_message = msg
            first_user_message_idx = idx
            break
    
    if not first_user_message:
        return EMPTY_EXTRACTION_RESULT.copy()
    
    first_user_timestamp = pd.to_datetime(first_user_message.get('created_at_utc'))
    first_user_text = first_user_message.get('message_body', '') or ''
    
    # Search for first link with valid spot_id in all messages
    first_link_with_spot_id = None
    spot_id = None
    link_in_first_message = False
    
    for idx, msg in enumerate(sorted_messages):
        message_body = msg.get('message_body', '') or ''
        urls = extract_urls_from_text(message_body)
        
        for url in urls:
            extracted_spot_id = extract_spot_id_robust(url)
            if extracted_spot_id:
                first_link_with_spot_id = url
                spot_id = extracted_spot_id
                link_in_first_message = (idx == first_user_message_idx)
                break
        
        if first_link_with_spot_id:
            break
    
    all_spot_ids = extract_all_spot_ids_from_user_messages(conversation)
    
    return {
        'first_user_message_timestamp': first_user_timestamp,
        'first_user_message_text': first_user_text,
        'first_link_with_spot_id': first_link_with_spot_id,
        'spot_id': spot_id,
        'link_in_first_message': link_in_first_message,
        'all_spot_ids': all_spot_ids
    }


def process_chats_for_matching(df_grouped: pd.DataFrame) -> pd.DataFrame:
    """Process conversations to extract first user message and first link with spot_id."""
    df = df_grouped.copy()
    
    extraction_results = df.apply(
        lambda row: get_first_user_message_and_link(row.to_dict()),
        axis=1
    )
    
    extraction_df = pd.DataFrame(list(extraction_results))
    if not extraction_df.empty:
        extraction_df = extraction_df.dropna(axis=1, how='all')
    
    return pd.concat([df.reset_index(drop=True), extraction_df], axis=1)


def filter_conversations_by_date_range(
    df_grouped: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """Filter conversations by date range."""
    if df_grouped.empty:
        return df_grouped.copy()
    
    df = df_grouped.copy()
    
    if 'conversation_start' not in df.columns:
        raise ValueError("DataFrame must have 'conversation_start' column")
    
    df['conversation_start'] = pd.to_datetime(df['conversation_start'])
    
    if start_date:
        start_dt = pd.to_datetime(start_date)
        if len(str(start_date)) <= 10:
            start_dt = start_dt.normalize()
        df = df[df['conversation_start'] >= start_dt]
    
    if end_date:
        end_dt = pd.to_datetime(end_date)
        if len(str(end_date)) <= 10:
            end_dt = end_dt.normalize() + pd.Timedelta(days=1)
        else:
            end_dt = end_dt + pd.Timedelta(days=1)
        df = df[df['conversation_start'] < end_dt]
    
    return df


def get_first_chat_per_phone_in_window(df: pd.DataFrame) -> pd.DataFrame:
    """Get first chat per phone_number in the filtered window."""
    if df.empty:
        return df.copy()
    
    df = df.copy().sort_values('conversation_start')
    return df.groupby('phone_number').first().reset_index()

