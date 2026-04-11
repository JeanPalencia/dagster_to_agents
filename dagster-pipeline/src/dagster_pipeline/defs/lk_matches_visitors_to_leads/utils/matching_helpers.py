"""Helper functions for matching operations."""
import pandas as pd
from typing import Optional, List

from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.url_processing import normalize_spot_id_for_comparison


EMPTY_MATCH_FIELDS = {
    'user_pseudo_id': None, 'match_count': 0, 'time_diff_seconds': None,
    'matched_spot_id': None, 'device_category': None, 'event_timestamp': None,
    'match_source': None, 'email_bq': None, 'phone_bq': None,
    'spotId_bq': None, 'from_param': None, 'channel': None, 'event_datetime': None,
    'event_name': None, 'source': None, 'medium': None, 'source_name': None,
    'campaign_name': None, 'page_location': None
}


def create_empty_match_result(chat_row: dict) -> dict:
    """Create a result row with no match."""
    result = chat_row.copy()
    result.update(EMPTY_MATCH_FIELDS)
    return result


def create_match_result(
    chat_row: dict, match_row: pd.Series, match_count: int,
    match_source: str, chat_spot_id: Optional[str]
) -> dict:
    """Create a result row with match."""
    result = chat_row.copy()
    result.update({
        'user_pseudo_id': match_row['user_pseudo_id'],
        'match_count': match_count,
        'time_diff_seconds': match_row['time_diff'],
        'matched_spot_id': chat_spot_id if chat_spot_id else match_row.get('spotId_bq'),
        'device_category': match_row['device_category'],
        'event_timestamp': match_row.get('event_timestamp'),
        'match_source': match_source,
        'email_bq': match_row.get('email_bq'),
        'phone_bq': match_row.get('phone_bq'),
        'spotId_bq': match_row.get('spotId_bq'),
        'from_param': match_row.get('from_param'),
        'channel': match_row.get('channel'),
        'event_datetime': match_row.get('event_datetime'),
        'event_name': match_row.get('event_name'),
        'source': match_row.get('source'),
        'medium': match_row.get('medium'),
        'source_name': match_row.get('source_name'),
        'campaign_name': match_row.get('campaign_name'),
        'page_location': match_row.get('page_location')
    })
    return result


def normalize_spot_ids_list(spot_ids: List[str]) -> set:
    """Normalize a list of spot_ids for comparison."""
    normalized = set()
    for sid in spot_ids:
        norm = normalize_spot_id_for_comparison(sid)
        if norm is not None:
            normalized.add(norm)
    return normalized


def apply_dual_matching_strategy(
    chat_spot_id: Optional[str], all_spot_ids: List[str],
    leads: pd.DataFrame, chat_timestamp: pd.Timestamp, time_buffer_seconds: int
) -> pd.DataFrame:
    """Apply dual matching: 1) spot_id from page_location, 2) spotId_bq vs all_spot_ids."""
    matches = pd.DataFrame()
    
    # Strategy 1: spot_id from page_location
    if chat_spot_id:
        matches = leads[
            (leads['spot_id'] == chat_spot_id) & (leads['spot_id'].notna())
        ].copy()
        
        if not matches.empty:
            matches['time_diff'] = (matches['event_datetime'] - chat_timestamp).dt.total_seconds().abs()
            matches = matches[matches['time_diff'] <= time_buffer_seconds]
    
    # Strategy 2: spotId_bq vs all_spot_ids
    if matches.empty and all_spot_ids:
        leads['time_diff'] = (leads['event_datetime'] - chat_timestamp).dt.total_seconds().abs()
        time_filtered = leads[leads['time_diff'] <= time_buffer_seconds].copy()
        
        if not time_filtered.empty and 'spotId_bq' in time_filtered.columns:
            normalized_chat_ids = normalize_spot_ids_list(all_spot_ids)
            if normalized_chat_ids:
                time_filtered['spotId_bq_normalized'] = (
                    time_filtered['spotId_bq'].apply(normalize_spot_id_for_comparison)
                )
                matches = time_filtered[
                    time_filtered['spotId_bq_normalized'].notna() &
                    time_filtered['spotId_bq_normalized'].isin(normalized_chat_ids)
                ].copy()
    
    return matches


def filter_subsequent_events(lead_events_all: pd.DataFrame, user_pseudo_ids) -> pd.DataFrame:
    """Filter subsequent events: only user_pseudo_id from df_first and clientRequestedWhatsappForm."""
    return lead_events_all[
        (lead_events_all['user_pseudo_id'].isin(user_pseudo_ids))
        & (lead_events_all['event_name'] == 'clientRequestedWhatsappForm')
    ].copy()


def get_channel_from_previous_event(
    user_pseudo_id: str, chat_timestamp: pd.Timestamp, lead_events_all: pd.DataFrame
) -> dict:
    """Get channel, event_datetime, event_name, source, medium, and source_name from most recent event before chat timestamp."""
    default_return = {
        'channel': None, 'event_datetime': None, 'event_name': None,
        'source': None, 'medium': None, 'source_name': None, 'campaign_name': None,
        'page_location': None
    }
    
    if lead_events_all.empty or 'channel' not in lead_events_all.columns:
        return default_return
    
    if pd.isna(user_pseudo_id) or pd.isna(chat_timestamp):
        return default_return
    
    user_events = lead_events_all[
        (lead_events_all['user_pseudo_id'] == user_pseudo_id) &
        (lead_events_all['event_datetime'] < chat_timestamp)
    ].copy()
    
    if user_events.empty:
        return default_return
    
    user_events = user_events.sort_values('event_datetime', ascending=False)
    most_recent_event = user_events.iloc[0]
    
    return {
        'channel': most_recent_event['channel'] if 'channel' in most_recent_event.index else None,
        'event_datetime': most_recent_event['event_datetime'] if 'event_datetime' in most_recent_event.index else None,
        'event_name': most_recent_event['event_name'] if 'event_name' in most_recent_event.index else None,
        'source': most_recent_event['source'] if 'source' in most_recent_event.index else None,
        'medium': most_recent_event['medium'] if 'medium' in most_recent_event.index else None,
        'source_name': most_recent_event['source_name'] if 'source_name' in most_recent_event.index else None,
        'campaign_name': most_recent_event['campaign_name'] if 'campaign_name' in most_recent_event.index else None,
        'page_location': most_recent_event['page_location'] if 'page_location' in most_recent_event.index else None
    }

