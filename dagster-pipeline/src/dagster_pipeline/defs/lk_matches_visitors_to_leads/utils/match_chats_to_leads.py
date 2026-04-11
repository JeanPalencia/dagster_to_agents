"""Main matching pipeline: match chats to lead events."""
import pandas as pd
from typing import Optional, Tuple

from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.chat_processing import (
    process_chats_for_matching,
    filter_conversations_by_date_range,
    get_first_chat_per_phone_in_window,
)
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.phone_utils import (
    compute_client_phone_clean,
    normalize_phone,
)
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.matching_helpers import (
    create_empty_match_result,
    create_match_result,
    apply_dual_matching_strategy,
    filter_subsequent_events,
    get_channel_from_previous_event,
)
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.dataframe_utils import (
    prepare_lead_events_for_matching,
    safe_concat,
)


def deduplicate_multi_user_matches(matched: pd.DataFrame, enabled: bool = True) -> pd.DataFrame:
    """For chats with multiple user_pseudo_id, keep only the closest to conversation_start."""
    if not enabled or matched.empty or 'user_pseudo_id' not in matched.columns:
        return matched
    
    if 'match_source' in matched.columns:
        client_match_sources = matched['match_source'].str.startswith('direct_client_match_') | \
                              matched['match_source'].str.startswith('via_client_')
        matches_to_deduplicate = matched[~client_match_sources].copy()
        matches_to_keep = matched[client_match_sources].copy()
    else:
        matches_to_deduplicate = matched.copy()
        matches_to_keep = pd.DataFrame()
    
    if matches_to_deduplicate.empty:
        return matched
    
    multi_user_chats = (
        matches_to_deduplicate[matches_to_deduplicate.user_pseudo_id.notna()]
        .groupby('phone_number')['user_pseudo_id']
        .nunique()
        .reset_index(name='num_users')
        .query('num_users > 1')
    )
    
    if multi_user_chats.empty:
        return matched
    
    matched_multi = matches_to_deduplicate[matches_to_deduplicate.phone_number.isin(multi_user_chats.phone_number)].copy()
    matched_single = matches_to_deduplicate[~matches_to_deduplicate.phone_number.isin(multi_user_chats.phone_number)].copy()
    
    selected_matches = []
    for phone in multi_user_chats.phone_number:
        chat_matches = matched_multi[matched_multi.phone_number == phone].copy()
        with_datetime = chat_matches[chat_matches.event_datetime.notna()].copy()
        
        if not with_datetime.empty:
            with_datetime['time_diff'] = (
                pd.to_datetime(with_datetime['event_datetime']) - 
                pd.to_datetime(with_datetime['conversation_start'])
            ).abs()
            best_match = with_datetime.loc[with_datetime['time_diff'].idxmin()]
            selected_matches.append(best_match.drop('time_diff'))
        else:
            selected_matches.append(chat_matches.iloc[0])
    
    result_parts = []
    if not matches_to_keep.empty:
        result_parts.append(matches_to_keep)
    if matched_single is not None and not matched_single.empty:
        result_parts.append(matched_single)
    if selected_matches:
        selected_df = pd.DataFrame(selected_matches)
        result_parts.append(selected_df)
    
    if result_parts:
        result = result_parts[0]
        for part in result_parts[1:]:
            result = safe_concat(result, part)
        return result
    
    return matched


def enrich_with_client_id(matched: pd.DataFrame, clients_df: pd.DataFrame) -> pd.DataFrame:
    """Enrich matched with client_id from clients for all phone_numbers present in clients."""
    if matched.empty or clients_df.empty or 'phone_number' not in matched.columns:
        return matched
    
    matched = matched.copy()
    
    matches_with_client_id = matched[matched['client_id'].notna()].copy() if 'client_id' in matched.columns else pd.DataFrame()
    matches_without_client_id = matched[matched['client_id'].isna()].copy() if 'client_id' in matched.columns else matched.copy()
    
    if matches_without_client_id.empty:
        return matched
    
    columns_to_drop = []
    if 'client_id' in matches_without_client_id.columns:
        columns_to_drop.append('client_id')
    for col in ['lead_min_at', 'lead_min_type', 'lead_type', 'email_clients']:
        if col in matches_without_client_id.columns:
            columns_to_drop.append(col)
    
    if columns_to_drop:
        matches_without_client_id = matches_without_client_id.drop(columns=columns_to_drop)
    
    if 'phone_clean' in matches_without_client_id.columns:
        # phone_clean is already normalized (from chat processing); do not re-normalize
        matches_without_client_id['phone_clean_for_merge'] = matches_without_client_id['phone_clean']
    else:
        matches_without_client_id['phone_clean_for_merge'] = matches_without_client_id['phone_number'].apply(normalize_phone)
    
    matches_to_merge = matches_without_client_id[matches_without_client_id['phone_clean_for_merge'].notna()].copy()
    matches_no_merge = matches_without_client_id[matches_without_client_id['phone_clean_for_merge'].isna()].copy()
    
    if not matches_to_merge.empty:
        clients = clients_df.copy()
        clients['phone_clean_for_merge'] = clients.apply(compute_client_phone_clean, axis=1)
        clients_for_merge = clients[clients['phone_clean_for_merge'].notna()].copy()
        
        client_cols = ['client_id', 'phone_clean_for_merge']
        if 'email' in clients_for_merge.columns:
            client_cols.append('email')
        for col in ['lead_min_at', 'lead_min_type', 'lead_type']:
            if col in clients_for_merge.columns:
                client_cols.append(col)
        
        clients_for_merge = clients_for_merge[client_cols].drop_duplicates(subset=['phone_clean_for_merge'])
        
        if 'email' in clients_for_merge.columns:
            clients_for_merge = clients_for_merge.rename(columns={'email': 'email_clients'})
        
        matches_to_merge = matches_to_merge.merge(
            clients_for_merge,
            left_on='phone_clean_for_merge',
            right_on='phone_clean_for_merge',
            how='left'
        )
        
        if 'phone_clean_for_merge' in matches_to_merge.columns:
            matches_to_merge = matches_to_merge.drop(columns=['phone_clean_for_merge'])
    
    result_parts = []
    if not matches_with_client_id.empty:
        result_parts.append(matches_with_client_id)
    if not matches_to_merge.empty:
        result_parts.append(matches_to_merge)
    if not matches_no_merge.empty:
        result_parts.append(matches_no_merge)
    
    if result_parts:
        return safe_concat(*result_parts)
    
    return matched


def match_chats_to_clients(chats_df: pd.DataFrame, clients_df: pd.DataFrame) -> pd.DataFrame:
    """Match chats to clients using the same cleaning logic from the notebook."""
    if chats_df.empty or clients_df.empty:
        return pd.DataFrame()

    chats = chats_df.copy()
    clients = clients_df.copy()

    if 'phone_clean' in chats.columns:
        chats['phone_clean_for_merge'] = chats['phone_clean']  # already normalized
    else:
        chats['phone_clean_for_merge'] = chats['phone_number'].apply(normalize_phone)
    clients['phone_clean_for_merge'] = clients.apply(compute_client_phone_clean, axis=1)
    
    client_cols = ['client_id', 'email', 'phone_number', 'phone_clean_for_merge']
    for col in ['lead_min_at', 'lead_min_type', 'lead_type']:
        if col in clients.columns:
            client_cols.append(col)
    
    clients_for_merge = clients[client_cols].copy()
    clients_for_merge = clients_for_merge.rename(columns={
        'phone_number': 'phone_number_client',
        'email': 'email_clients'
    })
    
    merged = chats.merge(
        clients_for_merge,
        left_on='phone_clean_for_merge',
        right_on='phone_clean_for_merge',
        how='inner'
    )
    
    if 'phone_clean_for_merge' in merged.columns:
        merged = merged.drop(columns=['phone_clean_for_merge'])
    
    return merged


def find_user_pseudo_ids_from_client(
    client_emails: pd.Series, client_phones: pd.Series, lead_events_all: pd.DataFrame
) -> dict:
    """Find user_pseudo_ids in lead_events matching client emails or phones."""
    user_pseudo_ids_email = set()
    user_pseudo_ids_phone = set()
    
    valid_emails = client_emails.dropna().str.lower().str.strip()
    if not valid_emails.empty:
        lead_events_emails = lead_events_all['email'].dropna().str.lower().str.strip()
        matching_emails = valid_emails[valid_emails.isin(lead_events_emails)]
        if not matching_emails.empty:
            email_matches = lead_events_all[
                lead_events_all['email'].str.lower().str.strip().isin(matching_emails)
            ]['user_pseudo_id'].unique()
            user_pseudo_ids_email.update(email_matches)
    
    valid_phones = client_phones.dropna()
    if not valid_phones.empty:
        client_phones_clean = valid_phones.apply(normalize_phone).dropna()
        if not client_phones_clean.empty:
            if 'phone_clean' in lead_events_all.columns:
                phone_matches = lead_events_all[
                    lead_events_all['phone_clean'].isin(client_phones_clean)
                ]['user_pseudo_id'].unique()
                user_pseudo_ids_phone.update(phone_matches)
            elif 'phone' in lead_events_all.columns:
                lead_events_phone_clean = lead_events_all['phone'].apply(normalize_phone)
                phone_matches = lead_events_all[
                    lead_events_phone_clean.isin(client_phones_clean)
                ]['user_pseudo_id'].unique()
                user_pseudo_ids_phone.update(phone_matches)
    
    all_user_ids = user_pseudo_ids_email | user_pseudo_ids_phone
    match_methods = {}
    for user_id in all_user_ids:
        matched_by_email = user_id in user_pseudo_ids_email
        matched_by_phone = user_id in user_pseudo_ids_phone
        if matched_by_email and matched_by_phone:
            match_methods[user_id] = 'both'
        elif matched_by_email:
            match_methods[user_id] = 'email'
        else:
            match_methods[user_id] = 'phone'
    
    return {'user_pseudo_ids': list(all_user_ids), 'match_methods': match_methods}


def match_users_to_clients_direct(
    lead_events_all: pd.DataFrame, clients_df: pd.DataFrame, chats_df: pd.DataFrame
) -> pd.DataFrame:
    """Direct matching: user_pseudo_id (from lead_events) -> clients -> chats."""
    if lead_events_all.empty or clients_df.empty or chats_df.empty:
        return pd.DataFrame()
    
    users_with_contact = lead_events_all[
        lead_events_all['email'].notna() | lead_events_all['phone'].notna()
    ]['user_pseudo_id'].unique()
    
    if len(users_with_contact) == 0:
        return pd.DataFrame()
    
    clients = clients_df.copy()
    clients['phone_clean_for_merge'] = clients.apply(compute_client_phone_clean, axis=1)
    if 'email' in clients.columns:
        clients['email_clean'] = clients['email'].str.lower().str.strip()
    else:
        clients['email_clean'] = None
    
    lead_events = lead_events_all[lead_events_all['user_pseudo_id'].isin(users_with_contact)].copy()
    if 'email' in lead_events.columns:
        lead_events['email_clean'] = lead_events['email'].str.lower().str.strip()
    else:
        lead_events['email_clean'] = None
    if 'phone_clean' in lead_events.columns:
        # phone_clean is already normalized at this point.
        lead_events['phone_clean_for_merge'] = lead_events['phone_clean']
    elif 'phone' in lead_events.columns:
        lead_events['phone_clean_for_merge'] = lead_events['phone'].apply(normalize_phone)
    else:
        lead_events['phone_clean_for_merge'] = None
    
    chats = chats_df.copy()
    if 'phone_clean' in chats.columns:
        chats['phone_clean_for_merge'] = chats['phone_clean']  # already normalized
    else:
        chats['phone_clean_for_merge'] = chats['phone_number'].apply(normalize_phone)

    matched_results = []
    
    for user_id in users_with_contact:
        user_events = lead_events[lead_events['user_pseudo_id'] == user_id]
        user_email = user_events['email_clean'].dropna().unique()
        user_phone_clean = user_events['phone_clean_for_merge'].dropna().unique()
        
        client_matches = []
        if len(user_email) > 0 and 'email_clean' in clients.columns and clients['email_clean'].notna().any():
            email_match = clients[clients['email_clean'].isin(user_email)]
            if not email_match.empty:
                for _, client_row in email_match.iterrows():
                    client_matches.append(('email', client_row))
        
        if len(user_phone_clean) > 0:
            phone_match = clients[clients['phone_clean_for_merge'].isin(user_phone_clean)]
            if not phone_match.empty:
                for _, client_row in phone_match.iterrows():
                    if not any(c[1]['client_id'] == client_row['client_id'] for c in client_matches):
                        client_matches.append(('phone', client_row))
        
        for match_method, client_row in client_matches:
            client_id = client_row['client_id']
            client_phone_clean = client_row['phone_clean_for_merge']
            match_source = f'direct_client_match_{match_method}'
            
            matching_chats = chats[chats['phone_clean_for_merge'] == client_phone_clean]
            
            if not matching_chats.empty:
                for _, chat_row in matching_chats.iterrows():
                    channel_info = {'channel': None, 'event_datetime': None, 'event_name': None, 'source': None, 'medium': None, 'source_name': None, 'campaign_name': None, 'page_location': None}
                    if 'conversation_start' in chat_row and pd.notna(chat_row.get('conversation_start')):
                        chat_start = pd.to_datetime(chat_row.get('conversation_start'))
                        channel_info = get_channel_from_previous_event(user_id, chat_start, lead_events_all)
                        
                        if channel_info['event_datetime'] is None:
                            user_events_all = lead_events_all[lead_events_all['user_pseudo_id'] == user_id]
                            if not user_events_all.empty:
                                first_event = user_events_all.sort_values('event_datetime').iloc[0]
                                channel_info = {
                                    'channel': first_event.get('channel') if 'channel' in first_event.index else None,
                                    'event_datetime': first_event.get('event_datetime'),
                                    'event_name': first_event.get('event_name') if 'event_name' in first_event.index else None,
                                    'source': first_event.get('source') if 'source' in first_event.index else None,
                                    'medium': first_event.get('medium') if 'medium' in first_event.index else None,
                                    'source_name': first_event.get('source_name') if 'source_name' in first_event.index else None,
                                    'campaign_name': first_event.get('campaign_name') if 'campaign_name' in first_event.index else None,
                                    'page_location': first_event.get('page_location') if 'page_location' in first_event.index else None
                                }
                    
                    result_row = chat_row.to_dict()
                    result_row.update({
                        'user_pseudo_id': user_id, 'match_count': 1, 'time_diff_seconds': None,
                        'matched_spot_id': None, 'device_category': None, 'event_timestamp': None,
                        'match_source': match_source, 'email_clients': client_row.get('email'),
                        'phone_bq': client_row.get('phone_number'), 'spotId_bq': None, 'from_param': None,
                        'channel': channel_info['channel'], 'event_datetime': channel_info['event_datetime'],
                        'event_name': channel_info['event_name'], 'source': channel_info['source'],
                        'medium': channel_info['medium'], 'source_name': channel_info['source_name'],
                        'campaign_name': channel_info.get('campaign_name'), 'client_id': client_id,
                        'page_location': channel_info.get('page_location')
                    })
                    
                    for col in ['lead_min_at', 'lead_min_type', 'lead_type']:
                        if col in client_row.index:
                            result_row[col] = client_row[col]
                    
                    matched_results.append(result_row)
            else:
                user_events_all = lead_events_all[lead_events_all['user_pseudo_id'] == user_id]
                channel_info = {'channel': None, 'event_datetime': None, 'event_name': None, 'source': None, 'medium': None, 'source_name': None, 'campaign_name': None, 'page_location': None}
                if not user_events_all.empty:
                    first_event = user_events_all.sort_values('event_datetime').iloc[0]
                    channel_info = {
                        'channel': first_event.get('channel') if 'channel' in first_event.index else None,
                        'event_datetime': first_event.get('event_datetime'),
                        'event_name': first_event.get('event_name') if 'event_name' in first_event.index else None,
                        'source': first_event.get('source') if 'source' in first_event.index else None,
                        'medium': first_event.get('medium') if 'medium' in first_event.index else None,
                        'source_name': first_event.get('source_name') if 'source_name' in first_event.index else None,
                        'campaign_name': first_event.get('campaign_name') if 'campaign_name' in first_event.index else None,
                        'page_location': first_event.get('page_location') if 'page_location' in first_event.index else None
                    }
                
                result_row = {
                    'user_pseudo_id': user_id, 'match_count': 1, 'time_diff_seconds': None,
                    'matched_spot_id': None, 'device_category': None, 'event_timestamp': None,
                    'match_source': match_source, 'email_clients': client_row.get('email'),
                    'phone_bq': client_row.get('phone_number'), 'spotId_bq': None, 'from_param': None,
                    'channel': channel_info['channel'], 'event_datetime': channel_info['event_datetime'],
                    'event_name': channel_info['event_name'], 'source': channel_info['source'],
                    'medium': channel_info['medium'], 'source_name': channel_info['source_name'],
                    'campaign_name': channel_info.get('campaign_name'), 'client_id': client_id,
                    'page_location': channel_info.get('page_location'),
                    'phone_number': None, 'phone_clean': None, 'conversation_id': None,
                    'conversation_start': None, 'conversation_end': None, 'conversation_text': None,
                    'message_count': None, 'messages': None, 'first_message': None,
                    'first_user_message_timestamp': None, 'first_user_message_text': None,
                    'first_link_with_spot_id': None, 'spot_id': None, 'link_in_first_message': None,
                    'all_spot_ids': None
                }
                
                for col in ['lead_min_at', 'lead_min_type', 'lead_type']:
                    if col in client_row.index:
                        result_row[col] = client_row[col]
                
                matched_results.append(result_row)
    
    return pd.DataFrame(matched_results) if matched_results else pd.DataFrame()


def match_via_clients(
    chats_without_match: pd.DataFrame, clients_df: pd.DataFrame, lead_events_all: pd.DataFrame
) -> pd.DataFrame:
    """Matching strategy: chats -> clients -> lead_events (via client email/phone)."""
    if chats_without_match.empty or clients_df.empty:
        return pd.DataFrame()
    
    chats_with_clients = match_chats_to_clients(chats_without_match, clients_df)
    if chats_with_clients.empty:
        return pd.DataFrame()
    
    matched_results = []
    for _, row in chats_with_clients.iterrows():
        client_email = row.get('email_clients')
        client_phone = row.get('phone_number_client')
        
        match_result = find_user_pseudo_ids_from_client(
            pd.Series([client_email]), pd.Series([client_phone]), lead_events_all
        )
        
        user_pseudo_ids = match_result['user_pseudo_ids']
        match_methods = match_result['match_methods']
        
        if user_pseudo_ids:
            for user_id in user_pseudo_ids:
                match_method = match_methods.get(user_id, 'unknown')
                if match_method == 'email':
                    match_source = 'via_client_email'
                elif match_method == 'phone':
                    match_source = 'via_client_phone'
                elif match_method == 'both':
                    match_source = 'via_client_both'
                else:
                    match_source = 'via_client'
                
                result_row = row.to_dict()
                
                channel_info = {'channel': None, 'event_datetime': None, 'event_name': None, 'source': None, 'medium': None, 'source_name': None, 'campaign_name': None, 'page_location': None}
                if 'conversation_start' in row and pd.notna(row.get('conversation_start')):
                    chat_start = pd.to_datetime(row.get('conversation_start'))
                    channel_info = get_channel_from_previous_event(user_id, chat_start, lead_events_all)
                    
                    if channel_info['event_datetime'] is None:
                        user_events_all = lead_events_all[lead_events_all['user_pseudo_id'] == user_id]
                        if not user_events_all.empty:
                            first_event = user_events_all.sort_values('event_datetime').iloc[0]
                            channel_info = {
                                'channel': first_event.get('channel') if 'channel' in first_event.index else None,
                                'event_datetime': first_event.get('event_datetime'),
                                'event_name': first_event.get('event_name') if 'event_name' in first_event.index else None,
                                'source': first_event.get('source') if 'source' in first_event.index else None,
                                'medium': first_event.get('medium') if 'medium' in first_event.index else None,
                                'source_name': first_event.get('source_name') if 'source_name' in first_event.index else None,
                                'campaign_name': first_event.get('campaign_name') if 'campaign_name' in first_event.index else None,
                                'page_location': first_event.get('page_location') if 'page_location' in first_event.index else None
                            }
                
                result_row.update({
                    'user_pseudo_id': user_id, 'match_count': 1, 'time_diff_seconds': None,
                    'matched_spot_id': None, 'device_category': None, 'event_timestamp': None,
                    'match_source': match_source, 'email_clients': client_email,
                    'phone_bq': client_phone, 'spotId_bq': None, 'from_param': None,
                    'channel': channel_info['channel'], 'event_datetime': channel_info['event_datetime'],
                    'event_name': channel_info['event_name'], 'source': channel_info['source'],
                    'medium': channel_info['medium'], 'source_name': channel_info['source_name'],
                    'campaign_name': channel_info.get('campaign_name'),
                    'page_location': channel_info.get('page_location')
                })
                
                for col in ['lead_min_at', 'lead_min_type', 'lead_type']:
                    if col in row.index:
                        result_row[col] = row[col]
                
                matched_results.append(result_row)

    return pd.DataFrame(matched_results) if matched_results else pd.DataFrame()


def match_leads_to_chats_by_phone(
    lead_events_all: pd.DataFrame,
    chats_without_match: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Match leads to chats only by phone (phone_clean).
    Usado para recuperar leads con email NULL que tienen teléfono; misma normalización
    (normalize_phone) en ambos lados para consistencia (+52, etc.).
    """
    if lead_events_all.empty or chats_without_match.empty:
        return pd.DataFrame()
    if 'phone_clean' not in lead_events_all.columns and 'phone' not in lead_events_all.columns:
        return pd.DataFrame()
    if 'event_datetime' not in lead_events_all.columns:
        return pd.DataFrame()

    lead_events = lead_events_all.copy()
    lead_events['event_datetime'] = pd.to_datetime(lead_events['event_datetime'])
    if start_date is not None and end_date is not None:
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        if len(str(end_date)) <= 10:
            end_dt = end_dt.normalize() + pd.Timedelta(days=1)
        # Alinear tz: si event_datetime es tz-aware (ej. UTC desde BigQuery), localizar start_dt/end_dt
        col_tz = getattr(lead_events["event_datetime"].dtype, "tz", None)
        if col_tz is not None and start_dt.tzinfo is None:
            start_dt = start_dt.tz_localize("UTC")
        if col_tz is not None and end_dt.tzinfo is None:
            end_dt = end_dt.tz_localize("UTC")
        lead_events = lead_events[
            (lead_events['event_datetime'] >= start_dt) &
            (lead_events['event_datetime'] < end_dt)
        ].copy()
    if lead_events.empty:
        return pd.DataFrame()

    # Usar phone_clean ya normalizado (assets usa normalize_phone); si falta, normalizar phone
    if 'phone_clean' in lead_events.columns:
        lead_events = lead_events[lead_events['phone_clean'].notna()].copy()
        lead_events = lead_events[lead_events['phone_clean'].astype(str).str.strip() != ''].copy()
    else:
        lead_events['phone_clean'] = lead_events['phone'].apply(normalize_phone)
        lead_events = lead_events[lead_events['phone_clean'].notna()].copy()
    if lead_events.empty:
        return pd.DataFrame()

    # Chats: misma normalización que leads (normalize_phone) para consistencia en el merge
    chats = chats_without_match.copy()
    if 'phone_clean' in chats.columns:
        na_mask = chats['phone_clean'].isna()
        if na_mask.any() and 'phone_number' in chats.columns:
            chats.loc[na_mask, 'phone_clean'] = chats.loc[na_mask, 'phone_number'].apply(normalize_phone)
    elif 'phone_number' in chats.columns:
        chats['phone_clean'] = chats['phone_number'].apply(normalize_phone)
    else:
        return pd.DataFrame()
    chats = chats[chats['phone_clean'].notna()].copy()
    chats['phone_clean'] = chats['phone_clean'].astype(str).str.strip()
    chats = chats[chats['phone_clean'] != ''].copy()
    if chats.empty:
        return pd.DataFrame()

    lead_events['phone_clean'] = lead_events['phone_clean'].astype(str).str.strip()
    # Por cada chat sin match, buscar leads con el mismo phone_clean y elegir el más cercano en tiempo
    matched_results = []
    for _, chat_row in chats.iterrows():
        chat_phone = chat_row.get('phone_clean')
        if pd.isna(chat_phone) or str(chat_phone).strip() == '':
            continue
        chat_start = pd.to_datetime(chat_row.get('conversation_start')) if pd.notna(chat_row.get('conversation_start')) else None
        leads_same_phone = lead_events[lead_events['phone_clean'] == str(chat_phone).strip()].copy()
        if leads_same_phone.empty:
            continue
        if chat_start is not None:
            leads_same_phone['time_diff'] = (pd.to_datetime(leads_same_phone['event_datetime']) - chat_start).dt.total_seconds().abs()
            best = leads_same_phone.loc[leads_same_phone['time_diff'].idxmin()]
        else:
            best = leads_same_phone.sort_values('event_datetime').iloc[0]
        user_id = best['user_pseudo_id']
        channel_info = get_channel_from_previous_event(user_id, chat_start, lead_events_all) if chat_start is not None else {
            'channel': None, 'event_datetime': None, 'event_name': None, 'source': None, 'medium': None, 'source_name': None, 'campaign_name': None, 'page_location': None
        }
        if chat_start is None and not lead_events_all.empty:
            user_events = lead_events_all[lead_events_all['user_pseudo_id'] == user_id]
            if not user_events.empty:
                first_event = user_events.sort_values('event_datetime').iloc[0]
                channel_info = {
                    'channel': first_event.get('channel'), 'event_datetime': first_event.get('event_datetime'),
                    'event_name': first_event.get('event_name'), 'source': first_event.get('source'),
                    'medium': first_event.get('medium'), 'source_name': first_event.get('source_name'),
                    'campaign_name': first_event.get('campaign_name'), 'page_location': first_event.get('page_location')
                }
        result_row = chat_row.to_dict()
        result_row.update({
            'user_pseudo_id': user_id, 'match_count': 1, 'time_diff_seconds': None,
            'matched_spot_id': None, 'device_category': best.get('device_category'), 'event_timestamp': best.get('event_timestamp'),
            'match_source': 'direct_phone_match', 'email_clients': None, 'phone_bq': best.get('phone'), 'spotId_bq': best.get('spotId'),
            'from_param': best.get('from_param'), 'channel': channel_info.get('channel'), 'event_datetime': channel_info.get('event_datetime'),
            'event_name': channel_info.get('event_name'), 'source': channel_info.get('source'), 'medium': channel_info.get('medium'),
            'source_name': channel_info.get('source_name'), 'campaign_name': channel_info.get('campaign_name'), 'client_id': None,
            'page_location': channel_info.get('page_location')
        })
        matched_results.append(result_row)

    return pd.DataFrame(matched_results) if matched_results else pd.DataFrame()


def match_chats_to_users(
    chats_df: pd.DataFrame, lead_events_df: pd.DataFrame,
    time_buffer_seconds: int = 60, match_source: str = "first_event"
) -> pd.DataFrame:
    """Match chats to users based on spot_id and timestamp proximity."""
    chats = chats_df.copy()
    leads = lead_events_df.copy()
    
    if 'first_user_message_timestamp' in chats.columns:
        chats['first_user_message_timestamp'] = pd.to_datetime(chats['first_user_message_timestamp'])
    leads['event_datetime'] = pd.to_datetime(leads['event_datetime'])
    
    matched_results = []
    for _, chat_row in chats.iterrows():
        chat_timestamp = chat_row.get('first_user_message_timestamp')
        chat_spot_id = chat_row.get('spot_id')
        all_spot_ids = chat_row.get('all_spot_ids', [])
        
        if pd.isna(chat_timestamp):
            matched_results.append(create_empty_match_result(chat_row.to_dict()))
            continue
        
        matches = apply_dual_matching_strategy(
            chat_spot_id, all_spot_ids, leads, chat_timestamp, time_buffer_seconds
        )
        
        if matches.empty:
            matched_results.append(create_empty_match_result(chat_row.to_dict()))
        else:
            match_counts = matches.groupby('user_pseudo_id').size().to_dict()
            for _, match_row in matches.iterrows():
                matched_results.append(
                    create_match_result(
                        chat_row.to_dict(), match_row,
                        match_counts.get(match_row['user_pseudo_id'], 1),
                        match_source, chat_spot_id
                    )
                )
    
    return pd.DataFrame(matched_results)


def match_subsequent_events(
    chats_without_match: pd.DataFrame, lead_events_all: pd.DataFrame,
    user_pseudo_ids, max_time_diff_minutes: int = 5
) -> pd.DataFrame:
    """Search for matches in subsequent events for chats without match."""
    if chats_without_match.empty:
        return pd.DataFrame()
    
    events_filtered = filter_subsequent_events(lead_events_all, user_pseudo_ids)
    if events_filtered.empty:
        return pd.DataFrame()
    
    events_prepared = prepare_lead_events_for_matching(
        events_filtered, start_date=None, end_date=None
    )
    
    if events_prepared.empty:
        return pd.DataFrame()
    
    max_time_diff_seconds = max_time_diff_minutes * 60
    matched_results = []
    
    if 'first_user_message_timestamp' in chats_without_match.columns:
        chats_without_match = chats_without_match.copy()
        chats_without_match['first_user_message_timestamp'] = pd.to_datetime(
            chats_without_match['first_user_message_timestamp']
        )
    
    events_prepared['event_datetime'] = pd.to_datetime(events_prepared['event_datetime'])
    
    for _, chat_row in chats_without_match.iterrows():
        chat_timestamp = chat_row.get('first_user_message_timestamp')
        chat_spot_id = chat_row.get('spot_id')
        all_spot_ids = chat_row.get('all_spot_ids', [])
        
        if pd.isna(chat_timestamp):
            matched_results.append(create_empty_match_result(chat_row.to_dict()))
            continue
        
        time_diff = (events_prepared['event_datetime'] - chat_timestamp).dt.total_seconds().abs()
        events_in_window = events_prepared[time_diff <= max_time_diff_seconds].copy()
        
        if events_in_window.empty:
            matched_results.append(create_empty_match_result(chat_row.to_dict()))
            continue
        
        matches = apply_dual_matching_strategy(
            chat_spot_id, all_spot_ids, events_in_window, chat_timestamp, max_time_diff_seconds
        )
        
        if matches.empty:
            matched_results.append(create_empty_match_result(chat_row.to_dict()))
        else:
            matches['time_diff'] = (matches['event_datetime'] - chat_timestamp).dt.total_seconds().abs()
            matches = matches.sort_values('time_diff')
            match_row = matches.iloc[0]
            match_count = len(matches[matches['user_pseudo_id'] == match_row['user_pseudo_id']])
            
            match_result = create_match_result(
                chat_row.to_dict(), match_row, match_count,
                'subsequent_event', chat_spot_id
            )
            
            if 'conversation_start' in chat_row and pd.notna(chat_row.get('conversation_start')):
                chat_start = pd.to_datetime(chat_row.get('conversation_start'))
                channel_info = get_channel_from_previous_event(
                    match_row['user_pseudo_id'], chat_start, lead_events_all
                )
                if channel_info['channel'] is not None:
                    match_result['channel'] = channel_info['channel']
                if channel_info['event_datetime'] is not None:
                    match_result['event_datetime'] = channel_info['event_datetime']
                if channel_info['event_name'] is not None:
                    match_result['event_name'] = channel_info['event_name']
                if channel_info['source'] is not None:
                    match_result['source'] = channel_info['source']
                if channel_info['medium'] is not None:
                    match_result['medium'] = channel_info['medium']
                if channel_info['source_name'] is not None:
                    match_result['source_name'] = channel_info['source_name']
            
            matched_results.append(match_result)
    
    return pd.DataFrame(matched_results) if matched_results else pd.DataFrame()


def match_chats_to_lead_events(
    df_grouped: pd.DataFrame, df_first: pd.DataFrame,
    start_date: Optional[str] = None, end_date: Optional[str] = None,
    time_buffer_seconds: int = 60, lead_events_all: Optional[pd.DataFrame] = None,
    try_subsequent_events: bool = False, max_time_diff_minutes: int = 5,
    clients_df: Optional[pd.DataFrame] = None, deduplicate_matches: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Complete pipeline: match chats to lead events."""
    chats_processed = process_chats_for_matching(df_grouped)
    chats_filtered = filter_conversations_by_date_range(
        chats_processed, start_date=start_date, end_date=end_date
    )
    chats_first = get_first_chat_per_phone_in_window(chats_filtered)
    
    matched_direct = pd.DataFrame()
    if clients_df is not None and lead_events_all is not None:
        users_in_range = set()
        if start_date and end_date:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            if len(str(end_date)) <= 10:
                end_dt = end_dt.normalize() + pd.Timedelta(days=1)
            # Alinear tz: si event_datetime es tz-aware (ej. UTC desde BigQuery), localizar start_dt/end_dt
            col_tz = getattr(lead_events_all["event_datetime"].dtype, "tz", None)
            if col_tz is not None and start_dt.tzinfo is None:
                start_dt = start_dt.tz_localize("UTC")
            if col_tz is not None and end_dt.tzinfo is None:
                end_dt = end_dt.tz_localize("UTC")
            users_in_range = set(lead_events_all[
                (lead_events_all['event_datetime'] >= start_dt) &
                (lead_events_all['event_datetime'] < end_dt)
            ]['user_pseudo_id'].unique())
        
        if users_in_range:
            lead_events_for_direct = lead_events_all[lead_events_all['user_pseudo_id'].isin(users_in_range)].copy()
        else:
            lead_events_for_direct = lead_events_all.copy()
        
        matched_direct = match_users_to_clients_direct(lead_events_for_direct, clients_df, chats_first)
    
    matched_via_clients = pd.DataFrame()
    if clients_df is not None and lead_events_all is not None:
        if not matched_direct.empty:
            matched_phone_numbers_direct = set(matched_direct['phone_number'].unique())
            chats_for_via_clients = chats_first[
                ~chats_first['phone_number'].isin(matched_phone_numbers_direct)
            ]
        else:
            chats_for_via_clients = chats_first
        
        matched_via_clients = match_via_clients(chats_for_via_clients, clients_df, lead_events_all)
    
    lead_events_prepared = prepare_lead_events_for_matching(
        df_first, start_date=start_date, end_date=end_date
    )
    
    matched_phone_numbers_all = set()
    if not matched_direct.empty:
        matched_phone_numbers_all.update(matched_direct['phone_number'].unique())
    if not matched_via_clients.empty:
        matched_phone_numbers_all.update(matched_via_clients['phone_number'].unique())
    
    if matched_phone_numbers_all:
        chats_for_normal_matching = chats_first[
            ~chats_first['phone_number'].isin(matched_phone_numbers_all)
        ]
    else:
        chats_for_normal_matching = chats_first
    
    matched_first = match_chats_to_users(
        chats_for_normal_matching, lead_events_prepared,
        time_buffer_seconds=time_buffer_seconds, match_source='first_event'
    )
    
    if not matched_direct.empty:
        matched_first = safe_concat(matched_direct, matched_first)
    
    if not matched_via_clients.empty:
        columns_to_drop = ['phone_number_client']
        columns_to_drop = [col for col in columns_to_drop if col in matched_via_clients.columns]
        if columns_to_drop:
            matched_via_clients = matched_via_clients.drop(columns=columns_to_drop)
        
        matched_first = safe_concat(matched_via_clients, matched_first)

    # Match por teléfono: recuperar leads con email NULL (o sin match por spot/cliente) usando solo phone_clean
    if lead_events_all is not None and not matched_first.empty:
        chats_without_match = matched_first[matched_first['user_pseudo_id'].isna()].copy()
        if not chats_without_match.empty:
            matched_by_phone = match_leads_to_chats_by_phone(
                lead_events_all, chats_without_match, start_date=start_date, end_date=end_date
            )
            if not matched_by_phone.empty:
                phones_matched = set(matched_by_phone['phone_number'].dropna().unique())
                still_unmatched = chats_without_match[~chats_without_match['phone_number'].isin(phones_matched)]
                matched_with_user = matched_first[matched_first['user_pseudo_id'].notna()]
                matched_first = safe_concat(matched_with_user, matched_by_phone)
                if not still_unmatched.empty:
                    matched_first = safe_concat(matched_first, still_unmatched)

    if try_subsequent_events and lead_events_all is not None:
        chats_without_match = matched_first[matched_first['user_pseudo_id'].isna()].copy()
        chats_with_match = matched_first[matched_first['user_pseudo_id'].notna()].copy()

        if not chats_without_match.empty:
            lead_events_sorted = lead_events_all.sort_values('event_datetime')
            lead_events_sorted['rn'] = lead_events_sorted.groupby('user_pseudo_id')['event_datetime'].rank(method='first')
            df_first_all = lead_events_sorted[lead_events_sorted['rn'] == 1].copy()
            user_pseudo_ids = df_first_all['user_pseudo_id'].unique()

            matched_subsequent = match_subsequent_events(
                chats_without_match, lead_events_all, user_pseudo_ids,
                max_time_diff_minutes=max_time_diff_minutes
            )

            if not matched_subsequent.empty:
                matched = safe_concat(chats_with_match, matched_subsequent)
            else:
                matched = matched_first
        else:
            matched = matched_first
    else:
        matched = matched_first

    matched = deduplicate_multi_user_matches(matched, enabled=deduplicate_matches)
    
    if not matched.empty and lead_events_all is not None and 'channel' in lead_events_all.columns:
        if 'conversation_start' in matched.columns and 'user_pseudo_id' in matched.columns:
            def enrich_channel_info(row):
                if pd.notna(row.get('user_pseudo_id')) and pd.notna(row.get('conversation_start')):
                    chat_start = pd.to_datetime(row['conversation_start'])
                    channel_info = get_channel_from_previous_event(
                        row['user_pseudo_id'], chat_start, lead_events_all
                    )
                    return pd.Series({
                        'channel': channel_info['channel'] if channel_info['channel'] is not None else row.get('channel'),
                        'event_datetime': channel_info['event_datetime'],
                        'event_name': channel_info['event_name'],
                        'source': channel_info['source'],
                        'medium': channel_info['medium'],
                        'source_name': channel_info['source_name'],
                        'campaign_name': channel_info.get('campaign_name')
                    })
                return pd.Series({
                    'channel': row.get('channel'),
                    'event_datetime': row.get('event_datetime'),
                    'event_name': row.get('event_name'),
                    'source': row.get('source'),
                    'medium': row.get('medium'),
                    'source_name': row.get('source_name'),
                    'campaign_name': row.get('campaign_name')
                })
            
            mask = matched['channel'].isna() | (matched['channel'] == '')
            if mask.any():
                enriched = matched[mask].apply(enrich_channel_info, axis=1)
                matched.loc[mask, 'channel'] = enriched['channel']
                rows_to_update = matched[mask]
                mask_no_datetime = rows_to_update['event_datetime'].isna()
                if mask_no_datetime.any():
                    matched.loc[rows_to_update[mask_no_datetime].index, 'event_datetime'] = enriched.loc[mask_no_datetime, 'event_datetime']
                mask_no_name = rows_to_update['event_name'].isna()
                if mask_no_name.any():
                    matched.loc[rows_to_update[mask_no_name].index, 'event_name'] = enriched.loc[mask_no_name, 'event_name']
                for col in ['source', 'medium', 'source_name']:
                    if col in matched.columns:
                        mask_no_col = rows_to_update[col].isna() if col in rows_to_update.columns else pd.Series([True] * len(rows_to_update), index=rows_to_update.index)
                        if mask_no_col.any():
                            matched.loc[rows_to_update[mask_no_col].index, col] = enriched.loc[mask_no_col, col]
                    else:
                        matched[col] = None
                        if mask.any():
                            matched.loc[mask, col] = enriched[col]
    
    if clients_df is not None and not matched.empty:
        matched = enrich_with_client_id(matched, clients_df)
        
    return (matched, chats_first, lead_events_prepared)

