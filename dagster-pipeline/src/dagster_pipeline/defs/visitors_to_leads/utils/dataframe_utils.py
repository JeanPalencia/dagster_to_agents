"""Utilities for DataFrame operations."""
import pandas as pd
from typing import Optional


COLUMN_MAPPING = {'email': 'email_bq', 'phone': 'phone_bq', 'spotId': 'spotId_bq'}


def safe_concat(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """Safely concatenate two DataFrames, aligning columns and dropping all-NA columns."""
    if df1.empty and df2.empty:
        return pd.DataFrame()
    if df1.empty:
        return df2.copy()
    if df2.empty:
        return df1.copy()
    
    df1_clean = df1.dropna(axis=1, how='all')
    df2_clean = df2.dropna(axis=1, how='all')
    
    result = pd.concat([df1_clean, df2_clean], ignore_index=True, sort=False)
    result = result.dropna(axis=1, how='all')
    
    return result


def safe_concat_multiple(*dataframes):
    """Safely concatenate multiple dataframes."""
    if not dataframes:
        return pd.DataFrame()
    result = dataframes[0]
    for df in dataframes[1:]:
        result = safe_concat(result, df)
    return result


def prepare_lead_events_for_matching(
    df_first: pd.DataFrame, start_date: Optional[str] = None, end_date: Optional[str] = None
) -> pd.DataFrame:
    """Prepare lead_events (df_first) for matching with chats."""
    from dagster_pipeline.defs.visitors_to_leads.utils.url_processing import extract_spot_id_robust, has_spot_url_structure
    
    df = df_first.copy()
    df = df[df['event_name'] == 'clientRequestedWhatsappForm'].copy()
    
    if df.empty:
        return pd.DataFrame(columns=[
            'user_pseudo_id', 'event_datetime', 'event_timestamp', 'page_location',
            'device_category', 'spot_id', 'has_spot_url_structure', 'channel'
        ])
    
    df['event_datetime'] = pd.to_datetime(df['event_datetime'])
    
    if start_date is None:
        start_date = df['event_datetime'].min()
    else:
        start_date = pd.to_datetime(start_date)
    
    if end_date is None:
        end_date = df['event_datetime'].max()
    else:
        end_date = pd.to_datetime(end_date) + pd.Timedelta(days=1)
    
    df = df[(df['event_datetime'] >= start_date) & (df['event_datetime'] < end_date)].copy()
    
    df['spot_id'] = df['page_location'].apply(extract_spot_id_robust)
    df['has_spot_url_structure'] = df['page_location'].apply(has_spot_url_structure)
    
    columns_to_select = ['user_pseudo_id', 'event_datetime', 'page_location',
                        'device_category', 'spot_id', 'has_spot_url_structure']
    
    if 'event_timestamp' in df.columns:
        columns_to_select.insert(2, 'event_timestamp')
    
    for col in ['email', 'phone', 'spotId', 'from_param', 'channel', 'event_name', 'source', 'medium', 'source_name', 'campaign_name']:
        if col in df.columns:
            columns_to_select.append(col)
    
    result = df[columns_to_select].copy()
    
    for old_col, new_col in COLUMN_MAPPING.items():
        if old_col in result.columns:
            result = result.rename(columns={old_col: new_col})
    
    return result

