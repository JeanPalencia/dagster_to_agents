"""Functions for fetching and processing conversations from Chatbot."""
import pandas as pd
from pathlib import Path
from typing import Optional

from dagster_pipeline.defs.visitors_to_leads.utils.text_and_labeling_utils import label_conversations
from dagster_pipeline.defs.visitors_to_leads.utils.database import get_engine
from dagster_pipeline.defs.visitors_to_leads.utils.phone_utils import normalize_phone

QUERIES_DIR = Path(__file__).parent.parent / "queries"


def get_messages_query(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    previous_day_start: Optional[str] = None
) -> str:
    """Generate SQL query for conversation messages with optional date filters."""
    # Build CTE conditions (phones with messages in date range)
    cte_conditions = []
    if start_date:
        cte_conditions.append(f"m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' >= '{start_date}'")
    if end_date:
        cte_conditions.append(f"m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' <= '{end_date}'")
    
    cte_where = " AND ".join(cte_conditions) if cte_conditions else "1=1"
    
    # Build main conditions (all messages for those phones)
    main_conditions = []
    if previous_day_start:
        main_conditions.append(f"m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' >= '{previous_day_start}'")
    if end_date:
        main_conditions.append(f"m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' <= '{end_date}'")
    
    main_where = " AND ".join(main_conditions) if main_conditions else "1=1"
    
    # Load query template from file
    query_path = QUERIES_DIR / "conversations.sql"
    with open(query_path, "r") as f:
        query_template = f.read()
    
    return query_template.format(cte_where=cte_where, main_where=main_where)


def fetch_conversations(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    database: str = "chatbot"
) -> pd.DataFrame:
    """Fetch conversation messages from database."""
    previous_day_start = None
    if start_date:
        start_dt = pd.to_datetime(start_date)
        previous_day_start_dt = start_dt - pd.Timedelta(days=1)
        previous_day_start = previous_day_start_dt.strftime('%Y-%m-%d %H:%M:%S')
    
    query = get_messages_query(start_date, end_date, previous_day_start)
    engine = get_engine(database)
    df = pd.read_sql(query, engine)
    
    if 'phone_number' in df.columns:
        df['phone_clean'] = df['phone_number'].apply(normalize_phone)
    
    return df


def group_conversations(df: pd.DataFrame, gap_hours: int = 24) -> pd.DataFrame:
    """Group messages by phone_number, separating conversations if gap > gap_hours."""
    df = df.copy()
    df['created_at_utc'] = pd.to_datetime(df['created_at_utc'])
    df['type_priority'] = df['type'].map({'user': 0, 'assistant': 1})
    df = df.sort_values(['phone_number', 'created_at_utc', 'type_priority', 'id']).drop('type_priority', axis=1)
    
    conversations_data = []
    
    for phone, group in df.groupby('phone_number'):
        group['time_diff'] = group['created_at_utc'].diff().dt.total_seconds() / 3600
        group['conversation_id'] = (group['time_diff'] > gap_hours).cumsum()
        
        phone_clean = normalize_phone(phone)
        
        for conv_id, conv_group in group.groupby('conversation_id'):
            first_msg = conv_group.iloc[0]
            last_msg = conv_group.iloc[-1]
            conversation_lines = [
                f"[{row['created_at_utc'].strftime('%Y-%m-%d %H:%M:%S')}] {row['type']}: {row.get('message_body', '') or ''}"
                for _, row in conv_group.iterrows()
            ]
            
            conversations_data.append({
                'phone_number': phone,
                'phone_clean': phone_clean,
                'conversation_id': f"{phone}_{conv_id}",
                'conversation_start': conv_group['created_at_utc'].min(),
                'conversation_end': conv_group['created_at_utc'].max(),
                'conversation_text': '\n'.join(conversation_lines),
                'message_count': len(conv_group),
                'messages': conv_group.to_dict('records'),
                'first_message': first_msg['message_body'],
                'first_message_type': first_msg['type'],
                'first_message_timestamp': first_msg['created_at_utc'].strftime('%Y-%m-%d %H:%M:%S'),
                'last_message': last_msg['message_body'],
                'last_message_type': last_msg['type'],
                'last_message_timestamp': last_msg['created_at_utc'].strftime('%Y-%m-%d %H:%M:%S')
            })
    
    conversations_df = pd.DataFrame(conversations_data)
    conversations_df = label_conversations(conversations_df)
    
    return conversations_df

