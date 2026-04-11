"""Assets for the Conversation Analysis pipeline."""

import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_pipeline.defs.conversation_analysis.config import (
    ALL_AI_TAGS,
    ConversationConfig,
    ConvVariablesConfig,
    ConvUpsertConfig,
    FinalOutputConfig,
    get_dynamic_start_date,
    get_dynamic_end_date,
)
from dagster_pipeline.defs.conversation_analysis.utils import (
    query_postgres,
    query_mysql,
    query_geospot,
    query_staging,
    phone_formats,
    group_conversations,
    create_conv_variables,
    upload_dataframe_to_s3,
    trigger_table_replacement,
)
from dagster_pipeline.defs.conversation_analysis.utils.s3_upload import sanitize_json_string

from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _normalize_jsonb_column(value) -> str:
    """
    Normalize a JSONB column value to a JSON string.
    Handles both Python objects (from Postgres) and existing JSON strings.
    """
    # Check None first (before pd.isna which can fail on arrays)
    if value is None:
        return '[]'
    
    # If it's a list or dict (from Postgres JSONB), serialize it
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    
    # If it's already a string, validate it's valid JSON
    if isinstance(value, str):
        if not value or value.strip() == '':
            return '[]'
        try:
            parsed = json.loads(value)
            return json.dumps(parsed, ensure_ascii=False)
        except json.JSONDecodeError:
            return '[]'
    
    # Check for NaN/None using scalar check (after type checks)
    try:
        if pd.isna(value):
            return '[]'
    except (ValueError, TypeError):
        pass
    
    # Fallback
    return '[]'


def _normalize_existing_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize JSONB columns in existing data from Geospot.
    Ensures all JSONB columns are JSON strings, not Python objects.
    """
    df_normalized = df.copy()
    
    jsonb_columns = ['conv_messages', 'conv_variables']
    
    for col in jsonb_columns:
        if col in df_normalized.columns:
            df_normalized[col] = df_normalized[col].apply(_normalize_jsonb_column)
    
    return df_normalized


# Categorías de conv_variables que usan IA: si el nuevo valor viene vacío, se conserva el existente
AI_VARIABLE_CATEGORIES = ("score", "ai_tag")


def _is_empty_variable_value(value) -> bool:
    """Considera vacío: None o string vacío (no 0 ni False)."""
    if value is None:
        return True
    if isinstance(value, str) and (not value or not value.strip()):
        return True
    return False


def _merge_conv_variables_for_upsert(
    existing_conv_variables_str,
    new_conv_variables_str,
    ai_categories=AI_VARIABLE_CATEGORIES,
) -> str:
    """
    Fusiona conv_variables: para variables de IA (score, ai_tag), si el nuevo valor
    viene vacío o falta, se conserva el valor existente. Así no se pisan con vacíos
    cuando el proceso se ejecuta sin correr DSPy/AI tags.
    """
    try:
        existing = json.loads(existing_conv_variables_str) if isinstance(existing_conv_variables_str, str) else (existing_conv_variables_str or [])
    except (TypeError, json.JSONDecodeError):
        existing = []
    try:
        new_list = json.loads(new_conv_variables_str) if isinstance(new_conv_variables_str, str) else (new_conv_variables_str or [])
    except (TypeError, json.JSONDecodeError):
        new_list = []

    if not isinstance(existing, list):
        existing = []
    if not isinstance(new_list, list):
        return json.dumps(existing, ensure_ascii=False)

    existing_by_key = {}
    for v in existing:
        if isinstance(v, dict) and "conv_variable_category" in v and "conv_variable_name" in v:
            key = (v["conv_variable_category"], v["conv_variable_name"])
            existing_by_key[key] = v

    new_by_key = {}
    for v in new_list:
        if isinstance(v, dict) and "conv_variable_category" in v and "conv_variable_name" in v:
            key = (v["conv_variable_category"], v["conv_variable_name"])
            new_by_key[key] = v

    merged = []
    seen_keys = set()

    for v in new_list:
        if not isinstance(v, dict) or "conv_variable_category" not in v or "conv_variable_name" not in v:
            merged.append(v)
            continue
        cat = v.get("conv_variable_category")
        name = v.get("conv_variable_name")
        key = (cat, name)
        seen_keys.add(key)
        # Solo conservar existente cuando: nuevo vacío Y existente tiene valor
        # Si existente estaba vacío, sí reemplazar con el nuevo (aunque sea vacío)
        if cat in ai_categories and _is_empty_variable_value(v.get("conv_variable_value")):
            existing_var = existing_by_key.get(key)
            if (
                existing_var is not None
                and not _is_empty_variable_value(existing_var.get("conv_variable_value"))
            ):
                merged.append(existing_var)
            else:
                merged.append(v)
        else:
            merged.append(v)

    for key, v in existing_by_key.items():
        if key not in seen_keys and key[0] in ai_categories:
            merged.append(v)

    return json.dumps(merged, ensure_ascii=False)


# Load SQL queries
QUERIES_DIR = Path(__file__).parent / "queries"


def _load_query(filename: str) -> str:
    """Load SQL query from file."""
    return (QUERIES_DIR / filename).read_text()


# ============================================================================
# ASSET 1: Raw Conversations Data
# ============================================================================

@asset
def conv_raw_conversations(
    context: AssetExecutionContext,
    config: ConversationConfig,
) -> pd.DataFrame:
    """Extract raw conversation messages from PostgreSQL."""
    def body():
        context.log.info("📊 Extracting conversations from PostgreSQL...")
        
        # Calculate dates dynamically at runtime if not provided
        start_date_str = config.start_date if config.start_date else get_dynamic_start_date()
        end_date_str = config.end_date if config.end_date else get_dynamic_end_date()
        
        context.log.info(f"📅 Calculated dates - start: {start_date_str}, end: {end_date_str}")
        
        start_date = pd.Timestamp(f"{start_date_str} 00:00:00")
        end_date = pd.Timestamp(f"{end_date_str} 23:59:59")
        previous_day_start = start_date - pd.Timedelta(days=1)
        
        context.log.info(f"📅 Date range: {start_date} to {end_date}")
        
        # Load and format query
        query_template = _load_query("messages.sql")
        query = query_template.format(
            start_date=start_date.strftime('%Y-%m-%d %H:%M:%S'),
            end_date=end_date.strftime('%Y-%m-%d %H:%M:%S'),
            previous_day_start=previous_day_start.strftime('%Y-%m-%d %H:%M:%S'),
        )
        
        df = query_postgres(query)
        
        context.log.info(f"✅ Extracted {len(df)} messages")
        context.log.info(f"📱 Unique phone numbers: {df['phone_number'].nunique()}")
        
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 2: Raw Clients Data
# ============================================================================

@asset
def conv_raw_clients(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract clients data from MySQL (lead_id, phone_number, fecha_creacion)."""
    def body():
        context.log.info("📊 Extracting clients from MySQL...")
        
        query = _load_query("clients.sql")
        df = query_mysql(query)
        
        context.log.info(f"✅ Extracted {len(df)} clients")
        context.log.info(f"📱 Unique phone numbers: {df['phone_number'].nunique()}")
        
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 3: Raw Project Requirements Data
# ============================================================================

@asset
def conv_raw_project_requirements(context: AssetExecutionContext) -> pd.DataFrame:
    """Extract project requirements with calendar appointments from MySQL."""
    def body():
        context.log.info("📊 Extracting project requirements from MySQL...")
        
        query = _load_query("project_requirements.sql")
        df = query_mysql(query)
        
        context.log.info(f"✅ Extracted {len(df)} project requirements")
        context.log.info(f"👤 Unique lead_ids: {df['lead_id'].nunique()}")
        
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 3.5: Raw lk_projects (for segment / seniority)
# ============================================================================

@asset
def conv_raw_lk_projects(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Load lk_projects from Geospot (columns needed for seniority).
    Used by conv_seniority to tag conv_id with segment.seniority_level.
    If lk_projects is in another system, replace this asset with one that uses MCP or query_mysql.
    """
    def body():
        context.log.info("📊 Loading lk_projects (seniority / segment)...")
        query = _load_query("lk_projects.sql")
        df = query_geospot(query)
        context.log.info(f"✅ Loaded {len(df)} lk_projects rows")
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 4: Grouped Conversations
# ============================================================================

@asset
def conv_grouped(
    context: AssetExecutionContext,
    conv_raw_conversations: pd.DataFrame,
) -> pd.DataFrame:
    """Group raw messages into conversations based on time gaps."""
    def body():
        context.log.info("🔄 Grouping conversations...")
        
        df_grouped = group_conversations(conv_raw_conversations, gap_hours=72)
        
        context.log.info(f"✅ Grouped into {len(df_grouped)} conversations")
        
        return df_grouped
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 4.5: Raw Events Data (from Staging)
# ============================================================================

@asset
def conv_raw_events(
    context: AssetExecutionContext,
    conv_raw_conversations: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extract conversation events from Staging database.
    Uses the same date range as the loaded messages (from conv_raw_conversations),
    so you only configure dates once (ConversationConfig on conv_raw_conversations).
    Credentials from .env (STAGE_*).
    """
    def body():
        context.log.info("📊 Extracting conversation_events from Staging...")
        
        # Derive date range from messages already loaded (no duplicate config)
        if conv_raw_conversations.empty or "created_at_utc" not in conv_raw_conversations.columns:
            context.log.warning("⚠️ No messages to derive date range; using empty events")
            return pd.DataFrame(
                columns=[
                    "conversation_id", "contact_id", "lead_id", "event_type",
                    "event_section", "event_data", "created_at_local",
                ]
            )
        created = pd.to_datetime(conv_raw_conversations["created_at_utc"], errors="coerce").dropna()
        if created.empty:
            context.log.warning("⚠️ No valid created_at_utc; using empty events")
            return pd.DataFrame(
                columns=[
                    "conversation_id", "contact_id", "lead_id", "event_type",
                    "event_section", "event_data", "created_at_local",
                ]
            )
        min_ts = created.min()
        max_ts = created.max()
        previous_day_start = min_ts - pd.Timedelta(days=1)
        end_date = max_ts + pd.Timedelta(seconds=1)  # include max timestamp
        
        context.log.info(f"📅 Event date range from messages: {previous_day_start} to {end_date}")
        
        query_template = _load_query("chat_events.sql")
        query = query_template.format(
            previous_day_start=previous_day_start.strftime("%Y-%m-%d %H:%M:%S"),
            end_date=end_date.strftime("%Y-%m-%d %H:%M:%S"),
        )
        
        df = query_staging(query)
        
        context.log.info(f"✅ Extracted {len(df)} events")
        context.log.info(f"📱 Unique contact_ids: {df['contact_id'].nunique()}")
        context.log.info(f"💬 Unique conversation_ids: {df['conversation_id'].nunique()}")
        context.log.info(f"📍 Event types: {df['event_type'].unique().tolist()}")
        
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 4.6: Conversations with Events (enrich messages with event_type)
# ============================================================================

def _parse_timestamp(ts):
    """Parse timestamp string to datetime."""
    from datetime import datetime
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, str):
        for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S']:
            try:
                return datetime.strptime(ts, fmt)
            except:
                continue
    return None


def _enrich_messages_with_events(messages, events_df, max_lookback_hours: int = 12):
    """
    Enrich each message with event_type and event_section based on timestamp.
    
    Args:
        messages: List of message dicts or JSON string
        events_df: DataFrame with events for this conversation_id (already filtered)
        max_lookback_hours: Maximum hours to look back for events (default 12).
                           Events older than this won't be assigned to avoid
                           contamination from previous conversations.
    
    Returns:
        List of enriched message dicts (always returns a list, even if empty)
    """
    from datetime import timedelta
    
    # Always return a list
    if not messages:
        return []
    
    if events_df.empty:
        # Return messages as-is if no events, but ensure it's a list
        if isinstance(messages, list):
            return messages
        return []
    
    # Parse messages if string
    if isinstance(messages, str):
        try:
            messages = json.loads(messages)
        except:
            return []
    
    if not isinstance(messages, list):
        return []
    
    enriched = []
    max_lookback = timedelta(hours=max_lookback_hours)
    
    # Sort events by timestamp
    events_sorted = events_df.sort_values('created_at_local').reset_index(drop=True)
    event_times = [_parse_timestamp(t) for t in events_sorted['created_at_local'].tolist()]
    event_types = events_sorted['event_type'].tolist()
    event_sections = events_sorted['event_section'].tolist() if 'event_section' in events_sorted.columns else [None] * len(events_sorted)
    
    for msg in messages:
        msg_copy = dict(msg)
        
        # Get message timestamp (created_at_utc field)
        msg_ts = msg.get('created_at_utc')
        msg_dt = _parse_timestamp(msg_ts)
        
        if msg_dt is None:
            # No timestamp - no event assignment (can't determine timeframe)
            msg_copy['event_type'] = None
            msg_copy['event_section'] = None
        else:
            # Find the most appropriate event for this message.
            # Strategy:
            # 1. Prefer events BEFORE or AT message time (most recent within lookback window)
            # 2. If no event before, consider events shortly AFTER (within 2 minutes)
            # 3. When multiple events are close, prefer the one with smallest time difference
            current_event_type = None
            current_event_section = None
            forward_lookahead = timedelta(minutes=2)  # Consider events up to 2 minutes after message
            
            best_event_idx = None
            best_time_diff = None
            
            # First pass: find most recent event BEFORE or AT message time
            # When multiple events are very close (within 5 seconds), prefer the first one
            # to capture the full sequence (e.g., explicit_visit_request before scheduling_form_sent)
            for i, event_time in enumerate(event_times):
                if event_time is None:
                    continue
                    
                # Check if event is BEFORE or AT message time
                if event_time <= msg_dt:
                    # Check if event is within the lookback window
                    time_diff = msg_dt - event_time
                    if time_diff <= max_lookback:
                        # If we already have a candidate and both are very close (within 5 seconds),
                        # prefer the earlier event to capture the sequence
                        if best_event_idx is not None and best_time_diff is not None:
                            if time_diff <= timedelta(seconds=5) and best_time_diff <= timedelta(seconds=5):
                                # Both events are very close, prefer the earlier one (smaller index = earlier)
                                if i < best_event_idx:
                                    best_event_idx = i
                                    best_time_diff = time_diff
                            else:
                                # Normal case: prefer the one closer to the message
                                if time_diff < best_time_diff:
                                    best_event_idx = i
                                    best_time_diff = time_diff
                        else:
                            # First candidate found
                            best_event_idx = i
                            best_time_diff = time_diff
                    # else: event too old, don't use it
                elif event_time > msg_dt:
                    # Events are sorted, so we can stop here for "before" events
                    # But continue to check "after" events
                    break
            
            # Second pass: if no event before message (or if we want to check "after" events too),
            # check for events shortly AFTER the message
            # Only use "after" events if we didn't find a "before" event, or if the "after" event is very close
            for i, event_time in enumerate(event_times):
                if event_time is None:
                    continue
                
                # Check if event is AFTER message time
                if event_time > msg_dt:
                    time_diff = event_time - msg_dt
                    if time_diff <= forward_lookahead:
                        # Use "after" event if:
                        # 1. No "before" event was found, OR
                        # 2. The "after" event is closer than the "before" event (within 30 seconds)
                        if best_event_idx is None:
                            best_event_idx = i
                            best_time_diff = time_diff
                        elif time_diff <= timedelta(seconds=30) and time_diff < best_time_diff:
                            # "After" event is very close and closer than "before" event
                            best_event_idx = i
                            best_time_diff = time_diff
                    else:
                        # Events are sorted, so we can stop here
                        break
            
            # Assign the best event found
            if best_event_idx is not None:
                current_event_type = event_types[best_event_idx]
                current_event_section = event_sections[best_event_idx]
            
            msg_copy['event_type'] = current_event_type
            msg_copy['event_section'] = current_event_section
        
        enriched.append(msg_copy)
    
    return enriched


@asset
def conv_with_events(
    context: AssetExecutionContext,
    conv_grouped: pd.DataFrame,
    conv_raw_events: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enrich conversation messages with event_type and event_section.
    
    For each message in conv_messages:
    - Match message.id (conversation_id) with events.conversation_id
    - Assign event_type and event_section based on message timestamp vs event timestamps
    - Preserve temporal order within each conversation_id
    """
    def body():
        context.log.info("🔗 Enriching messages with event_type...")
        
        df = conv_grouped.copy()
        df_events = conv_raw_events.copy()
        
        # Group events by conversation_id (preserve temporal order)
        context.log.info("📊 Grouping events by conversation_id...")
        events_by_conversation = df_events.groupby('conversation_id')
        context.log.info(f"   Found {len(events_by_conversation)} unique conversation_ids in events")
        
        # Enrich messages
        context.log.info("🔄 Enriching messages with event_type...")
        
        def enrich_conversation(row):
            messages = row['messages']
            
            # Ensure we always return a list
            if not messages:
                return []
            
            # Parse messages if needed
            if isinstance(messages, str):
                try:
                    messages = json.loads(messages)
                except:
                    return []
            
            if not isinstance(messages, list):
                return []
            
            # Enrich each message individually with events from its own conversation_id
            enriched = []
            for msg in messages:
                if not isinstance(msg, dict):
                    enriched.append(msg)
                    continue
                
                msg_conv_id = msg.get('id')  # id = conversation_id from messages query
                
                if not msg_conv_id:
                    # No conversation_id, return original message
                    enriched.append(msg)
                    continue
                
                # Get events for this specific conversation_id only
                if msg_conv_id in events_by_conversation.groups:
                    conv_events = events_by_conversation.get_group(msg_conv_id)
                    # Sort events by time (preserve temporal order)
                    conv_events = conv_events.sort_values('created_at_local').reset_index(drop=True)
                    # Enrich this single message with events from its conversation_id
                    enriched_msg = _enrich_messages_with_events([msg], conv_events)
                    # Ensure we get a list and take first element
                    if isinstance(enriched_msg, list) and len(enriched_msg) > 0:
                        enriched.append(enriched_msg[0])
                    else:
                        enriched.append(msg)  # Fallback to original message if enrichment fails
                else:
                    # No events for this conversation_id, keep message as-is
                    enriched.append(msg)
            
            return enriched
        
        df['messages'] = df.apply(enrich_conversation, axis=1)
        
        # Count enriched
        def has_event_type(messages):
            if isinstance(messages, list) and len(messages) > 0:
                first = messages[0]
                if isinstance(first, dict):
                    return 'event_type' in first and first.get('event_type') is not None
            return False
        
        enriched_count = df['messages'].apply(has_event_type).sum()
        context.log.info(f"✅ Conversations with enriched messages: {enriched_count}/{len(df)}")
        
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 5: Merged Conversation Data (with clients)
# ============================================================================

@asset
def conv_merged(
    context: AssetExecutionContext,
    conv_with_events: pd.DataFrame,
    conv_raw_clients: pd.DataFrame,
) -> pd.DataFrame:
    """Merge conversations with clients data using phone_formats matching."""
    def body():
        context.log.info("🔧 Creating phone number mapping from clients...")
        
        # Create phone map from clients
        clients_phone_map = defaultdict(list)
        for ix, phone in conv_raw_clients['phone_number'].items():
            if pd.notna(phone):
                for pf in phone_formats(phone):
                    clients_phone_map[pf].append(ix)
        
        context.log.info(f"📊 Unique formats in clients map: {len(clients_phone_map)}")
        
        # Merge
        context.log.info("🔗 Executing merge...")
        
        merged_rows = []
        matched_count = 0
        unmatched_count = 0
        
        for conv_idx, conv_row in conv_with_events.iterrows():
            p = conv_row['phone_number']
            search_phones = phone_formats(p)
            matched_indices = set()
            
            for sp in search_phones:
                matched_indices.update(clients_phone_map.get(sp, []))
            
            if matched_indices:
                matched_count += 1
                first_match_idx = list(matched_indices)[0]
                client_row = conv_raw_clients.loc[first_match_idx]
                combined = {
                    **{col: conv_row[col] for col in conv_with_events.columns},
                    'lead_id': client_row['lead_id'],
                    'fecha_creacion_cliente': client_row['fecha_creacion_cliente'],
                }
                merged_rows.append(combined)
            else:
                unmatched_count += 1
                merged = {**{col: conv_row[col] for col in conv_with_events.columns}}
                merged['lead_id'] = pd.NA
                merged['fecha_creacion_cliente'] = pd.NA
                merged_rows.append(merged)
        
        df_merged = pd.DataFrame(merged_rows)
        
        context.log.info(f"✅ Merge completed:")
        context.log.info(f"   - Conversations with match: {matched_count}")
        context.log.info(f"   - Conversations without match: {unmatched_count}")
        context.log.info(f"   - Match rate: {matched_count / (matched_count + unmatched_count) * 100:.1f}%")
        
        return df_merged
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 6: Conversations with Funnel Paths (from events)
# ============================================================================

@asset
def conv_with_funnels(
    context: AssetExecutionContext,
    conv_merged: pd.DataFrame,
    conv_raw_events: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate funnel_type and funnel_section directly from events table.
    
    For each conversation (by conversation_id from messages):
    - Groups events by conversation_id
    - Filters events to conversation time window [conversation_start - 2h, conversation_end + 2h]
    - Orders events chronologically by created_at_local
    - Builds funnel paths by removing consecutive duplicates
    - Adds funnel_type and funnel_section columns
    """
    def body():
        context.log.info("🔄 Calculating funnel paths from events...")
        
        df = conv_merged.copy()
        df_events = conv_raw_events.copy()
        
        # Ensure created_at_local is datetime
        df_events['created_at_local'] = pd.to_datetime(df_events['created_at_local'])
        
        # Group events by conversation_id and sort chronologically
        context.log.info("📊 Grouping events by conversation_id...")
        events_by_conv = df_events.groupby('conversation_id')
        context.log.info(f"   Found {len(events_by_conv)} unique conversation_ids in events")
        
        # Initialize funnel columns
        df['funnel_type'] = None
        df['funnel_section'] = None
        
        # Helper function to build funnel path
        def build_funnel_path(events_df, field_name: str) -> str:
            """Build funnel path from events, removing consecutive duplicates."""
            if events_df.empty:
                return None
            
            # Sort by timestamp
            events_sorted = events_df.sort_values('created_at_local')
            
            # Extract field values
            values = []
            for _, event in events_sorted.iterrows():
                val = event.get(field_name)
                if pd.notna(val) and isinstance(val, str) and val.strip():
                    values.append(val)
            
            # Remove consecutive duplicates
            unique_values = []
            for val in values:
                if not unique_values or unique_values[-1] != val:
                    unique_values.append(val)
            
            # Join with " > " delimiter
            return " > ".join(unique_values) if unique_values else None
        
        # Margen de horas antes/después de la ventana de la conversación para incluir eventos
        FUNNEL_WINDOW_MARGIN_HOURS = 2
    
        # Process each row in df
        # For each conversation, extract all conversation_ids from messages and combine their events
        for idx, row in df.iterrows():
            messages = row.get('messages', [])
            
            # Parse messages if string
            if isinstance(messages, str):
                try:
                    messages = json.loads(messages)
                except:
                    messages = []
            
            if not isinstance(messages, list):
                messages = []
            
            # Extract all unique conversation_ids from messages
            conversation_ids = set()
            for msg in messages:
                if isinstance(msg, dict):
                    conv_id = msg.get('id')
                    if conv_id is not None:
                        conversation_ids.add(conv_id)
            
            if not conversation_ids:
                continue
            
            # Ventana de la conversación con margen (horas antes y después)
            conv_start = pd.to_datetime(row.get('conversation_start'))
            conv_end = pd.to_datetime(row.get('conversation_end'))
            margin = pd.Timedelta(hours=FUNNEL_WINDOW_MARGIN_HOURS)
            window_start = conv_start - margin
            window_end = conv_end + margin
            
            # Collect all events for all conversation_ids in this grouped conversation
            all_events = []
            for conv_id in conversation_ids:
                if conv_id in events_by_conv.groups:
                    conv_events = events_by_conv.get_group(conv_id)
                    all_events.append(conv_events)
            
            if not all_events:
                continue
            
            # Combine all events and sort chronologically
            combined_events = pd.concat(all_events, ignore_index=True)
            combined_events = combined_events.sort_values('created_at_local').reset_index(drop=True)
            
            # Filtrar eventos a la ventana de la conversación (con margen)
            combined_events = combined_events[
                (combined_events['created_at_local'] >= window_start) &
                (combined_events['created_at_local'] <= window_end)
            ].reset_index(drop=True)
            
            # Build funnel paths from combined events (solo los que caen en la ventana)
            funnel_type = build_funnel_path(combined_events, 'event_type')
            funnel_section = build_funnel_path(combined_events, 'event_section')
            
            df.at[idx, 'funnel_type'] = funnel_type
            df.at[idx, 'funnel_section'] = funnel_section
        
        context.log.info(f"✅ Funnel paths calculated:")
        context.log.info(f"   - Conversations with funnel_type: {(df['funnel_type'].notna()).sum()}")
        context.log.info(f"   - Conversations with funnel_section: {(df['funnel_section'].notna()).sum()}")
        context.log.info(f"   - Total conversations: {len(df)}")
        
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 7: Conversations with Project Data
# ============================================================================

@asset
def conv_with_projects(
    context: AssetExecutionContext,
    conv_with_funnels: pd.DataFrame,
    conv_raw_project_requirements: pd.DataFrame,
) -> pd.DataFrame:
    """Add scheduled_visit and created_project counts to conversations.
    
    For each conversation (by lead_id and date range):
    - scheduled_visit: (1) count of visits where visit_created_at in [conv_start, conv_end+5h];
      (2) if that is 0, count of visits where visit_updated_at in that range;
      (3) if still 0, count of event_type=visit_scheduled in conversation messages (events).
    - created_project: count of projects created between (conv_start - 3 hours) and conv_end
                       (includes projects created up to 3 hours before conversation started)
    """
    def body():
        from datetime import timedelta
        
        context.log.info("📅 Adding project/visit counts to conversations...")
        
        df = conv_with_funnels.copy()
        df_projects = conv_raw_project_requirements.copy()
        
        # Ensure datetime columns
        df['conversation_start'] = pd.to_datetime(df['conversation_start'])
        df['conversation_end'] = pd.to_datetime(df['conversation_end'])
        df_projects['visit_created_at'] = pd.to_datetime(df_projects['visit_created_at'])
        if 'visit_updated_at' in df_projects.columns:
            df_projects['visit_updated_at'] = pd.to_datetime(df_projects['visit_updated_at'])
        df_projects['project_created_at'] = pd.to_datetime(df_projects['project_created_at'])
        
        # Lookback window for projects (3 hours before conv_start)
        PROJECT_LOOKBACK = timedelta(hours=3)
        # For scheduled_visit: allow 5 hours after conv_end (creation/update can be delayed)
        VISIT_WINDOW_AFTER_END = timedelta(hours=5)
        
        # Initialize counts
        df['scheduled_visit_count'] = 0
        df['created_project_count'] = 0
        
        # Process only rows with lead_id
        rows_with_lead = df[df['lead_id'].notna()].index
        context.log.info(f"📊 Processing {len(rows_with_lead)} conversations with lead_id...")
        
        for idx in rows_with_lead:
            row = df.loc[idx]
            lead_id = row['lead_id']
            conv_start = row['conversation_start']
            conv_end = row['conversation_end']
            visit_end = conv_end + VISIT_WINDOW_AFTER_END  # allow 5h after conv end
            
            # Filter projects for this lead
            lead_projects = df_projects[df_projects['lead_id'] == lead_id]
            
            if not lead_projects.empty:
                # 1) Count visits created between conv_start and conv_end + 5h
                visits_in_range = lead_projects[
                    (lead_projects['visit_created_at'] >= conv_start) & 
                    (lead_projects['visit_created_at'] <= visit_end) &
                    (lead_projects['visit_created_at'].notna())
                ]
                count_by_created = len(visits_in_range)
                # 2) If none by creation, second validation: visits updated in that range (e.g. visit associated to older project but agendada in this conversation)
                if count_by_created > 0:
                    df.loc[idx, 'scheduled_visit_count'] = count_by_created
                elif 'visit_updated_at' in lead_projects.columns:
                    visits_updated_in_range = lead_projects[
                        (lead_projects['visit_updated_at'] >= conv_start) &
                        (lead_projects['visit_updated_at'] <= visit_end) &
                        (lead_projects['visit_updated_at'].notna())
                    ]
                    df.loc[idx, 'scheduled_visit_count'] = len(visits_updated_in_range)
                else:
                    df.loc[idx, 'scheduled_visit_count'] = 0
                
                # Count projects created between (conv_start - 3 hours) and conv_end
                # This captures projects created shortly before the conversation started
                project_start_window = conv_start - PROJECT_LOOKBACK
                projects_in_range = lead_projects[
                    (lead_projects['project_created_at'] >= project_start_window) & 
                    (lead_projects['project_created_at'] <= conv_end) &
                    (lead_projects['project_created_at'].notna())
                ]
                df.loc[idx, 'created_project_count'] = len(projects_in_range)
        
        # Second validation for scheduled_visit: when still 0, count event_type=visit_scheduled in conversation events (from messages)
        zero_visit_idx = df[df['scheduled_visit_count'] == 0].index
        filled_by_events = 0
        for idx in zero_visit_idx:
            messages = df.at[idx, 'messages']
            if isinstance(messages, str):
                try:
                    messages = json.loads(messages)
                except Exception:
                    messages = []
            if not isinstance(messages, list):
                continue
            count_visit_scheduled = sum(
                1 for m in messages
                if isinstance(m, dict) and (m.get('event_type') or '').strip() == 'visit_scheduled'
            )
            if count_visit_scheduled > 0:
                df.at[idx, 'scheduled_visit_count'] = count_visit_scheduled
                filled_by_events += 1
        if filled_by_events > 0:
            context.log.info(f"📅 scheduled_visit: segunda validación por eventos → {filled_by_events} conversaciones con visit_scheduled en eventos")
        
        # Log stats
        total_visits = df['scheduled_visit_count'].sum()
        total_projects = df['created_project_count'].sum()
        conv_with_visits = (df['scheduled_visit_count'] > 0).sum()
        conv_with_projects = (df['created_project_count'] > 0).sum()
        
        context.log.info(f"✅ Project data added:")
        context.log.info(f"   - Total scheduled visits: {total_visits}")
        context.log.info(f"   - Conversations with visits: {conv_with_visits}")
        context.log.info(f"   - Total created projects: {total_projects}")
        context.log.info(f"   - Conversations with projects: {conv_with_projects}")
        
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 7.5: Seniority per conversation (segment.seniority_level)
# ============================================================================

@asset
def conv_seniority(
    context: AssetExecutionContext,
    conv_with_projects: pd.DataFrame,
    conv_raw_lk_projects: pd.DataFrame,
) -> pd.DataFrame:
    """
    Tag each conv_id with seniority (SS, Top SS, Jr, Mid, Sr) from lk_projects.
    Joins by lead_id only (lk_projects as single source); takes most recent project per lead
    (project_updated_at DESC). Avoids MySQL project_requirements so IDs always match.
    Output: conversation_id, seniority_level (for merge into conv_variables as segment.seniority_level).
    """
    def body():
        from dagster_pipeline.defs.conversation_analysis.utils.variables.segment import (
            build_conv_seniority_map,
        )
    
        context.log.info("📊 Building seniority (segment) per conversation from lk_projects by lead_id...")
        df = build_conv_seniority_map(conv_with_projects, conv_raw_lk_projects)
        tagged = df["seniority_level"].notna().sum()
        context.log.info(f"✅ Seniority: {tagged} conversations with segment.seniority_level")
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 8: Conversation with Variables
# ============================================================================

def _compute_incremental_mask(
    df: pd.DataFrame,
    query_geospot_fn,
    table_name: str,
    log=None,
) -> tuple:
    """
    Compute which rows are new or changed (to_process) by comparing with Geospot.
    Signature: conv_id in Geospot AND len(messages) == len(conv_messages) -> skip.
    When the user adds messages, len(conv_messages) changes, so we reprocess.
    Query uses a wider date window (+/- 3 days) so we don't miss rows.
    Returns (df with _to_process column, df_geospot for merge, or (df, None) on error).
    """
    _log = log if callable(log) else None
    if df.empty:
        df = df.copy()
        df["_to_process"] = True
        return df, None
    df = df.copy()
    try:
        start_ts = pd.to_datetime(df["conversation_start"]).min()
        end_ts = pd.to_datetime(df["conversation_end"]).max()
        start_str = start_ts.strftime("%Y-%m-%d") if hasattr(start_ts, "strftime") else str(start_ts)[:10]
        end_str = end_ts.strftime("%Y-%m-%d") if hasattr(end_ts, "strftime") else str(end_ts)[:10]
        start_wide = (pd.to_datetime(start_str) - pd.Timedelta(days=3)).strftime("%Y-%m-%d")
        end_wide = (pd.to_datetime(end_str) + pd.Timedelta(days=3)).strftime("%Y-%m-%d")
    except Exception as e:
        if _log:
            _log(f"Incremental mask: failed to get date range: {e}")
        df["_to_process"] = True
        return df, None
    try:
        query = (
            f"SELECT conv_id, conv_start_date, conv_end_date, conv_text, conv_messages, conv_variables "
            f"FROM {table_name} "
            f"WHERE conv_start_date >= '{start_wide}' AND conv_start_date <= '{end_wide}'"
        )
        if _log:
            _log(f"Incremental mask: query date range (wide) [{start_wide}, {end_wide}] (data range [{start_str}, {end_str}])")
        df_geo = query_geospot_fn(query)
    except Exception as e:
        if _log:
            _log(f"Incremental mask: Geospot query failed: {e}")
        df["_to_process"] = True
        return df, None
    if df_geo.empty:
        if _log:
            _log("Incremental mask: Geospot returned 0 rows -> marking all to_process")
        df["_to_process"] = True
        return df, None
    if _log:
        _log(f"Incremental mask: Geospot returned {len(df_geo)} rows")
        sample_geo = list(df_geo["conv_id"].head(3))
        _log(f"Incremental mask: sample conv_ids in Geospot: {sample_geo}")
    # Use same criterion as pipeline: len(sanitize_json_string(...)) so we don't skip incorrectly when DB returns different JSON formatting
    if "conv_messages" not in df_geo.columns:
        df_geo["_sig_len"] = df_geo["conv_text"].astype(str).apply(lambda x: len(sanitize_json_string(x)))
    else:
        df_geo["_sig_len"] = df_geo["conv_messages"].astype(str).apply(lambda x: len(sanitize_json_string(x)))
    df_geo["_cid_norm"] = df_geo["conv_id"].astype(str).str.strip()
    geo_by_cid = df_geo.set_index("_cid_norm")
    geo_index_set = set(geo_by_cid.index)
    to_process = []
    n_not_in_geo = 0
    n_len_mismatch = 0
    for idx in df.index:
        row = df.loc[idx]
        cid = row.get("conversation_id")
        cid_str = (str(cid).strip() if cid is not None else None) or ""
        # Pipeline column is "messages"; use same sanitization as on write so length matches Geospot
        row_messages = row.get("messages") or ""
        row_len = len(sanitize_json_string(row_messages))
        if cid_str not in geo_index_set:
            to_process.append(True)
            n_not_in_geo += 1
            continue
        g = geo_by_cid.loc[cid_str]
        g_len = g["_sig_len"]
        if row_len != g_len:
            to_process.append(True)
            n_len_mismatch += 1
        else:
            to_process.append(False)
    df["_to_process"] = to_process
    n_process = sum(to_process)
    n_skip = len(to_process) - n_process
    if _log:
        _log(f"Incremental mask: {n_process} to_process, {n_skip} skip (conv_id + len(conv_messages) match)")
        _log(f"Incremental mask: breakdown -> not_in_Geospot={n_not_in_geo}, len_mismatch={n_len_mismatch}, skip={n_skip}")
    return df, df_geo


def _get_events_in_conversation_window(
    row,
    events_by_conversation,
    conversation_start,
    conversation_end,
):
    """
    Get events for this row's conversation_ids that fall within [conversation_start, conversation_end].
    Both conversation_start/end and events' created_at_local are already in Mexico time
    (messages.sql and chat_events.sql convert to America/Mexico_City), so we compare naive timestamps as-is.
    """
    messages = row.get("messages", [])
    if isinstance(messages, str):
        try:
            messages = json.loads(messages)
        except Exception:
            return pd.DataFrame()
    if not isinstance(messages, list):
        return pd.DataFrame()

    conversation_ids = set()
    for msg in messages:
        if isinstance(msg, dict):
            cid = msg.get("id")
            if cid is not None:
                conversation_ids.add(cid)

    if not conversation_ids:
        return pd.DataFrame()

    start_ts = pd.to_datetime(conversation_start)
    end_ts = pd.to_datetime(conversation_end)

    parts = []
    for cid in conversation_ids:
        if cid in events_by_conversation.groups:
            part = events_by_conversation.get_group(cid)
            parts.append(part)
    if not parts:
        return pd.DataFrame()

    combined = pd.concat(parts, ignore_index=True)
    combined["_ts"] = pd.to_datetime(combined["created_at_local"])
    in_window = combined[(combined["_ts"] >= start_ts) & (combined["_ts"] <= end_ts)]
    return in_window


@asset
def conv_with_variables(
    context: AssetExecutionContext,
    config: ConvVariablesConfig,
    conv_with_projects: pd.DataFrame,
    conv_raw_events: pd.DataFrame,
    conv_seniority: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add conversation variables to data with project counts.

    Includes experiment variables (test_schedule, test_schedule_result) for convs
    started on or after 2026-01-28, using events within the conversation time window
    and test_schedule_result only from events after explicit_visit_request.
    Uses async batch processing for AI tags (20 concurrent requests).
    """
    def body():
        import time
        from dagster_pipeline.defs.conversation_analysis.utils.variables.ai_tags import (
            process_ai_tags_batch,
        )
        from dagster_pipeline.defs.conversation_analysis.utils.variables.experiments import (
            get_bot_graph_variant,
            get_experiment_variables,
            get_unification_mgs_variant,
        )
        from dagster_pipeline.defs.conversation_analysis.utils.variables.tags import (
            get_follow_up_success,
        )
        from dagster_pipeline.defs.conversation_analysis.utils.variables.funnel_events import (
            get_funnel_events_variables,
        )
        from dagster_pipeline.defs.conversation_analysis.utils.variables.segment import (
            get_seniority_variable,
        )
    
        context.log.info("📊 Creating conversation variables...")
    
        df = conv_with_projects.copy()
        df_events = conv_raw_events.copy()
        df_events["created_at_local"] = pd.to_datetime(df_events["created_at_local"])
        events_by_conversation = df_events.groupby("conversation_id")
    
        # Apply test mode sampling if enabled
        if config.test_mode:
            sample_size = min(config.test_sample_size, len(df))
            df = df.sample(n=sample_size, random_state=42)
            context.log.info(f"🧪 TEST MODE: Using {sample_size} sample records")
    
        # Incremental: detect new/changed conv_id so we only run heavy steps for those (configurable)
        use_mask_raw = getattr(config, "use_incremental_mask", True)
        if use_mask_raw is None:
            use_incremental_mask = True  # default when not set (e.g. config key mismatch)
        elif isinstance(use_mask_raw, str):
            use_incremental_mask = use_mask_raw.lower() in ("true", "1", "yes")
        else:
            use_incremental_mask = bool(use_mask_raw)
        context.log.info(f"📋 use_incremental_mask={use_incremental_mask} (raw={use_mask_raw!r})")
        if use_incremental_mask:
            df, df_geospot = _compute_incremental_mask(
                df, query_geospot, "bt_conv_conversations", log=context.log.info
            )
        else:
            context.log.info("📋 Incremental DISABLED: processing all rows")
            df["_to_process"] = True
            df_geospot = None
        to_process_mask = df["_to_process"]
        n_to_process = to_process_mask.sum()
        n_skip = len(df) - n_to_process
        context.log.info(f"📋 Incremental: {n_to_process} to process (new/changed), {n_skip} unchanged (reuse Geospot)")
    
        # Initialize conv_variables: unchanged rows get existing from Geospot
        df["conv_variables"] = None
        if df_geospot is not None and not df_geospot.empty:
            geo_by_cid = df_geospot.set_index("conv_id")
            for idx in df.index:
                if not df.at[idx, "_to_process"]:
                    cid = df.at[idx, "conversation_id"]
                    if cid in geo_by_cid.index:
                        raw = geo_by_cid.at[cid, "conv_variables"]
                        try:
                            df.at[idx, "conv_variables"] = json.loads(raw) if isinstance(raw, str) else (raw if isinstance(raw, list) else [])
                        except Exception:
                            df.at[idx, "conv_variables"] = []
    
        # STEP 1: Create base variables only for rows to process
        context.log.info("📝 Creating base variables (metrics + tags)...")
        start_time = time.time()
        for idx in df.index:
            if not df.at[idx, "_to_process"]:
                continue
            df.at[idx, "conv_variables"] = create_conv_variables(df.loc[idx], enable_ai_tags=False)
    
        base_time = time.time() - start_time
        context.log.info(f"✅ Base variables created in {base_time:.2f}s")
    
        # STEP 1.5: Add experiment variables only for rows to process
        context.log.info("🧪 Adding experiment variables (test_schedule, test_schedule_result)...")
        exp_count = 0
        for idx in df.index:
            if not df.at[idx, "_to_process"]:
                continue
            row = df.loc[idx]
            events_in_window = _get_events_in_conversation_window(
                row,
                events_by_conversation,
                row.get("conversation_start"),
                row.get("conversation_end"),
            )
            exp_vars = get_experiment_variables(
                events_in_window,
                row.get("conversation_start"),
                row.get("conversation_end"),
            )
            if exp_vars:
                df.at[idx, "conv_variables"] = df.at[idx, "conv_variables"] + exp_vars
                exp_count += 1
            # follow_up_success: 1 if visit_scheduled after scheduling_reminder, 0 if reminder but no visit; omit if no scheduling_reminder
            follow_up_var = get_follow_up_success(row, events_in_window)
            if follow_up_var is not None:
                df.at[idx, "conv_variables"] = df.at[idx, "conv_variables"] + [follow_up_var]
            # unification_mgs: "control" | "combined" from first first_interaction_variant event; omit if event missing
            unification_var = get_unification_mgs_variant(events_in_window)
            if unification_var is not None:
                df.at[idx, "conv_variables"] = df.at[idx, "conv_variables"] + [unification_var]
            # bot_graph: "monolith" | "control" from ab_variants["monolith_graph"]; event priority: conversation_start > spot_link_received > spot_confirmation > other
            bot_graph_var = get_bot_graph_variant(events_in_window)
            if bot_graph_var is not None:
                df.at[idx, "conv_variables"] = df.at[idx, "conv_variables"] + [bot_graph_var]
            # funnel_events: spot_link_to_spot_conf, spot_conf_to_request_visit, visit_request_to_schedule, reco_index, schedule_form_index, profiling_form_index
            funnel_events_vars = get_funnel_events_variables(events_in_window, row)
            if funnel_events_vars:
                df.at[idx, "conv_variables"] = df.at[idx, "conv_variables"] + funnel_events_vars
        context.log.info(f"✅ Experiment variables added for {exp_count} conversations (started >= 2026-01-28 with variant)")
        context.log.info("✅ follow_up_success, unification_mgs, bot_graph and funnel_events added where applicable")
    
        # STEP 2: Add AI tags if enabled, only for rows to process
        if config.enable_openai_analysis:
            df_for_ai = df[df["_to_process"]]
            n_ai = len(df_for_ai)
            context.log.info("🤖 OpenAI analysis ENABLED - Starting async batch processing")
            context.log.info(f"🚀 Processing {n_ai} conversations (incremental) with 20 concurrent requests...")
            
            # When ai_tags_filter is empty, use ALL_AI_TAGS from config (is_broker, info_requested, missed_messages, scaling_agent)
            tags_filter = config.ai_tags_filter if config.ai_tags_filter else ALL_AI_TAGS
            context.log.info(f"🏷️ AI Tags: {tags_filter}")
            
            # Progress callback for logging
            def log_progress(completed: int, total: int):
                context.log.info(f"   🔄 Progress: {completed}/{total} ({completed/total*100:.1f}%)")
            
            # Process AI tags in batch only for _to_process rows
            ai_start_time = time.time()
            ai_tags_by_index = process_ai_tags_batch(
                df_for_ai,
                tags_filter=tags_filter,
                progress_callback=log_progress
            ) if n_ai > 0 else {}
            ai_time = time.time() - ai_start_time
            
            context.log.info(f"✅ AI tags generated in {ai_time:.2f}s")
            if n_ai > 0:
                context.log.info(f"⚡ Avg time per conversation: {ai_time/n_ai*1000:.1f}ms")
            
            # Merge AI tags with base variables (only _to_process rows have new ai_tags)
            context.log.info("🔗 Merging AI tags with base variables...")
            for idx in df.index:
                base_vars = df.at[idx, 'conv_variables']
                ai_tags = ai_tags_by_index.get(idx, [])
                df.at[idx, 'conv_variables'] = base_vars + ai_tags
            
            context.log.info(f"✅ Merged AI tags for {len(df)} records")
        else:
            context.log.info("🔇 OpenAI analysis disabled - skipping AI tags")
    
        # STEP 2.5: Merge seniority (segment.seniority_level) from conv_seniority
        seniority_map = conv_seniority.set_index("conversation_id")["seniority_level"].to_dict()
        seg_count = 0
        for idx in df.index:
            vars_list = df.at[idx, "conv_variables"]
            if not isinstance(vars_list, list):
                continue
            cid = df.at[idx, "conversation_id"]
            level = seniority_map.get(cid)
            var = get_seniority_variable(level)
            if var is not None:
                vars_list.append(var)
                seg_count += 1
            df.at[idx, "conv_variables"] = vars_list
        context.log.info(f"✅ Segment seniority_level added for {seg_count} conversations")
    
        # STEP 3: Serialize to JSON
        context.log.info("📦 Serializing conv_variables to JSON...")
        df['conv_variables'] = df['conv_variables'].apply(
            lambda x: json.dumps(x, ensure_ascii=False)
        )
    
        total_time = time.time() - start_time
        context.log.info(f"✅ Created conv_variables for {len(df)} records in {total_time:.2f}s")
        
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 9: Add DSPy Evaluator Scores
# ============================================================================

@asset
def conv_with_dspy_scores(
    context: AssetExecutionContext,
    config: ConvVariablesConfig,
    conv_with_variables: pd.DataFrame,
) -> pd.DataFrame:
    """
    Agrega evaluaciones DSPy a conv_variables.
    
    Este asset:
    1. Toma conv_variables existentes (JSON string)
    2. Evalúa conversaciones con DSPy en batch (concurrencia limitada)
    3. Agrega nuevas variables de score sin romper el JSON existente
    4. Integra con LangSmith si está configurado
    
    Las nuevas variables se agregan con:
    - conv_variable_category: 'score'
    - conv_variable_name: 'dspy_score', 'dspy_evaluation_json', etc.
    """
    def body():
        import time
        import os
        from dagster_pipeline.defs.conversation_analysis.utils.variables.dspy_evaluator import (
            process_dspy_evaluator_batch,
        )
        
        context.log.info("📊 Agregando evaluaciones DSPy a conv_variables...")
        
        # Verificar si está habilitado desde la configuración de Dagster
        enable_dspy = config.enable_dspy_evaluator
        
        context.log.info(f"🔍 enable_dspy_evaluator={enable_dspy}")
        
        if not enable_dspy:
            context.log.info("🔇 DSPy evaluator deshabilitado (configurar enable_dspy_evaluator: true en Dagster)")
            context.log.info("💡 En la UI de Dagster, configura 'enable_dspy_evaluator: true' en ConvVariablesConfig")
            return conv_with_variables
        
        context.log.info("✅ DSPy evaluator HABILITADO - procesando evaluaciones...")
        
        df = conv_with_variables.copy()
        
        # Incremental: only run DSPy for rows with _to_process (new/changed)
        if "_to_process" in df.columns:
            df_valid_base = df[df["_to_process"]]
            context.log.info(f"📋 Incremental: running DSPy only for {len(df_valid_base)} new/changed conversations")
        else:
            df_valid_base = df
            context.log.info("📋 No _to_process column: running DSPy for all conversations")
        
        # Aplicar test mode sampling si está habilitado
        if config.test_mode:
            if "_to_process" in df.columns:
                n_tp = int(df["_to_process"].sum())
                sample_size = min(config.test_sample_size, n_tp)
                if sample_size > 0 and n_tp > 0:
                    to_process_idx = df.index[df["_to_process"]]
                    sample_idx = df.loc[to_process_idx].sample(n=min(sample_size, len(to_process_idx)), random_state=42).index
                    df["_to_process"] = False
                    df.loc[sample_idx, "_to_process"] = True
                    context.log.info(f"🧪 TEST MODE: Muestra de {len(sample_idx)} de {n_tp} a procesar")
            else:
                sample_size = min(config.test_sample_size, len(df))
                df = df.sample(n=sample_size, random_state=42)
                context.log.info(f"🧪 TEST MODE: Usando {sample_size} registros de muestra")
        
        # Preparar textos de conversación en formato inline
        from dagster_pipeline.defs.conversation_analysis.utils.variables.llm_judge import (
            format_conversation_with_markers,
        )
        
        context.log.info("🔄 Formateando mensajes a formato inline...")
        
        def format_to_inline(row):
            messages = row.get('messages', [])
            if isinstance(messages, str):
                try:
                    messages = json.loads(messages)
                except Exception:
                    messages = []
            if isinstance(messages, list) and len(messages) > 0:
                return format_conversation_with_markers(messages)
            conv_text = row.get('conversation_text', '')
            if isinstance(conv_text, str) and '[event_section=' in conv_text:
                return conv_text
            return ''
        
        df['messages_inline'] = df.apply(format_to_inline, axis=1)
        
        # Filtrar: solo filas a procesar y con texto inline válido para DSPy
        if "_to_process" in df.columns:
            df_valid = df[df["_to_process"] & (df['messages_inline'].astype(str).str.len() > 0)].copy()
        else:
            df_valid = df[df['messages_inline'].astype(str).str.len() > 0].copy()
        
        context.log.info(f"📊 Total conversaciones: {len(df)}")
        context.log.info(f"📊 Conversaciones con formato inline válido: {len(df_valid)}")
        
        if len(df_valid) == 0:
            context.log.warning("⚠️ No hay conversaciones con formato inline válido - saltando evaluación DSPy")
            context.log.warning("   Verifica que los mensajes tengan 'event_section' y 'event_type'")
            # Mostrar ejemplo de mensajes para debugging
            if len(df) > 0:
                sample_messages = df.iloc[0].get('messages', [])
                context.log.info(f"   Ejemplo de messages (tipo: {type(sample_messages)}): {str(sample_messages)[:200]}")
            return df
        
        context.log.info(f"🚀 Evaluando {len(df_valid)} conversaciones con DSPy (max 5 concurrentes)...")
        
        # Progress callback para logging
        def log_progress(completed: int, total: int):
            context.log.info(f"   🔄 Progreso: {completed}/{total} ({completed/total*100:.1f}%)")
        
        # Procesar evaluaciones DSPy en batch
        dspy_start_time = time.time()
        
        try:
            # Opción B: Sin golden dataset - usando firma mejorada
            context.log.info("📝 Usando evaluador DSPy sin golden dataset (firma mejorada)")
            context.log.info("   Categorías de fallo: user_abandonment, user_intent_change, bot_error, conversation_loop")
            
            dspy_variables_by_index = process_dspy_evaluator_batch(
                df_valid,
                golden_dataset_path=None,  # Ya no se usa
                max_workers=5,  # Límite conservador para evitar rate limits
                progress_callback=log_progress
            )
            
            dspy_time = time.time() - dspy_start_time
            context.log.info(f"✅ Evaluaciones DSPy completadas en {dspy_time:.2f}s")
            context.log.info(f"⚡ Tiempo promedio por conversación: {dspy_time/len(df_valid)*1000:.1f}ms")
            
            # Agregar variables DSPy a conv_variables existentes
            context.log.info("🔗 Agregando variables DSPy a conv_variables existentes...")
            
            for idx in df.index:
                # Obtener conv_variables existentes (JSON string)
                existing_vars_str = df.at[idx, 'conv_variables']
                
                # Parsear JSON existente
                try:
                    if isinstance(existing_vars_str, str):
                        existing_vars = json.loads(existing_vars_str)
                    elif isinstance(existing_vars_str, list):
                        existing_vars = existing_vars_str
                    else:
                        existing_vars = []
                except:
                    existing_vars = []
                
                # Agregar nuevas variables DSPy si existen
                if idx in dspy_variables_by_index:
                    dspy_vars = dspy_variables_by_index[idx]
                    if dspy_vars:
                        existing_vars.extend(dspy_vars)
                
                # Serializar de vuelta a JSON
                df.at[idx, 'conv_variables'] = json.dumps(existing_vars, ensure_ascii=False)
            
            context.log.info(f"✅ Variables DSPy agregadas a {len(df)} registros")
            
        except Exception as e:
            import traceback
            context.log.error(f"❌ Error en evaluación DSPy: {e}")
            context.log.error(f"   Traceback: {traceback.format_exc()}")
            context.log.warning("⚠️ Continuando sin variables DSPy")
            # Retornar DataFrame sin modificar en caso de error
        
        total_time = time.time() - dspy_start_time
        context.log.info(f"✅ Proceso DSPy completado en {total_time:.2f}s")
        
        return df
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 10: Upsert with Existing Data
# ============================================================================

@asset
def conv_upsert(
    context: AssetExecutionContext,
    config: ConvUpsertConfig,
    conv_with_dspy_scores: pd.DataFrame,
) -> pd.DataFrame:
    """
    Upsert new conversations with existing data from bt_conv_conversations (canonical).

    - Reads existing rows from Geospot PostgreSQL (lakehouse table)
    - For each new conversation (by conversation_id):
      - If exists: update with new data
      - If not exists: insert new record
    - preserve_existing_conv_variables (config): if True, merge conv_variables so IA vars are not overwritten with empty; if False, replace with new only.
    - Returns the combined DataFrame ready for upload
    """
    def body():
        context.log.info("🔄 Starting upsert process...")
        
        # Prepare new data with final column names (drop incremental temp column if present)
        df_new = conv_with_dspy_scores.copy()
        df_new = df_new.drop(columns=["_to_process"], errors="ignore")
        
        # Column rename map (same as final_output)
        column_rename_map = {
            'conversation_id': 'conv_id',
            'phone_number': 'lead_phone_number',
            'conversation_start': 'conv_start_date',
            'conversation_end': 'conv_end_date',
            'conversation_text': 'conv_text',
            'messages': 'conv_messages',
            'timestamp_ultimo_mensaje': 'conv_last_message_date',
            'lead_id': 'lead_id',
            'fecha_creacion_cliente': 'lead_created_at',
            'conv_variables': 'conv_variables',
        }
        
        # Select and rename columns
        available_columns = [col for col in column_rename_map.keys() if col in df_new.columns]
        df_new = df_new[available_columns].copy()
        rename_dict = {k: v for k, v in column_rename_map.items() if k in available_columns}
        df_new.rename(columns=rename_dict, inplace=True)
        
        # Clean phone number
        if 'lead_phone_number' in df_new.columns:
            df_new['lead_phone_number'] = df_new['lead_phone_number'].astype(str).str.lstrip('+')
        
        # conv_messages is already a JSON string from processors.py
        # No conversion needed here
        context.log.info("📝 conv_messages already in JSON format")
    
        # Drop duplicates by conv_id before upsert (keep first occurrence)
        len_before_dedup = len(df_new)
        df_new = df_new.drop_duplicates(subset=["conv_id"], keep="first")
        if len(df_new) < len_before_dedup:
            context.log.info(f"🧹 Dropped {len_before_dedup - len(df_new)} duplicate rows by conv_id (keep='first')")
    
        n_new_rows = len(df_new)
        n_unique_ids = df_new["conv_id"].nunique()
        context.log.info(f"📊 New records to process: {n_new_rows} rows, {n_unique_ids} unique conv_id")
        new_conv_ids = set(df_new['conv_id'].unique())
        
        # Read existing data from Geospot
        context.log.info("📥 Reading existing data from bt_conv_conversations...")
        try:
            query = "SELECT * FROM bt_conv_conversations"
            context.log.info(f"🔍 Executing query: {query}")
            df_existing = query_geospot(query)
            context.log.info(f"✅ Found {len(df_existing)} existing records")
            if not df_existing.empty:
                context.log.info(f"📊 Existing data columns: {list(df_existing.columns)}")
                context.log.info(f"📊 Sample conv_ids: {list(df_existing['conv_id'].head(5))}")
                # Normalize JSONB columns (Postgres returns them as Python objects)
                context.log.info("🔧 Normalizing JSONB columns from existing data...")
                df_existing = _normalize_existing_data(df_existing)
                context.log.info("✅ JSONB columns normalized to JSON strings")
            else:
                context.log.warning(f"⚠️ Query returned EMPTY DataFrame - this will cause data loss!")
        except Exception as e:
            context.log.error(f"❌ FAILED to read existing table: {type(e).__name__}: {e}")
            context.log.error(f"❌ This will cause ALL existing data to be LOST!")
            # Raise the error instead of silently continuing
            raise RuntimeError(f"Cannot proceed without reading existing data: {e}")
        
        if df_existing.empty:
            # No existing data - all new records are inserts
            context.log.info("📝 No existing data found. All records will be inserted.")
            df_result = df_new.copy()
            inserted_count = len(df_new)
            updated_count = 0
        else:
            existing_conv_ids = set(df_existing['conv_id'].unique())
            context.log.info(f"📊 Existing conversation IDs: {len(existing_conv_ids)}")
            
            # Identify which records to update vs insert
            ids_to_update = new_conv_ids & existing_conv_ids
            ids_to_insert = new_conv_ids - existing_conv_ids
            ids_to_keep = existing_conv_ids - new_conv_ids  # Existing records not in new data
            
            context.log.info(f"🔄 Records to update: {len(ids_to_update)}")
            context.log.info(f"➕ Records to insert: {len(ids_to_insert)}")
            context.log.info(f"📦 Existing records to keep: {len(ids_to_keep)}")
            
            # Build result DataFrame:
            # 1. Existing records NOT in new data (keep as-is)
            df_keep = df_existing[df_existing['conv_id'].isin(ids_to_keep)].copy()
            
            # 2. New records (both updates and inserts come from new data)
            df_upserted = df_new.copy()
            
            # Para registros que se ACTUALIZAN: fusionar conv_variables solo si config.preserve_existing_conv_variables es True
            preserve_raw = getattr(config, "preserve_existing_conv_variables", True)
            if preserve_raw is None:
                preserve = True
            elif isinstance(preserve_raw, str):
                preserve = preserve_raw.lower() in ("true", "1", "yes")
            else:
                preserve = bool(preserve_raw)
            context.log.info(f"🔗 preserve_existing_conv_variables={preserve} (raw={preserve_raw!r})")
            if len(ids_to_update) > 0 and preserve:
                existing_by_conv_id = df_existing.set_index('conv_id')
                merged_count = 0
                for idx in df_upserted.index:
                    cid = df_upserted.at[idx, 'conv_id']
                    if cid in ids_to_update and cid in existing_by_conv_id.index:
                        existing_vars = existing_by_conv_id.at[cid, 'conv_variables']
                        new_vars = df_upserted.at[idx, 'conv_variables']
                        df_upserted.at[idx, 'conv_variables'] = _merge_conv_variables_for_upsert(
                            existing_vars, new_vars
                        )
                        merged_count += 1
                context.log.info(f"🔗 conv_variables fusionados (preserve_existing_conv_variables=True): {merged_count} registros")
            elif len(ids_to_update) > 0:
                context.log.info("🔗 conv_variables: reemplazo directo (preserve_existing_conv_variables=False)")
            
            # Combine: kept existing + new/updated
            df_result = pd.concat([df_keep, df_upserted], ignore_index=True)
            
            inserted_count = len(ids_to_insert)
            updated_count = len(ids_to_update)
        
        context.log.info(f"✅ Upsert completed:")
        context.log.info(f"   - Inserted: {inserted_count}")
        context.log.info(f"   - Updated: {updated_count}")
        context.log.info(f"   - Total records: {len(df_result)}")
        
        return df_result
    yield from iter_job_wrapped_compute(context, body)


# ============================================================================
# ASSET 11: Final Output
# ============================================================================

@asset
def conv_final_output(
    context: AssetExecutionContext,
    config: FinalOutputConfig,
    conv_upsert: pd.DataFrame,
) -> pd.DataFrame:
    """Export final result to S3 and trigger table replacement."""
    def body():
        context.log.info("💾 Preparing final output...")
        
        df_final = conv_upsert.copy()
        
        # Add/update audit columns
        now = datetime.now()
        today = now.date()
        
        # Only add these columns if they don't exist (for new records)
        if 'vis_pseudo_id' not in df_final.columns:
            df_final['vis_pseudo_id'] = None
        if 'spot_id' not in df_final.columns:
            df_final['spot_id'] = None
        if 'aud_inserted_date' not in df_final.columns:
            df_final['aud_inserted_date'] = today
        if 'aud_inserted_at' not in df_final.columns:
            df_final['aud_inserted_at'] = now
        
        # Always update these columns
        df_final['aud_updated_date'] = today
        df_final['aud_updated_at'] = now
        df_final['aud_job'] = 'conversation_analysis_pipeline'
        
        context.log.info(f"📊 Final output: {len(df_final)} records with {len(df_final.columns)} columns")
        
        # Upload to S3
        context.log.info("☁️ Uploading to S3...")
        s3_url, s3_key = upload_dataframe_to_s3(df_final, test_mode=config.test_mode)
        context.log.info(f"✅ Uploaded to {s3_url}")
        
        # Trigger table replacement (only in production)
        if not config.test_mode and not config.dev_mode:
            context.log.info("🔄 Triggering table replacement for bt_conv_conversations_bck...")
            try:
                api_response = trigger_table_replacement(
                    s3_key=s3_key, table_name="bt_conv_conversations_bck"
                )
                context.log.info(f"✅ Table replacement triggered: {api_response}")
            except Exception as e:
                context.log.error(f"❌ Error triggering table replacement: {str(e)}")
                raise
        elif config.test_mode:
            context.log.info("🧪 TEST MODE: Skipping table replacement")
        elif config.dev_mode:
            context.log.info("🛠️ DEV MODE: Skipping table replacement (S3 upload completed)")
        
        return df_final
    yield from iter_job_wrapped_compute(context, body)
