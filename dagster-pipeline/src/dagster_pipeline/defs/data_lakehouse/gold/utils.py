# defs/data_lakehouse/gold/utils.py
"""
Generic utilities for Gold layer assets.

Reusable functions for:
- Audit fields
- Common aggregations
- Funnel stage priority
- Validations
"""
import dagster as dg
import polars as pl
from datetime import datetime, date
from typing import List, Optional

from dagster_pipeline.defs.data_lakehouse.gold.bt_conv_variables.phone_formats import (
    phone_formats as _conversation_phone_formats,
)


def add_audit_fields(
    df: pl.DataFrame,
    job_name: str,
) -> pl.DataFrame:
    """
    Adds standard audit fields to a gold DataFrame.
    
    Args:
        df: DataFrame to add audit fields to
        job_name: Job name (e.g., "lk_leads", "lk_projects")
    
    Returns:
        DataFrame with audit fields added
    """
    return df.with_columns([
        pl.lit(datetime.now()).alias("aud_inserted_at"),
        pl.lit(date.today()).alias("aud_inserted_date"),
        pl.lit(datetime.now()).alias("aud_updated_at"),
        pl.lit(date.today()).alias("aud_updated_date"),
        pl.lit(job_name).alias("aud_job"),
    ])


def get_funnel_stage_priority(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds a funnel_stage_priority column to determine how advanced a project is.
    
    Priority (higher = more advanced):
    - 6: Has transaction date (won)
    - 5: Has contract date
    - 4: Has LOI date
    - 3: Has visit confirmed
    - 2: Has visit realized
    - 1: Has visit created
    - 0: Lead only (no visits)
    
    Args:
        df: DataFrame with funnel columns
    
    Returns:
        DataFrame with _funnel_stage_priority column added
    """
    return df.with_columns(
        pl.when(pl.col("project_funnel_transaction_date").is_not_null())
        .then(6)
        .when(pl.col("project_funnel_contract_date").is_not_null())
        .then(5)
        .when(pl.col("project_funnel_loi_date").is_not_null())
        .then(4)
        .when(pl.col("project_funnel_visit_confirmed_at").is_not_null())
        .then(3)
        .when(pl.col("project_funnel_visit_realized_at").is_not_null())
        .then(2)
        .when(pl.col("project_funnel_visit_created_date").is_not_null())
        .then(1)
        .otherwise(0)
        .alias("_funnel_stage_priority")
    )


def aggregate_count_by(
    df: pl.DataFrame,
    group_by_col: str,
    count_col_name: str = "count",
) -> pl.DataFrame:
    """
    Aggregates by counting rows per column (e.g., projects per lead).
    
    Args:
        df: DataFrame to aggregate
        group_by_col: Column to group by
        count_col_name: Name of the resulting count column
    
    Returns:
        Aggregated DataFrame with count column
    """
    return df.group_by(group_by_col).agg(pl.len().alias(count_col_name))


def get_first_by_priority(
    df: pl.DataFrame,
    group_by_col: str,
    priority_cols: List[str],
    priority_desc: List[bool],
    prefix: str = "",
) -> pl.DataFrame:
    """
    Gets the first row per group based on column priority.
    
    Useful for getting the most advanced or most recent project per lead.
    
    Args:
        df: DataFrame to process
        group_by_col: Column to group by
        priority_cols: List of columns to sort by (priority order)
        priority_desc: List of booleans indicating descending sort
        prefix: Prefix to rename resulting columns
    
    Returns:
        DataFrame with first row per group
    """
    # Sort by priority
    df_sorted = df.sort(
        by=[group_by_col] + priority_cols,
        descending=[False] + priority_desc
    )
    
    # Take first row per group
    return df_sorted.group_by(group_by_col).first()


def validate_gold_joins(
    df_main: pl.DataFrame,
    df_joined: pl.DataFrame,
    context: dg.AssetExecutionContext,
    main_name: str = "main",
) -> dict:
    """
    Validates that joins haven't lost critical rows.
    
    Args:
        df_main: Main DataFrame (before join)
        df_joined: DataFrame after join
        context: Dagster context
        main_name: Descriptive name of the main DataFrame
    
    Returns:
        Dict with validation results
    """
    main_rows = df_main.height
    joined_rows = df_joined.height
    
    results = {
        "main_rows": main_rows,
        "joined_rows": joined_rows,
        "rows_lost": main_rows - joined_rows,
        "validation_passed": main_rows == joined_rows,
    }
    
    if results["rows_lost"] > 0:
        context.log.warning(
            f"⚠️  Join resulted in {results['rows_lost']:,} fewer rows "
            f"({main_name}: {main_rows:,} → {joined_rows:,})"
        )
    else:
        context.log.info(
            f"✅ Join preserved all rows: {joined_rows:,} rows"
        )
    
    return results


def add_table_prefix_to_columns(
    df: pl.DataFrame,
    table_name: str,
    foreign_key_columns: Optional[List[str]] = None,
    exclude_columns: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Generic function to add table prefix to native columns.
    
    Rules:
    - Native columns: Add table prefix (e.g., `id` → `lead_id`)
    - FK columns: Keep without prefix (e.g., `user_id`, `vis_pseudo_id`, `spot_id`)
    - Excluded columns: Keep without prefix (e.g., `aud_*` already added)
    
    Args:
        df: DataFrame to process
        table_name: Table name (e.g., "lk_leads", "lk_projects", "lk_spots")
            Prefix is extracted automatically (e.g., "lead", "project", "spot")
        foreign_key_columns: List of FK columns that should remain without prefix
            If None, uses a common default list
        exclude_columns: List of columns to exclude from prefix (e.g., ["aud_inserted_at"])
    
    Returns:
        DataFrame with native columns renamed with prefix
    """
    # Extract prefix from table name
    # Examples: "lk_leads" -> "lead", "lk_projects" -> "project", "bt_lds_lead_spots" -> "lds"
    if table_name.startswith("lk_"):
        prefix = table_name.replace("lk_", "").rstrip("s")  # "lk_leads" -> "lead"
    elif table_name.startswith("bt_"):
        # For bridge tables, use first component after bt_
        parts = table_name.replace("bt_", "").split("_")
        prefix = parts[0]  # "bt_lds_lead_spots" -> "lds"
    else:
        # Fallback: use full name without underscores
        prefix = table_name.replace("_", "")
    
    # Common FKs that should remain without prefix
    default_fks = [
        "user_id",
        "lead_id",
        "project_id",
        "spot_id",
        "vis_pseudo_id",
        "agent_id",
        "agent_kam_id",
    ]
    
    if foreign_key_columns is None:
        foreign_key_columns = default_fks
    else:
        # Combine with defaults
        foreign_key_columns = list(set(foreign_key_columns + default_fks))
    
    # Columns to exclude (aud_* and others)
    default_excludes = [
        "aud_inserted_at",
        "aud_inserted_date",
        "aud_updated_at",
        "aud_updated_date",
        "aud_job",
    ]
    
    if exclude_columns is None:
        exclude_columns = default_excludes
    else:
        exclude_columns = list(set(exclude_columns + default_excludes))
    
    # Create renaming mapping
    renames = {}
    for col in df.columns:
        # If already has prefix, don't rename
        if col.startswith(f"{prefix}_"):
            continue
        
        # If is FK, keep without prefix
        if col in foreign_key_columns:
            continue
        
        # If in exclude, keep without prefix
        if col in exclude_columns:
            continue
        
        # If already has prefix from another table, keep (don't rename)
        # Common prefixes according to schema: lead, project, spot, user, vis, agent, lds, vlu, conv, transaction, spot_historic
        other_prefixes = ["lead", "project", "spot", "user", "vis", "agent", "lds", "vlu", "conv", "transaction", "spot_historic"]
        if any(col.startswith(f"{other_prefix}_") for other_prefix in other_prefixes):
            continue
        
        # Add prefix to native column
        renames[col] = f"{prefix}_{col}"
    
    # Apply renamings
    if renames:
        df_renamed = df.rename(renames)
    else:
        df_renamed = df
    
    return df_renamed


def _canonical_phone_polars(phone_col: pl.Expr) -> pl.Expr:
    """
    Same logic as conversation_analysis get_canonical_phone: digits only,
    if len >= 10 then "52" + last 10 digits else "52" + digits; empty -> original stripped or "_".
    """
    digits = phone_col.cast(pl.Utf8).str.replace_all(r"\D", "")
    stripped = phone_col.cast(pl.Utf8).str.strip_chars()
    return (
        pl.when(digits.str.len_chars() == 0)
        .then(pl.when(stripped.fill_null("").str.len_chars() == 0).then(pl.lit("_")).otherwise(stripped.fill_null("_")))
        .when(digits.str.len_chars() >= 10)
        .then(pl.lit("52") + digits.str.slice(-10, 10))
        .otherwise(pl.lit("52") + digits)
    )


def group_messages_into_conversations(
    df_messages: pl.DataFrame,
    gap_hours: int = 72,
    use_source_conversation_id: bool = False,
    expand_segment_by_day: bool = False,
    one_conv_per_phone: bool = False,
) -> pl.DataFrame:
    """
    Groups messages into conversations by canonical phone.

    Modes (mutually exclusive, evaluated in order):

    one_conv_per_phone=True (governance mode):
        One conversation per canonical phone number — all messages collapsed into a single row.
        conv_id = the Chatbot conversation_id (column "id") that appears most frequently among
        the messages of that phone. When a phone has a single conversation_id this is trivial;
        when January-style splits produced multiple ids, the most frequent one wins.
        Each message in the JSON keeps its original "id" (chatbot conv id) for event matching.

    use_source_conversation_id=True (granular mode, rarely used):
        Groups by (phone, chatbot conversation_id). WARNING: very granular (~6x more rows).

    default (gap mode):
        A new segment starts when there are more than gap_hours without messages from the
        same phone. conv_id format: phone_canonical_YYYYMMDD of first message in segment.

    Args:
        df_messages: DataFrame with messages (phone_number, created_at_utc, message_body, type, id)
            and optionally conversation_id from source.
        gap_hours: Hours of inactivity to start a new conversation (gap mode only).
        use_source_conversation_id: Group by source chatbot conversation_id (can inflate rows).
        expand_segment_by_day: Emit one row per calendar day within each gap segment (gap mode only).
        one_conv_per_phone: One conversation per phone; conv_id = most frequent chatbot conv id.

    Returns:
        DataFrame with one row per conversation.
    """
    import json

    # Ensure created_at_utc is datetime
    if df_messages["created_at_utc"].dtype == pl.String:
        df = df_messages.with_columns(
            pl.col("created_at_utc").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
        )
    else:
        df = df_messages.clone()

    type_priority = (
        pl.when(pl.col("type") == "user")
        .then(0)
        .when(pl.col("type") == "assistant")
        .then(1)
        .otherwise(2)
    )
    df = df.with_columns([
        _canonical_phone_polars(pl.col("phone_number")).alias("_phone_canonical"),
        type_priority.alias("_type_priority"),
    ])
    _tiebreak = "message_row_id" if "message_row_id" in df.columns else "id"
    df = df.sort(["_phone_canonical", "created_at_utc", "_type_priority", _tiebreak])

    if one_conv_per_phone:
        # Group ALL messages for a phone into one conversation.
        # conv_id = most frequent chatbot conversation_id ("id" column) for that phone.
        # All messages per phone (full history for messages JSON)
        agg_cols = [
            pl.col("phone_number").first().alias("phone_number"),
            pl.col("id").count().alias("message_count"),
            pl.col("id").alias("_message_ids"),
            pl.col("message_body").alias("_message_bodies"),
            pl.col("type").alias("_message_types"),
            pl.col("created_at_utc").alias("_message_timestamps"),
            pl.col("message_body").last().alias("last_message"),
            pl.col("type").last().alias("last_message_type"),
            pl.col("created_at_utc").max().alias("last_message_timestamp"),
        ]
        conversations = df.group_by("_phone_canonical").agg(agg_cols)

        # Dominant conv_id = most frequent chatbot conversation_id per phone
        conv_id_counts = (
            df.group_by(["_phone_canonical", "id"])
            .agg(pl.len().alias("_cnt"))
            .sort(["_phone_canonical", "_cnt", "id"], descending=[False, True, False])
            .group_by("_phone_canonical", maintain_order=False)
            .agg(pl.col("id").first().alias("_dominant_conv_id"))
        )
        conversations = conversations.join(conv_id_counts, on="_phone_canonical", how="left")
        conversations = conversations.with_columns(
            pl.col("_dominant_conv_id").cast(pl.Utf8).alias("conversation_id")
        ).drop("_dominant_conv_id")

        # conversation_start / conversation_end = full time range of ALL messages for the phone.
        # With one_conv_per_phone, the entire phone history is one conversation, so the event
        # window must cover all chatbot conversation_ids (including January splits).
        # Using only the dominant conv_id's window would mis-date the conversation and filter
        # out events from earlier/later chatbot sessions.
        full_window = (
            df.group_by("_phone_canonical")
            .agg([
                pl.col("created_at_utc").min().alias("conversation_start"),
                pl.col("created_at_utc").max().alias("conversation_end"),
            ])
        )
        conversations = conversations.join(full_window, on="_phone_canonical", how="left")

    elif (
        use_source_conversation_id
        and "conversation_id" in df.columns
        and df["conversation_id"].null_count() < df.height
    ):
        df = df.with_columns(pl.col("conversation_id").cast(pl.Utf8).fill_null("").alias("_conv_key"))
        group_cols = ["_phone_canonical", "_conv_key"]
        conversations = df.group_by(group_cols).agg([
            pl.col("phone_number").first().alias("phone_number"),
            pl.col("created_at_utc").min().alias("conversation_start"),
            pl.col("created_at_utc").max().alias("conversation_end"),
            pl.col("id").count().alias("message_count"),
            pl.col("id").alias("_message_ids"),
            pl.col("message_body").alias("_message_bodies"),
            pl.col("type").alias("_message_types"),
            pl.col("created_at_utc").alias("_message_timestamps"),
            pl.col("message_body").last().alias("last_message"),
            pl.col("type").last().alias("last_message_type"),
            pl.col("created_at_utc").max().alias("last_message_timestamp"),
        ])
        conversations = conversations.with_columns(
            pl.col("_conv_key").alias("conversation_id")
        ).drop("_conv_key")

    else:
        # Per canonical phone: diff in hours; new segment when gap > gap_hours
        def _add_segment(group: pl.DataFrame) -> pl.DataFrame:
            diff_seconds = group["created_at_utc"].diff().dt.total_seconds() / 3600
            segment = (diff_seconds > gap_hours).fill_null(False).cast(pl.UInt32).cum_sum()
            return group.with_columns(segment.alias("_segment"))

        df = df.group_by("_phone_canonical", maintain_order=True).map_groups(_add_segment)
        agg_cols = [
            pl.col("phone_number").first().alias("phone_number"),
            pl.col("created_at_utc").min().alias("_seg_start"),
            pl.col("created_at_utc").max().alias("conversation_end"),
            pl.col("id").count().alias("message_count"),
            pl.col("id").alias("_message_ids"),
            pl.col("message_body").alias("_message_bodies"),
            pl.col("type").alias("_message_types"),
            pl.col("created_at_utc").alias("_message_timestamps"),
            pl.col("message_body").last().alias("last_message"),
            pl.col("type").last().alias("last_message_type"),
            pl.col("created_at_utc").max().alias("last_message_timestamp"),
        ]
        if "conversation_id" in df.columns:
            agg_cols.append(pl.col("conversation_id").alias("_message_conversation_ids"))
        seg_agg = df.group_by(["_phone_canonical", "_segment"]).agg(agg_cols)
        if expand_segment_by_day:
            by_day = df.with_columns(pl.col("created_at_utc").dt.date().alias("_day")).group_by(
                ["_phone_canonical", "_segment", "_day"]
            ).agg(pl.col("created_at_utc").min().alias("conversation_start"))
            conversations = by_day.join(seg_agg, on=["_phone_canonical", "_segment"]).drop("_day", "_seg_start")
        else:
            conversations = seg_agg.with_columns(pl.col("_seg_start").alias("conversation_start")).drop("_seg_start")

        conversations = conversations.with_columns(
            (pl.col("_phone_canonical") + "_" + pl.col("conversation_start").dt.strftime("%Y%m%d")).alias("conversation_id")
        )

    # Build conversation_text and messages JSON (structure aligned with Geospot bt_conv_conversations export).
    # conv_text: single space between turns (same as tabla legacy en Geospot); bodies keep internal newlines.
    # created_at_utc in each message: second precision only (YYYY-MM-DD HH:MM:SS) to align with legacy truncation and reduce conv_messages length diffs (QA Patron A).
    # When source has conversation_id, each message gets conversation_id for event enrichment (legacy parity).
    def _build_messages_json(struct_row):
        try:
            ids = struct_row.get("_message_ids", []) or []
            bodies = struct_row.get("_message_bodies", []) or []
            types = struct_row.get("_message_types", []) or []
            timestamps = struct_row.get("_message_timestamps", []) or []
            phone = struct_row.get("phone_number")
            conv_ids = struct_row.get("_message_conversation_ids", []) or []
        except (TypeError, AttributeError):
            ids, bodies, types, timestamps, phone, conv_ids = [], [], [], [], None, []
        n = len(ids)
        messages_list = []
        for i in range(n):
            ts = timestamps[i] if i < len(timestamps) else None
            if ts is None:
                created_at_str = None
            elif hasattr(ts, "strftime"):
                created_at_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            else:
                s = str(ts)
                created_at_str = s[:19] if len(s) >= 19 else s
            body_val = bodies[i] if i < len(bodies) else None
            # Normalize empty message_body to None (legacy stores NULL; '' != NULL in SQL IS DISTINCT FROM)
            if body_val is None or (isinstance(body_val, str) and body_val.strip() == ""):
                message_body = None
            else:
                message_body = str(body_val)
            msg = {
                "id": int(ids[i]) if i < len(ids) and ids[i] is not None else None,
                "phone_number": str(phone) if phone is not None else "",
                "message_body": message_body,
                "type": str(types[i]) if i < len(types) and types[i] is not None else None,
                "created_at_utc": created_at_str,
            }
            if i < len(conv_ids) and conv_ids[i] is not None:
                msg["conversation_id"] = str(conv_ids[i])
            messages_list.append(msg)
        return json.dumps(messages_list, ensure_ascii=False)

    conversations = conversations.with_columns([
        pl.struct(["_message_timestamps", "_message_types", "_message_bodies"])
        .map_elements(
            lambda x: " ".join([
                f"[{str(x['_message_timestamps'][i])[:19]}] {x['_message_types'][i]}: {x['_message_bodies'][i] or ''}"
                for i in range(len(x["_message_timestamps"]))
            ]),
            return_dtype=pl.String,
        )
        .alias("conversation_text"),
    ])
    msg_struct_cols = ["_message_ids", "phone_number", "_message_bodies", "_message_types", "_message_timestamps"]
    if "_message_conversation_ids" in conversations.columns:
        msg_struct_cols.append("_message_conversation_ids")
    conversations = conversations.with_columns(
        pl.struct(msg_struct_cols).map_elements(_build_messages_json, return_dtype=pl.String).alias("messages"),
    )

    return conversations.select([
        "conversation_id",
        "phone_number",
        "conversation_start",
        "conversation_end",
        "conversation_text",
        "message_count",
        "messages",
        "last_message",
        "last_message_type",
        "last_message_timestamp",
    ])


def match_conversations_to_clients(
    df_conversations: pl.DataFrame,
    df_clients: pl.DataFrame,
    context: Optional[dg.AssetExecutionContext] = None,
) -> pl.DataFrame:
    """
    Matches conversations to clients by phone — same logic as conversation_analysis conv_merged
    (assets.py): base variants from phone_formats(); we add last_9 and last_8 (legacy "hasta 8")
    to recover matches that phone_formats alone misses.
    """

    def _digits_only(phone) -> str:
        return "".join(c for c in str(phone) if c.isdigit()) if phone else ""

    def _last_n_variants(digits: str, n: int) -> set:
        """Same style as phone_formats: last n digits + 521/52/1/+52/+521."""
        if len(digits) < n:
            return set()
        last = digits[-n:]
        return {
            last, "521" + last, "52" + last, "1" + last,
            "+52" + last, "+521" + last,
        }

    def get_variants(phone):
        if phone is None:
            return []
        try:
            formats = set(_conversation_phone_formats(phone))
            digits = _digits_only(phone)
            if digits:
                formats.update(_conversation_phone_formats(digits))
                # Legacy "hasta 8": add last_9 and last_8 (phone_formats only has last_10 and last_9)
                formats.update(_last_n_variants(digits, 9))
                formats.update(_last_n_variants(digits, 8))
            return list(formats)
        except Exception:
            return []

    # Create phone mapping from clients
    # Handle both 'id' and 'client_id' column names
    id_col = "id" if "id" in df_clients.columns else "client_id"
    created_at_col = (
        "created_at" if "created_at" in df_clients.columns
        else ("fecha_creacion_cliente" if "fecha_creacion_cliente" in df_clients.columns else "fecha_creacion")
    )
    # Prefer full_phone_number when present (silver has it; MySQL clients query does not)
    phone_col = "full_phone_number" if "full_phone_number" in df_clients.columns else "phone_number"

    # Sort clients: newest first (created_at desc, lead_id desc) — legacy bt_conv_conversations parity (most recent lead per phone).
    if id_col in df_clients.columns:
        by_cols = [created_at_col, id_col] if created_at_col in df_clients.columns else [id_col]
        descending = [True, True] if created_at_col in df_clients.columns else [True]
        df_clients = df_clients.sort(by=by_cols, descending=descending, nulls_last=True)

    clients_expanded = []
    for row in df_clients.iter_rows(named=True):
        phone = row.get(phone_col) or row.get("phone_number")
        if phone:
            variants = get_variants(phone)
            for variant in variants:
                clients_expanded.append({
                    "phone_variant": variant,
                    "lead_id": row.get(id_col),
                    "fecha_creacion_cliente": row.get(created_at_col),
                })
    
    if not clients_expanded:
        if context:
            context.log.warning("⚠️  No clients with phone numbers found for matching")
        return df_conversations.with_columns([
            pl.lit(None).alias("lead_id"),
            pl.lit(None).alias("fecha_creacion_cliente"),
        ])
    
    df_clients_map = pl.DataFrame(clients_expanded)
    
    # Create phone variants for conversations
    conv_variants = []
    for row in df_conversations.iter_rows(named=True):
        phone = row.get("phone_number")
        if phone:
            variants = get_variants(phone)
            for variant in variants:
                conv_variants.append({
                    "conversation_id": row.get("conversation_id"),
                    "phone_variant": variant,
                })
    
    df_conv_map = pl.DataFrame(conv_variants)
    
    # Join to find matches; sort newest first so .first() picks the most recent lead per conversation.
    matches = df_conv_map.join(
        df_clients_map,
        on="phone_variant",
        how="left"
    )
    if "fecha_creacion_cliente" in matches.columns and "lead_id" in matches.columns:
        matches = matches.sort(["fecha_creacion_cliente", "lead_id"], descending=[True, True], nulls_last=True)
    elif "fecha_creacion_cliente" in matches.columns:
        matches = matches.sort("fecha_creacion_cliente", descending=True, nulls_last=True)
    elif "lead_id" in matches.columns:
        matches = matches.sort("lead_id", descending=True, nulls_last=True)
    matches = matches.group_by("conversation_id").agg([
        pl.col("lead_id").first(),
        pl.col("fecha_creacion_cliente").first(),
    ])
    
    # Join back to conversations
    result = df_conversations.join(
        matches,
        on="conversation_id",
        how="left"
    )
    
    matched_count = result.filter(pl.col("lead_id").is_not_null()).height
    unmatched_count = result.filter(pl.col("lead_id").is_null()).height
    
    if context:
        total = matched_count + unmatched_count
        match_rate = (matched_count / total * 100) if total > 0 else 0
        context.log.info(
            f"✅ Phone matching completed: "
            f"{matched_count:,} matched, {unmatched_count:,} unmatched "
            f"({match_rate:.1f}% match rate)"
        )
    
    return result
