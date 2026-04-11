# defs/data_lakehouse/gold/gold_bt_conv_conversations_new.py
"""
Gold layer: BT_CONV_CONVERSATIONS table (NEW VERSION).

This asset integrates conversation data from multiple sources:
- Messages from Chatbot PostgreSQL
- Clients, appointments, projects from MySQL Production

Similar to conversation_analysis but using the new standardized pipeline system.

IMPORTANT: One row per conversation (conv_id). Multiple conversations per phone are valid
and must be preserved — we never deduplicate by lead_phone_number. A same phone can have
conversations on different dates (e.g. 22-feb, 24-feb); all must appear in the table.

Structure:
- conv_id: Unique conversation identifier (PK)
- lead_phone_number: Phone number associated with conversation
- conv_start_date: Conversation start timestamp
- conv_end_date: Conversation end timestamp
- conv_text: Full conversation text
- conv_messages: JSON string with all messages
- conv_last_message_date: Last message timestamp
- lead_id: Matched lead ID (FK to LK_LEADS). When multiple clients share the same phone, we assign the **most recent** lead (created_at / lead_id desc), matching legacy `bt_conv_conversations_bck`. lk_leads fill uses the same rule and last-10-digits match key when length >= 10 (fewer 52 vs 1 prefix collisions).
- lead_created_at: Lead creation date
- conv_variables: JSON array — full parity with legacy: metrics, funnel_metric, tags (scheduled_visit, created_project),
  experiment variables (follow_up_success, unification_mgs, bot_graph, test_schedule*), AI tags (process_ai_tags_batch),
  and DSPy scores (process_dspy_evaluator_batch). Configurable via env/convention (enable_openai_analysis, enable_dspy_evaluator).
- aud_*: Audit fields

Matches legacy conversation_analysis output (Geospot `bt_conv_conversations_bck`): rows that exist in both
should be identical when scopes align. conv_variables is populated via create_conv_variables. Before writing to S3/Geospot,
we upsert with existing `bt_conv_conversations`: keep rows not in this run, merge
conv_variables so AI (score, ai_tag) values are preserved when the new value is empty.

Parity with legacy (conversation_analysis README):
- Input scope: bronze raw_cb_messages_new runs the same SQL as messages.sql (inline in the asset),
  3-day window with partition day as end (default ConversationConfig behavior).
- Incremental: _compute_incremental_mask (legacy) with governance table; only _to_process
  (new or len(messages) changed) get new conv_variables; rest reuse from Geospot.
- AI/DSPy: only for _to_process AND last_message in 72h window (same as legacy intent).
- scheduled_visit: from project_requirements (visit_created_at, visit_updated_at) + fallback
  event_type=visit_scheduled in messages (README 5.4). created_project: project_created_at
  in [conv_start-3h, conv_end] + scheduling_form_completed in messages.
- Row count: per run we have N rows (conversations in the 3-day window ending ref_date). Total
  in Geospot grows by upsert across runs. bt_conv_* jobs do not use partition day; ref_date is
  today in MX unless the run carries tag `dagster/partition` (backfill).
"""
import json
import os
import time
from typing import List, Optional, Tuple

import dagster as dg
import polars as pl
import pandas as pd
from datetime import datetime, timedelta

from dagster_pipeline.defs.data_lakehouse.shared import (
    build_gold_s3_key,
    write_polars_to_s3,
    read_silver_from_s3,
    load_to_geospot,
    read_governance_from_db,
    resolve_stale_reference_date,
)
from dagster_pipeline.defs.data_lakehouse.notifications import send_stale_data_notification
from dagster_pipeline.defs.data_lakehouse.gold.utils import (
    add_audit_fields,
    match_conversations_to_clients,
    validate_gold_joins,
)
from dagster_pipeline.defs.data_lakehouse.gold.bt_conv_variables.config import ConvVariablesConfig
from dagster_pipeline.defs.data_lakehouse.gold.bt_conv_message_event_type_parity import (
    apply_legacy_event_type_labels_to_messages,
)
from dagster_pipeline.defs.data_lakehouse.gold.bt_conv_variables.vendor import (
    _compute_incremental_mask,
    _enrich_messages_with_events,
    _merge_conv_variables_for_upsert,
    _normalize_jsonb_column as _normalize_jsonb_value,
    build_conv_seniority_map,
    sanitize_json_string,
)
from dagster_pipeline.defs.data_lakehouse.gold.conv_variables_flow import run_conv_variables_flow
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _normalize_empty_message_body_and_sanitize(messages):  # noqa: C901
    """Normalize message_body '' to None (legacy uses NULL) then sanitize for conv_messages."""
    if messages is None:
        return sanitize_json_string("[]")
    if isinstance(messages, str):
        try:
            parsed = json.loads(messages)
        except (TypeError, json.JSONDecodeError):
            return sanitize_json_string(messages)
    elif isinstance(messages, list):
        parsed = messages
    else:
        return sanitize_json_string("[]")
    if not isinstance(parsed, list):
        return sanitize_json_string("[]")
    for msg in parsed:
        if isinstance(msg, dict) and "message_body" in msg:
            b = msg["message_body"]
            if b is not None and isinstance(b, str) and b.strip() == "":
                msg["message_body"] = None
    return sanitize_json_string(json.dumps(parsed, ensure_ascii=False))


FUNNEL_WINDOW_MARGIN_HOURS = 2
PROJECT_LOOKBACK_HOURS = 3
VISIT_WINDOW_AFTER_END_HOURS = 5

# Same columns as conversation_analysis/queries/lk_projects.sql (seniority / segment).
# Geospot lk_projects is the governance table loaded by gold_lk_projects_new (LK_PROJECTS_GOVERNANCE_COLUMN_ORDER):
# commercial sector is spot_sector (from lead); project_sector does not exist on that table.
_LK_PROJECTS_SENIORITY_COLUMNS = [
    "project_id",
    "lead_id",
    "project_created_at",
    "project_funnel_visit_created_date",
    "project_funnel_visit_confirmed_at",
    "project_funnel_visit_realized_at",
    "project_funnel_loi_date",
    "project_updated_at",
    "spot_sector",
    "project_min_square_space",
    "project_max_square_space",
    "project_min_rent_price",
    "project_max_rent_price",
    "project_min_sale_price",
    "project_max_sale_price",
]


def _upsert_bt_conv_conversations(
    df_new: pl.DataFrame,
    context,
) -> pl.DataFrame:
    """
    Prepare batch for API upsert: merge conv_variables with existing for updates, return only rows to upload.
    Rows not in this run stay in the table (not sent). Caller writes returned DataFrame to S3 and loads with mode=upsert.
    """
    table_name = "bt_conv_conversations"
    context.log.info(f"📥 Reading existing data from {table_name} for upsert...")
    try:
        df_existing = read_governance_from_db(
            table_name,
            context,
            columns=BT_CONV_CONVERSATIONS_FINAL_COLUMN_ORDER,
            jsonb_columns=["conv_messages", "conv_variables"],
        )
    except Exception as e:
        context.log.error(f"❌ Could not read existing table ({e}); ABORTING to avoid data loss.")
        raise

    # Normalize JSONB columns from Geospot (ensure string; driver may return other representation)
    for col in ["conv_messages", "conv_variables"]:
        if col in df_existing.columns:
            df_existing = df_existing.with_columns(
                pl.col(col).map_elements(_normalize_jsonb_value, return_dtype=pl.Utf8)
            )

    if df_existing.is_empty():
        context.log.info("   No existing rows; using new data only.")
        return df_new.select(BT_CONV_CONVERSATIONS_FINAL_COLUMN_ORDER)

    new_ids = set(df_new["conv_id"].to_list())
    existing_ids = set(df_existing["conv_id"].to_list())
    ids_to_keep = existing_ids - new_ids
    ids_to_update = existing_ids & new_ids
    ids_to_insert = new_ids - existing_ids

    context.log.info(
        f"   Upsert: keep={len(ids_to_keep)}, update={len(ids_to_update)}, insert={len(ids_to_insert)}"
    )

    df_keep = df_existing.filter(pl.col("conv_id").is_in(list(ids_to_keep)))

    # Build upserted block: legacy _merge_conv_variables_for_upsert for each update (preserves score/ai_tag when new is empty)
    existing_for_update = df_existing.filter(pl.col("conv_id").is_in(list(ids_to_update)))
    existing_map = {}
    if not existing_for_update.is_empty():
        for row in existing_for_update.iter_rows(named=True):
            existing_map[row["conv_id"]] = row.get("conv_variables") or "[]"
    merged_cv = []
    for row in df_new.iter_rows(named=True):
        cid = row["conv_id"]
        new_cv = row.get("conv_variables") or "[]"
        if cid in ids_to_update and cid in existing_map:
            merged_cv.append(_merge_conv_variables_for_upsert(existing_map[cid], new_cv))
        else:
            merged_cv.append(new_cv)
    if ids_to_update:
        context.log.info(f"   conv_variables: {len(ids_to_update)} updates merged with legacy _merge_conv_variables_for_upsert")
    # Log distribution to verify data (0 vars = bug, 14 = base, 31+ = full)
    try:
        lengths = [len(json.loads(s)) if isinstance(s, str) and s and s.strip() else 0 for s in merged_cv]
        n0 = sum(1 for L in lengths if L == 0)
        n_base = sum(1 for L in lengths if 1 <= L < 14)
        n_14 = sum(1 for L in lengths if 14 <= L < 25)
        n_full = sum(1 for L in lengths if L >= 25)
        context.log.info(
            f"   conv_variables distribution: 0 vars={n0}, 1–13={n_base}, 14–24={n_14}, 25+={n_full}"
        )
        if n0 > 0:
            context.log.warning(f"   ⚠️ {n0} rows still have 0 conv_variables after merge")
    except Exception as e:
        context.log.warning(f"   Could not compute conv_variables distribution: {e}")
    df_upserted = df_new.with_columns(pl.Series("conv_variables", merged_cv))

    # Ensure same column set and order before concat
    final_cols = [c for c in BT_CONV_CONVERSATIONS_FINAL_COLUMN_ORDER if c in df_upserted.columns]
    df_keep = df_keep.select([c for c in final_cols if c in df_keep.columns])
    for c in final_cols:
        if c not in df_keep.columns and c in df_upserted.columns:
            df_keep = df_keep.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    # Return only the batch to upload (API upsert). Rows in df_keep stay in the table.
    df_upserted = df_upserted.select(final_cols)
    context.log.info(f"   Returning {df_upserted.height:,} rows for API upsert (keep {df_keep.height:,} not sent)")
    return df_upserted

# The 10 business columns (no aud_*). Same set as the first 10 in FINAL column order.
BT_CONV_CONVERSATIONS_CORE_COLUMNS = [
    "conv_id",
    "lead_phone_number",
    "conv_start_date",
    "conv_end_date",
    "conv_text",
    "conv_messages",
    "conv_last_message_date",
    "lead_id",
    "lead_created_at",
    "conv_variables",
]

# Physical order = CREATE TABLE column order (ordinal_position 1..15, no gaps after table recreate):
#   conv_id … lead_created_at, conv_variables,
#   aud_inserted_date, aud_inserted_at, aud_updated_date, aud_updated_at, aud_job
# Must stay aligned with bt_conv_conversations DDL in Geospot (Parquet/COPY by position).
BT_CONV_CONVERSATIONS_FINAL_COLUMN_ORDER = [
    "conv_id",
    "lead_phone_number",
    "conv_start_date",
    "conv_end_date",
    "conv_text",
    "conv_messages",
    "conv_last_message_date",
    "lead_id",
    "lead_created_at",
    "conv_variables",
    "aud_inserted_date",
    "aud_inserted_at",
    "aud_updated_date",
    "aud_updated_at",
    "aud_job",
]

# PostgreSQL jsonb via Geospot Parquet COPY: normalize these before S3 write (shared.write_polars_to_s3).
_GEOSPOT_GOVERNANCE_JSONB_COLUMNS = ["conv_messages", "conv_variables"]

# Diagnostics: True or env GEOSPOT_BT_CONV_MINIMAL_JSONB=1 forces conv_messages/conv_variables = "[]"
# before S3 and Geospot, and disables jsonb normalization in parquet (isolate COPY/upsert failures by payload).
_GEOSPOT_DIAG_MINIMAL_JSONB_BEFORE_LOAD = False


def _geospot_diag_minimal_jsonb_enabled() -> bool:
    raw = os.environ.get("GEOSPOT_BT_CONV_MINIMAL_JSONB", "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return _GEOSPOT_DIAG_MINIMAL_JSONB_BEFORE_LOAD


def _prepare_df_for_geospot_parquet_jsonb(
    df: pl.DataFrame,
    context: dg.AssetExecutionContext,
) -> Tuple[pl.DataFrame, Optional[List[str]]]:
    """Returns (df, geospot_jsonb_columns) for write_polars_to_s3."""
    if not _geospot_diag_minimal_jsonb_enabled():
        return df, _GEOSPOT_GOVERNANCE_JSONB_COLUMNS
    context.log.warning(
        "🧪 GEOSPOT DIAG: conv_messages and conv_variables replaced with '[]' before S3/load; "
        "jsonb normalization disabled. Turn off: GEOSPOT_BT_CONV_MINIMAL_JSONB=0 or "
        "_GEOSPOT_DIAG_MINIMAL_JSONB_BEFORE_LOAD=False"
    )
    if df.height == 0:
        return df, None
    return (
        df.with_columns(
            pl.lit("[]").alias("conv_messages"),
            pl.lit("[]").alias("conv_variables"),
        ),
        None,
    )


def _geospot_bt_conv_post_verify_seconds() -> Optional[float]:
    """
    Geospot returns 200 while COPY runs async. Optionally wait N seconds then log COUNT(*) (see shared.load_to_geospot).

    Env GEOSPOT_BT_CONV_UPSERT_VERIFY_SECONDS: disabled by default. Set a number > 0 to enable the wait.
    """
    raw = os.environ.get("GEOSPOT_BT_CONV_UPSERT_VERIFY_SECONDS", "").strip().lower()
    if raw in ("", "0", "false", "no", "off"):
        return None
    try:
        v = float(raw)
        return v if v > 0 else None
    except ValueError:
        return None


def _enrich_conv_messages_with_events(
    df_merged: pl.DataFrame,
    df_events: pl.DataFrame,
    context,
) -> pl.DataFrame:
    """
    Enrich each row's messages with event_type and event_section (same as conversation_analysis
    conv_with_events) so conv_messages matches legacy. Uses conversation_id from each message
    to get events from Staging.
    """
    if "messages" not in df_merged.columns or df_events.is_empty():
        return df_merged
    events_pd = df_events.to_pandas() if hasattr(df_events, "to_pandas") else pd.DataFrame(df_events)
    if "conversation_id" not in events_pd.columns or events_pd.empty:
        return df_merged
    events_pd["conversation_id"] = events_pd["conversation_id"].astype(str)
    if "created_at_local" in events_pd.columns:
        events_pd["created_at_local"] = pd.to_datetime(events_pd["created_at_local"], errors="coerce")
    events_by_cid = events_pd.groupby("conversation_id")
    context.log.info("🔗 Enriching messages with event_type/event_section (conv_with_events parity)...")
    new_messages = []
    for row in df_merged.iter_rows(named=True):
        raw = row.get("messages")
        if not raw:
            new_messages.append("[]")
            continue
        if isinstance(raw, str):
            try:
                msgs = json.loads(raw)
            except Exception:
                new_messages.append(raw)
                continue
        else:
            msgs = raw if isinstance(raw, list) else []
        if not msgs:
            new_messages.append("[]")
            continue
        enriched_list = []
        for msg in msgs:
            if not isinstance(msg, dict):
                enriched_list.append(msg)
                continue
            cid = msg.get("conversation_id") or msg.get("id")
            if cid is not None:
                cid_str = str(cid)
                if cid_str in events_by_cid.groups:
                    ev_df = events_by_cid.get_group(cid_str)
                    out = _enrich_messages_with_events([msg], ev_df)
                    enriched_list.append(out[0] if isinstance(out, list) and len(out) else msg)
                else:
                    msg_copy = dict(msg)
                    msg_copy["event_type"] = None
                    msg_copy["event_section"] = None
                    enriched_list.append(msg_copy)
            else:
                msg_copy = dict(msg)
                msg_copy["event_type"] = None
                msg_copy["event_section"] = None
                enriched_list.append(msg_copy)
        apply_legacy_event_type_labels_to_messages(enriched_list)
        new_messages.append(json.dumps(enriched_list, ensure_ascii=False))
    return df_merged.with_columns(pl.Series("messages", new_messages))


def _build_funnel_path_from_events(events_df: pd.DataFrame, field_name: str) -> str | None:
    """Build funnel path from events (chronological, consecutive dedup). Parity with conversation_analysis conv_with_funnels."""
    if events_df.empty:
        return None
    events_sorted = events_df.sort_values("created_at_local")
    values = []
    for _, event in events_sorted.iterrows():
        val = event.get(field_name)
        if pd.notna(val) and isinstance(val, str) and val.strip():
            values.append(val)
    unique_values = []
    for val in values:
        if not unique_values or unique_values[-1] != val:
            unique_values.append(val)
    return " > ".join(unique_values) if unique_values else None


def _add_funnel_from_events(
    df_merged: pl.DataFrame,
    df_events: pl.DataFrame,
    window_margin_hours: int,
    context,
) -> pl.DataFrame:
    """
    Add funnel_type and funnel_section from conversation events (same logic as
    conversation_analysis conv_with_funnels). For each row, extract Chatbot conversation_ids
    from messages (msg['conversation_id'] or msg['id']), collect events for those ids in
    [conv_start - margin, conv_end + margin], build path with consecutive dedup.

    Events in Staging use Chatbot conversation_id, not our synthetic conv_id (phone_YYYYMMDD),
    so we must get conversation_ids from the messages, not from df_merged.conversation_id.
    """
    if df_events.is_empty() or "conversation_id" not in df_events.columns:
        return df_merged.with_columns([
            pl.lit(None).cast(pl.Utf8).alias("funnel_type"),
            pl.lit(None).cast(pl.Utf8).alias("funnel_section"),
        ])
    required = ["event_type", "event_section", "created_at_local"]
    if not all(c in df_events.columns for c in required):
        context.log.warning("Events missing event_type/event_section/created_at_local; funnel_type/section will be null.")
        return df_merged.with_columns([
            pl.lit(None).cast(pl.Utf8).alias("funnel_type"),
            pl.lit(None).cast(pl.Utf8).alias("funnel_section"),
        ])
    events_pd = df_events.to_pandas()
    events_pd["conversation_id"] = events_pd["conversation_id"].astype(str)
    events_pd["created_at_local"] = pd.to_datetime(events_pd["created_at_local"], errors="coerce")
    events_by_conv = events_pd.groupby("conversation_id")

    funnel_types = []
    funnel_sections = []
    margin = pd.Timedelta(hours=window_margin_hours)
    for row in df_merged.iter_rows(named=True):
        messages = row.get("messages") or []
        if isinstance(messages, str):
            try:
                messages = json.loads(messages)
            except Exception:
                messages = []
        if not isinstance(messages, list):
            messages = []
        conversation_ids = set()
        for msg in messages:
            if isinstance(msg, dict):
                cid = msg.get("conversation_id") or msg.get("id")
                if cid is not None:
                    conversation_ids.add(str(cid))
        conv_start = row.get("conversation_start")
        conv_end = row.get("conversation_end")
        if not conversation_ids or conv_start is None or conv_end is None:
            funnel_types.append(None)
            funnel_sections.append(None)
            continue
        conv_start = pd.to_datetime(conv_start)
        conv_end = pd.to_datetime(conv_end)
        window_start = conv_start - margin
        window_end = conv_end + margin
        all_events = []
        for cid in conversation_ids:
            if cid in events_by_conv.groups:
                all_events.append(events_by_conv.get_group(cid))
        if not all_events:
            funnel_types.append(None)
            funnel_sections.append(None)
            continue
        combined = pd.concat(all_events, ignore_index=True)
        combined = combined.sort_values("created_at_local").reset_index(drop=True)
        in_window = combined[
            (combined["created_at_local"] >= window_start)
            & (combined["created_at_local"] <= window_end)
        ]
        funnel_type = _build_funnel_path_from_events(in_window, "event_type") if not in_window.empty else None
        funnel_section = _build_funnel_path_from_events(in_window, "event_section") if not in_window.empty else None
        funnel_types.append(funnel_type)
        funnel_sections.append(funnel_section)

    out = df_merged.with_columns([
        pl.Series("funnel_type", funnel_types),
        pl.Series("funnel_section", funnel_sections),
    ])
    n_with_funnel = out.filter(pl.col("funnel_type").is_not_null()).height
    context.log.info(f"   Funnel from events: {n_with_funnel:,} conversations with funnel_type")
    return out


def _add_scheduled_visit_and_created_project(
    df_merged: pl.DataFrame,
    df_projects: pl.DataFrame,
    df_appointments: pl.DataFrame,
    context,
) -> pl.DataFrame:
    """
    Add scheduled_visit_count and created_project_count (same logic as conversation_analysis
    conv_with_projects). Required so create_conv_variables emits scheduled_visit and created_project.
    Legacy aligns created_project with scheduling_form_completed events in messages; we use that
    as the source for created_project_count so governance matches legacy (fixes 0→1 and 2→1 discrepancies).
    """
    out = df_merged.with_columns([
        pl.lit(0).cast(pl.UInt32).alias("scheduled_visit_count"),
        pl.lit(0).cast(pl.UInt32).alias("created_project_count"),
    ])
    if df_projects.is_empty() or "lead_id" not in df_projects.columns or "project_created_at" not in df_projects.columns:
        return out
    # Align lead_id type
    if out["lead_id"].dtype == pl.Utf8:
        df_projects = df_projects.with_columns(pl.col("lead_id").cast(pl.Utf8))
    conv_base = out.filter(pl.col("lead_id").is_not_null()).select(
        ["conversation_id", "lead_id", "conversation_start", "conversation_end"]
    )
    if conv_base.is_empty():
        return out

    # Same constants as conversation_analysis conv_with_projects
    # created_project: count projects with project_created_at in [conv_start-PROJECT_LOOKBACK_HOURS, conv_end]
    proj = df_projects.select(["lead_id", "project_created_at"]).filter(pl.col("project_created_at").is_not_null())
    if not proj.is_empty():
        j = conv_base.join(proj, on="lead_id", how="inner")
        j = j.filter(
            (pl.col("project_created_at") >= pl.col("conversation_start") - pl.duration(hours=PROJECT_LOOKBACK_HOURS))
            & (pl.col("project_created_at") <= pl.col("conversation_end"))
        )
        cnt = j.group_by("conversation_id").len().rename({"len": "created_project_count"})
        out = out.drop("created_project_count").join(cnt, on="conversation_id", how="left")
        out = out.with_columns(pl.col("created_project_count").fill_null(0))

    # Align with legacy: use scheduling_form_completed in messages as source for created_project_count
    # (legacy uses this event as trigger; project-based count alone causes 0→1 and 2→1 discrepancies)
    if "messages" in out.columns:
        def _count_scheduling_form_completed(m):
            try:
                if m is None:
                    return 0
                s = json.loads(m) if isinstance(m, str) and m.strip() else m
                if not isinstance(s, list):
                    return 0
                return sum(
                    1 for x in s
                    if isinstance(x, dict) and (x.get("event_type") or "").strip() == "scheduling_form_completed"
                )
            except Exception:
                return 0
        created_from_events = [_count_scheduling_form_completed(row.get("messages")) for row in out.iter_rows(named=True)]
        out = out.with_columns(
            pl.Series(created_from_events).cast(pl.UInt32).alias("created_project_count")
        )

    # scheduled_visit (README 5.4 / legacy conv_with_projects): (1) visit_created_at in [conv_start, conv_end+5h],
    # (2) if 0, visit_updated_at in that range, (3) if still 0, event_type=visit_scheduled in messages.
    # Source: project_requirements (stg_s2p_project_requirements_conv_new) with visit_created_at, visit_updated_at.
    if not df_projects.is_empty() and "visit_created_at" in df_projects.columns:
        visit_end = conv_base.with_columns(
            (pl.col("conversation_end") + pl.duration(hours=VISIT_WINDOW_AFTER_END_HOURS)).alias("_end")
        )
        pr_visits = df_projects.filter(pl.col("lead_id").is_not_null()).select(
            ["lead_id", "visit_created_at", "visit_updated_at"] if "visit_updated_at" in df_projects.columns else ["lead_id", "visit_created_at"]
        )
        if "visit_updated_at" not in pr_visits.columns:
            pr_visits = pr_visits.with_columns(pl.lit(None).cast(pl.Datetime).alias("visit_updated_at"))
        v = visit_end.join(pr_visits, on="lead_id", how="inner")
        by_created = v.filter(
            (pl.col("visit_created_at") >= pl.col("conversation_start"))
            & (pl.col("visit_created_at") <= pl.col("_end"))
            & pl.col("visit_created_at").is_not_null()
        ).group_by("conversation_id").len().rename({"len": "sv"})
        by_updated = v.filter(
            pl.col("visit_updated_at").is_not_null()
            & (pl.col("visit_updated_at") >= pl.col("conversation_start"))
            & (pl.col("visit_updated_at") <= pl.col("_end"))
        ).group_by("conversation_id").len().rename({"len": "svu"})
        out = out.drop("scheduled_visit_count").join(by_created, on="conversation_id", how="left").join(by_updated, on="conversation_id", how="left")
        out = out.with_columns(
            pl.when(pl.col("sv").fill_null(0) > 0).then(pl.col("sv"))
            .when(pl.col("svu").fill_null(0) > 0).then(pl.col("svu"))
            .otherwise(pl.lit(0)).cast(pl.UInt32).alias("scheduled_visit_count")
        ).drop(["sv", "svu"])

    # Second validation: if still 0, count event_type=visit_scheduled in messages
    if "messages" in out.columns:
        def _count(m):
            try:
                if m is None: return 0
                s = json.loads(m) if isinstance(m, str) and m.strip() else m
                if not isinstance(s, list): return 0
                return sum(1 for x in s if isinstance(x, dict) and (x.get("event_type") or "").strip() == "visit_scheduled")
            except Exception:
                return 0
        msg_counts = [0] * out.height
        for i, row in enumerate(out.iter_rows(named=True)):
            if row.get("scheduled_visit_count", 0) == 0:
                msg_counts[i] = _count(row.get("messages"))
        out = out.with_columns(
            pl.when(pl.Series(msg_counts) > 0).then(pl.Series(msg_counts).cast(pl.UInt32))
            .otherwise(pl.col("scheduled_visit_count"))
            .alias("scheduled_visit_count")
        )
    context.log.info(
        f"   scheduled_visit/created_project: {(out.filter(pl.col('scheduled_visit_count') > 0).height)} convs with visits, "
        f"{(out.filter(pl.col('created_project_count') > 0).height)} convs with projects"
    )
    return out


def _transform_bt_conv_conversations(
    df_conversations: pl.DataFrame,
    df_clients: pl.DataFrame,
    df_appointments: pl.DataFrame,
    df_projects: pl.DataFrame,
    df_events: pl.DataFrame,
    context,
    df_lk_leads: pl.DataFrame | None = None,
    ai_window_start_dt: datetime | None = None,
    ai_window_end_dt: datetime | None = None,
    df_existing_governance: pl.DataFrame | None = None,
    df_conversations_merged: pl.DataFrame | None = None,
    df_lk_projects: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """
    Transforms conversations by:
    1. Matching to clients by phone number (skipped if df_conversations_merged is provided)
    2. Filling null lead_id from lk_leads (join on digits-only phone) when provided
    3. Enriching messages with events (skipped if df_conversations_merged is provided — already enriched in silver)
    4. Funnel, scheduled_visit/created_project, incremental, conv_variables

    When df_conversations_merged is provided (from stg_bt_conv_merged_new), client match and event enrichment are skipped.
    """
    # ========== MERGED CONVERSATIONS: from silver or match here ==========
    if df_conversations_merged is not None and not df_conversations_merged.is_empty():
        context.log.info("📥 Using pre-merged conversations from silver (stg_bt_conv_merged_new).")
        df_merged = df_conversations_merged
    else:
        context.log.info("🔗 Matching conversations to clients by phone number...")
        df_merged = match_conversations_to_clients(df_conversations, df_clients, context)

    # ========== FILL NULL lead_id FROM LK_LEADS ==========
    # Newest lead per _phone_match_key (legacy parity). Match key = last 10 digits if len>=10, else full digit string
    # (reduces 52 vs 1 prefix collisions when lead_full_phone_number formats differ).
    if df_lk_leads is not None and df_lk_leads.height > 0 and "lead_id" in df_lk_leads.columns and "lead_full_phone_number" in df_lk_leads.columns:
        sort_cols = ["lead_created_at", "lead_id"] if "lead_created_at" in df_lk_leads.columns else ["lead_id"]
        descending = [True, True] if "lead_created_at" in df_lk_leads.columns else [True]
        df_lk_sorted = df_lk_leads.sort(sort_cols, descending=descending, nulls_last=True)
        _digits_lk = pl.col("lead_full_phone_number").fill_null("").str.replace_all(r"\D", "").alias("_digits_lk")
        _key_lk = (
            pl.when(pl.col("_digits_lk").str.len_chars() >= 10)
            .then(pl.col("_digits_lk").str.slice(-10))
            .otherwise(pl.col("_digits_lk"))
            .alias("_phone_match_key")
        )
        phone_digits_leads = (
            df_lk_sorted.with_columns([_digits_lk])
            .with_columns([_key_lk])
            .filter(pl.col("_phone_match_key").str.len_chars() > 0)
            .select(
                pl.col("_phone_match_key"),
                pl.col("lead_id").alias("lead_id_lk"),
                pl.col("lead_created_at").alias("fecha_creacion_lk"),
            )
            .unique(subset=["_phone_match_key"], keep="first")
        )
        conv_side = df_merged.with_columns(
            pl.col("phone_number").fill_null("").str.replace_all(r"\D", "").alias("_digits_conv")
        ).with_columns(
            pl.when(pl.col("_digits_conv").str.len_chars() >= 10)
            .then(pl.col("_digits_conv").str.slice(-10))
            .otherwise(pl.col("_digits_conv"))
            .alias("_phone_match_key")
        )
        filled = conv_side.filter(pl.col("lead_id").is_null()).join(
            phone_digits_leads, on="_phone_match_key", how="inner"
        ).select(pl.col("conversation_id"), pl.col("lead_id_lk"), pl.col("fecha_creacion_lk"))
        if filled.height > 0:
            df_merged = (
                df_merged.join(filled, on="conversation_id", how="left")
                .with_columns([
                    pl.when(pl.col("lead_id").is_null()).then(pl.col("lead_id_lk")).otherwise(pl.col("lead_id")).alias("lead_id"),
                    pl.when(pl.col("fecha_creacion_cliente").is_null()).then(pl.col("fecha_creacion_lk")).otherwise(pl.col("fecha_creacion_cliente")).alias("fecha_creacion_cliente"),
                ])
                .drop(["lead_id_lk", "fecha_creacion_lk"])
            )
            context.log.info(f"📱 Filled {filled.height:,} null lead_id from lk_leads (digits-only phone join)")
    
    # ========== ENRICH WITH APPOINTMENTS (if available) ==========
    # TODO: Add appointment matching logic when needed
    # For now, we'll add placeholder columns
    if df_appointments.height > 0:
        context.log.info("📅 Enriching with appointments...")
        # Match appointments by lead_id (client_id) if available
        # This is a placeholder - implement actual matching logic when needed
        pass
    
    # ========== ENRICH WITH PROJECTS (if available) ==========
    # TODO: Add project matching logic when needed
    # For now, we'll add placeholder columns
    if df_projects.height > 0:
        context.log.info("📋 Enriching with projects...")
        # Match projects by lead_id (client_id) if available
        # This is a placeholder - implement actual matching logic when needed
        pass

    # ========== CONVERSATION EVENTS (from Staging) ==========
    # Events available for funnel_type, funnel_section (aligned with conversation_analysis conv_with_funnels).
    # Skip message enrichment when using pre-merged silver (messages already enriched in stg_bt_conv_with_events_new).
    if df_events.height > 0:
        context.log.info(
            f"📊 Conversation events loaded: {df_events.height:,} rows, "
            f"{df_events['conversation_id'].n_unique():,} unique conversation_ids"
        )
        if df_conversations_merged is None or df_conversations_merged.is_empty():
            # Enrich messages with event_type/event_section (same as conv_with_events) so conv_messages matches legacy.
            df_merged = _enrich_conv_messages_with_events(df_merged, df_events, context)

    # ========== FUNNEL_TYPE / FUNNEL_SECTION FROM EVENTS (for conv_variables) ==========
    # Same logic as conversation_analysis conv_with_funnels (local constants and _build_funnel_path_from_events).
    df_merged = _add_funnel_from_events(df_merged, df_events, FUNNEL_WINDOW_MARGIN_HOURS, context)

    # ========== SCHEDULED_VISIT / CREATED_PROJECT COUNTS (for conv_variables tags) ==========
    # Same logic as conversation_analysis conv_with_projects; required so create_conv_variables emits scheduled_visit and created_project.
    df_merged = _add_scheduled_visit_and_created_project(df_merged, df_projects, df_appointments, context)

    # ========== INCREMENTAL MASK (legacy: _compute_incremental_mask with governance data) ==========
    # Skip criterion: conv_id in Geospot AND len(sanitize_json_string(messages)) == len(sanitize_json_string(conv_messages)).
    # Do NOT use full JSON comparison (e.g. existing_content_map): message id/timestamp formatting always differs and forces full AI re-run.
    # _compute_incremental_mask via gold.bt_conv_variables.vendor (len-based, same as existing_geo_map).
    df_merged_pd = df_merged.to_pandas()
    df_merged_pd["timestamp_ultimo_mensaje"] = df_merged_pd.get("last_message_timestamp", pd.Series(dtype="datetime64[ns]"))
    df_geo = None
    if df_existing_governance is not None and not df_existing_governance.is_empty():
        start_ts = pd.to_datetime(df_merged_pd["conversation_start"]).min()
        end_ts = pd.to_datetime(df_merged_pd["conversation_end"]).max()
        start_str = start_ts.strftime("%Y-%m-%d") if hasattr(start_ts, "strftime") else str(start_ts)[:10]
        end_str = end_ts.strftime("%Y-%m-%d") if hasattr(end_ts, "strftime") else str(end_ts)[:10]
        start_wide = (pd.to_datetime(start_str) - pd.Timedelta(days=3)).strftime("%Y-%m-%d")
        end_wide = (pd.to_datetime(end_str) + pd.Timedelta(days=3)).strftime("%Y-%m-%d")
        existing_pd = df_existing_governance.to_pandas()
        existing_pd["conv_messages"] = existing_pd["conv_messages"].apply(_normalize_jsonb_value)
        existing_pd["conv_variables"] = existing_pd["conv_variables"].apply(_normalize_jsonb_value)
        existing_pd["conv_start_date"] = pd.to_datetime(existing_pd["conv_start_date"], errors="coerce")
        existing_filtered = existing_pd[
            (existing_pd["conv_start_date"] >= start_wide) & (existing_pd["conv_start_date"] <= end_wide)
        ]

        def _query_governance(_q):
            return existing_filtered
        df_merged_pd, df_geo = _compute_incremental_mask(
            df_merged_pd,
            _query_governance,
            "bt_conv_conversations",
            log=context.log.info,
        )
    else:
        df_merged_pd["_to_process"] = True

    # ========== BUILD conv_variables (base + experiments + funnel_events + seniority) ==========
    cv_config = ConvVariablesConfig(
        use_incremental_mask=True,
    )
    context.log.info("📊 Building conv_variables (base + experiments + funnel_events + seniority)...")
    df_events_pd = df_events.to_pandas()
    # Do not coerce conversation_id to str: _get_events_in_conversation_window matches msg['id'] as-is
    # from JSON; if groupby uses str and id is int, there is no group and bot_graph / funnel_events stay empty.
    seniority_by_conversation_id = None
    if (
        df_lk_projects is not None
        and df_lk_projects.height > 0
        and "conversation_id" in df_merged_pd.columns
        and "lead_id" in df_merged_pd.columns
    ):
        lk_pd = df_lk_projects.to_pandas()
        need_cols = [c for c in _LK_PROJECTS_SENIORITY_COLUMNS if c in lk_pd.columns]
        if len(need_cols) >= 2 and "lead_id" in need_cols:
            lk_pd = lk_pd[need_cols].copy()
            conv_subset = df_merged_pd[["conversation_id", "lead_id"]].copy()
            n_lead = int(conv_subset["lead_id"].notna().sum())
            context.log.info(
                f"📊 seniority: {n_lead}/{len(conv_subset)} conversations with lead_id; "
                f"{len(lk_pd)} lk_projects rows (seniority columns={len(need_cols)}); "
                "matching via segment._normalize_lead_id only (legacy parity)"
            )
            seniority_df = build_conv_seniority_map(conv_subset, lk_pd)
            n_lvl = int(seniority_df["seniority_level"].notna().sum())
            context.log.info(
                f"📊 seniority: build_conv_seniority_map returned a level for {n_lvl}/{len(seniority_df)} rows"
            )
            # Same as legacy: conv_seniority.set_index("conversation_id")["seniority_level"].to_dict()
            _s = seniority_df.dropna(subset=["seniority_level"])
            _s = _s[_s["seniority_level"].astype(str).str.strip() != ""]
            seniority_by_conversation_id = _s.set_index("conversation_id")["seniority_level"].to_dict()
            context.log.info(
                f"📊 seniority_level: conv_id → level map for {len(seniority_by_conversation_id)} conversations"
            )
        else:
            context.log.warning(
                "⚠️ lk_projects missing minimum columns for seniority; skipping segment.seniority_level"
            )

    df_merged_pd = run_conv_variables_flow(
        df_merged_pd,
        df_events_pd,
        cv_config,
        context,
        ai_window_start_dt=ai_window_start_dt,
        ai_window_end_dt=ai_window_end_dt,
        df_geospot=df_geo,
        seniority_by_conversation_id=seniority_by_conversation_id,
    )
    df_merged = df_merged.with_columns(pl.Series("conv_variables", df_merged_pd["conv_variables"].tolist()))
    # Drop incremental helper column so it doesn't leak into output
    if "_to_process" in df_merged.columns:
        df_merged = df_merged.drop("_to_process")

    # ========== RENAME COLUMNS TO MATCH SCHEMA (based on conversation_analysis + legacy) ==========
    # Parquet/Geospot column order must match physical order in Postgres (see BT_CONV_CONVERSATIONS_FINAL_COLUMN_ORDER).
    # conv_text: normalize \r\n → \n and strip. Silver already joins turns with a space (group_messages_into_conversations); Geospot legacy parity.
    conv_text_normalized = (
        pl.col("conversation_text")
        .fill_null("")
        .str.replace_all("\r\n", "\n")
        .str.replace_all("\r", "\n")
        .str.strip_chars()
    )
    # lead_phone_number: digits only so downstream joins with lk_leads.lead_full_phone_number
    # use REGEXP_REPLACE(lead_full_phone_number, '[^0-9]', '', 'g') = lead_phone_number
    lead_phone_digits = pl.col("phone_number").str.replace_all(r"\D", "")
    # lead_id: legacy is text; cast to string so WHERE lead_id = '12345' works in governance
    lead_id_text = pl.when(pl.col("lead_id").is_null()).then(pl.lit(None).cast(pl.Utf8)).otherwise(pl.col("lead_id").cast(pl.Utf8))
    # Truncate timestamps to second precision (legacy stores seconds only; parity avoids 100% diff on these columns).
    conv_start_sec = pl.col("conversation_start").dt.truncate("1s")
    conv_end_sec = pl.col("conversation_end").dt.truncate("1s")
    conv_last_sec = pl.col("last_message_timestamp").dt.truncate("1s")
    # Rename to gold schema (table has conv_id, not conversation_id).
    df_final = df_merged.with_columns([
        pl.col("conversation_id").alias("conv_id"),
        lead_phone_digits.alias("lead_phone_number"),
        conv_start_sec.alias("conv_start_date"),
        conv_end_sec.alias("conv_end_date"),
        conv_last_sec.alias("conv_last_message_date"),
        conv_text_normalized.alias("conv_text"),
        pl.col("messages").map_elements(_normalize_empty_message_body_and_sanitize, return_dtype=pl.Utf8).alias("conv_messages"),
        lead_id_text.alias("lead_id"),
        pl.col("fecha_creacion_cliente").alias("lead_created_at"),
        pl.col("conv_variables"),
    ])
    # Business columns only; caller adds aud_* via add_audit_fields then reorders to FINAL.
    df_final = df_final.select(BT_CONV_CONVERSATIONS_CORE_COLUMNS)

    # Ensure we did not drop conversations (one row per conv_id; multiple per lead_phone_number allowed)
    if df_final["conv_id"].n_unique() != df_final.height:
        context.log.error(
            f"Duplicate conv_id in output: {df_final.height} rows but {df_final['conv_id'].n_unique()} unique conv_id. "
            "This must not happen."
        )

    return df_final


def _gold_bt_conv_conversations_new_body(
    context,
    stg_bt_conv_merged_new: pl.DataFrame,
) -> pl.DataFrame:
    """
    Creates the gold conversations table. Upstream silver stg_bt_conv_merged_new provides
    conversations (grouped 72h, enriched with events, merged with clients). Gold adds:
    funnel_type/funnel_section, scheduled_visit/created_project, conv_variables (metrics, AI, DSPy),
    incremental mask, upsert with governance, audit fields, S3 + Geospot.
    """
    ref_date = resolve_stale_reference_date(context)

    # ========== READ SILVERS FROM S3 (events, projects, appointments for funnel & counts) ==========
    stale_tables = []

    # Read clients (only needed when stg_bt_conv_merged_new is empty and we fall back to transform without merged)
    stg_s2p_clients_new, meta_clients = read_silver_from_s3("stg_s2p_clients_new", context)
    if meta_clients.get("is_stale"):
        stale_tables.append({
            "table_name": "stg_s2p_clients_new",
            "expected_date": ref_date,
            "available_date": meta_clients.get("file_date"),
            "layer": "silver",
            "file_path": meta_clients.get("file_path"),
        })
    
    # Read appointments (optional)
    try:
        stg_s2p_appointments_new, meta_appointments = read_silver_from_s3(
            "stg_s2p_appointments_new", context
        )
        if meta_appointments['is_stale']:
            stale_tables.append({
                'table_name': 'stg_s2p_appointments_new',
                'expected_date': ref_date,
                'available_date': meta_appointments['file_date'],
                'layer': 'silver',
                'file_path': meta_appointments['file_path'],
            })
    except Exception as e:
        context.log.warning(f"⚠️  Could not read appointments: {e}")
        stg_s2p_appointments_new = pl.DataFrame()
    
    # Read projects for conv (legacy parity: project_requirements JOIN calendar_appointments WHERE ca.origin=2)
    try:
        stg_s2p_project_requirements_conv_new, meta_projects = read_silver_from_s3(
            "stg_s2p_project_requirements_conv_new", context
        )
        if meta_projects['is_stale']:
            stale_tables.append({
                'table_name': 'stg_s2p_project_requirements_conv_new',
                'expected_date': ref_date,
                'available_date': meta_projects['file_date'],
                'layer': 'silver',
                'file_path': meta_projects['file_path'],
            })
    except Exception as e:
        context.log.warning(f"⚠️  Could not read project_requirements_conv: {e}")
        stg_s2p_project_requirements_conv_new = pl.DataFrame()
    
    # Read conversation events (Staging) for enrichment - aligned with conversation_analysis
    try:
        stg_staging_conversation_events_new, meta_events = read_silver_from_s3(
            "stg_staging_conversation_events_new", context
        )
        if meta_events["is_stale"]:
            stale_tables.append({
                "table_name": "stg_staging_conversation_events_new",
                "expected_date": ref_date,
                "available_date": meta_events["file_date"],
                "layer": "silver",
                "file_path": meta_events["file_path"],
            })
        context.log.info(f"📊 Conversation events: {stg_staging_conversation_events_new.height:,} rows")
    except Exception as e:
        context.log.warning(f"⚠️  Could not read conversation events: {e}")
        stg_staging_conversation_events_new = pl.DataFrame()

    # ========== USE MERGED CONVERSATIONS FROM SILVER ==========
    if stg_bt_conv_merged_new.is_empty():
        context.log.warning("⚠️  No conversations from silver (stg_bt_conv_merged_new) - writing empty result to S3 and Geospot")
        # Same schema and order as governance (15 columns)
        df_final = pl.DataFrame(
            schema={
                "conv_id": pl.Utf8,
                "lead_phone_number": pl.Utf8,
                "conv_start_date": pl.Datetime,
                "conv_end_date": pl.Datetime,
                "conv_text": pl.Utf8,
                "conv_messages": pl.Utf8,
                "conv_last_message_date": pl.Datetime,
                "lead_id": pl.Utf8,
                "lead_created_at": pl.Datetime,
                "conv_variables": pl.Utf8,
            }
        )
        df_final = add_audit_fields(df_final, job_name="bt_conv_conversations_new")
        df_final = df_final.select(BT_CONV_CONVERSATIONS_FINAL_COLUMN_ORDER)
        df_to_write = _upsert_bt_conv_conversations(df_final, context)
        s3_key = build_gold_s3_key("gold_bt_conv_conversations_new", ref_date, file_format="parquet")
        df_s3, jsonb_cols = _prepare_df_for_geospot_parquet_jsonb(df_to_write, context)
        write_polars_to_s3(
            df_s3,
            s3_key,
            context,
            file_format="parquet",
            geospot_jsonb_columns=jsonb_cols,
        )
        if df_to_write.height == 0:
            context.log.warning("⚠️  No rows to load: skipping load_to_geospot(upsert)")
        else:
            update_cols_empty = [c for c in BT_CONV_CONVERSATIONS_FINAL_COLUMN_ORDER if c != "conv_id"]
            context.log.info(f"📤 Loading to Geospot: table=bt_conv_conversations, s3_key={s3_key}, mode=upsert")
            try:
                load_to_geospot(
                    s3_key=s3_key,
                    table_name="bt_conv_conversations",
                    mode="upsert",
                    context=context,
                    conflict_columns=["conv_id"],
                    update_columns=update_cols_empty,
                    post_async_verify_seconds=_geospot_bt_conv_post_verify_seconds(),
                )
                context.log.info("✅ Geospot load completed for bt_conv_conversations")
            except Exception as e:
                context.log.error(f"❌ Geospot load failed: {e}")
                raise
        if stale_tables:
            send_stale_data_notification(
                context,
                stale_tables,
                current_asset_name="gold_bt_conv_conversations_new",
                current_layer="gold",
            )
        return df_to_write

    # Log input from silver
    context.log.info(f"📊 Conversations from silver: {stg_bt_conv_merged_new.height:,} rows")
    unique_conv_ids = stg_bt_conv_merged_new["conversation_id"].n_unique()
    context.log.info(f"📊 Unique conversation_ids: {unique_conv_ids:,}")

    # lead_id comes from silver (stg_bt_conv_merged_new). Do not fill from lk_leads for legacy parity.
    df_lk_leads = None

    # Load mode: "replace" truncates the table and reloads all rows (one-time migration use).
    # Set env var GOVERNANCE_BT_CONV_LOAD_MODE=replace for that run; subsequent runs default to "upsert".
    load_mode = os.environ.get("GOVERNANCE_BT_CONV_LOAD_MODE", "upsert").strip().lower()
    if load_mode not in ("replace", "upsert"):
        load_mode = "upsert"
    context.log.info(f"📋 Load mode: {load_mode} (set GOVERNANCE_BT_CONV_LOAD_MODE=replace for one-time full reload)")

    partition_date = datetime.strptime(ref_date, "%Y-%m-%d").date()

    # Load existing governance for incremental (_compute_incremental_mask) and for upsert.
    # Skipped when load_mode=replace: all rows are processed fresh and table is fully reloaded.
    df_existing_governance = None
    if load_mode == "upsert":
        try:
            df_existing_governance = read_governance_from_db(
                "bt_conv_conversations",
                context,
                columns=BT_CONV_CONVERSATIONS_FINAL_COLUMN_ORDER,
                jsonb_columns=["conv_messages", "conv_variables"],
            )
            if df_existing_governance is not None and not df_existing_governance.is_empty():
                context.log.info(
                    f"📋 Loaded {df_existing_governance.height:,} existing rows for incremental mask"
                )
            else:
                df_existing_governance = None
        except Exception as e:
            context.log.warning(f"Could not load existing governance: {e}; will process all rows")
    else:
        context.log.info("🔄 load_mode=replace: skipping existing governance load — all rows will be processed")

    # lk_projects in PostgreSQL/Geospot: same idea as legacy conv_raw_lk_projects (query lk_projects in Geospot).
    # Not reading gold parquet from S3; this is the table populated by gold_lk_projects_new via load_to_geospot.
    df_lk_projects = None
    try:
        df_lk_projects_full = read_governance_from_db(
            "lk_projects",
            context,
            columns=_LK_PROJECTS_SENIORITY_COLUMNS,
        )
        if df_lk_projects_full is not None and df_lk_projects_full.height > 0:
            avail = [c for c in _LK_PROJECTS_SENIORITY_COLUMNS if c in df_lk_projects_full.columns]
            if "lead_id" in avail and len(avail) >= 2:
                df_lk_projects = df_lk_projects_full.select(avail)
                context.log.info(
                    f"📊 lk_projects loaded for seniority: {df_lk_projects.height:,} rows, {len(avail)} columns"
                )
            else:
                context.log.warning("⚠️ lk_projects has insufficient columns for seniority; skipping")
    except Exception as e:
        context.log.warning(f"⚠️ Could not read lk_projects for seniority: {e}")

    # ========== TRANSFORM AND ENRICH (funnel, projects, conv_variables with legacy) ==========
    df_final = _transform_bt_conv_conversations(
        pl.DataFrame(),
        stg_s2p_clients_new,
        stg_s2p_appointments_new,
        stg_s2p_project_requirements_conv_new,
        stg_staging_conversation_events_new,
        context,
        df_lk_leads=df_lk_leads,
        ai_window_start_dt=None,
        ai_window_end_dt=None,
        df_existing_governance=df_existing_governance,
        df_conversations_merged=stg_bt_conv_merged_new,
        df_lk_projects=df_lk_projects,
    )

    # ========== KEEP CONVERSATIONS FROM CURRENT YEAR (PLUS 31-DEC OF PREVIOUS YEAR) ==========
    # Current year only; include 31-Dec of previous year so conv_id like *_20251231 is not dropped (QA gap recovery).
    current_year = datetime.now().year
    rows_before = df_final.height
    is_current_year = pl.col("conv_start_date").dt.year() == current_year
    is_last_day_prev_year = (
        (pl.col("conv_start_date").dt.year() == current_year - 1)
        & (pl.col("conv_start_date").dt.month() == 12)
        & (pl.col("conv_start_date").dt.day() == 31)
    )
    df_final = df_final.filter(is_current_year | is_last_day_prev_year)
    context.log.info(
        f"Filtered to year {current_year} (+ 31-Dec {current_year - 1}): {rows_before:,} -> {df_final.height:,} rows"
    )

    # Same scope as legacy per run: 3-day window of conversations (bronze 3-day by partition). Total in Geospot = upsert over runs.
    context.log.info(
        f"gold_bt_conv_conversations_new: {df_final.height:,} rows for ref_date={ref_date} (legacy-equivalent 3-day scope)"
    )
    
    # ========== ADD AUDIT FIELDS ==========
    df_final = add_audit_fields(df_final, job_name="bt_conv_conversations_new")
    # Same order as CREATE TABLE in Postgres (conv_variables before aud_*) for Parquet → Geospot.
    df_final = df_final.select(BT_CONV_CONVERSATIONS_FINAL_COLUMN_ORDER)
    # ========== UPSERT OR REPLACE ==========
    if load_mode == "replace":
        # Full replace: write all rows as-is, table will be truncated and reloaded.
        context.log.info(f"🔄 load_mode=replace: writing all {df_final.height:,} rows (no merge with existing)")
        df_to_write = df_final
    else:
        df_to_write = _upsert_bt_conv_conversations(df_final, context)

    # ========== COLUMN BREAKDOWN LOG ==========
    # Count columns by category based on conversation_analysis structure
    all_columns = df_to_write.columns
    
    # Core columns (conv_id through conv_variables = 10 columns; then aud_*)
    core_columns_from_analysis = list(BT_CONV_CONVERSATIONS_CORE_COLUMNS)
    
    # Audit columns (added by add_audit_fields)
    audit_columns = [col for col in all_columns if col.startswith("aud_")]
    
    # Count each category
    core_count = len([col for col in all_columns if col in core_columns_from_analysis])
    audit_count = len(audit_columns)
    
    # Find missing core columns
    missing_core = [col for col in core_columns_from_analysis if col not in all_columns]
    
    # Other columns (not in any category)
    other_columns = [
        col for col in all_columns 
        if col not in core_columns_from_analysis 
        and col not in audit_columns
    ]
    other_count = len(other_columns)
    
    # Log breakdown
    expected_total = len(core_columns_from_analysis) + 5  # core + audit
    context.log.info(
        f"📊 Column breakdown for gold_bt_conv_conversations_new:\n"
        f"   • Core columns (from conversation_analysis): {core_count}/{len(core_columns_from_analysis)}\n"
        f"   • Audit columns (aud_*): {audit_count}/5\n"
        f"   • Other columns: {other_count}\n"
        f"   • TOTAL: {df_to_write.width} columns (expected: {expected_total})"
    )
    
    if missing_core:
        context.log.warning(
            f"⚠️  Missing {len(missing_core)} core columns: {', '.join(missing_core[:10])}"
            + (f" ... and {len(missing_core) - 10} more" if len(missing_core) > 10 else "")
        )
    
    if other_count > 0:
        context.log.warning(
            f"⚠️  Found {other_count} unexpected columns: {', '.join(other_columns[:10])}"
            + (f" ... and {other_count - 10} more" if other_count > 10 else "")
        )
    
    # ========== PERSIST TO S3 ==========
    s3_key = build_gold_s3_key("gold_bt_conv_conversations_new", ref_date, file_format="parquet")
    df_s3, jsonb_cols = _prepare_df_for_geospot_parquet_jsonb(df_to_write, context)
    context.log.info(
        "📋 Parquet column order (must match ordinal_position in Postgres): "
        + ", ".join(df_s3.columns)
    )
    write_polars_to_s3(
        df_s3,
        s3_key,
        context,
        file_format="parquet",
        geospot_jsonb_columns=jsonb_cols,
    )

    n_rows = df_to_write.height
    if n_rows == 0:
        context.log.warning(
            f"⚠️  df_to_write has 0 rows: skipping load_to_geospot({load_mode})"
        )
    else:
        context.log.info(
            f"📤 Loading to Geospot: table=bt_conv_conversations, s3_key={s3_key}, mode={load_mode}, rows={n_rows:,}"
        )
        try:
            if load_mode == "replace":
                load_to_geospot(
                    s3_key=s3_key,
                    table_name="bt_conv_conversations",
                    mode="replace",
                    context=context,
                )
            else:
                update_cols = [c for c in BT_CONV_CONVERSATIONS_FINAL_COLUMN_ORDER if c != "conv_id"]
                load_to_geospot(
                    s3_key=s3_key,
                    table_name="bt_conv_conversations",
                    mode="upsert",
                    context=context,
                    conflict_columns=["conv_id"],
                    update_columns=update_cols,
                    post_async_verify_seconds=_geospot_bt_conv_post_verify_seconds(),
                )
            context.log.info(f"✅ Geospot load completed for bt_conv_conversations (mode={load_mode})")
        except Exception as e:
            context.log.error(f"❌ Geospot load failed: {e}")
            raise

    # ========== SEND NOTIFICATIONS IF STALE DATA USED ==========
    if stale_tables:
        send_stale_data_notification(
            context,
            stale_tables,
            current_asset_name="gold_bt_conv_conversations_new",
            current_layer="gold",
        )

    return df_to_write


@dg.asset(
    group_name="gold",
    retry_policy=dg.RetryPolicy(max_retries=2),
    io_manager_key="s3_gold",
    description=(
        "Gold: BT_CONV_CONVERSATIONS (bt_conv_conversations). No daily_partitions; "
        "ref_date for S3/windows = resolve_stale_reference_date (MX today unless run has partition tag)."
    ),
)
def gold_bt_conv_conversations_new(
    context,
    stg_bt_conv_merged_new: pl.DataFrame,
):
    def body():
        return _gold_bt_conv_conversations_new_body(context, stg_bt_conv_merged_new)

    yield from iter_job_wrapped_compute(context, body)
