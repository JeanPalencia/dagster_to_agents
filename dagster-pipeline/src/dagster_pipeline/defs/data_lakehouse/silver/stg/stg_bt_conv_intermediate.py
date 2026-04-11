# defs/data_lakehouse/silver/stg/stg_bt_conv_intermediate.py
"""
Silver intermediate assets for BT conversations pipeline (conv_grouped → conv_with_events → conv_merged).
Replicates conversation_analysis steps so gold only does funnel, projects, conv_variables, upsert.

- stg_bt_conv_grouped_new: group messages (72h gap). Reads raw_cb_messages_new (SQL aligned with messages.sql).
- stg_bt_conv_with_events_new: enrich messages with event_type, event_section from Staging events.
- stg_bt_conv_merged_new: merge with clients by phone (lead_id, fecha_creacion_cliente).
"""
from __future__ import annotations

import json

import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import (
    build_silver_s3_key,
    write_polars_to_s3,
    read_silver_from_s3,
)
from dagster_pipeline.defs.data_lakehouse.gold.utils import (
    group_messages_into_conversations,
    match_conversations_to_clients,
)
from dagster_pipeline.defs.data_lakehouse.gold.bt_conv_variables.vendor import _enrich_messages_with_events
from dagster_pipeline.defs.data_lakehouse.gold.bt_conv_message_event_type_parity import (
    apply_legacy_event_type_labels_to_messages,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


# -------- stg_bt_conv_grouped_new --------

@dg.asset(
    group_name="silver",
    retry_policy=dg.RetryPolicy(max_retries=2),
    description="Silver: group messages into conversations (one per phone). conv_id = most frequent chatbot conversation_id for that phone.",
    io_manager_key="s3_silver",
)
def stg_bt_conv_grouped_new(context, raw_cb_messages_new: pl.DataFrame):
    """Group messages into conversations (72h gap). Uses bronze raw_cb_messages_new (3-day window like legacy)."""
    def body():
        context.log.info("📥 Using messages from bronze raw_cb_messages_new (3-day window)...")
        df_messages = raw_cb_messages_new
        if df_messages.height == 0:
            context.log.warning("⚠️  No messages — returning empty conversations.")
            return pl.DataFrame()
        context.log.info(f"📥 Messages: {df_messages.height:,} rows. Grouping into conversations (one per phone, conv_id = most frequent chatbot conv id)...")
        df_conversations = group_messages_into_conversations(
            df_messages, one_conv_per_phone=True
        )
        context.log.info(f"✅ Grouped: {df_conversations.height:,} conversations")
        s3_key = build_silver_s3_key("stg_bt_conv_grouped_new", file_format="parquet")
        write_polars_to_s3(df_conversations, s3_key, context, file_format="parquet")
        return df_conversations

    yield from iter_job_wrapped_compute(context, body)


# -------- stg_bt_conv_with_events_new --------


@dg.asset(
    group_name="silver",
    retry_policy=dg.RetryPolicy(max_retries=2),
    description="Silver: enrich conversation messages with event_type, event_section. Replicates conv_with_events.",
    io_manager_key="s3_silver",
)
def stg_bt_conv_with_events_new(
    context,
    stg_bt_conv_grouped_new: pl.DataFrame,
):
    """Enrich each message with event_type and event_section from Staging conversation_events."""
    def body():
        import pandas as pd

        if stg_bt_conv_grouped_new.is_empty():
            context.log.warning("⚠️  No conversations — returning empty.")
            return stg_bt_conv_grouped_new
        df_events, _ = read_silver_from_s3("stg_staging_conversation_events_new", context)
        if df_events.is_empty() or "conversation_id" not in df_events.columns:
            context.log.info("   No events; returning conversations without event enrichment.")
            s3_key = build_silver_s3_key("stg_bt_conv_with_events_new", file_format="parquet")
            write_polars_to_s3(stg_bt_conv_grouped_new, s3_key, context, file_format="parquet")
            return stg_bt_conv_grouped_new
        events_pd = df_events.to_pandas()
        events_pd["conversation_id"] = events_pd["conversation_id"].astype(str)
        if "created_at_local" in events_pd.columns:
            events_pd["created_at_local"] = pd.to_datetime(
                events_pd["created_at_local"], errors="coerce"
            )
        events_by_cid = events_pd.groupby("conversation_id")
        context.log.info("🔗 Enriching messages with event_type/event_section...")
        new_messages = []
        for row in stg_bt_conv_grouped_new.iter_rows(named=True):
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
                        enriched_list.append(
                            out[0] if isinstance(out, list) and len(out) else msg
                        )
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
            new_messages.append(json.dumps(enriched_list, ensure_ascii=False))
        df_out = stg_bt_conv_grouped_new.with_columns(pl.Series("messages", new_messages))
        context.log.info(f"✅ Enriched: {df_out.height:,} conversations")
        s3_key = build_silver_s3_key("stg_bt_conv_with_events_new", file_format="parquet")
        write_polars_to_s3(df_out, s3_key, context, file_format="parquet")
        return df_out

    yield from iter_job_wrapped_compute(context, body)


# -------- stg_bt_conv_merged_new --------


@dg.asset(
    group_name="silver",
    retry_policy=dg.RetryPolicy(max_retries=2),
    description="Silver: merge conversations with clients by phone. Replicates conv_merged.",
    io_manager_key="s3_silver",
)
def stg_bt_conv_merged_new(
    context,
    stg_bt_conv_with_events_new: pl.DataFrame,
):
    """Match conversations to clients by phone (lead_id, fecha_creacion_cliente)."""
    def body():
        if stg_bt_conv_with_events_new.is_empty():
            context.log.warning("⚠️  No conversations — returning empty.")
            return stg_bt_conv_with_events_new
        df_clients, _ = read_silver_from_s3("stg_s2p_clients_new", context)
        if df_clients.is_empty():
            context.log.info("   No clients; lead_id and fecha_creacion_cliente will be null.")
            df_out = stg_bt_conv_with_events_new.with_columns([
                pl.lit(None).cast(pl.Utf8).alias("lead_id"),
                pl.lit(None).cast(pl.Datetime).alias("fecha_creacion_cliente"),
            ])
        else:
            # Ensure columns expected by match_conversations_to_clients
            if "id" not in df_clients.columns and "lead_id" in df_clients.columns:
                df_clients = df_clients.with_columns(pl.col("lead_id").alias("id"))
            if "created_at" not in df_clients.columns and "fecha_creacion_cliente" in df_clients.columns:
                df_clients = df_clients.with_columns(
                    pl.col("fecha_creacion_cliente").alias("created_at")
                )
            df_out = match_conversations_to_clients(
                stg_bt_conv_with_events_new,
                df_clients,
                context=context,
            )
        context.log.info(f"✅ Merged: {df_out.height:,} conversations")
        s3_key = build_silver_s3_key("stg_bt_conv_merged_new", file_format="parquet")
        write_polars_to_s3(df_out, s3_key, context, file_format="parquet")
        return df_out

    yield from iter_job_wrapped_compute(context, body)
