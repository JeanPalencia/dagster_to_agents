# defs/data_lakehouse/bronze/raw_cb_messages_new.py
"""
Bronze: Chatbot messages. SQL aligned with messages.sql plus message_row_id column (governance only).
(CTE, conversation_id AS id, to_char in CDMX). 3-day window with
**resolve_stale_reference_date(context)** as the **end** calendar day (23:59:59): today in MX for
non-partitioned runs; for partitioned jobs, the run's `dagster/partition` tag.
"""
import os
from datetime import datetime, timedelta

import dagster as dg

from dagster_pipeline.defs.data_lakehouse.bronze.base import _run_bronze_extract
from dagster_pipeline.defs.data_lakehouse.shared import resolve_stale_reference_date
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute

# Based on conversation_analysis/queries/messages.sql; governance adds m.id AS message_row_id
# (lakehouse only — do not edit conversation_analysis's file).
_MESSAGES_SQL = """
WITH conversations_with_messages_today AS (
            SELECT DISTINCT m.phone_number
            FROM messages m
            WHERE m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' >= '{start_date}'
            AND m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' <= '{end_date}'
        )
        SELECT
            m.conversation_id AS id,
            m.id AS message_row_id,
            m.phone_number,
            CASE
                WHEN m.content->'text'->>'body' IS NOT NULL
                THEN m.content->'text'->>'body'

                WHEN jsonb_typeof(m.content) = 'string'
                THEN trim(both '"' from m.content::text)

                WHEN m.content ? 'body'
                     AND NULLIF(trim(m.content->>'body'), '') IS NOT NULL
                THEN m.content->>'body'

                WHEN m.content->'interactive'->'action'->'buttons' IS NOT NULL
                THEN CONCAT(
                    m.content->'interactive'->'body'->>'text',
                    E'\\n\\n[',
                    COALESCE(m.content->'interactive'->'action'->'buttons'->0->'reply'->>'title', ''),
                    CASE
                        WHEN m.content->'interactive'->'action'->'buttons'->1->'reply'->>'title' IS NOT NULL
                        THEN CONCAT(' o ', m.content->'interactive'->'action'->'buttons'->1->'reply'->>'title')
                        ELSE ''
                    END,
                    ']'
                )
                WHEN m.content->'interactive'->'body'->>'text' IS NOT NULL
                THEN m.content->'interactive'->'body'->>'text'
                WHEN m.content->'interactive'->'button_reply'->>'title' IS NOT NULL
                THEN m.content->'interactive'->'button_reply'->>'title'
                ELSE NULL
            END AS message_body,
            m.type,
            to_char(m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City', 'YYYY-MM-DD HH24:MI:SS') AS created_at_utc,
            DATE(m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City') AS created_dt
        FROM messages m
        INNER JOIN conversations_with_messages_today c ON m.phone_number = c.phone_number
        WHERE m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' >= '{previous_day_start}'
        AND m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' <= '{end_date}'
        ORDER BY m.phone_number, m.created_at_utc ASC, m.id ASC
"""


def _legacy_message_date_params(partition_key: str, n_days: int = 3) -> tuple[str, str, str]:
    end_d = datetime.strptime(partition_key, "%Y-%m-%d").date()
    start_d = end_d - timedelta(days=n_days)
    prev_d = start_d - timedelta(days=1)
    start_date = f"{start_d.strftime('%Y-%m-%d')} 00:00:00"
    end_date = f"{end_d.strftime('%Y-%m-%d')} 23:59:59"
    previous_day_start = f"{prev_d.strftime('%Y-%m-%d')} 00:00:00"
    return start_date, end_date, previous_day_start


def _build_cb_messages_query(partition_key: str, n_days: int = 3) -> str:
    start_date, end_date, previous_day_start = _legacy_message_date_params(partition_key, n_days=n_days)
    return _MESSAGES_SQL.format(
        start_date=start_date,
        end_date=end_date,
        previous_day_start=previous_day_start,
    )


@dg.asset(
    name="raw_cb_messages_new",
    group_name="bronze",
    description="Bronze: Chatbot messages (messages.sql logic inline, legacy 3-day window). Set GOVERNANCE_BT_CONV_N_DAYS env var to override lookback window (e.g. 100 for full-year migration).",
    io_manager_key="s3_bronze",
)
def raw_cb_messages_new(context):
    def body():
        end_key = resolve_stale_reference_date(context)
        n_days = int(os.environ.get("GOVERNANCE_BT_CONV_N_DAYS", "3"))
        if n_days != 3:
            context.log.info(f"📅 GOVERNANCE_BT_CONV_N_DAYS={n_days} — using extended lookback window (migration mode)")
        query = _build_cb_messages_query(end_key, n_days=n_days)
        return _run_bronze_extract(
            context,
            asset_name="raw_cb_messages_new",
            source_type="chatbot_postgres",
            query=query,
        )

    yield from iter_job_wrapped_compute(context, body)
