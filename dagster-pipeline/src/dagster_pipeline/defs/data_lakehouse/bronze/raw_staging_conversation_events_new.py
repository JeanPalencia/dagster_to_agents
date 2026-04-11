# defs/data_lakehouse/bronze/raw_staging_conversation_events_new.py
"""
Bronze: conversation_events (Staging). Same query as conversation_analysis/queries/chat_events.sql.
{previous_day_start} and {end_date} match the message window for the same partition.
"""
import os
from datetime import datetime, timedelta

import dagster as dg

from dagster_pipeline.defs.data_lakehouse.bronze.base import _run_bronze_extract
from dagster_pipeline.defs.data_lakehouse.shared import resolve_stale_reference_date
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute

# Keep in sync with defs/conversation_analysis/queries/chat_events.sql
_CHAT_EVENTS_SQL = """
SELECT
    conversation_id,
    contact_id,
    client_id AS lead_id,
    event_type,
    event_section,
    event_data,
    to_char(
        created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City',
        'YYYY-MM-DD HH24:MI:SS'
    ) AS created_at_local
FROM conversation_events
WHERE created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' >= '{previous_day_start}'
  AND created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' <= '{end_date}'
ORDER BY conversation_id, created_at_local ASC
"""


def _legacy_message_date_params(partition_key: str, n_days: int = 3) -> tuple[str, str, str]:
    """Same window as raw_cb_messages_new (end = partition_key, start = end - n_days)."""
    end_d = datetime.strptime(partition_key, "%Y-%m-%d").date()
    start_d = end_d - timedelta(days=n_days)
    prev_d = start_d - timedelta(days=1)
    start_date = f"{start_d.strftime('%Y-%m-%d')} 00:00:00"
    end_date = f"{end_d.strftime('%Y-%m-%d')} 23:59:59"
    previous_day_start = f"{prev_d.strftime('%Y-%m-%d')} 00:00:00"
    return start_date, end_date, previous_day_start


def _build_conversation_events_query(partition_key: str, n_days: int = 3) -> str:
    _start_date, end_date, previous_day_start = _legacy_message_date_params(partition_key, n_days=n_days)
    return _CHAT_EVENTS_SQL.format(
        previous_day_start=previous_day_start,
        end_date=end_date,
    )


@dg.asset(
    name="raw_staging_conversation_events_new",
    group_name="bronze",
    description="Bronze: conversation_events (chat_events.sql inline, Mexico City filter). Set GOVERNANCE_BT_CONV_N_DAYS env var to override lookback window (e.g. 100 for full-year migration).",
    io_manager_key="s3_bronze",
)
def raw_staging_conversation_events_new(context):
    def body():
        end_key = resolve_stale_reference_date(context)
        n_days = int(os.environ.get("GOVERNANCE_BT_CONV_N_DAYS", "3"))
        if n_days != 3:
            context.log.info(f"📅 GOVERNANCE_BT_CONV_N_DAYS={n_days} — using extended lookback window for events (migration mode)")
        query = _build_conversation_events_query(end_key, n_days=n_days)
        return _run_bronze_extract(
            context,
            asset_name="raw_staging_conversation_events_new",
            source_type="staging_postgres",
            query=query,
        )

    yield from iter_job_wrapped_compute(context, body)
