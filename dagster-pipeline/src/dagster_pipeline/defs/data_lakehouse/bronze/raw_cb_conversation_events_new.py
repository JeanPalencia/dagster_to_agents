# defs/data_lakehouse/bronze/raw_cb_conversation_events_new.py
"""
Bronze: conversation_events (spot_confirmation only) from Chat2 Staging.

Full historical extract (no temporal window) of spot confirmation events.
Uses staging_postgres (spot_chatbot_* SSM credentials) which has read
permissions on conversation_events.

Distinct from raw_staging_conversation_events_new which uses a 3-day window
and extracts all event types.
"""
import dagster as dg

from dagster_pipeline.defs.data_lakehouse.bronze.base import _run_bronze_extract
from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute

_SPOT_CONFIRMATION_SQL = """
SELECT
    conversation_id,
    client_id,
    spot_id,
    event_data,
    created_at
FROM conversation_events
WHERE event_type = 'spot_confirmation'
"""


@dg.asset(
    name="raw_cb_conversation_events_new",
    partitions_def=daily_partitions,
    group_name="bronze",
    description=(
        "Bronze: conversation_events (spot_confirmation only) from Chat2 Staging. "
        "Full historical extract without temporal window. "
        "Used for chatbot channel attribution in bt_lds_lead_spots."
    ),
    io_manager_key="s3_bronze",
)
def raw_cb_conversation_events_new(context):
    def body():
        return _run_bronze_extract(
            context,
            asset_name="raw_cb_conversation_events_new",
            source_type="staging_postgres",
            query=_SPOT_CONFIRMATION_SQL,
        )

    yield from iter_job_wrapped_compute(context, body)
