# defs/spot_state_transitions/bronze/rawi_s2p_sst_new_transitions.py
"""
Bronze Internal: New/edited transitions from MySQL S2P since watermark.

Queries spot_state_transitions WHERE updated_at >= watermark OR id > max_id.
Both criteria come from rawi_gs_sst_watermark (single upstream query).
Only feeds other Bronze assets (to extract affected spot_ids) — no STG needed.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_state_transitions.shared import query_bronze_source


QUERY_NEW_TRANSITIONS = """
SELECT
  id AS source_sst_id,
  spot_id,
  state,
  reason,
  2 AS source_id,
  'spot_state_transitions' AS source,
  created_at AS source_sst_created_at,
  updated_at AS source_sst_updated_at
FROM spot_state_transitions
WHERE updated_at >= '{watermark}' OR id > {max_id}
ORDER BY spot_id, created_at, id
"""


@dg.asset(
    group_name="sst_bronze",
    description="Bronze Internal: new/edited spot_state_transitions from MySQL S2P since watermark.",
)
def rawi_s2p_sst_new_transitions(
    context: dg.AssetExecutionContext,
    rawi_gs_sst_watermark: dict,
) -> pl.DataFrame:
    """Queries MySQL for transitions updated since the watermark or with id > max_id."""
    watermark = rawi_gs_sst_watermark["watermark"]
    max_id = rawi_gs_sst_watermark["max_id"]
    watermark_str = str(watermark)[:19]  # "YYYY-MM-DD HH:MM:SS"

    query = QUERY_NEW_TRANSITIONS.format(watermark=watermark_str, max_id=max_id)
    df = query_bronze_source(query, source_type="mysql_prod", context=context)

    context.log.info(
        f"rawi_s2p_sst_new_transitions: {df.height:,} new/edited records "
        f"(watermark={watermark_str}, max_id={max_id:,})"
    )

    if df.height == 0:
        context.log.warning("No new transitions found since watermark.")

    return df
