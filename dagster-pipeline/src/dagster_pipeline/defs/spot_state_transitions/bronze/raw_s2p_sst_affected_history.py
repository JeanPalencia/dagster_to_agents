# defs/spot_state_transitions/bronze/raw_s2p_sst_affected_history.py
"""
Bronze External: Full history from MySQL S2P for affected spots.

Extracts spot_ids from rawi_s2p_sst_new_transitions, then queries MySQL
for the complete history of those spots.
Has a corresponding STG asset for type normalization.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_state_transitions.shared import query_bronze_source


MYSQL_CUT_DATE = "2025-09-12 13:28:00"

QUERY_SPOTS_HISTORY = """
WITH ids_last_before AS (
  SELECT
    sst.id,
    ROW_NUMBER() OVER (
      PARTITION BY sst.spot_id
      ORDER BY sst.created_at DESC, sst.id DESC
    ) AS rn
  FROM spot_state_transitions sst
  WHERE sst.created_at < '{cut_date}'
    AND sst.spot_id IN ({spot_ids})
),
last_before_cut AS (
  SELECT sst.*
  FROM spot_state_transitions sst
  JOIN ids_last_before i ON i.id = sst.id
  WHERE i.rn = 1
),
post_cut AS (
  SELECT *
  FROM spot_state_transitions
  WHERE created_at >= '{cut_date}'
    AND spot_id IN ({spot_ids})
),
combined AS (
  SELECT * FROM last_before_cut
  UNION ALL
  SELECT * FROM post_cut
),
sequenced AS (
  SELECT
    c.*,
    LAG(c.state)  OVER (PARTITION BY c.spot_id ORDER BY c.created_at, c.id) AS prev_state,
    LAG(c.reason) OVER (PARTITION BY c.spot_id ORDER BY c.created_at, c.id) AS prev_reason
  FROM combined c
),
dedup AS (
  SELECT s.*
  FROM sequenced s
  WHERE
    s.prev_state IS NULL
    OR s.state <> s.prev_state
    OR NOT (s.reason <=> s.prev_reason)
)
SELECT
  id AS source_sst_id,
  spot_id,
  state,
  reason,
  2 AS source_id,
  'spot_state_transitions' AS source,
  created_at AS source_sst_created_at,
  updated_at AS source_sst_updated_at
FROM dedup
ORDER BY spot_id, created_at, id
"""


@dg.asset(
    group_name="sst_bronze",
    description="Bronze: full spot_state_transitions history from MySQL S2P for affected spots.",
)
def raw_s2p_sst_affected_history(
    context: dg.AssetExecutionContext,
    rawi_s2p_sst_new_transitions: pl.DataFrame,
) -> pl.DataFrame:
    """Queries MySQL for the complete transition history of affected spots."""
    if rawi_s2p_sst_new_transitions.height == 0:
        context.log.warning("No affected spots — returning empty DataFrame.")
        return pl.DataFrame(schema={
            "source_sst_id": pl.Int64, "spot_id": pl.Int64,
            "state": pl.Int64, "reason": pl.Int64,
            "source_id": pl.Int64, "source": pl.Utf8,
            "source_sst_created_at": pl.Datetime, "source_sst_updated_at": pl.Datetime,
        })

    affected_ids = rawi_s2p_sst_new_transitions["spot_id"].unique().to_list()
    ids_str = ",".join(str(int(sid)) for sid in affected_ids if sid is not None)

    context.log.info(f"Querying MySQL history for {len(affected_ids)} affected spots...")

    query = QUERY_SPOTS_HISTORY.format(cut_date=MYSQL_CUT_DATE, spot_ids=ids_str)
    df = query_bronze_source(query, source_type="mysql_prod", context=context)

    context.log.info(f"raw_s2p_sst_affected_history: {df.height:,} rows")
    return df
