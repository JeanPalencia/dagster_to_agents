# defs/spot_state_transitions/bronze/raw_gs_sst_snapshots.py
"""
Bronze External: Snapshots from GeoSpot lk_spot_status_history for affected spots.

Extracts spot_ids from rawi_s2p_sst_new_transitions, then queries GeoSpot
for snapshot records (source_id != 2) of those spots.
Has a corresponding STG asset for type normalization.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_state_transitions.shared import query_bronze_source


SNAPSHOT_QUERY_TEMPLATE = """
SELECT
    source_sst_id, spot_id,
    spot_status_id AS state,
    spot_status_reason_id AS reason,
    source_id, source,
    source_sst_created_at, source_sst_updated_at
FROM lk_spot_status_history
WHERE source_id != 2
  AND spot_id IN ({spot_ids})
ORDER BY spot_id, source_sst_created_at
"""


@dg.asset(
    group_name="sst_bronze",
    description="Bronze: snapshots from GeoSpot lk_spot_status_history for affected spots.",
)
def raw_gs_sst_snapshots(
    context: dg.AssetExecutionContext,
    rawi_s2p_sst_new_transitions: pl.DataFrame,
) -> pl.DataFrame:
    """Queries GeoSpot for snapshot records of affected spots."""
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

    context.log.info(f"Querying GeoSpot snapshots for {len(affected_ids)} affected spots...")

    query = SNAPSHOT_QUERY_TEMPLATE.format(spot_ids=ids_str)
    df = query_bronze_source(query, source_type="geospot_postgres", context=context)

    context.log.info(f"raw_gs_sst_snapshots: {df.height:,} rows")
    return df
