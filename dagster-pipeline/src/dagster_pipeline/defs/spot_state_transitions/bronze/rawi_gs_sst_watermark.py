# defs/spot_state_transitions/bronze/rawi_gs_sst_watermark.py
"""
Bronze Internal: Reads watermark (timestamp + max_id) from GeoSpot lk_spot_status_history.

Returns MAX(source_sst_updated_at) and MAX(source_sst_id) for source_id = 2
in a single query. Both values are used downstream to detect new/edited transitions.
Only feeds other Bronze assets — no STG needed.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.spot_state_transitions.shared import query_bronze_source


@dg.asset(
    group_name="sst_bronze",
    description="Bronze Internal: watermark (timestamp + max_id) from GeoSpot lk_spot_status_history.",
)
def rawi_gs_sst_watermark(
    context: dg.AssetExecutionContext,
) -> dict:
    """Queries GeoSpot for the latest source_sst_updated_at and source_sst_id where source_id = 2."""
    query = """
    SELECT
        MAX(source_sst_updated_at) AS watermark,
        MAX(source_sst_id) AS max_id
    FROM lk_spot_status_history
    WHERE source_id = 2
    """
    df = query_bronze_source(query, source_type="geospot_postgres", context=context)

    if df.height == 0 or df["watermark"][0] is None:
        raise dg.Failure(
            description="No watermark found in lk_spot_status_history (source_id=2). "
            "Cannot proceed with incremental extraction."
        )

    watermark = df["watermark"][0]
    max_id = int(df["max_id"][0])
    context.log.info(f"rawi_gs_sst_watermark: {watermark}  |  max_id: {max_id:,}")

    return {"watermark": watermark, "max_id": max_id}
