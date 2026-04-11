# defs/effective_supply/bronze/raw_gs_effective_supply_run_id.py
"""
Bronze: Fetch the current maximum aud_run_id from GeoSpot lk_effective_supply.

Used to compute the next run_id for the current pipeline execution.
Handles the case where the column doesn't exist yet (first deployment).
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.effective_supply.shared import query_bronze_source


@dg.asset(
    group_name="effective_supply_bronze",
    description="Bronze: current max aud_run_id from GeoSpot lk_effective_supply.",
)
def raw_gs_effective_supply_run_id(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    try:
        query = """
        SELECT COALESCE(MAX(aud_run_id), 0) AS max_run_id
        FROM lk_effective_supply
        """
        df = query_bronze_source(query, source_type="geospot_postgres", context=context)
        max_val = df["max_run_id"][0] if df.height > 0 else 0
    except Exception as e:
        context.log.warning(
            f"aud_run_id column not yet available in GeoSpot ({e}). Defaulting to 0."
        )
        df = pl.DataFrame({"max_run_id": [0]})
        max_val = 0

    context.log.info(f"raw_gs_effective_supply_run_id: current max = {max_val}")
    return df
