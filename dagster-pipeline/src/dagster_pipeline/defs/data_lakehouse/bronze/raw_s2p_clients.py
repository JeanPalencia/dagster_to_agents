# defs/data_lakehouse/bronze/raw_s2p_clients.py
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    get_partition_bounds,
    query_mysql_to_polars,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


@dg.asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Bronze: raw extraction de clients from Spot2 Platform.",
)
def raw_s2p_clients(context: dg.AssetExecutionContext):
    def body():
        query = """
        SELECT
           *
        FROM clients
        """

        df = query_mysql_to_polars(query, context=context)
        context.log.info(f"raw_s2p_clients: {df.height} rows for {context.partition_key}")

        return df

    yield from iter_job_wrapped_compute(context, body)
