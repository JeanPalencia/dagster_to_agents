# defs/data_lakehouse/bronze/raw_s2p_activity_log_projects.py
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    query_mysql_to_polars,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


@dg.asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Bronze: raw extraction de activity_log for projects from Spot2 Platform.",
)
def raw_s2p_activity_log_projects(context: dg.AssetExecutionContext):
    def body():
        query = """
        SELECT
           *
        FROM activity_log
        WHERE event IN (
            -- Project events
            'projectSpotIntentionLetter',
            'projectSpotDocumentation',
            'projectSpotContract',
            'projectSpotWon'
        )
        """

        df = query_mysql_to_polars(query, context=context)
        context.log.info(f"raw_s2p_activity_log_projects: {df.height} rows for {context.partition_key}")

        return df

    yield from iter_job_wrapped_compute(context, body)
