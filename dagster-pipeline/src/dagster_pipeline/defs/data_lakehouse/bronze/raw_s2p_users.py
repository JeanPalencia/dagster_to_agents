# defs/data_lakehouse/bronze/raw_s2p_users.py
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    query_mysql_to_polars,
)


@dg.asset(
    partitions_def=daily_partitions,
    group_name="bronze",
    description="Bronze: raw extraction de users from Spot2 Platform.",
)
def raw_s2p_users(context: dg.AssetExecutionContext) -> pl.DataFrame:
    query = """
    SELECT
       * 
    FROM users
    """

    df = query_mysql_to_polars(query, context=context)
    context.log.info(f"raw_s2p_users: {df.height} rows for {context.partition_key}")
    

    return df
