# defs/data_lakehouse/silver/stg/stg_s2p_user_activity_agg_new.py
"""
Silver 1:1 from raw_s2p_activity_log_users_new. Bronze already returns aggregated rows (last 6 months).
COMMENTED OUT: asset disabled for now.
"""
# import dagster as dg
# import polars as pl
#
# from dagster_pipeline.defs.data_lakehouse.shared import (
#     daily_partitions,
#     build_silver_s3_key,
#     write_polars_to_s3,
#     read_bronze_from_s3,
# )
#
#
# @dg.asset(
#     name="stg_s2p_user_activity_agg_new",
#     partitions_def=daily_partitions,
#     group_name="silver",
#     retry_policy=dg.RetryPolicy(max_retries=2),
#     description=(
#         "Silver from raw_s2p_activity_log_users_new (1:1, already aggregated in MySQL, last 6 months). "
#         "Outputs user_last_log_at, user_days_since_last_log, user_total_log_count for stg_s2p_users_new."
#     ),
# )
# def stg_s2p_user_activity_agg_new(context: dg.AssetExecutionContext) -> pl.DataFrame:
#     df, _ = read_bronze_from_s3("raw_s2p_activity_log_users_new", context)
#     context.log.info(
#         f"stg_s2p_user_activity_agg_new: {df.height} users for partition {context.partition_key}"
#     )
#     s3_key = build_silver_s3_key("stg_s2p_user_activity_agg_new", file_format="parquet")
#     write_polars_to_s3(df, s3_key, context, file_format="parquet")
#     return df
