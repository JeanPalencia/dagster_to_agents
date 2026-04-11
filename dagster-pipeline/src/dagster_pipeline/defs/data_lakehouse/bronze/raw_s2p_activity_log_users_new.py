# defs/data_lakehouse/bronze/raw_s2p_activity_log_users_new.py
"""
Bronze: activity_log aggregated by user (causer_id), last 6 months.
COMMENTED OUT: asset disabled for now.
"""
# import dagster as dg
# import polars as pl
#
# from dagster_pipeline.defs.data_lakehouse.shared import (
#     daily_partitions,
#     query_bronze_source,
#     build_bronze_s3_key,
#     write_polars_to_s3,
# )
#
# ACTIVITY_LOG_QUERY = """
# SELECT
#     causer_id AS user_id,
#     MAX(created_at) AS user_last_log_at,
#     DATEDIFF(CURRENT_DATE, MAX(created_at)) AS user_days_since_last_log,
#     COUNT(*) AS user_total_log_count
# FROM activity_log
# WHERE causer_type = 'App\\\\Models\\\\User'
#   AND causer_id IS NOT NULL
#   AND created_at >= DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH)
# GROUP BY causer_id
# """
#
#
# @dg.asset(
#     name="raw_s2p_activity_log_users_new",
#     partitions_def=daily_partitions,
#     group_name="bronze",
#     description="Bronze: activity_log aggregated by user (last 6 months). For lk_users activity metrics.",
# )
# def raw_s2p_activity_log_users_new(context: dg.AssetExecutionContext) -> pl.DataFrame:
#     df = query_bronze_source(ACTIVITY_LOG_QUERY, source_type="mysql_prod", context=context)
#     context.log.info(
#         f"📥 raw_s2p_activity_log_users_new: {df.height:,} users (last 6 months) for partition {context.partition_key}"
#     )
#     s3_key = build_bronze_s3_key("raw_s2p_activity_log_users_new", file_format="parquet")
#     write_polars_to_s3(df, s3_key, context, file_format="parquet")
#     return df
