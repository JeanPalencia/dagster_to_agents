# defs/data_lakehouse/bronze/raw_bq_funnel_with_channel_new.py
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset

# asset_name explícito porque puede diferir del nombre de tabla en BQ
raw_bq_funnel_with_channel_new = make_bronze_asset(
    "bigquery",
    table_name="spot2-mx-ga4-bq.analitics_spot2.funnel_with_channel",
    asset_name="raw_bq_funnel_with_channel_new",
)
