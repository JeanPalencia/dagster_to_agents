import dagster as dg
import mysql.connector
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import pandas as pd
import boto3
import io
import pyarrow as pa
import pyarrow.parquet as pq

s3 = boto3.client('s3', region_name='us-east-1')
ssm = boto3.client('ssm', region_name='us-east-1')

analysis_db_endpoint = 'spot2-production-analysis.c4x5kddqib5v.us-east-1.rds.amazonaws.com'
s3_bucket = 'dagster-assets-production'


def get_analysis_credentials():
    credentials = ssm.get_parameter(Name='/dagster/DB_ANALYSIS_USERNAME', WithDecryption=True)
    username = credentials['Parameter']['Value']

    credentials = ssm.get_parameter(Name='/dagster/DB_ANALYSIS_PASSWORD', WithDecryption=True)
    password = credentials['Parameter']['Value']

    return username, password


daily_partitions = dg.DailyPartitionsDefinition(
    start_date=(date.today() - relativedelta(months=1)).strftime("%Y-%m-%d")
)


@dg.asset(
    partitions_def=daily_partitions,
    description="Extract spot search activity data from activity_log table, filtered by specific causer_ids and partitioned by day"
)
def spot_search_activity_data(context: dg.AssetExecutionContext) -> pd.DataFrame:
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
    next_day = partition_date + timedelta(days=1)
    
    username, password = get_analysis_credentials()
    
    conn = mysql.connector.connect(
        host=analysis_db_endpoint,
        user=username,
        password=password,
        database='spot2_service'
    )
    
    causer_ids = [
        20899, 12300, 8153, 16262, 2406, 14910, 10359, 19005, 9541, 20555,
        19256, 10608, 17487, 2360, 10458, 20644, 17567, 9149, 10902, 14516,
        20898, 17740, 12831, 11726, 18631, 14247, 12306, 17739, 15509, 15932,
        19438
    ]
    
    causer_ids_str = ','.join(map(str, causer_ids))
    
    query = f"""
    SELECT * FROM activity_log
    WHERE event = 'spotSearched'
    AND causer_id IN ({causer_ids_str})
    AND created_at >= %s
    AND created_at < %s
    ORDER BY id DESC
    """
    
    context.log.info(f"Extracting spot search activity for date: {partition_date_str}")
    
    df = pd.read_sql(query, conn, params=(partition_date_str, next_day.strftime("%Y-%m-%d")))
    
    conn.close()
    
    context.log.info(f"Extracted {len(df)} records for {partition_date_str}")
    
    return df


@dg.asset(
    deps=[spot_search_activity_data],
    partitions_def=daily_partitions,
    description="Save spot search activity data as Parquet file on S3"
)
def spot_search_activity_to_s3(context: dg.AssetExecutionContext, spot_search_activity_data: pd.DataFrame):
    partition_date_str = context.partition_key
    
    if spot_search_activity_data.empty:
        context.log.info(f"No data to save for {partition_date_str}")
        return "No data to save"
    
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
    year = partition_date.year
    month = f"{partition_date.month:02d}"
    day = f"{partition_date.day:02d}"
    
    s3_key = f"spot_search_activity/year={year}/month={month}/day={day}/data.parquet"
    
    schema = pa.schema([
        ('id', pa.int64()),
        ('log_name', pa.string()),
        ('description', pa.string()),
        ('subject_type', pa.string()),
        ('event', pa.string()),
        ('subject_id', pa.int64()),
        ('causer_type', pa.string()),
        ('causer_id', pa.int64()),
        ('properties', pa.string()),
        ('phone_number', pa.string()),
        ('email', pa.string()),
        ('spot_id', pa.int32()),
        ('project_id', pa.int32()),
        ('batch_uuid', pa.string()),
        ('created_at', pa.timestamp('ns')),
        ('updated_at', pa.timestamp('ns'))
    ])
    
    table = pa.Table.from_pandas(spot_search_activity_data, schema=schema, preserve_index=False)
    
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)
    
    s3.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=buffer.getvalue()
    )
    
    context.log.info(f"Saved {len(spot_search_activity_data)} records for {partition_date_str} to s3://{s3_bucket}/{s3_key}")
    
    return f"Successfully saved {len(spot_search_activity_data)} records to S3"
