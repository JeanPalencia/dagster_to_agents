import dagster as dg
import pandas as pd
import boto3
import io
import mysql.connector

s3 = boto3.client('s3', region_name='us-east-1')
ssm = boto3.client('ssm', region_name='us-east-1')

sample_data_file = 'src/dagster_pipeline/defs/data/sample_data.csv'
sample_data_bucket = 'dagster-assets-production'

analysis_db_endpoint = 'spot2-production-analysis.c4x5kddqib5v.us-east-1.rds.amazonaws.com'


# geospot credentials
def get_geospot_credentials():
    credentials = ssm.get_parameter(Name='/dagster/DB_GEOSPOT_USERNAME', WithDecryption=True)
    username = credentials['Parameter']['Value']

    credentials = ssm.get_parameter(Name='/dagster/DB_GEOSPOT_PASSWORD', WithDecryption=True)
    password = credentials['Parameter']['Value']

    return username, password


# analysis credentials
def get_analysis_credentials():
    credentials = ssm.get_parameter(Name='/dagster/DB_ANALYSIS_USERNAME', WithDecryption=True)
    username = credentials['Parameter']['Value']

    credentials = ssm.get_parameter(Name='/dagster/DB_ANALYSIS_PASSWORD', WithDecryption=True)
    password = credentials['Parameter']['Value']

    return username, password


@dg.asset
def processed_data():
    username, password = get_analysis_credentials()

    # connect to the analysis database using mysql connector
    conn = mysql.connector.connect(
        host=analysis_db_endpoint,
        user=username,
        password=password,
        database='spot2_service'
    )

     # get a single row from the analytics table
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM analytics LIMIT 1")

    row = cursor.fetchone()

    cursor.close()
    conn.close()

    # save the row to s3
    buffer = io.BytesIO() # create a buffer to store the data

    s3.put_object(
        Bucket=sample_data_bucket,
        Key='processed_data.txt',
        Body=str(row)
    )

    return "Data loaded and processed successfully"