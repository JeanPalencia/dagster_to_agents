#!/usr/bin/env python3
"""
Utility script to inspect the Parquet schema from S3 files
and generate the correct Redshift schema.
"""
import boto3
import pyarrow.parquet as pq
import io

s3 = boto3.client('s3', region_name='us-east-1')

bucket = 'dagster-assets-production'
key = 'spot_search_activity/year=2025/month=10/day=12/data.parquet'

print(f"Fetching schema from: s3://{bucket}/{key}")
print("-" * 80)

try:
    response = s3.get_object(Bucket=bucket, Key=key)
    parquet_data = response['Body'].read()
    
    parquet_file = pq.read_table(io.BytesIO(parquet_data))
    
    print("\nParquet Schema:")
    print("=" * 80)
    print(parquet_file.schema)
    
    print("\n\nRedshift Schema Mapping:")
    print("=" * 80)
    
    type_mapping = {
        'int64': 'BIGINT',
        'int32': 'INT',
        'double': 'DOUBLE PRECISION',
        'float': 'REAL',
        'string': 'VARCHAR(MAX)',
        'timestamp[us]': 'TIMESTAMP',
        'timestamp[ms]': 'TIMESTAMP',
        'timestamp[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN',
    }
    
    print("\nCREATE TABLE activity_log (")
    for i, field in enumerate(parquet_file.schema):
        arrow_type = str(field.type)
        redshift_type = type_mapping.get(arrow_type, f'VARCHAR(MAX)  -- Unknown Arrow type: {arrow_type}')
        comma = "," if i < len(parquet_file.schema) - 1 else ""
        print(f"    {field.name} {redshift_type}{comma}")
    print(");")
    
    print("\n\nColumn Details:")
    print("=" * 80)
    for field in parquet_file.schema:
        print(f"{field.name}: {field.type}")
        
except Exception as e:
    print(f"Error: {e}")
    print("\nMake sure you have AWS credentials configured and access to the S3 bucket.")
