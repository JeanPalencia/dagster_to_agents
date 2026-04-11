# Spot Search Activity Pipeline

## Overview

This Dagster pipeline extracts spot search activity data from the geospot database's `activity_log` table and saves it as Parquet files on S3. The pipeline is designed to process data incrementally on a daily basis using Dagster's partitioning feature.

## Assets

### 1. `spot_search_activity_data`

This asset extracts data from the source database (analysis) for a specific day.

**Source Database:** `spot2-production-analysis.c4x5kddqib5v.us-east-1.rds.amazonaws.com`
**Source Table:** `spot2_service.activity_log`

**Query Details:**
- Filters events where `event = 'spotSearched'`
- Filters by specific causer_ids (31 user IDs)
- Partitioned by day using `created_at` timestamp
- Orders results by `id DESC`

**Returns:** pandas DataFrame with all columns from activity_log table for the partition date

### 2. `spot_search_activity_to_s3`

This asset saves the extracted data as Parquet files on S3.

**S3 Bucket:** `dagster-assets-production`
**S3 Path Pattern:** `spot_search_activity/year={YYYY}/month={MM}/day={DD}/data.parquet`

**Features:**
- Saves data in Parquet format with Snappy compression
- Organized in a Hive-style partitioned directory structure
- Efficient columnar storage format ideal for analytics
- Each partition creates a single Parquet file for that day's data

## Partitioning

The pipeline uses **Daily Partitions** starting from one month ago. This means:
- Each partition represents one calendar day
- Partitions are available for the last 30 days (rolling window)
- Each day's data is processed independently
- The start date automatically adjusts as time progresses (always showing the last month)

## Usage

### Running a Specific Partition

To run the pipeline for a specific date:

1. Open the Dagster UI: `dg dev`
2. Navigate to the Assets view
3. Select `spot_search_activity_data` or `spot_search_activity_to_s3`
4. Click "Materialize" and select the partition date(s) you want to process

### Backfilling Historical Data

To backfill multiple days at once:

1. Click on the asset in the UI
2. Select "Materialize" > "Partition selection"
3. Choose the date range you want to backfill
4. Click "Launch Runs"

### Setting Up Schedules

You can create a schedule to run this pipeline daily by adding to the definitions:

```python
from dagster import ScheduleDefinition

spot_search_activity_schedule = ScheduleDefinition(
    name="daily_spot_search_activity",
    target=spot_search_activity_to_s3,
    cron_schedule="0 2 * * *",  # Run at 2 AM daily
)
```

## S3 Output Structure

The Parquet files are organized in a Hive-style partitioned structure:

```
s3://dagster-assets-production/spot_search_activity/
├── year=2024/
│   ├── month=01/
│   │   ├── day=01/
│   │   │   └── data.parquet
│   │   ├── day=02/
│   │   │   └── data.parquet
│   │   └── ...
│   ├── month=02/
│   │   └── ...
│   └── ...
└── ...
```

This structure makes it easy to query with tools like AWS Athena, Spark, or other analytics engines that support partitioned Parquet files.

## Credentials

The pipeline uses AWS SSM Parameter Store for database credentials:

- `/dagster/DB_ANALYSIS_USERNAME` - Analysis database username
- `/dagster/DB_ANALYSIS_PASSWORD` - Analysis database password

## Querying from Redshift

The Parquet files include all 16 columns from the activity_log table, including the virtual generated columns (phone_number, email, spot_id, project_id).

### COPY into Redshift

```sql
-- First create the table with the correct schema
CREATE TABLE activity_log (
    id BIGINT,
    log_name VARCHAR(MAX),
    description VARCHAR(MAX),
    subject_type VARCHAR(MAX),
    event VARCHAR(MAX),
    subject_id BIGINT,
    causer_type VARCHAR(MAX),
    causer_id BIGINT,
    properties VARCHAR(MAX),
    phone_number VARCHAR(MAX),
    email VARCHAR(MAX),
    spot_id INT,
    project_id INT,
    batch_uuid VARCHAR(MAX),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Then load the data from S3
COPY activity_log
FROM 's3://dagster-assets-production/spot_search_activity/'
IAM_ROLE 'arn:aws:iam::your-account:role/your-redshift-role'
FORMAT AS PARQUET;
```

### Redshift Spectrum (query directly from S3)

```sql
CREATE EXTERNAL SCHEMA spot_activity
FROM DATA CATALOG
DATABASE 'your_glue_database'
IAM_ROLE 'arn:aws:iam::your-account:role/your-redshift-role';

CREATE EXTERNAL TABLE spot_activity.spot_search_activity (
    id BIGINT,
    log_name VARCHAR(MAX),
    description VARCHAR(MAX),
    subject_type VARCHAR(MAX),
    event VARCHAR(MAX),
    subject_id BIGINT,
    causer_type VARCHAR(MAX),
    causer_id BIGINT,
    properties VARCHAR(MAX),
    phone_number VARCHAR(MAX),
    email VARCHAR(MAX),
    spot_id INT,
    project_id INT,
    batch_uuid VARCHAR(MAX),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION 's3://dagster-assets-production/spot_search_activity/';

-- Then query it:
SELECT * FROM spot_activity.spot_search_activity 
WHERE year=2025 AND month=10 AND day=15;
```

## Monitored User IDs

The pipeline currently monitors spot searches for these causer_ids:
20899, 12300, 8153, 16262, 2406, 14910, 10359, 19005, 9541, 20555, 19256, 10608, 17487, 2360, 10458, 20644, 17567, 9149, 10902, 14516, 20898, 17740, 12831, 11726, 18631, 14247, 12306, 17739, 15509, 15932, 19438

To modify this list, edit the `causer_ids` list in the `spot_search_activity_data` function in `spot_search_activity.py`.
