"""
Capture baseline parquet from the current gold_bt_lds_lead_spots_new output in S3.

Usage (from project root, with AWS credentials active):
    python -m lakehouse-sdk.tests.bt_lds_channel_attribution.capture_baseline

Reads the latest gold parquet from S3 for bt_lds_lead_spots,
strips aud_* columns, and saves locally as baseline.parquet.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "dagster-pipeline", "src"))

import polars as pl
import boto3
import io
from datetime import date, timedelta

S3_BUCKET = "dagster-assets-production"
s3 = boto3.client("s3", region_name="us-east-1")

BASELINE_PATH = os.path.join(os.path.dirname(__file__), "baseline.parquet")


def _find_latest_gold_key() -> str:
    today = date.today()
    for offset in range(30):
        d = today - timedelta(days=offset)
        key = f"gold/bt_lds_lead_spots/{d.year}/{d.month:02d}/data-{d.day:02d}.parquet"
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=key)
            return key
        except Exception:
            continue
    raise FileNotFoundError("No gold parquet found for bt_lds_lead_spots in the last 30 days")


def main():
    key = _find_latest_gold_key()
    print(f"Reading gold from S3: s3://{S3_BUCKET}/{key}")

    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    df = pl.read_parquet(io.BytesIO(obj["Body"].read()))

    print(f"  Rows: {df.height:,}")
    print(f"  Columns: {df.width}")
    print(f"  Schema: {dict(zip(df.columns, [str(t) for t in df.dtypes]))}")

    aud_cols = [c for c in df.columns if c.startswith("aud_")]
    df_baseline = df.drop(aud_cols)

    print(f"\nBaseline (without aud_*): {df_baseline.width} columns, {df_baseline.height:,} rows")
    df_baseline.write_parquet(BASELINE_PATH)
    print(f"Saved to {BASELINE_PATH}")


if __name__ == "__main__":
    main()
