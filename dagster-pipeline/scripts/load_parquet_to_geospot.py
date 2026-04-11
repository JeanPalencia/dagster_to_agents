#!/usr/bin/env python3
"""
One-off script to load a parquet from S3 into a Geospot PostgreSQL table (replace mode).

Use case: restore bt_conv_conversations from a previous run's parquet after
accidental data loss (e.g. replace-only fallback).

Requires: AWS credentials for SSM (API key), network to Geospot API.

Example (restore 3,529 rows from 2026-03-11 run):
  python scripts/load_parquet_to_geospot.py
  python scripts/load_parquet_to_geospot.py --s3-key gold/gold_bt_conv_conversations_new/2026/03/data-11.parquet --table bt_conv_conversations
"""
from __future__ import annotations

import argparse
import sys

import boto3
import requests

# Same as shared.py
S3_BUCKET = "dagster-assets-production"
GEOSPOT_LAKEHOUSE_ENDPOINT = "https://geospot.spot2.mx/data-lake-house/dagster/"
GEOSPOT_API_KEY_PARAM = "/dagster/API_GEOSPOT_LAKEHOUSE_KEY"

DEFAULT_RESTORE_S3_KEY = "gold/gold_bt_conv_conversations_new/2026/03/data-11.parquet"
DEFAULT_TABLE = "bt_conv_conversations"


def get_api_key() -> str:
    ssm = boto3.client("ssm", region_name="us-east-1")
    resp = ssm.get_parameter(Name=GEOSPOT_API_KEY_PARAM, WithDecryption=True)
    return resp["Parameter"]["Value"]


def load_to_geospot(s3_key: str, table_name: str, mode: str = "replace", timeout: int = 300) -> None:
    api_key = get_api_key()
    payload = {
        "bucket_name": S3_BUCKET,
        "s3_key": s3_key,
        "table_name": table_name,
        "mode": mode,
    }
    headers = {"Authorization": f"Api-Key {api_key}", "Content-Type": "application/json"}
    print(f"POST {GEOSPOT_LAKEHOUSE_ENDPOINT}")
    print(f"Payload: {payload}")
    resp = requests.post(GEOSPOT_LAKEHOUSE_ENDPOINT, headers=headers, json=payload, timeout=timeout)
    print(f"Status: {resp.status_code}")
    print(f"Body: {resp.text[:1000]}")
    if resp.status_code != 200:
        sys.exit(1)


def main() -> None:
    p = argparse.ArgumentParser(description="Load parquet from S3 into Geospot table (replace).")
    p.add_argument(
        "--s3-key",
        default=DEFAULT_RESTORE_S3_KEY,
        help=f"S3 key (no bucket). Default: {DEFAULT_RESTORE_S3_KEY}",
    )
    p.add_argument("--table", default=DEFAULT_TABLE, help=f"Target table. Default: {DEFAULT_TABLE}")
    p.add_argument("--mode", default="replace", choices=("replace", "append"))
    p.add_argument("--timeout", type=int, default=300)
    args = p.parse_args()
    load_to_geospot(s3_key=args.s3_key, table_name=args.table, mode=args.mode, timeout=args.timeout)


if __name__ == "__main__":
    main()
