# defs/spot_state_transitions/shared.py
"""
Shared utilities for the Spot State Transitions pipeline.

Re-exports helpers from data_lakehouse to avoid code duplication.
"""

from dagster_pipeline.defs.data_lakehouse.shared import (
    # Database query helpers
    query_bronze_source,
    # S3 helpers
    write_polars_to_s3,
    build_gold_s3_key,
)

__all__ = [
    "query_bronze_source",
    "write_polars_to_s3",
    "build_gold_s3_key",
]
