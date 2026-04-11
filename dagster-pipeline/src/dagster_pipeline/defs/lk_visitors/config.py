"""Configuration for the LK Visitors pipeline."""

from typing import Literal

from dagster import Config


class LKVisitorsConfig(Config):
    """Configuration for the LK Visitors pipeline."""
    local_output: bool = False
    upload_to_s3: bool = True
    trigger_table_replacement: bool = True
    table_load_mode: Literal["replace", "upsert"] = "upsert"
    output_format: str = "parquet"
    output_dir: str = "output"
