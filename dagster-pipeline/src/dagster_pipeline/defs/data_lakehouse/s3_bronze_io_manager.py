# defs/data_lakehouse/s3_bronze_io_manager.py
"""
IO manager for bronze assets that are read by other jobs (e.g. silver in a separate run).

Bronze assets already write to S3 in their own code. This manager:
- dump_to_path: writes a tiny sentinel so we don't duplicate large DataFrames on disk.
- load_from_path: reads the DataFrame from S3 (same path the bronze asset wrote to).

Use io_manager_key="s3_bronze" on bronze assets that are upstream of assets in a different
job (e.g. raw_cb_messages_new used by stg_bt_conv_grouped_new in bt_conv_conversations_silver_new).
Fixes FileNotFoundError when the silver job runs in a separate run/worker and the default
io manager would look for a local path that only exists where the bronze run executed.
"""
from __future__ import annotations

import pickle
from typing import TYPE_CHECKING, Any

import dagster as dg
from dagster._core.storage.upath_io_manager import UPathIOManager
from upath import UPath

from dagster_pipeline.defs.data_lakehouse.shared import (
    build_bronze_s3_key,
    read_polars_from_s3,
)

if TYPE_CHECKING:
    from dagster._core.execution.context.input import InputContext
    from dagster._core.execution.context.output import OutputContext

_S3_BRONZE_SENTINEL = {"_s3_bronze_ref": True}
PICKLE_PROTOCOL = getattr(pickle, "DEFAULT_PROTOCOL", 4)


class S3BronzeIOManager(UPathIOManager):
    """Stores only a sentinel on disk; loads from S3 for bronze table assets."""

    extension: str = ".pickle"

    def dump_to_path(self, context: "OutputContext", obj: Any, path: UPath) -> None:
        with path.open("wb") as f:
            pickle.dump(_S3_BRONZE_SENTINEL, f, protocol=PICKLE_PROTOCOL)

    def load_from_path(self, context: "InputContext", path: UPath) -> Any:
        if context.asset_key is None:
            raise ValueError("S3BronzeIOManager.load_from_path requires asset_key")
        table_name = context.asset_key.to_user_string()
        s3_key = build_bronze_s3_key(table_name, file_format="parquet")
        return read_polars_from_s3(s3_key, file_format="parquet")


def make_s3_bronze_io_manager(init_context: dg.InitResourceContext) -> S3BronzeIOManager:
    base_dir = (
        init_context.instance.storage_directory()
        if init_context.instance
        else "/tmp/dagster_s3_bronze"
    )
    return S3BronzeIOManager(base_path=UPath(base_dir))


s3_bronze_io_manager = dg.io_manager(make_s3_bronze_io_manager)
