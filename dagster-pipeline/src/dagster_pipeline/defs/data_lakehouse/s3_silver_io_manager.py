# defs/data_lakehouse/s3_silver_io_manager.py
"""
IO manager that avoids storing large DataFrames on disk for silver assets.

Silver assets already write their output to S3 in their own code. This manager:
- dump_to_path: writes a tiny sentinel instead of pickling the DataFrame,
  so we don't fill disk (fixes OSError: [Errno 28] No space left on device when
  materializing e.g. stg_s2p_photos_new).
- load_from_path: reads the DataFrame from S3 (same path the asset wrote to).

Use io_manager_key="s3_silver" on silver assets that write to S3.
"""
from __future__ import annotations

import pickle
from typing import TYPE_CHECKING, Any

import dagster as dg
from dagster._core.storage.upath_io_manager import UPathIOManager
from upath import UPath

from dagster_pipeline.defs.data_lakehouse.shared import (
    build_silver_s3_key,
    read_polars_from_s3,
)

if TYPE_CHECKING:
    from dagster._core.execution.context.input import InputContext
    from dagster._core.execution.context.output import OutputContext

# Sentinel written to disk instead of the full DataFrame (saves disk space)
_S3_REF_SENTINEL = {"_s3_silver_ref": True}
PICKLE_PROTOCOL = getattr(pickle, "DEFAULT_PROTOCOL", 4)


class S3SilverIOManager(UPathIOManager):
    """Stores only a sentinel on disk; loads from S3 for silver table assets."""

    extension: str = ".pickle"

    def dump_to_path(self, context: "OutputContext", obj: Any, path: UPath) -> None:
        # Write a tiny pickle so we don't fill disk; the real data is already in S3
        with path.open("wb") as f:
            pickle.dump(_S3_REF_SENTINEL, f, protocol=PICKLE_PROTOCOL)

    def load_from_path(self, context: "InputContext", path: UPath) -> Any:
        if context.asset_key is None:
            raise ValueError("S3SilverIOManager.load_from_path requires asset_key")
        table_name = context.asset_key.to_user_string()
        s3_key = build_silver_s3_key(table_name, file_format="parquet")
        return read_polars_from_s3(s3_key, file_format="parquet")


def make_s3_silver_io_manager(init_context: dg.InitResourceContext) -> S3SilverIOManager:
    """Resource factory: use instance storage dir so paths match default layout."""
    from upath import UPath

    base_dir = (
        init_context.instance.storage_directory()
        if init_context.instance
        else "/tmp/dagster_s3_silver"
    )
    return S3SilverIOManager(base_path=UPath(base_dir))


# IOManagerDefinition so Dagster accepts it for io_manager_key="s3_silver"
s3_silver_io_manager = dg.io_manager(make_s3_silver_io_manager)
