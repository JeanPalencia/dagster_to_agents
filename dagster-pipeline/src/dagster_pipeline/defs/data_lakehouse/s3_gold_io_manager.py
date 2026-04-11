# defs/data_lakehouse/s3_gold_io_manager.py
"""
IO manager so gold assets do not write large DataFrames to disk.

Gold assets already write their output to S3 in their own code. This manager:
- dump_to_path: writes a tiny sentinel instead of pickling the DataFrame.
- load_from_path: reads the DataFrame from S3 (same path the asset wrote to).

Use io_manager_key="s3_gold" on gold assets that write to S3.
"""
from __future__ import annotations

import pickle
from typing import TYPE_CHECKING, Any

import dagster as dg
from dagster._core.storage.upath_io_manager import UPathIOManager
from upath import UPath

from dagster_pipeline.defs.data_lakehouse.shared import (
    build_gold_s3_key,
    calendar_today_mx_iso,
    read_polars_from_s3,
)

if TYPE_CHECKING:
    from dagster._core.execution.context.input import InputContext
    from dagster._core.execution.context.output import OutputContext

_S3_GOLD_SENTINEL = {"_s3_gold_ref": True}
PICKLE_PROTOCOL = getattr(pickle, "DEFAULT_PROTOCOL", 4)

# Asset name -> S3 table name (used in build_gold_s3_key). Gold assets write to S3 with this name.
GOLD_ASSET_TO_TABLE: dict[str, str] = {
    "gold_lk_visitors_new": "lk_visitors_new",
    "gold_lk_spots_new": "lk_spots_new",
    "gold_lk_projects_new": "lk_projects_new",
    "gold_lk_leads_new": "lk_leads",
    "gold_lk_users_new": "lk_users",
    "gold_lk_matches_visitors_to_leads_new": "lk_matches_visitors_to_leads_new",
    "gold_bt_transactions_new": "bt_transactions",
    "gold_bt_lds_lead_spots_new": "bt_lds_lead_spots",
    "gold_bt_conv_conversations_new": "gold_bt_conv_conversations_new",
}


class S3GoldIOManager(UPathIOManager):
    """Stores only a sentinel on disk; loads from S3 for gold table assets."""

    extension: str = ".pickle"

    def dump_to_path(self, context: "OutputContext", obj: Any, path: UPath) -> None:
        with path.open("wb") as f:
            pickle.dump(_S3_GOLD_SENTINEL, f, protocol=PICKLE_PROTOCOL)

    def load_from_path(self, context: "InputContext", path: UPath) -> Any:
        if context.asset_key is None:
            raise ValueError("S3GoldIOManager.load_from_path requires asset_key")
        asset_name = context.asset_key.to_user_string()
        table_name = GOLD_ASSET_TO_TABLE.get(asset_name)
        if not table_name:
            raise ValueError(
                f"S3GoldIOManager: unknown gold asset {asset_name}. "
                f"Add it to GOLD_ASSET_TO_TABLE in s3_gold_io_manager.py."
            )
        partition_key = getattr(context, "asset_partition_key", None) or getattr(
            context, "partition_key", None
        )
        if not partition_key:
            if asset_name == "gold_bt_conv_conversations_new":
                partition_key = calendar_today_mx_iso()
            else:
                raise ValueError(
                    f"S3GoldIOManager.load_from_path: no partition_key for {asset_name}. "
                    "Gold assets are partitioned."
                )
        s3_key = build_gold_s3_key(table_name, partition_key, file_format="parquet")
        return read_polars_from_s3(s3_key, file_format="parquet")


def make_s3_gold_io_manager(init_context: dg.InitResourceContext) -> S3GoldIOManager:
    base_dir = (
        init_context.instance.storage_directory()
        if init_context.instance
        else "/tmp/dagster_s3_gold"
    )
    return S3GoldIOManager(base_path=UPath(base_dir))


s3_gold_io_manager = dg.io_manager(make_s3_gold_io_manager)
