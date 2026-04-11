# defs/effective_supply/silver/stg/__init__.py
"""Silver STG assets for Effective Supply pipeline."""

from dagster_pipeline.defs.effective_supply.silver.stg.stg_bq_spot_contact_view_event_counts import (
    stg_bq_spot_contact_view_event_counts,
)
from dagster_pipeline.defs.effective_supply.silver.stg.stg_gs_bt_lds_spot_added import (
    stg_gs_bt_lds_spot_added,
)
from dagster_pipeline.defs.effective_supply.silver.stg.stg_gs_lk_spots import (
    stg_gs_lk_spots,
)
from dagster_pipeline.defs.effective_supply.silver.stg.stg_gs_effective_supply_run_id import (
    stg_gs_effective_supply_run_id,
)

__all__ = [
    "stg_bq_spot_contact_view_event_counts",
    "stg_gs_bt_lds_spot_added",
    "stg_gs_lk_spots",
    "stg_gs_effective_supply_run_id",
]
