# defs/spot_state_transitions/__init__.py
"""
Spot State Transitions Pipeline (SST).

Incrementally updates lk_spot_status_history by detecting new/edited
transitions in MySQL spot_state_transitions, combining with existing
GeoSpot snapshots, and rebuilding the affected spots' full history.

Data Sources:
- MySQL S2P: spot_state_transitions (incremental via watermark)
- GeoSpot PostgreSQL: lk_spot_status_history (snapshots), lk_spots (active spot IDs)

Pipeline Structure:
- Bronze Internal: rawi_gs_sst_watermark, rawi_s2p_sst_new_transitions
- Bronze External: raw_s2p_sst_affected_history, raw_gs_sst_snapshots, raw_gs_active_spot_ids
- Silver STG: stg_s2p_sst_affected_history, stg_gs_sst_snapshots, stg_gs_active_spot_ids
- Silver Core: core_sst_rebuild_history (dedup + prev/next + sstd IDs)
- Gold: gold_lk_spot_status_history (hard-delete flag + audit)
- Publish: S3 CSV

Schedule: Daily at 9:00 AM Mexico City
"""

# Bronze Internal assets
from dagster_pipeline.defs.spot_state_transitions.bronze.rawi_gs_sst_watermark import (
    rawi_gs_sst_watermark,
)
from dagster_pipeline.defs.spot_state_transitions.bronze.rawi_s2p_sst_new_transitions import (
    rawi_s2p_sst_new_transitions,
)

# Bronze External assets
from dagster_pipeline.defs.spot_state_transitions.bronze.raw_s2p_sst_affected_history import (
    raw_s2p_sst_affected_history,
)
from dagster_pipeline.defs.spot_state_transitions.bronze.raw_gs_sst_snapshots import (
    raw_gs_sst_snapshots,
)
from dagster_pipeline.defs.spot_state_transitions.bronze.raw_gs_active_spot_ids import (
    raw_gs_active_spot_ids,
)

# Silver STG assets
from dagster_pipeline.defs.spot_state_transitions.silver.stg.stg_s2p_sst_affected_history import (
    stg_s2p_sst_affected_history,
)
from dagster_pipeline.defs.spot_state_transitions.silver.stg.stg_gs_sst_snapshots import (
    stg_gs_sst_snapshots,
)
from dagster_pipeline.defs.spot_state_transitions.silver.stg.stg_gs_active_spot_ids import (
    stg_gs_active_spot_ids,
)

# Silver Core assets
from dagster_pipeline.defs.spot_state_transitions.silver.core.core_sst_rebuild_history import (
    core_sst_rebuild_history,
)

# Gold assets
from dagster_pipeline.defs.spot_state_transitions.gold.gold_lk_spot_status_history import (
    gold_lk_spot_status_history,
)

# Publish assets
from dagster_pipeline.defs.spot_state_transitions.publish.lk_spot_status_history import (
    lk_spot_status_history_to_s3,
)

# Jobs and schedules
from dagster_pipeline.defs.spot_state_transitions.jobs import (
    spot_state_transitions_daily_job,
    spot_state_transitions_daily_schedule,
)

__all__ = [
    # Bronze Internal
    "rawi_gs_sst_watermark",
    "rawi_s2p_sst_new_transitions",
    # Bronze External
    "raw_s2p_sst_affected_history",
    "raw_gs_sst_snapshots",
    "raw_gs_active_spot_ids",
    # Silver STG
    "stg_s2p_sst_affected_history",
    "stg_gs_sst_snapshots",
    "stg_gs_active_spot_ids",
    # Silver Core
    "core_sst_rebuild_history",
    # Gold
    "gold_lk_spot_status_history",
    # Publish
    "lk_spot_status_history_to_s3",
    # Jobs
    "spot_state_transitions_daily_job",
    "spot_state_transitions_daily_schedule",
]
