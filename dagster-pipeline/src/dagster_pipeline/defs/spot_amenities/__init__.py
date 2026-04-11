"""
Spot Amenities Pipeline.

Bridge table resolving the many-to-many relationship between spots
and amenities, enriched with amenity metadata (name, description,
category classification).

Data Sources:
- MySQL S2P: amenities, spot_amenities

Pipeline Structure:
- Bronze: raw_s2p_amenities, raw_s2p_spot_amenities
- Silver STG: stg_s2p_amenities, stg_s2p_spot_amenities
- Silver Core: core_bt_spot_amenities (LEFT JOIN)
- Gold: gold_bt_spot_amenities (audit fields)
- Publish: bt_spot_amenities_to_s3, bt_spot_amenities_to_geospot

Schedule: Daily at 5:00 AM Mexico City
"""

from dagster_pipeline.defs.spot_amenities.bronze.raw_s2p_amenities import (
    raw_s2p_amenities,
)
from dagster_pipeline.defs.spot_amenities.bronze.raw_s2p_spot_amenities import (
    raw_s2p_spot_amenities,
)

from dagster_pipeline.defs.spot_amenities.silver.stg.stg_s2p_amenities import (
    stg_s2p_amenities,
)
from dagster_pipeline.defs.spot_amenities.silver.stg.stg_s2p_spot_amenities import (
    stg_s2p_spot_amenities,
)

from dagster_pipeline.defs.spot_amenities.silver.core.core_bt_spot_amenities import (
    core_bt_spot_amenities,
)

from dagster_pipeline.defs.spot_amenities.gold.gold_bt_spot_amenities import (
    gold_bt_spot_amenities,
)

from dagster_pipeline.defs.spot_amenities.publish.bt_spot_amenities import (
    bt_spot_amenities_to_s3,
    bt_spot_amenities_to_geospot,
)

from dagster_pipeline.defs.spot_amenities.jobs import (
    spot_amenities_job,
    spot_amenities_schedule,
)

__all__ = [
    # Bronze
    "raw_s2p_amenities",
    "raw_s2p_spot_amenities",
    # Silver STG
    "stg_s2p_amenities",
    "stg_s2p_spot_amenities",
    # Silver Core
    "core_bt_spot_amenities",
    # Gold
    "gold_bt_spot_amenities",
    # Publish
    "bt_spot_amenities_to_s3",
    "bt_spot_amenities_to_geospot",
    # Jobs
    "spot_amenities_job",
    "spot_amenities_schedule",
]
