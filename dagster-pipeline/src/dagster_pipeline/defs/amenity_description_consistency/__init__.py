"""
Amenity Description Consistency Pipeline.

Detects tagged amenities that are NOT mentioned in the spot description,
classifying each spot by omission level (all mentioned, partial omission,
total omission).

Data Sources:
- GeoSpot PG: lk_spots, bt_spot_amenities

Pipeline Structure:
- Bronze: adc_raw_gs_lk_spots, adc_raw_gs_bt_spot_amenities
- Silver STG: adc_stg_gs_lk_spots, adc_stg_gs_bt_spot_amenities
- Silver Core: core_amenity_desc_consistency (regex matching + classification)
- Gold: gold_amenity_desc_consistency (audit fields)
- Publish: rpt_amenity_desc_consistency (S3 CSV + GeoSpot)

Trigger: sensor fires after spot_amenities_job with 10-min delay.
"""

from dagster_pipeline.defs.amenity_description_consistency.bronze.raw_gs_lk_spots import (
    adc_raw_gs_lk_spots,
)
from dagster_pipeline.defs.amenity_description_consistency.bronze.raw_gs_bt_spot_amenities import (
    adc_raw_gs_bt_spot_amenities,
)

from dagster_pipeline.defs.amenity_description_consistency.silver.stg.stg_gs_lk_spots import (
    adc_stg_gs_lk_spots,
)
from dagster_pipeline.defs.amenity_description_consistency.silver.stg.stg_gs_bt_spot_amenities import (
    adc_stg_gs_bt_spot_amenities,
)

from dagster_pipeline.defs.amenity_description_consistency.silver.core.core_amenity_desc_consistency import (
    core_amenity_desc_consistency,
)

from dagster_pipeline.defs.amenity_description_consistency.gold.gold_amenity_desc_consistency import (
    gold_amenity_desc_consistency,
)

from dagster_pipeline.defs.amenity_description_consistency.publish.rpt_amenity_desc_consistency import (
    rpt_amenity_desc_consistency_to_s3,
    rpt_amenity_desc_consistency_to_geospot,
)

from dagster_pipeline.defs.amenity_description_consistency.jobs import (
    amenity_desc_consistency_job,
    adc_after_spot_amenities_sensor,
)

__all__ = [
    # Bronze
    "adc_raw_gs_lk_spots",
    "adc_raw_gs_bt_spot_amenities",
    # Silver STG
    "adc_stg_gs_lk_spots",
    "adc_stg_gs_bt_spot_amenities",
    # Silver Core
    "core_amenity_desc_consistency",
    # Gold
    "gold_amenity_desc_consistency",
    # Publish
    "rpt_amenity_desc_consistency_to_s3",
    "rpt_amenity_desc_consistency_to_geospot",
    # Jobs & Sensors
    "amenity_desc_consistency_job",
    "adc_after_spot_amenities_sensor",
]
