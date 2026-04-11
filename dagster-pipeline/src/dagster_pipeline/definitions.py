from dagster import Definitions

from dagster_pipeline.defs.amenity_description_consistency import (
    adc_raw_gs_lk_spots,
    adc_raw_gs_bt_spot_amenities,
    adc_stg_gs_lk_spots,
    adc_stg_gs_bt_spot_amenities,
    core_amenity_desc_consistency,
    gold_amenity_desc_consistency,
    rpt_amenity_desc_consistency_to_s3,
    rpt_amenity_desc_consistency_to_geospot,
    amenity_desc_consistency_job,
    adc_after_spot_amenities_sensor,
)
from dagster_pipeline.defs.maintenance.assets import cleanup_storage

# Active flows in this Railway test environment.
# To add a new flow: import its assets/jobs/sensors and add them below.
defs = Definitions(
    assets=[
        # amenity_description_consistency
        adc_raw_gs_lk_spots,
        adc_raw_gs_bt_spot_amenities,
        adc_stg_gs_lk_spots,
        adc_stg_gs_bt_spot_amenities,
        core_amenity_desc_consistency,
        gold_amenity_desc_consistency,
        rpt_amenity_desc_consistency_to_s3,
        rpt_amenity_desc_consistency_to_geospot,
        # maintenance
        cleanup_storage,
    ],
    jobs=[amenity_desc_consistency_job],
    sensors=[adc_after_spot_amenities_sensor],
)
