# defs/data_lakehouse/lk_spots_concurrency.py
"""
Tags for multiprocess concurrency on lk_spots bronze/silver/gold jobs.

Assign these to assets that participate in lk_spots_* jobs; jobs pass
tag_concurrency_limits in execution config so only those runs throttle.
Other jobs that materialize the same assets are unaffected unless they
define the same limits.
"""
LK_SPOTS_PIPELINE_TAG_KEY = "lk_spots_pipeline"

TAGS_LK_SPOTS_BRONZE = {LK_SPOTS_PIPELINE_TAG_KEY: "bronze"}
TAGS_LK_SPOTS_SILVER = {LK_SPOTS_PIPELINE_TAG_KEY: "silver"}
TAGS_LK_SPOTS_GOLD = {LK_SPOTS_PIPELINE_TAG_KEY: "gold"}

# Defaults for lk_spots_* jobs (tune via jobs.py if hosts are larger/smaller).
LK_SPOTS_BRONZE_CONCURRENCY_LIMIT = 3
LK_SPOTS_SILVER_CONCURRENCY_LIMIT = 2
LK_SPOTS_GOLD_CONCURRENCY_LIMIT = 1
