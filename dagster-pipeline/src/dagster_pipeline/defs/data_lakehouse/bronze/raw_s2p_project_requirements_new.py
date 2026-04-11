# defs/data_lakehouse/bronze/raw_s2p_project_requirements_new.py
"""Bronze: raw project_requirements from Spot2 MySQL."""
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset

raw_s2p_project_requirements_new = make_bronze_asset(
    "mysql_prod",
    table_name="project_requirements",
    description="Bronze: raw extraction of project_requirements from Spot2 Platform (MySQL Production).",
    partitioned=False,
)
