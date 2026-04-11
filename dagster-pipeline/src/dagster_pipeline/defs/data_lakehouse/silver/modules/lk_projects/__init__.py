# lk_projects silver module: funnel (clients, projects, visits, LOI, contracts, flow)
from dagster_pipeline.defs.data_lakehouse.silver.modules.lk_projects.funnel import (
    transform_project_funnel_new,
)

__all__ = ["transform_project_funnel_new"]
