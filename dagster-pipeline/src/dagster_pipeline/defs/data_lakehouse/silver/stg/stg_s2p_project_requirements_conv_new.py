# defs/data_lakehouse/silver/stg/stg_s2p_project_requirements_conv_new.py
"""
Silver STG: project_requirements + calendar_appointments con filtro ca.origin = 2.

Replica la query legacy conversation_analysis project_requirements.sql en Silver:
  project_requirements pr
  LEFT JOIN calendar_appointments ca ON ca.project_requirement_id = pr.id
  WHERE ca.origin = 2

Schema de salida (mismo que legacy): project_id, lead_id, visit_created_at, visit_updated_at,
project_created_at, calendar_origin, project_origin. Usado por gold_bt_conv_conversations_new
para scheduled_visit_count y created_project_count (paridad con conversation_analysis).
"""
from __future__ import annotations

import dagster as dg
import polars as pl

from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.shared import (
    build_silver_s3_key,
    write_polars_to_s3,
    read_bronze_from_s3,
    resolve_stale_reference_date,
)


@dg.asset(
    name="stg_s2p_project_requirements_conv_new",
    group_name="silver",
    retry_policy=dg.RetryPolicy(max_retries=2),
    deps=["raw_s2p_project_requirements_new", "raw_s2p_calendar_appointments_new"],
    description=(
        "Silver: project_requirements JOIN calendar_appointments WHERE ca.origin=2. "
        "Same schema as conversation_analysis project_requirements.sql. Used by gold_bt_conv_conversations_new."
    ),
    io_manager_key="s3_silver",
)
def stg_s2p_project_requirements_conv_new(context):
    """
    Replica en Silver la query legacy:
      SELECT pr.id AS project_id, pr.client_id AS lead_id, ca.created_at AS visit_created_at,
             ca.updated_at AS visit_updated_at, pr.created_at AS project_created_at,
             ca.origin AS calendar_origin, pr.origin AS project_origin
      FROM project_requirements pr
      LEFT JOIN calendar_appointments ca ON ca.project_requirement_id = pr.id
      WHERE ca.origin = 2
    """
    def body():
        df_pr, meta_pr = read_bronze_from_s3("raw_s2p_project_requirements_new", context)
        df_ca, meta_ca = read_bronze_from_s3("raw_s2p_calendar_appointments_new", context)

        if df_pr.is_empty():
            context.log.warning(
                "raw_s2p_project_requirements_new is empty; returning empty conv project_requirements"
            )
            return pl.DataFrame(
                schema={
                    "project_id": pl.Int64,
                    "lead_id": pl.Int64,
                    "visit_created_at": pl.Datetime,
                    "visit_updated_at": pl.Datetime,
                    "project_created_at": pl.Datetime,
                    "calendar_origin": pl.Int64,
                    "project_origin": pl.Int64,
                }
            )

        if df_ca.is_empty():
            context.log.warning(
                "raw_s2p_calendar_appointments_new is empty; no rows pass ca.origin=2"
            )
            return pl.DataFrame(
                schema={
                    "project_id": pl.Int64,
                    "lead_id": pl.Int64,
                    "visit_created_at": pl.Datetime,
                    "visit_updated_at": pl.Datetime,
                    "project_created_at": pl.Datetime,
                    "calendar_origin": pl.Int64,
                    "project_origin": pl.Int64,
                }
            )

        ca_filtered = df_ca.filter(pl.col("origin") == 2)
        if ca_filtered.is_empty():
            context.log.warning("No calendar_appointments with origin=2")
            return pl.DataFrame(
                schema={
                    "project_id": pl.Int64,
                    "lead_id": pl.Int64,
                    "visit_created_at": pl.Datetime,
                    "visit_updated_at": pl.Datetime,
                    "project_created_at": pl.Datetime,
                    "calendar_origin": pl.Int64,
                    "project_origin": pl.Int64,
                }
            )

        joined = df_pr.join(
            ca_filtered,
            left_on="id",
            right_on="project_requirement_id",
            how="inner",
        )

        out = joined.select([
            pl.col("id").alias("project_id"),
            pl.col("client_id").alias("lead_id"),
            pl.col("created_at_right").alias("visit_created_at"),
            pl.col("updated_at").alias("visit_updated_at"),
            pl.col("created_at").alias("project_created_at"),
            pl.col("origin_right").alias("calendar_origin"),
            pl.col("origin").alias("project_origin"),
        ])

        ref = resolve_stale_reference_date(context)
        context.log.info(
            f"stg_s2p_project_requirements_conv_new: {out.height:,} rows (pr+ca, ca.origin=2) ref_date={ref}"
        )
        s3_key = build_silver_s3_key(
            "stg_s2p_project_requirements_conv_new", file_format="parquet"
        )
        write_polars_to_s3(out, s3_key, context, file_format="parquet")
        return out

    yield from iter_job_wrapped_compute(context, body)
