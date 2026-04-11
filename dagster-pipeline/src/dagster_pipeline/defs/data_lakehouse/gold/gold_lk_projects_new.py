"""
Gold layer: lk_projects for governance.
Reads _new silvers from S3, runs same procedure as gold_lk_projects (with clients→leads mapping),
adds governance layer and loads to lk_projects in Geospot (tabla principal; antes lk_projects_governance).
user_id is taken only from project (not from lead/client) to match legacy lk_projects_v2 — see QA report.
See COMPARISON_LEGACY_VS_NEW_STG.md for why _new inputs can yield different data than legacy.
"""
from datetime import date, datetime

import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_gold_s3_key,
    write_polars_to_s3,
    read_silver_from_s3,
    load_to_geospot,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.notifications import send_stale_data_notification
from dagster_pipeline.defs.data_lakehouse.gold.utils import validate_gold_joins

_LK_PROJECTS_UPSTREAM_SILVERS = (
    "stg_s2p_projects_new",
    "stg_s2p_clients_new",
    "stg_s2p_profiles_new",
    "core_project_funnel_new",
)

LK_PROJECTS_GOVERNANCE_COLUMN_ORDER = (
    "project_id", "project_name", "lead_id", "spot_sector", "project_state_ids",
    "lead_max_type", "lead_campaign_type", "lead_profiling_completed_at", "lead_project_funnel_relevant_id",
    "user_id", "user_industria_role_id", "user_industria_role", "spot_sector_id",
    "project_min_square_space", "project_max_square_space", "project_currency_id",
    "spot_currency_sale_id", "spot_currency_sale", "spot_price_sqm",
    "project_min_rent_price", "project_max_rent_price", "project_min_sale_price", "project_max_sale_price",
    "spot_price_sqm_mxn_sale_min", "spot_price_sqm_mxn_sale_max",
    "spot_price_sqm_mxn_rent_min", "spot_price_sqm_mxn_rent_max",
    "project_rent_months", "project_commission",
    "project_last_spot_stage_id", "project_last_spot_stage", "project_enable_id", "project_enable",
    "project_disable_reason_id", "project_disable_reason",
    "project_funnel_relevant_id", "project_funnel_relevant", "project_won_date",
    "project_funnel_lead_date", "project_funnel_visit_created_date",
    "project_funnel_visit_created_per_project_date", "project_funnel_visit_status",
    "project_funnel_visit_realized_at", "project_funnel_visit_realized_per_project_date",
    "project_funnel_visit_confirmed_at", "project_funnel_visit_confirmed_per_project_date",
    "project_funnel_loi_date", "project_funnel_loi_per_project_date",
    "project_funnel_contract_date", "project_funnel_contract_per_project_date",
    "project_funnel_transaction_date", "project_funnel_transaction_per_project_date",
    "project_funnel_flow", "project_funnel_events",
    "project_created_at", "project_created_date", "project_updated_at", "project_updated_date",
    "aud_inserted_at", "aud_inserted_date", "aud_updated_at", "aud_updated_date", "aud_job",
)


def _dtype_for_governance(col: str):
    if col in ("project_id", "lead_id", "user_id"):
        return pl.Int64
    if col in ("project_state_ids", "project_funnel_events"):
        return pl.Utf8
    if "_id" in col and "project_enable" not in col:
        return pl.Int32
    if col in ("project_enable_id", "project_disable_reason_id", "lead_project_funnel_relevant_id",
               "project_funnel_relevant_id", "project_last_spot_stage_id"):
        return pl.Int32
    if col.endswith("_at"):
        return pl.Datetime
    if col.endswith("_date") and not col.startswith("aud_"):
        return pl.Date
    if col.startswith("aud_"):
        return pl.Datetime if col.endswith("_at") else pl.Date
    if any(x in col for x in ("price", "sqm", "commission", "square_space")):
        return pl.Float64
    if col == "project_rent_months":
        return pl.Int32
    return pl.Utf8


def _transform_lk_projects_new(
    stg_s2p_projects_new: pl.DataFrame,
    stg_s2p_clients_new: pl.DataFrame,
    stg_s2p_profiles_new: pl.DataFrame,
    core_project_funnel_new: pl.DataFrame,
) -> pl.DataFrame:
    """Same procedure as _transform_lk_projects; sources are _new, clients mapped to lead columns."""
    lead_cols = [
        pl.col("id").alias("lead_id"),
        pl.col("spot_type_text").alias("spot_sector"),
        pl.col("lead_max_type"),
        pl.col("campaign_type").alias("lead_campaign_type"),
        pl.col("project_funnel_relevant_id").alias("lead_project_funnel_relevant_id"),
    ]
    # Do NOT bring user_id from clients/leads: legacy uses only project's user_id (stg_s2p_leads
    # has no user_id). Using lead's user_id caused 11.6% row diffs in QA lk_projects_v2 vs governance.
    if "profiling_completed_at" in stg_s2p_clients_new.columns:
        lead_cols.append(pl.col("profiling_completed_at").alias("lead_profiling_completed_at"))
    else:
        lead_cols.append(pl.lit(None).cast(pl.Datetime).alias("lead_profiling_completed_at"))
    # Dedupe by lead_id so the join does not multiply project rows (one row per project).
    # raw_s2p_clients_new is ORDER BY id, created_at DESC, so "first" = latest by created_at.
    df_leads = stg_s2p_clients_new.select(lead_cols).unique(subset=["lead_id"], keep="first")

    df_profiles = stg_s2p_profiles_new.select([
        "user_id", "user_industria_role_id", "user_industria_role",
    ])

    df_funnel = (
        core_project_funnel_new
        .sort("project_funnel_lead_date", nulls_last=True)
        .group_by("project_id")
        .agg([
            pl.col("project_funnel_lead_date").first(),
            pl.col("project_funnel_visit_created_date").first(),
            pl.col("project_funnel_visit_created_per_project_date").first(),
            pl.col("project_funnel_visit_status").first(),
            pl.col("project_funnel_visit_realized_at").first(),
            pl.col("project_funnel_visit_realized_per_project_date").first(),
            pl.col("project_funnel_visit_confirmed_at").first(),
            pl.col("project_funnel_visit_confirmed_per_project_date").first(),
            pl.col("project_funnel_loi_date").first(),
            pl.col("project_funnel_loi_per_project_date").first(),
            pl.col("project_funnel_contract_date").first(),
            pl.col("project_funnel_contract_per_project_date").first(),
            pl.col("project_funnel_transaction_date").first(),
            pl.col("project_funnel_transaction_per_project_date").first(),
            pl.col("project_funnel_flow").first(),
            pl.col("project_funnel_event").drop_nulls().unique().sort().alias("project_funnel_events"),
        ])
    )
    # Empty groups get null from agg; normalize to [] so Parquet/Postgres never see 100% NULL
    df_funnel = df_funnel.with_columns(
        pl.when(pl.col("project_funnel_events").is_null())
        .then(pl.lit([]).cast(pl.List(pl.Utf8)))
        .otherwise(pl.col("project_funnel_events"))
        .alias("project_funnel_events")
    )

    df_joined = (
        stg_s2p_projects_new
        .join(df_leads, on="lead_id", how="left")
    )
    # user_id: use only project's user_id to match legacy (gold_lk_projects uses stg_s2p_projects.user_id only).
    df_joined = (
        df_joined
        .join(df_profiles, on="user_id", how="left")
        .join(df_funnel, on="project_id", how="left")
    )

    return df_joined.select([
        pl.col("project_id"), pl.col("project_name"), pl.col("lead_id"),
        pl.col("spot_sector"), pl.col("project_state_ids"), pl.col("lead_max_type"),
        pl.col("lead_campaign_type"), pl.col("lead_profiling_completed_at"),
        pl.col("lead_project_funnel_relevant_id"), pl.col("user_id"),
        pl.col("user_industria_role_id"), pl.col("user_industria_role"),
        pl.col("spot_sector_id"), pl.col("project_min_square_space"), pl.col("project_max_square_space"),
        pl.col("project_currency_id"), pl.col("spot_currency_sale_id"), pl.col("spot_currency_sale"),
        pl.col("spot_price_sqm"), pl.col("project_min_rent_price"), pl.col("project_max_rent_price"),
        pl.col("project_min_sale_price"), pl.col("project_max_sale_price"),
        pl.col("spot_price_sqm_mxn_sale_min"), pl.col("spot_price_sqm_mxn_sale_max"),
        pl.col("spot_price_sqm_mxn_rent_min"), pl.col("spot_price_sqm_mxn_rent_max"),
        pl.col("project_rent_months"), pl.col("project_commission"),
        pl.col("project_last_spot_stage_id"), pl.col("project_last_spot_stage"),
        pl.col("project_enable_id"), pl.col("project_enable"),
        pl.col("project_disable_reason_id"), pl.col("project_disable_reason"),
        pl.col("project_funnel_relevant_id"), pl.col("project_funnel_relevant"),
        pl.col("project_won_date"),
        pl.col("project_funnel_lead_date"), pl.col("project_funnel_visit_created_date"),
        pl.col("project_funnel_visit_created_per_project_date"), pl.col("project_funnel_visit_status"),
        pl.col("project_funnel_visit_realized_at"), pl.col("project_funnel_visit_realized_per_project_date"),
        pl.col("project_funnel_visit_confirmed_at"), pl.col("project_funnel_visit_confirmed_per_project_date"),
        pl.col("project_funnel_loi_date"), pl.col("project_funnel_loi_per_project_date"),
        pl.col("project_funnel_contract_date"), pl.col("project_funnel_contract_per_project_date"),
        pl.col("project_funnel_transaction_date"), pl.col("project_funnel_transaction_per_project_date"),
        pl.col("project_funnel_flow"), pl.col("project_funnel_events"),
        pl.col("project_created_at"), pl.col("project_created_date"),
        pl.col("project_updated_at"), pl.col("project_updated_date"),
        pl.lit(datetime.now()).alias("aud_inserted_at"),
        pl.lit(date.today()).alias("aud_inserted_date"),
        pl.lit(datetime.now()).alias("aud_updated_at"),
        pl.lit(date.today()).alias("aud_updated_date"),
        pl.lit("lk_projects").alias("aud_job"),
    ])


def _load_lk_projects_silvers(context: dg.AssetExecutionContext):
    data = {}
    stale_tables = []
    for name in _LK_PROJECTS_UPSTREAM_SILVERS:
        df, meta = read_silver_from_s3(name, context)
        data[name] = df
        if meta.get("is_stale"):
            stale_tables.append({
                "table_name": name,
                "expected_date": context.partition_key,
                "available_date": meta.get("file_date"),
                "layer": "silver",
                "file_path": meta.get("file_path"),
            })
    return data, stale_tables


@dg.asset(
    partitions_def=daily_partitions,
    group_name="gold",
    retry_policy=dg.RetryPolicy(max_retries=2),
    io_manager_key="s3_gold",
    description="Gold: lk_projects for governance. Uses _new silvers; same procedure as gold_lk_projects.",
)
def gold_lk_projects_new(context):
    def body():
        data, stale_tables = _load_lk_projects_silvers(context)

        df_projects = _transform_lk_projects_new(
            stg_s2p_projects_new=data["stg_s2p_projects_new"],
            stg_s2p_clients_new=data["stg_s2p_clients_new"],
            stg_s2p_profiles_new=data["stg_s2p_profiles_new"],
            core_project_funnel_new=data["core_project_funnel_new"],
        )

        validate_gold_joins(
            df_main=data["stg_s2p_projects_new"],
            df_joined=df_projects,
            context=context,
            main_name="stg_s2p_projects_new",
        )

        if df_projects["project_id"].null_count() > 0:
            context.log.warning(
                f"Dropping {df_projects['project_id'].null_count()} rows with null project_id"
            )
            df_projects = df_projects.filter(pl.col("project_id").is_not_null())

        if "project_funnel_events" in df_projects.columns:
            df_projects = df_projects.with_columns(
                pl.when(pl.col("project_funnel_events").is_null())
                .then(pl.lit("[]"))
                .when(pl.col("project_funnel_events").list.len() == 0)
                .then(pl.lit("[]"))
                .otherwise(
                    pl.lit("[\"")
                    + pl.col("project_funnel_events").list.join("\", \"")
                    + pl.lit("\"]")
                )
                .alias("project_funnel_events")
            )

        for c in LK_PROJECTS_GOVERNANCE_COLUMN_ORDER:
            if c not in df_projects.columns:
                df_projects = df_projects.with_columns(
                    pl.lit(None).cast(_dtype_for_governance(c)).alias(c)
                )

        for c in ("project_id", "lead_id", "user_id"):
            if c in df_projects.columns:
                df_projects = df_projects.with_columns(pl.col(c).cast(pl.Int64).alias(c))
        for c in ("project_enable_id", "project_disable_reason_id", "lead_project_funnel_relevant_id",
                  "project_funnel_relevant_id", "project_last_spot_stage_id", "user_industria_role_id",
                  "spot_sector_id", "project_currency_id", "spot_currency_sale_id"):
            if c in df_projects.columns:
                df_projects = df_projects.with_columns(pl.col(c).cast(pl.Int32).alias(c))
        if "spot_price_sqm" in df_projects.columns:
            df_projects = df_projects.with_columns(pl.col("spot_price_sqm").cast(pl.Float64).alias("spot_price_sqm"))
        if "project_rent_months" in df_projects.columns:
            df_projects = df_projects.with_columns(pl.col("project_rent_months").cast(pl.Int32).alias("project_rent_months"))
        for c in ("project_min_square_space", "project_max_square_space", "project_min_rent_price",
                  "project_max_rent_price", "project_min_sale_price", "project_max_sale_price",
                  "spot_price_sqm_mxn_sale_min", "spot_price_sqm_mxn_sale_max",
                  "spot_price_sqm_mxn_rent_min", "spot_price_sqm_mxn_rent_max", "project_commission"):
            if c in df_projects.columns:
                df_projects = df_projects.with_columns(pl.col(c).cast(pl.Float64).alias(c))
        for c in df_projects.columns:
            if c.endswith("_at"):
                df_projects = df_projects.with_columns(pl.col(c).cast(pl.Datetime).alias(c))
            elif c.endswith("_date"):
                df_projects = df_projects.with_columns(pl.col(c).cast(pl.Date).alias(c))

        df_projects = df_projects.select(LK_PROJECTS_GOVERNANCE_COLUMN_ORDER)

        context.log.info(
            f"gold_lk_projects_new: {df_projects.height} rows, {df_projects.width} columns "
            f"for partition {context.partition_key}"
        )

        s3_key = build_gold_s3_key("lk_projects_new", context.partition_key, file_format="parquet")
        write_polars_to_s3(df_projects, s3_key, context, file_format="parquet")

        context.log.info(f"📤 Loading to Geospot: table=lk_projects, s3_key={s3_key}, mode=replace")
        try:
            load_to_geospot(s3_key=s3_key, table_name="lk_projects", mode="replace", context=context)
            context.log.info("✅ Geospot load completed for lk_projects")
        except Exception as e:
            context.log.error(f"❌ Geospot load failed: {e}")
            raise

        if stale_tables:
            send_stale_data_notification(
                context,
                stale_tables,
                current_asset_name="gold_lk_projects_new",
                current_layer="gold",
            )

        return df_projects

    yield from iter_job_wrapped_compute(context, body)
