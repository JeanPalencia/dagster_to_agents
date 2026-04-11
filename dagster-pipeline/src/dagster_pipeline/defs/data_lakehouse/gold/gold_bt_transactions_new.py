# defs/data_lakehouse/gold/gold_bt_transactions_new.py
"""
Gold layer: bt_transactions (project spot wins) — NEW VERSION.

Equivalent to the legacy view built from lk_projects + lk_spots: projects at stage 6 (won),
one row per spot. Legacy expands lk_projects.project_spots_at_last_stage (JSONB array);
this pipeline derives spots from project_requirement_spots (prs) at stage 6, then joins lk_spots for user_id.

Reads from S3: gold lk_projects_new, gold lk_spots_new, silver stg_s2p_prs_project_spots_new.
Writes parquet to S3 and loads to Geospot (bt_transactions).

Validation: join on (project_id, lead_id, spot_id); rows in common should match user_id and project_won_date.
See docs/references/bt_transactions_validation.md for legacy view definition and optional alignment via project_spots_at_last_stage.
"""
import dagster as dg
import polars as pl
from dagster import AssetKey

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_gold_s3_key,
    write_polars_to_s3,
    read_gold_from_s3,
    read_silver_from_s3,
    load_to_geospot,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.notifications import send_stale_data_notification
from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields

# Stage 6 = won (project_last_spot_stage_id / project_spot_status_id)
PROJECT_LAST_STAGE_WON_ID = 6


def _transform_bt_transactions_new(
    df_projects: pl.DataFrame,
    df_prs: pl.DataFrame,
    df_spots: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds bt_transactions (project spot wins): projects at stage 6, spots at stage 6, join to spots for user_id.

    - Filter projects with project_last_spot_stage_id = 6.
    - Filter prs with project_spot_status_id = 6 (spots at last stage).
    - Join projects to prs on project_id -> (project_id, lead_id, spot_id, project_won_date).
    - Left join to lk_spots on spot_id for user_id.
    - Drop rows where spot_id is null (per original SQL).
    """
    # Projects won (stage 6)
    projects_won = df_projects.filter(
        pl.col("project_last_spot_stage_id") == PROJECT_LAST_STAGE_WON_ID
    ).select(["project_id", "lead_id", "project_won_date"])

    # Project-spot at last stage (stage 6)
    prs_won = df_prs.filter(
        pl.col("project_spot_status_id") == PROJECT_LAST_STAGE_WON_ID
    ).select(["project_id", "spot_id"])

    # (project_id, lead_id, spot_id, project_won_date)
    psp = projects_won.join(prs_won, on="project_id", how="inner")

    # Spots: spot_id -> user_id (only columns we need)
    spots_lookup = df_spots.select(["spot_id", "user_id"]).unique(subset=["spot_id"])

    # Left join to get user_id; then filter out null spot_id (should not happen after inner join)
    df = psp.join(spots_lookup, on="spot_id", how="left").filter(
        pl.col("spot_id").is_not_null()
    )

    # Final column order: project_id, lead_id, spot_id, user_id, project_won_date
    return df.select(["project_id", "lead_id", "spot_id", "user_id", "project_won_date"])


@dg.asset(
    name="gold_bt_transactions_new",
    partitions_def=daily_partitions,
    group_name="gold",
    retry_policy=dg.RetryPolicy(max_retries=2),
    io_manager_key="s3_gold",
    description=(
        "Gold: bt_transactions (project spot wins). "
        "Reads gold lk_projects_new, gold lk_spots_new and silver stg_s2p_prs_project_spots_new from S3, "
        "filters stage 6 (won), joins and writes parquet + loads to Geospot (bt_transactions)."
    ),
)
def gold_bt_transactions_new(context):
    """
    Builds the gold bt_transactions table (lk_project_spot_wins logic):
    projects at last spot stage 6, expanded by spot_id from prs at stage 6, with user_id from lk_spots.
    """
    def body():
        stale_tables = []

        df_projects, meta_projects = read_gold_from_s3("lk_projects_new", context)
        if meta_projects.get("is_stale"):
            stale_tables.append({
                "table_name": "gold_lk_projects_new",
                "expected_date": context.partition_key,
                "available_date": meta_projects.get("file_date", ""),
                "layer": "gold",
                "file_path": meta_projects.get("file_path", ""),
            })

        df_spots, meta_spots = read_gold_from_s3("lk_spots_new", context)
        if meta_spots.get("is_stale"):
            stale_tables.append({
                "table_name": "gold_lk_spots_new",
                "expected_date": context.partition_key,
                "available_date": meta_spots.get("file_date", ""),
                "layer": "gold",
                "file_path": meta_spots.get("file_path", ""),
            })

        df_prs, meta_prs = read_silver_from_s3("stg_s2p_prs_project_spots_new", context)
        if meta_prs.get("is_stale"):
            stale_tables.append({
                "table_name": "stg_s2p_prs_project_spots_new",
                "expected_date": context.partition_key,
                "available_date": meta_prs.get("file_date", ""),
                "layer": "silver",
                "file_path": meta_prs.get("file_path", ""),
            })

        df = _transform_bt_transactions_new(df_projects, df_prs, df_spots)
        df = add_audit_fields(df, job_name="bt_transactions_new")

        context.log.info(
            f"gold_bt_transactions_new: {df.height:,} rows, {df.width} columns for partition {context.partition_key}"
        )

        s3_key = build_gold_s3_key("bt_transactions", context.partition_key, file_format="parquet")
        write_polars_to_s3(df, s3_key, context, file_format="parquet")

        context.log.info(
            f"📤 Loading to Geospot: table=bt_transactions, s3_key={s3_key}, mode=replace"
        )
        try:
            load_to_geospot(s3_key=s3_key, table_name="bt_transactions", mode="replace", context=context)
            context.log.info("✅ Geospot load completed for bt_transactions")
        except Exception as e:
            context.log.error(f"❌ Geospot load failed: {e}")
            raise

        if stale_tables:
            send_stale_data_notification(
                context,
                stale_tables,
                current_asset_name="gold_bt_transactions_new",
                current_layer="gold",
            )

        return df

    yield from iter_job_wrapped_compute(context, body)
