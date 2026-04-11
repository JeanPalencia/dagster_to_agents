# defs/data_lakehouse/jobs.py
"""
Jobs and Schedules for the Data Lakehouse.

This module defines:
- lakehouse_daily_job: Materializes all publish assets (and their dependencies)
- lakehouse_daily_schedule: Runs the job every day at 6:00 AM

NEW STANDARDIZED PIPELINE JOBS:
- Gold jobs (one per gold _new table): lk_leads_new, lk_projects_new, lk_spots_new, lk_users_new,
  bt_lds_lead_spots_new, bt_conv_conversations_new — each materializes only that gold asset.
- Per-gold bronze/silver jobs: for each gold _new table, two jobs materialize only the upstream
  assets needed for that gold in that layer. Names: {base}_bronze_new, {base}_silver_new
  (e.g. lk_leads_bronze_new, lk_leads_silver_new; lk_leads_new is the gold job).

All jobs include cleanup_storage at the end to free disk space.

BT conversations bronze schedules use the **calendar day of the run (Mexico City)** as partition_key
(legacy parity with ConversationConfig dynamic end_date = today). Other bronze schedules still use
**previous calendar day** for closed-day snapshots.

Run failure notifications (Google Chat): sensor lakehouse_new_pipeline_failure_chat monitors the
standardized lakehouse jobs list. Webhook: DAGSTER_LAKEHOUSE_FAILURE_WEBHOOK_URL or SSM
`/dagster/data-lake-errors/google_chat_webhook`. Mismo link a la UI del run que gold completion
(`DAGSTER_UI_RUNS_BASE_URL` o SSM `/dagster/RUNS_BASE_URL`).

Gold completion (Google Chat): sensors lakehouse_gold_completion_success_chat y
lakehouse_gold_completion_failure_chat para jobs gold (lk_spots_new, lk_projects_new, …). Webhook:
DAGSTER_LAKEHOUSE_COMPLETION_WEBHOOK_URL o SSM `/dagster/data-lake-logs/google_chat_webhook` (override:
DAGSTER_LAKEHOUSE_COMPLETION_WEBHOOK_SSM_PARAMETER). Link al run en la UI: `DAGSTER_UI_RUNS_BASE_URL` o SSM
`/dagster/RUNS_BASE_URL` (valor ejemplo `https://dagster.spot2.mx/runs/`; se concatena el run_id).
"""
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import boto3
import dagster as dg
import requests
from botocore.exceptions import ClientError

from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import (
    LK_SPOTS_BRONZE_CONCURRENCY_LIMIT,
    LK_SPOTS_GOLD_CONCURRENCY_LIMIT,
    LK_SPOTS_PIPELINE_TAG_KEY,
    LK_SPOTS_SILVER_CONCURRENCY_LIMIT,
)
from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions

# Sensores que exponen `.log` y pueden resolver la base URL de runs (SSM / env).
_DagsterUiUrlContext = dg.SensorEvaluationContext | dg.RunFailureSensorContext

# Google Chat: webhook de fallos (espacio típico data-lake-errors; el mensaje ya no nombra el canal).
SSM_DATA_LAKE_ERRORS_WEBHOOK_PARAMETER = "/dagster/data-lake-errors/google_chat_webhook"

# Google Chat: webhook de avisos gold (éxito / fallo); mismo para ambos sensores salvo override.
SSM_DATA_LAKE_COMPLETION_WEBHOOK_PARAMETER = "/dagster/data-lake-logs/google_chat_webhook"
# Base URL de la vista de runs en la UI de Dagster (con o sin `/` final); en el mensaje se agrega `{run_id}`.
SSM_DAGSTER_UI_RUNS_BASE_URL_PARAMETER = "/dagster/RUNS_BASE_URL"

_TZ_MX = ZoneInfo("America/Mexico_City")


def _resolve_lakehouse_failure_webhook_url(context: dg.RunFailureSensorContext) -> str | None:
    """Orden: DAGSTER_LAKEHOUSE_FAILURE_WEBHOOK_URL, luego SSM. Región: AWS_* / us-east-1."""
    direct = (os.environ.get("DAGSTER_LAKEHOUSE_FAILURE_WEBHOOK_URL") or "").strip()
    if direct:
        return direct
    param_name = (
        os.environ.get("DAGSTER_LAKEHOUSE_FAILURE_WEBHOOK_SSM_PARAMETER")
        or SSM_DATA_LAKE_ERRORS_WEBHOOK_PARAMETER
    ).strip()
    if not param_name:
        return None
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    try:
        ssm = boto3.client("ssm", region_name=region)
        resp = ssm.get_parameter(Name=param_name, WithDecryption=True)
        val = (resp.get("Parameter") or {}).get("Value") or ""
        return val.strip() or None
    except ClientError as exc:
        context.log.warning("No se pudo leer el webhook desde SSM %s: %s", param_name, exc)
        return None


def _resolve_lakehouse_completion_webhook_url(context: dg.SensorEvaluationContext) -> str | None:
    """Orden: DAGSTER_LAKEHOUSE_COMPLETION_WEBHOOK_URL, luego SSM. Región: AWS_* / us-east-1."""
    direct = (os.environ.get("DAGSTER_LAKEHOUSE_COMPLETION_WEBHOOK_URL") or "").strip()
    if direct:
        return direct
    param_name = (
        os.environ.get("DAGSTER_LAKEHOUSE_COMPLETION_WEBHOOK_SSM_PARAMETER")
        or SSM_DATA_LAKE_COMPLETION_WEBHOOK_PARAMETER
    ).strip()
    if not param_name:
        return None
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    try:
        ssm = boto3.client("ssm", region_name=region)
        resp = ssm.get_parameter(Name=param_name, WithDecryption=True)
        val = (resp.get("Parameter") or {}).get("Value") or ""
        return val.strip() or None
    except ClientError as exc:
        context.log.warning("No se pudo leer el webhook de completion desde SSM %s: %s", param_name, exc)
        return None


def _resolve_dagster_ui_runs_base_url(context: _DagsterUiUrlContext) -> str | None:
    direct = (os.environ.get("DAGSTER_UI_RUNS_BASE_URL") or "").strip()
    if direct:
        return direct
    param_name = (
        os.environ.get("DAGSTER_UI_RUNS_BASE_URL_SSM_PARAMETER")
        or SSM_DAGSTER_UI_RUNS_BASE_URL_PARAMETER
    ).strip()
    if not param_name:
        return None
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    try:
        ssm = boto3.client("ssm", region_name=region)
        resp = ssm.get_parameter(Name=param_name, WithDecryption=True)
        val = (resp.get("Parameter") or {}).get("Value") or ""
        return val.strip() or None
    except ClientError as exc:
        context.log.warning(
            "No se pudo leer la base URL de runs Dagster desde SSM %s: %s", param_name, exc
        )
        return None


def _dagster_run_ui_url(context: _DagsterUiUrlContext, run_id: str) -> str | None:
    base = _resolve_dagster_ui_runs_base_url(context)
    if not base:
        return None
    return base.rstrip("/") + "/" + run_id.lstrip("/")


def _lk_spots_multiprocess_limits(*, bronze: int, silver: int, gold: int) -> dict:
    """Execution config: cap concurrent steps per lk_spots layer (tag key lk_spots_pipeline)."""
    return {
        "execution": {
            "config": {
                "multiprocess": {
                    "tag_concurrency_limits": [
                        {
                            "key": LK_SPOTS_PIPELINE_TAG_KEY,
                            "value": "bronze",
                            "limit": bronze,
                        },
                        {
                            "key": LK_SPOTS_PIPELINE_TAG_KEY,
                            "value": "silver",
                            "limit": silver,
                        },
                        {
                            "key": LK_SPOTS_PIPELINE_TAG_KEY,
                            "value": "gold",
                            "limit": gold,
                        },
                    ],
                },
            },
        },
    }


# ============ Cleanup Selection ============
# Added to all jobs to clean up storage after pipeline completes
cleanup_selection = dg.AssetSelection.assets("cleanup_storage")


# ============ Asset Selection ============

# Select final publish assets AND all their upstream dependencies
# This ensures the entire pipeline runs: bronze -> silver -> gold -> publish

# Projects pipeline
lk_projects_selection = (
    dg.AssetSelection.assets("lk_projects_to_geospot")
    .upstream()  # Include all upstream dependencies
)

# Leads pipeline
lk_leads_selection = (
    dg.AssetSelection.assets("lk_leads_to_geospot")
    .upstream()  # Include all upstream dependencies
)

# Lead-Spots event log pipeline
bt_lds_lead_spots_selection = (
    dg.AssetSelection.assets("bt_lds_lead_spots_to_geospot")
    .upstream()  # Include all upstream dependencies
)

# Combine all selections + cleanup at the end
all_publish_selection = lk_projects_selection | lk_leads_selection | bt_lds_lead_spots_selection | cleanup_selection


# Gold layer: One job per gold table + cleanup at the end
gold_lk_leads_new_selection = dg.AssetSelection.assets("gold_lk_leads_new") | cleanup_selection
gold_lk_projects_new_selection = dg.AssetSelection.assets("gold_lk_projects_new") | cleanup_selection
gold_lk_spots_new_selection = dg.AssetSelection.assets("gold_lk_spots_new") | cleanup_selection
gold_lk_users_new_selection = dg.AssetSelection.assets("gold_lk_users_new") | cleanup_selection
gold_bt_lds_lead_spots_new_selection = dg.AssetSelection.assets("gold_bt_lds_lead_spots_new") | cleanup_selection
gold_lk_matches_visitors_to_leads_new_selection = dg.AssetSelection.assets("gold_lk_matches_visitors_to_leads_new") | cleanup_selection
gold_lk_visitors_new_selection = dg.AssetSelection.assets("gold_lk_visitors_new") | cleanup_selection
gold_bt_transactions_new_selection = dg.AssetSelection.assets("gold_bt_transactions_new") | cleanup_selection

# Gold QA: only qa_gold_new (reads _governance tables from DB, runs checks). No gold assets, no S3 writes.
qa_gold_new_selection = (
    dg.AssetSelection.assets("qa_gold_new")
    | cleanup_selection
)

# Per-gold bronze/silver: explicit tables each gold reads from S3 (no graph upstream).
# Bronze/silver lists derived from gold read_silver_from_s3 + silver read_bronze_from_s3 / stg_lk_spots_new inputs.
GOLD_LK_LEADS_BRONZE = (
    "raw_s2p_clients_new",
    "raw_s2p_project_requirements_new",
    "raw_s2p_calendar_appointments_new",
    "raw_s2p_calendar_appointment_dates_new",
    "raw_s2p_activity_log_projects_new",
    "raw_s2p_client_event_histories_new",
)
GOLD_LK_LEADS_SILVER = (
    "stg_s2p_clients_new",
    "stg_s2p_projects_new",
    "stg_s2p_appointments_new",
    "stg_s2p_apd_appointment_dates_new",
    "stg_s2p_alg_activity_log_projects_new",
    "stg_s2p_leh_lead_event_histories_new",
    "core_project_funnel_new",
)
GOLD_LK_PROJECTS_BRONZE = (
    "raw_s2p_project_requirements_new",
    "raw_s2p_clients_new",
    "raw_s2p_profiles_new",
    "raw_s2p_calendar_appointments_new",
    "raw_s2p_calendar_appointment_dates_new",
    "raw_s2p_activity_log_projects_new",
    "raw_s2p_client_event_histories_new",
)
GOLD_LK_PROJECTS_SILVER = (
    "stg_s2p_projects_new",
    "stg_s2p_clients_new",
    "stg_s2p_profiles_new",
    "stg_s2p_appointments_new",
    "stg_s2p_apd_appointment_dates_new",
    "stg_s2p_alg_activity_log_projects_new",
    "stg_s2p_leh_lead_event_histories_new",
    "core_project_funnel_new",
)
# gold_lk_spots_new reads stg_lk_spots_new; stg_lk_spots_new needs 21 stg_s2p_* and those need 21 raw_s2p_*
# stg_s2p_users_new also needs stg_s2p_kyc_agg_new (KYC); kyc_agg needs raw_s2p_users_new, raw_s2p_profiles_new, raw_s2p_persona_inquiries_new
GOLD_LK_SPOTS_BRONZE = (
    "raw_s2p_exchanges_new",
    "raw_s2p_prices_new",
    "raw_s2p_spots_new",
    "raw_s2p_zip_codes_new",
    "raw_s2p_spot_reports_new",
    "raw_s2p_report_history_new",
    "raw_s2p_listings_spots_new",
    "raw_s2p_listings_new",
    "raw_s2p_photos_new",
    "raw_s2p_contacts_new",
    "raw_s2p_users_new",
    "raw_s2p_profiles_new",
    "raw_s2p_model_has_roles_new",
    "raw_s2p_data_states_new",
    "raw_s2p_states_new",
    "raw_s2p_data_municipalities_new",
    "raw_s2p_cities_new",
    "raw_s2p_data_settlements_new",
    "raw_s2p_zones_new",
    "raw_s2p_spot_photos_new",
    "raw_s2p_spot_rankings_new",
    "raw_s2p_persona_inquiries_new",  # for stg_s2p_kyc_agg_new → stg_s2p_users_new
)
GOLD_LK_SPOTS_SILVER = (
    "stg_s2p_kyc_agg_new",  # upstream of stg_s2p_users_new
    "stg_s2p_exchanges_new",
    "stg_s2p_prices_new",
    "stg_s2p_spots_new",
    "stg_s2p_zip_codes_new",
    "stg_s2p_spot_reports_new",
    "stg_s2p_report_history_new",
    "stg_s2p_listings_spots_new",
    "stg_s2p_listings_new",
    "stg_s2p_photos_new",
    "stg_s2p_contacts_new",
    "stg_s2p_users_new",
    "stg_s2p_profiles_new",
    "stg_s2p_model_has_roles_new",
    "stg_s2p_data_states_new",
    "stg_s2p_states_new",
    "stg_s2p_data_municipalities_new",
    "stg_s2p_cities_new",
    "stg_s2p_data_settlements_new",
    "stg_s2p_zones_new",
    "stg_s2p_spot_photos_new",
    "stg_s2p_spot_rankings_new",
    "stg_lk_spots_new",
)
# gold_lk_users_new reads stg_s2p_users_new, stg_s2p_clients_new, stg_lk_spots_new (optional, for spot counts/sqm)
# stg_s2p_users_new reads raw_s2p_users_new + raw_s2p_profiles_new; KYC from stg_s2p_kyc_agg_new (users LEFT JOIN persona_inquiries)
GOLD_LK_USERS_BRONZE = (
    "raw_s2p_users_new",
    "raw_s2p_profiles_new",
    "raw_s2p_clients_new",
    "raw_s2p_persona_inquiries_new",
    # "raw_s2p_activity_log_users_new",  # commented out
)
GOLD_LK_USERS_SILVER = (
    # "stg_s2p_user_activity_agg_new",  # commented out
    "stg_s2p_kyc_agg_new",
    "stg_s2p_users_new",
    "stg_s2p_clients_new",
    # stg_lk_spots_new omitted: it pulls 20+ lk_spots upstream; gold_lk_users_new reads it from S3 when present (else spots 0/null)
)
# gold_bt_lds_lead_spots_new reads 7 core silver + 3 channel attribution silver tables
GOLD_BT_LDS_LEAD_SPOTS_BRONZE = (
    "raw_s2p_clients_new",
    "raw_s2p_project_requirements_new",
    "raw_s2p_project_requirement_spots_new",
    "raw_s2p_calendar_appointments_new",
    "raw_s2p_calendar_appointment_dates_new",
    "raw_s2p_activity_log_projects_new",
    "raw_s2p_profiles_new",
    # Channel attribution bronze
    "raw_gs_recommendation_projects_new",
    "raw_gs_recommendation_chatbot_new",
    "raw_cb_conversation_events_new",
)
GOLD_BT_LDS_LEAD_SPOTS_SILVER = (
    "stg_s2p_clients_new",
    "stg_s2p_projects_new",
    "stg_s2p_prs_project_spots_new",
    "stg_s2p_appointments_new",
    "stg_s2p_apd_appointment_dates_new",
    "stg_s2p_alg_activity_log_projects_new",
    "stg_s2p_profiles_new",
    # Channel attribution silver
    "stg_gs_recommendation_projects_exploded_new",
    "stg_gs_recommendation_chatbot_exploded_new",
    "stg_cb_conversation_events_new",
)
# gold_bt_conv_conversations_new depends on stg_bt_conv_merged_new (which depends on stg_bt_conv_with_events_new, stg_bt_conv_grouped_new)
# and reads from S3: stg_s2p_clients_new, stg_s2p_appointments_new, stg_s2p_project_requirements_conv_new, stg_staging_conversation_events_new
GOLD_BT_CONV_CONVERSATIONS_BRONZE = (
    "raw_cb_messages_new",
    "raw_s2p_clients_new",
    "raw_s2p_calendar_appointments_new",
    "raw_s2p_project_requirements_new",
    "raw_staging_conversation_events_new",
)
GOLD_BT_CONV_CONVERSATIONS_SILVER = (
    "stg_cb_messages_new",
    "stg_bt_conv_grouped_new",
    "stg_bt_conv_with_events_new",
    "stg_bt_conv_merged_new",
    "stg_s2p_clients_new",
    "stg_s2p_appointments_new",
    "stg_s2p_project_requirements_conv_new",
    "stg_staging_conversation_events_new",
)
# gold_lk_matches_visitors_to_leads_new reads only stg_gs_lk_matches_visitors_to_leads_new
GOLD_LK_MATCHES_VISITORS_TO_LEADS_BRONZE = ("raw_gs_lk_matches_visitors_to_leads_new",)
GOLD_LK_MATCHES_VISITORS_TO_LEADS_SILVER = ("stg_gs_lk_matches_visitors_to_leads_new",)

# gold_lk_visitors_new reads only stg_bq_funnel_with_channel_new (Geospot table: lk_visitors)
GOLD_LK_VISITORS_BRONZE = ("raw_bq_funnel_with_channel_new",)
GOLD_LK_VISITORS_SILVER = ("stg_bq_funnel_with_channel_new",)


# ============ Jobs ============

# Job that materializes all publish assets AND their dependencies
lakehouse_daily_job = dg.define_asset_job(
    name="lakehouse_daily_job",
    description="Daily job that materializes all data lakehouse publish assets and their upstream dependencies.",
    selection=all_publish_selection,
    partitions_def=daily_partitions,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "tag_concurrency_limits": [
                        {
                            "key": "geospot_api",
                            "value": "write",
                            "limit": 1,
                        }
                    ],
                }
            }
        }
    },
)

# ============ NEW STANDARDIZED PIPELINE JOBS ============

# Gold jobs: One job per gold table (reads from S3 silver, writes to S3)
gold_lk_leads_new_job = dg.define_asset_job(
    name="lk_leads_new",
    description=(
        "Daily job that materializes gold_lk_leads_new. "
        "Reads silver data from S3 (can use stale data if silver failed) "
        "and writes to S3 with daily partitioning."
    ),
    selection=gold_lk_leads_new_selection,
    partitions_def=daily_partitions,
)

gold_lk_projects_new_job = dg.define_asset_job(
    name="lk_projects_new",
    description=(
        "Daily job that materializes gold_lk_projects_new. "
        "Reads silver data from S3 (can use stale data if silver failed) "
        "and writes to S3 with daily partitioning."
    ),
    selection=gold_lk_projects_new_selection,
    partitions_def=daily_partitions,
)

gold_lk_spots_new_job = dg.define_asset_job(
    name="lk_spots_new",
    description=(
        "Daily job that materializes gold_lk_spots_new. "
        "Reads stg_lk_spots_new from S3, adds filter/filter_sub and audit fields, writes to S3."
    ),
    selection=gold_lk_spots_new_selection,
    partitions_def=daily_partitions,
    config=_lk_spots_multiprocess_limits(
        bronze=LK_SPOTS_BRONZE_CONCURRENCY_LIMIT,
        silver=LK_SPOTS_SILVER_CONCURRENCY_LIMIT,
        gold=LK_SPOTS_GOLD_CONCURRENCY_LIMIT,
    ),
)

gold_lk_users_new_job = dg.define_asset_job(
    name="lk_users_new",
    description=(
        "Daily job that materializes gold_lk_users_new. "
        "Reads silver data from S3 (can use stale data if silver failed) "
        "and writes to S3 with daily partitioning."
    ),
    selection=gold_lk_users_new_selection,
    partitions_def=daily_partitions,
)

gold_bt_lds_lead_spots_new_job = dg.define_asset_job(
    name="bt_lds_lead_spots_new",
    description=(
        "Daily job that materializes gold_bt_lds_lead_spots_new. "
        "Reads _new silver tables from S3, reuses legacy LDS transform, writes to S3 and Geospot."
    ),
    selection=gold_bt_lds_lead_spots_new_selection,
    partitions_def=daily_partitions,
)

gold_lk_matches_visitors_to_leads_new_job = dg.define_asset_job(
    name="lk_matches_visitors_to_leads_new",
    description=(
        "Daily job that materializes gold_lk_matches_visitors_to_leads_new. "
        "Reads silver from S3, adds audit, writes to S3 parquet and Geospot (lk_matches_visitors_to_leads_governance)."
    ),
    selection=gold_lk_matches_visitors_to_leads_new_selection,
    partitions_def=daily_partitions,
)

gold_lk_visitors_new_job = dg.define_asset_job(
    name="lk_visitors_gold_new",
    description=(
        "Daily job that materializes gold_lk_visitors_new. "
        "Reads stg_bq_funnel_with_channel_new from S3, applies dedup/vis_id/vis_*/aud_*, writes parquet (S3 lk_visitors_new) and loads to Geospot table lk_visitors."
    ),
    selection=gold_lk_visitors_new_selection,
    partitions_def=daily_partitions,
)

gold_bt_transactions_new_job = dg.define_asset_job(
    name="bt_transactions_new",
    description=(
        "Daily job that materializes gold_bt_transactions_new (project spot wins). "
        "Reads gold lk_projects_new, gold lk_spots_new and silver stg_s2p_prs_project_spots_new from S3, "
        "writes parquet and loads to Geospot (bt_transactions)."
    ),
    selection=gold_bt_transactions_new_selection,
    partitions_def=daily_partitions,
)

qa_gold_new_job = dg.define_asset_job(
    name="gold_qa_new",
    description=(
        "Runs only qa_gold_new: reads each _governance table from Geospot PostgreSQL and runs QA checks "
        "(rows, PK uniqueness, PK not null, column count, expected columns, partition). Does not run gold assets or write to S3."
    ),
    selection=qa_gold_new_selection,
    partitions_def=daily_partitions,
)

# Gold job: Conversations (depends on silver stg_bt_conv_* intermedios)
from dagster_pipeline.defs.data_lakehouse.silver.stg.stg_bt_conv_intermediate import (
    stg_bt_conv_grouped_new,
    stg_bt_conv_with_events_new,
    stg_bt_conv_merged_new,
)
from dagster_pipeline.defs.data_lakehouse.gold.gold_bt_conv_conversations_new import gold_bt_conv_conversations_new

gold_bt_conv_conversations_new_selection = dg.AssetSelection.assets(gold_bt_conv_conversations_new) | cleanup_selection

gold_bt_conv_conversations_new_job = dg.define_asset_job(
    name="bt_conv_conversations_new",
    description=(
        "Gold bt_conv_conversations: no partition day. "
        "S3 gold uses ref_date = today MX (resolve_stale_reference_date) per run."
    ),
    selection=gold_bt_conv_conversations_new_selection,
)

# ============ Per-gold bronze/silver jobs ============
# One bronze job and one silver job per gold _new table: materialize only the upstream assets
# needed for that gold table in each layer. Job names: {base}_bronze_new, {base}_silver_new.

lk_leads_bronze_new_job = dg.define_asset_job(
    name="lk_leads_bronze_new",
    description="Bronze tables needed for gold_lk_leads_new (clients, projects, funnel inputs).",
    selection=dg.AssetSelection.assets(*GOLD_LK_LEADS_BRONZE) | cleanup_selection,
    partitions_def=daily_partitions,
)
lk_leads_silver_new_job = dg.define_asset_job(
    name="lk_leads_silver_new",
    description="Silver tables needed for gold_lk_leads_new (clients, projects, core_project_funnel).",
    selection=dg.AssetSelection.assets(*GOLD_LK_LEADS_SILVER) | cleanup_selection,
    partitions_def=daily_partitions,
)

lk_projects_bronze_new_job = dg.define_asset_job(
    name="lk_projects_bronze_new",
    description="Bronze tables needed for gold_lk_projects_new.",
    selection=dg.AssetSelection.assets(*GOLD_LK_PROJECTS_BRONZE) | cleanup_selection,
    partitions_def=daily_partitions,
)
lk_projects_silver_new_job = dg.define_asset_job(
    name="lk_projects_silver_new",
    description="Silver tables needed for gold_lk_projects_new.",
    selection=dg.AssetSelection.assets(*GOLD_LK_PROJECTS_SILVER) | cleanup_selection,
    partitions_def=daily_partitions,
)

lk_spots_bronze_new_job = dg.define_asset_job(
    name="lk_spots_bronze_new",
    description="Bronze tables needed for gold_lk_spots_new (22 raw_s2p_* including persona_inquiries for KYC and model_has_roles; feed stg_lk_spots_new).",
    selection=dg.AssetSelection.assets(*GOLD_LK_SPOTS_BRONZE) | cleanup_selection,
    partitions_def=daily_partitions,
    config=_lk_spots_multiprocess_limits(
        bronze=LK_SPOTS_BRONZE_CONCURRENCY_LIMIT,
        silver=LK_SPOTS_SILVER_CONCURRENCY_LIMIT,
        gold=LK_SPOTS_GOLD_CONCURRENCY_LIMIT,
    ),
)
lk_spots_silver_new_job = dg.define_asset_job(
    name="lk_spots_silver_new",
    description="Silver tables needed for gold_lk_spots_new (stg_s2p_kyc_agg_new + 21 stg_s2p_* + stg_lk_spots_new).",
    selection=dg.AssetSelection.assets(*GOLD_LK_SPOTS_SILVER) | cleanup_selection,
    partitions_def=daily_partitions,
    config=_lk_spots_multiprocess_limits(
        bronze=LK_SPOTS_BRONZE_CONCURRENCY_LIMIT,
        silver=LK_SPOTS_SILVER_CONCURRENCY_LIMIT,
        gold=LK_SPOTS_GOLD_CONCURRENCY_LIMIT,
    ),
)

lk_users_bronze_new_job = dg.define_asset_job(
    name="lk_users_bronze_new",
    description="Bronze tables needed for gold_lk_users_new (users, profiles, clients).",
    selection=dg.AssetSelection.assets(*GOLD_LK_USERS_BRONZE) | cleanup_selection,
    partitions_def=daily_partitions,
)
lk_users_silver_new_job = dg.define_asset_job(
    name="lk_users_silver_new",
    description="Silver tables needed for gold_lk_users_new (stg_s2p_kyc_agg_new, stg_s2p_users_new, stg_s2p_clients_new).",
    selection=dg.AssetSelection.assets(*GOLD_LK_USERS_SILVER) | cleanup_selection,
    partitions_def=daily_partitions,
)

bt_lds_lead_spots_bronze_new_job = dg.define_asset_job(
    name="bt_lds_lead_spots_bronze_new",
    description="Bronze tables needed for gold_bt_lds_lead_spots_new.",
    selection=dg.AssetSelection.assets(*GOLD_BT_LDS_LEAD_SPOTS_BRONZE) | cleanup_selection,
    partitions_def=daily_partitions,
)
bt_lds_lead_spots_silver_new_job = dg.define_asset_job(
    name="bt_lds_lead_spots_silver_new",
    description="Silver tables needed for gold_bt_lds_lead_spots_new.",
    selection=dg.AssetSelection.assets(*GOLD_BT_LDS_LEAD_SPOTS_SILVER) | cleanup_selection,
    partitions_def=daily_partitions,
)

bt_conv_conversations_bronze_new_job = dg.define_asset_job(
    name="bt_conv_conversations_bronze_new",
    description=(
        "Bronze for bt_conv_conversations: no partition day. "
        "All raw assets in this selection use partitioned=False; dates via resolve_stale_reference_date."
    ),
    selection=dg.AssetSelection.assets(*GOLD_BT_CONV_CONVERSATIONS_BRONZE) | cleanup_selection,
)
bt_conv_conversations_silver_new_job = dg.define_asset_job(
    name="bt_conv_conversations_silver_new",
    description=(
        "Silver for bt_conv_conversations: no partition day; "
        "stg assets in this selection use partitioned=False."
    ),
    selection=dg.AssetSelection.assets(*GOLD_BT_CONV_CONVERSATIONS_SILVER) | cleanup_selection,
)

lk_matches_visitors_to_leads_bronze_new_job = dg.define_asset_job(
    name="lk_matches_visitors_to_leads_bronze_new",
    description="Bronze table needed for gold_lk_matches_visitors_to_leads_new (raw_gs_lk_matches).",
    selection=dg.AssetSelection.assets(*GOLD_LK_MATCHES_VISITORS_TO_LEADS_BRONZE) | cleanup_selection,
    partitions_def=daily_partitions,
)
lk_matches_visitors_to_leads_silver_new_job = dg.define_asset_job(
    name="lk_matches_visitors_to_leads_silver_new",
    description="Silver table needed for gold_lk_matches_visitors_to_leads_new (stg_gs_lk_matches).",
    selection=dg.AssetSelection.assets(*GOLD_LK_MATCHES_VISITORS_TO_LEADS_SILVER) | cleanup_selection,
    partitions_def=daily_partitions,
)

lk_visitors_bronze_new_job = dg.define_asset_job(
    name="lk_visitors_bronze_new",
    description="Bronze table needed for gold_lk_visitors_new (raw_bq_funnel_with_channel_new).",
    selection=dg.AssetSelection.assets(*GOLD_LK_VISITORS_BRONZE) | cleanup_selection,
    partitions_def=daily_partitions,
)
lk_visitors_silver_new_job = dg.define_asset_job(
    name="lk_visitors_silver_new",
    description="Silver table needed for gold_lk_visitors_new (stg_bq_funnel_with_channel_new).",
    selection=dg.AssetSelection.assets(*GOLD_LK_VISITORS_SILVER) | cleanup_selection,
    partitions_def=daily_partitions,
)

# Single source of truth for monitored jobs (see pipeline_asset_error_handling).
# All jobs that publish to GeoSpot must be listed here so the failure sensor
# (lakehouse_new_pipeline_failure_chat) sends failure alerts to the failure webhook (SSM / env).
from dagster_pipeline.defs.effective_supply.jobs import effective_supply_monthly_job
from dagster_pipeline.defs.funnel_ptd.jobs import funnel_ptd_job
from dagster_pipeline.defs.spot_amenities.jobs import spot_amenities_job
from dagster_pipeline.defs.amenity_description_consistency.jobs import amenity_desc_consistency_job
from dagster_pipeline.defs.browse_visitors.jobs import browse_visitors_job
from dagster_pipeline.defs.gsc_spots_performance.jobs import gsc_spots_performance_job

LAKEHOUSE_NEW_PIPELINE_JOBS = (
    # --- Lakehouse _new jobs ---
    gold_lk_leads_new_job,
    gold_lk_projects_new_job,
    gold_lk_spots_new_job,
    gold_lk_users_new_job,
    gold_bt_lds_lead_spots_new_job,
    gold_lk_matches_visitors_to_leads_new_job,
    gold_lk_visitors_new_job,
    gold_bt_transactions_new_job,
    qa_gold_new_job,
    gold_bt_conv_conversations_new_job,
    lk_leads_bronze_new_job,
    lk_leads_silver_new_job,
    lk_projects_bronze_new_job,
    lk_projects_silver_new_job,
    lk_spots_bronze_new_job,
    lk_spots_silver_new_job,
    lk_users_bronze_new_job,
    lk_users_silver_new_job,
    bt_lds_lead_spots_bronze_new_job,
    bt_lds_lead_spots_silver_new_job,
    bt_conv_conversations_bronze_new_job,
    bt_conv_conversations_silver_new_job,
    lk_matches_visitors_to_leads_bronze_new_job,
    lk_matches_visitors_to_leads_silver_new_job,
    lk_visitors_bronze_new_job,
    lk_visitors_silver_new_job,
    # --- Other jobs that publish to GeoSpot ---
    effective_supply_monthly_job,
    funnel_ptd_job,
    spot_amenities_job,
    amenity_desc_consistency_job,
    browse_visitors_job,
    gsc_spots_performance_job,
)

LAKEHOUSE_NEW_PIPELINE_JOB_NAMES: tuple[str, ...] = tuple(j.name for j in LAKEHOUSE_NEW_PIPELINE_JOBS)

# ============ Run status sensors (strict job chain: order is mandatory) ============
# For each gold _new: bronze -> silver -> gold. Each step runs only when the previous succeeds (same partition).

# All chains: (trigger_job_name, next_job)
_JOB_CHAINS = (
    # lk_leads
    ("lk_leads_bronze_new", lk_leads_silver_new_job),
    ("lk_leads_silver_new", gold_lk_leads_new_job),
    # lk_projects
    ("lk_projects_bronze_new", lk_projects_silver_new_job),
    ("lk_projects_silver_new", gold_lk_projects_new_job),
    # lk_spots
    ("lk_spots_bronze_new", lk_spots_silver_new_job),
    ("lk_spots_silver_new", gold_lk_spots_new_job),
    # lk_users
    ("lk_users_bronze_new", lk_users_silver_new_job),
    ("lk_users_silver_new", gold_lk_users_new_job),
    # bt_lds_lead_spots
    ("bt_lds_lead_spots_bronze_new", bt_lds_lead_spots_silver_new_job),
    ("bt_lds_lead_spots_silver_new", gold_bt_lds_lead_spots_new_job),
    # bt_conv_conversations
    ("bt_conv_conversations_bronze_new", bt_conv_conversations_silver_new_job),
    ("bt_conv_conversations_silver_new", gold_bt_conv_conversations_new_job),
    # lk_matches_visitors_to_leads
    ("lk_matches_visitors_to_leads_bronze_new", lk_matches_visitors_to_leads_silver_new_job),
    ("lk_matches_visitors_to_leads_silver_new", gold_lk_matches_visitors_to_leads_new_job),
    # lk_visitors
    ("lk_visitors_bronze_new", lk_visitors_silver_new_job),
    ("lk_visitors_silver_new", gold_lk_visitors_new_job),
)

# bt_transactions_new runs when BOTH silver jobs (lk_projects_silver_new and lk_spots_silver_new) have succeeded (same partition).
_BT_TRANSACTIONS_UPSTREAM_SILVER_JOBS = ("lk_projects_silver_new", "lk_spots_silver_new")  # job names

# All jobs that can trigger the next step (bronze + silver of each chain)
_CHAIN_MONITORED_JOBS = [
    lk_leads_bronze_new_job, lk_leads_silver_new_job,
    lk_projects_bronze_new_job, lk_projects_silver_new_job,
    lk_spots_bronze_new_job, lk_spots_silver_new_job,
    lk_users_bronze_new_job, lk_users_silver_new_job,
    bt_lds_lead_spots_bronze_new_job, bt_lds_lead_spots_silver_new_job,
    bt_conv_conversations_bronze_new_job, bt_conv_conversations_silver_new_job,
    lk_matches_visitors_to_leads_bronze_new_job, lk_matches_visitors_to_leads_silver_new_job,
    lk_visitors_bronze_new_job, lk_visitors_silver_new_job,
]
# All jobs that the sensor can launch (silver + gold of each chain + bt_transactions_new)
_CHAIN_REQUEST_JOBS = [
    lk_leads_silver_new_job, gold_lk_leads_new_job,
    lk_projects_silver_new_job, gold_lk_projects_new_job,
    lk_spots_silver_new_job, gold_lk_spots_new_job,
    lk_users_silver_new_job, gold_lk_users_new_job,
    bt_lds_lead_spots_silver_new_job, gold_bt_lds_lead_spots_new_job,
    bt_conv_conversations_silver_new_job, gold_bt_conv_conversations_new_job,
    lk_matches_visitors_to_leads_silver_new_job, gold_lk_matches_visitors_to_leads_new_job,
    lk_visitors_silver_new_job, gold_lk_visitors_new_job,
    gold_bt_transactions_new_job,
]


def _both_silver_jobs_succeeded_for_partition(instance, partition_key: str) -> bool:
    """True if both lk_projects_silver_new and lk_spots_silver_new have a successful run for partition_key."""
    from dagster import RunsFilter
    for job_name in _BT_TRANSACTIONS_UPSTREAM_SILVER_JOBS:
        runs = instance.get_runs(
            filters=RunsFilter(job_name=job_name, statuses=[dg.DagsterRunStatus.SUCCESS]),
            limit=50,
        )
        if not any((r.tags or {}).get("dagster/partition") == partition_key for r in runs):
            return False
    return True


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    name="per_gold_chain_sensor",
    description=(
        "Strict chains: bronze → silver → gold. Same partition except bt_conv (three jobs with no partition day)."
    ),
    monitored_jobs=_CHAIN_MONITORED_JOBS,
    request_jobs=_CHAIN_REQUEST_JOBS,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def per_gold_chain_sensor(context: dg.RunStatusSensorContext):
    job_name = context.dagster_run.job_name
    partition_key = getattr(context, "partition_key", None) or (
        (context.dagster_run.tags or {}).get("dagster/partition")
    )
    for trigger_job_name, next_job in _JOB_CHAINS:
        if job_name == trigger_job_name:
            pk_for_request = partition_key
            if next_job in (
                bt_conv_conversations_silver_new_job,
                gold_bt_conv_conversations_new_job,
            ):
                pk_for_request = None
            rid = (context.dagster_run.run_id or "norun")[:8]
            run_key = (
                f"chain_{next_job.name}_{pk_for_request}"
                if pk_for_request
                else f"chain_{next_job.name}_nopart_{rid}"
            )
            return dg.RunRequest(
                run_key=run_key, partition_key=pk_for_request, job_name=next_job.name
            )
    return dg.SkipReason(f"Job {job_name} is not a chain trigger (bronze or silver step).")


# Jobs gold `_new` a notificar al terminar (editar esta tupla para sumar o quitar).
LAKEHOUSE_GOLD_COMPLETION_NOTIFY_JOBS = (
    gold_lk_leads_new_job,
    gold_lk_projects_new_job,
    gold_lk_spots_new_job,
    gold_lk_users_new_job,
    gold_bt_lds_lead_spots_new_job,
    gold_bt_conv_conversations_new_job,
    gold_lk_visitors_new_job,
    gold_bt_transactions_new_job,
)


def _to_utc(ts: float | datetime | None) -> datetime | None:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    return datetime.fromtimestamp(float(ts), tz=timezone.utc)


def _format_ts_mx(dt: datetime | None) -> str:
    if dt is None:
        return "(no disponible)"
    return dt.astimezone(_TZ_MX).strftime("%Y-%m-%d %H:%M:%S")


def _format_duration_es(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "(no disponible)"
    if seconds < 60:
        return f"{seconds:.1f} segundos"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m} min {s} s" if s else f"{m} min"
    h, m2 = divmod(m, 60)
    parts = [f"{h} h"]
    if m2:
        parts.append(f"{m2} min")
    if s:
        parts.append(f"{s} s")
    return " ".join(parts)


def _run_timing_from_instance(context: dg.RunStatusSensorContext) -> tuple[str, str, str]:
    """(inicio_mx, fin_mx, duración_es) para el run del contexto."""
    run_id = context.dagster_run.run_id
    records = list(
        context.instance.get_run_records(
            filters=dg.RunsFilter(run_ids=[run_id]),
            limit=1,
        )
    )
    if not records:
        return ("(no disponible)", "(no disponible)", "(no disponible)")
    rec = records[0]
    st = _to_utc(rec.start_time)
    et = _to_utc(rec.end_time)
    dur_s: float | None = None
    if st is not None and et is not None:
        dur_s = (et - st).total_seconds()
    return (_format_ts_mx(st), _format_ts_mx(et), _format_duration_es(dur_s))


def _post_completion_google_chat(
    webhook: str, text: str, context: dg.SensorEvaluationContext
) -> bool:
    try:
        resp = requests.post(webhook, json={"text": text}, timeout=15)
        if not resp.ok:
            context.log.warning(
                "Webhook de completion respondió %s: %s", resp.status_code, resp.text[:500]
            )
            return False
        return True
    except requests.RequestException:
        context.log.exception("Fallo al enviar notificación de completion a Google Chat")
        return False


def _gold_completion_success_message(context: dg.RunStatusSensorContext) -> str:
    run = context.dagster_run
    partition = (run.tags or {}).get("dagster/partition")
    start_mx, end_mx, dur = _run_timing_from_instance(context)
    lines = [
        "*Lakehouse · Gold completado*",
        "",
        f"*Job:* `{run.job_name}`",
        f"*Duración:* {dur}",
        f"*Inicio / fin (America/Mexico_City):* `{start_mx}` → `{end_mx}`",
        "",
        "Bronze → silver → gold: cadena completada con éxito para esta corrida.",
        "",
        f"*Run ID:* `{run.run_id}`",
    ]
    run_url = _dagster_run_ui_url(context, run.run_id)
    if run_url:
        lines.append(f"*Abrir en Dagster:* {run_url}")
    if partition:
        lines.append(f"*Partición:* `{partition}`")
    return "\n".join(lines)


def _gold_completion_failure_message(context: dg.RunStatusSensorContext) -> str:
    run = context.dagster_run
    partition = (run.tags or {}).get("dagster/partition")
    start_mx, end_mx, dur = _run_timing_from_instance(context)
    lines = [
        "*Lakehouse · Gold fallido*",
        "",
        f"*Job:* `{run.job_name}`",
        f"*Duración:* {dur}",
        f"*Inicio / fin (America/Mexico_City):* `{start_mx}` → `{end_mx}`",
        "",
        "Revisá el run en Dagster y el espacio de alertas de fallos para el detalle extendido.",
        "",
        f"*Run ID:* `{run.run_id}`",
    ]
    run_url = _dagster_run_ui_url(context, run.run_id)
    if run_url:
        lines.append(f"*Abrir en Dagster:* {run_url}")
    if partition:
        lines.append(f"*Partición:* `{partition}`")
    return "\n".join(lines)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    name="lakehouse_gold_completion_success_chat",
    description=(
        "Aviso a Google Chat cuando un job gold `_new` termina en SUCCESS. "
        f"Webhook por env o SSM (default `{SSM_DATA_LAKE_COMPLETION_WEBHOOK_PARAMETER}`)."
    ),
    monitored_jobs=list(LAKEHOUSE_GOLD_COMPLETION_NOTIFY_JOBS),
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def lakehouse_gold_completion_success_chat(context: dg.RunStatusSensorContext):
    run = context.dagster_run
    webhook = _resolve_lakehouse_completion_webhook_url(context)
    if not webhook:
        context.log.warning(
            "lakehouse_gold_completion_success_chat: sin webhook "
            "(DAGSTER_LAKEHOUSE_COMPLETION_WEBHOOK_URL o SSM %s)",
            SSM_DATA_LAKE_COMPLETION_WEBHOOK_PARAMETER,
        )
        return dg.SkipReason(
            "Webhook no configurada: SSM o DAGSTER_LAKEHOUSE_COMPLETION_WEBHOOK_URL"
        )
    context.log.info(
        "lakehouse_gold_completion_success_chat: enviando Chat para job=%s run_id=%s",
        run.job_name,
        run.run_id,
    )
    text = _gold_completion_success_message(context)
    if not _post_completion_google_chat(webhook, text, context):
        return dg.SkipReason("Error de red al llamar al webhook")
    return dg.SkipReason("Notificado a Google Chat (gold SUCCESS)")


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.FAILURE,
    name="lakehouse_gold_completion_failure_chat",
    description=(
        "Aviso a Google Chat cuando un job gold `_new` termina en FAILURE (mismo webhook que SUCCESS). "
        "El sensor lakehouse_new_pipeline_failure_chat sigue enviando el detalle ampliado al webhook de fallos."
    ),
    monitored_jobs=list(LAKEHOUSE_GOLD_COMPLETION_NOTIFY_JOBS),
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def lakehouse_gold_completion_failure_chat(context: dg.RunStatusSensorContext):
    run = context.dagster_run
    webhook = _resolve_lakehouse_completion_webhook_url(context)
    if not webhook:
        context.log.warning(
            "lakehouse_gold_completion_failure_chat: sin webhook "
            "(DAGSTER_LAKEHOUSE_COMPLETION_WEBHOOK_URL o SSM %s)",
            SSM_DATA_LAKE_COMPLETION_WEBHOOK_PARAMETER,
        )
        return dg.SkipReason(
            "Webhook no configurada: SSM o DAGSTER_LAKEHOUSE_COMPLETION_WEBHOOK_URL"
        )
    context.log.info(
        "lakehouse_gold_completion_failure_chat: enviando Chat para job=%s run_id=%s",
        run.job_name,
        run.run_id,
    )
    text = _gold_completion_failure_message(context)
    if not _post_completion_google_chat(webhook, text, context):
        return dg.SkipReason("Error de red al llamar al webhook")
    return dg.SkipReason("Notificado a Google Chat (gold FAILURE)")


def _leaf_serializable_error(err):
    """Recorre `.cause` hasta el error más interno (p. ej. RuntimeError del user code)."""
    if err is None:
        return None
    cur = err
    while getattr(cur, "cause", None) is not None:
        cur = cur.cause
    return cur


def _compact_error_summary(err, *, max_len: int = 480) -> str:
    """
    Texto corto para Chat: clase + mensaje de la causa hoja (sin stack de Dagster).
    """
    leaf = _leaf_serializable_error(err)
    if leaf is None:
        return "(sin detalle)"
    cls_name = getattr(leaf, "cls_name", None) or "Error"
    msg = (getattr(leaf, "message", None) or "").strip()
    # A veces el mensaje trae "Stack Trace:" embebido sin cadena `cause`; no mandar eso a Chat.
    if "Stack Trace:" in msg:
        msg = msg.split("Stack Trace:", 1)[0].strip()
    msg = " ".join(msg.split())
    if not msg:
        raw = leaf.to_string() if hasattr(leaf, "to_string") else str(leaf)
        if "Stack Trace:" in raw:
            raw = raw.split("Stack Trace:", 1)[0]
        msg = " ".join(raw.split())[:max_len]
    piece = f"{cls_name}: {msg}" if msg else cls_name
    return piece if len(piece) <= max_len else piece[: max_len - 1] + "…"


def _format_step_failure_lines(
    step_events: list,
    *,
    max_steps: int = 5,
) -> list[str]:
    lines: list[str] = []
    for ev in step_events[:max_steps]:
        err = ev.event_specific_data.error
        summary = _compact_error_summary(err)
        lines.append(f"• *`{ev.step_key}`*\n    {summary}")
    return lines


@dg.run_failure_sensor(
    name="lakehouse_new_pipeline_failure_chat",
    description=(
        f"Notifica fallos de los jobs monitoreados ({len(LAKEHOUSE_NEW_PIPELINE_JOBS)} jobs) vía Google Chat. "
        "Mensaje: job, run ID, partición, link a la UI, resumen por step. "
        f"Webhook: SSM `{SSM_DATA_LAKE_ERRORS_WEBHOOK_PARAMETER}` o env DAGSTER_LAKEHOUSE_FAILURE_WEBHOOK_URL."
    ),
    monitored_jobs=list(LAKEHOUSE_NEW_PIPELINE_JOBS),
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def lakehouse_new_pipeline_failure_chat(context: dg.RunFailureSensorContext):
    webhook = _resolve_lakehouse_failure_webhook_url(context)
    if not webhook:
        return dg.SkipReason(
            "Webhook no configurada: SSM o DAGSTER_LAKEHOUSE_FAILURE_WEBHOOK_URL"
        )

    run = context.dagster_run
    partition = (run.tags or {}).get("dagster/partition")

    lines = [
        "*Dagster · Run fallido*",
        "",
        f"*Job:* `{run.job_name}`",
        f"*Run ID:* `{run.run_id}`",
    ]
    if partition:
        lines.append(f"*Partición:* `{partition}`")

    run_url = _dagster_run_ui_url(context, run.run_id)
    if run_url:
        lines.extend(["", f"*Abrir en Dagster:* {run_url}"])

    step_events = list(context.get_step_failure_events())
    step_lines = _format_step_failure_lines(step_events)
    if step_lines:
        lines.extend(["", "*Steps con error:*"])
        lines.extend(step_lines)
        if len(step_events) > len(step_lines):
            lines.append(f"_…y {len(step_events) - len(step_lines)} step(s) más_")
    else:
        fe = context.failure_event
        if fe is not None and getattr(fe, "message", None):
            msg = " ".join((fe.message or "").split())
            if len(msg) > 800:
                msg = msg[:797] + "…"
            lines.extend(["", f"*Error:* {msg}"])

    text = "\n".join(lines)
    try:
        resp = requests.post(webhook, json={"text": text}, timeout=15)
        if not resp.ok:
            context.log.warning(
                "Webhook de fallos respondió %s: %s", resp.status_code, resp.text[:500]
            )
            return dg.SkipReason("Webhook respondió error HTTP")
    except requests.RequestException:
        context.log.exception("Fallo al enviar notificación a Google Chat")
        return dg.SkipReason("Error de red al llamar al webhook")

    return dg.SkipReason("Notificado a Google Chat")


# ============ Schedules ============

def _previous_day_partition(context: dg.ScheduleEvaluationContext) -> str:
    """Partition key for the previous day (data for yesterday is complete today)."""
    from datetime import timedelta
    previous_day = context.scheduled_execution_time - timedelta(days=1)
    return previous_day.strftime("%Y-%m-%d")


def _schedule_calendar_day_partition_mc(context: dg.ScheduleEvaluationContext) -> str:
    """
    Partition key = calendar day of the schedule tick in execution_timezone (America/Mexico_City).

    Parity with conversation_analysis: empty ConversationConfig uses end_date = today (get_dynamic_end_date),
    not yesterday. raw_cb_messages_new uses partition_key as the end of the 3-day window; therefore this
    schedule must request **today's** partition, not _previous_day_partition, to align with conv_schedule_4am
    / conv_schedule_hourly. Manual backfill: pick another date in the UI.
    """
    return context.scheduled_execution_time.strftime("%Y-%m-%d")


def _bt_conv_bronze_run_key(context: dg.ScheduleEvaluationContext, suffix: str) -> str:
    pk = _schedule_calendar_day_partition_mc(context)
    ts = context.scheduled_execution_time.strftime("%Y%m%d_%H%M")
    return f"bt_conv_bronze_{suffix}_{pk}_{ts}"


# Legacy: publish pipeline (lk_leads, lk_projects, bt_lds_lead_spots)
@dg.schedule(
    cron_schedule="0 6 * * *",  # 6:00 AM daily
    job=lakehouse_daily_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def lakehouse_daily_schedule(context: dg.ScheduleEvaluationContext):
    """Runs publish assets and upstream for the previous day."""
    partition_key = _previous_day_partition(context)
    yield dg.RunRequest(run_key=partition_key, partition_key=partition_key)


# Only bronze jobs have schedules; silver and gold are triggered by per_gold_chain_sensor when the previous one finishes.
# Spots bronze 00:10 MX (heaviest). Projects 01:30 leaves ~80 min for the spots chain before the next bronze wave.

@dg.schedule(
    cron_schedule="10 0 * * *",  # 00:10 — spots bronze (heaviest chain: 20 raw + gold)
    job=lk_spots_bronze_new_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def lk_spots_bronze_new_schedule(context: dg.ScheduleEvaluationContext):
    partition_key = _previous_day_partition(context)
    yield dg.RunRequest(run_key=partition_key, partition_key=partition_key)


@dg.schedule(
    cron_schedule="30 1 * * *",  # 01:30 — 30 min for the spots chain to finish
    job=lk_projects_bronze_new_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def lk_projects_bronze_new_schedule(context: dg.ScheduleEvaluationContext):
    partition_key = _previous_day_partition(context)
    yield dg.RunRequest(run_key=partition_key, partition_key=partition_key)


@dg.schedule(
    cron_schedule="45 1 * * *",  # 01:45
    job=lk_leads_bronze_new_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def lk_leads_bronze_new_schedule(context: dg.ScheduleEvaluationContext):
    partition_key = _previous_day_partition(context)
    yield dg.RunRequest(run_key=partition_key, partition_key=partition_key)


@dg.schedule(
    cron_schedule="0 2 * * *",  # 02:00
    job=lk_users_bronze_new_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def lk_users_bronze_new_schedule(context: dg.ScheduleEvaluationContext):
    partition_key = _previous_day_partition(context)
    yield dg.RunRequest(run_key=partition_key, partition_key=partition_key)


@dg.schedule(
    cron_schedule="15 2 * * *",  # 02:15
    job=bt_lds_lead_spots_bronze_new_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def bt_lds_lead_spots_bronze_new_schedule(context: dg.ScheduleEvaluationContext):
    partition_key = _previous_day_partition(context)
    yield dg.RunRequest(run_key=partition_key, partition_key=partition_key)


# BT conv bronze: unpartitioned job; run_key includes MX day for traceability.
# (1) daily 4:00 (2) Mon–Fri 8–18 hourly.
@dg.schedule(
    cron_schedule="0 4 * * *",
    job=bt_conv_conversations_bronze_new_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def bt_conv_conversations_bronze_new_schedule(context: dg.ScheduleEvaluationContext):
    yield dg.RunRequest(run_key=_bt_conv_bronze_run_key(context, "4am"))


@dg.schedule(
    cron_schedule="0 8-18 * * 1-5",  # Mon–Fri hourly 8:00–18:00 (MX); weekends only the 4:00 run
    job=bt_conv_conversations_bronze_new_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def bt_conv_conversations_bronze_new_schedule_business_hours(context: dg.ScheduleEvaluationContext):
    yield dg.RunRequest(run_key=_bt_conv_bronze_run_key(context, "hr"))


@dg.schedule(
    cron_schedule="45 2 * * *",  # 02:45
    job=lk_matches_visitors_to_leads_bronze_new_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def lk_matches_visitors_to_leads_bronze_new_schedule(context: dg.ScheduleEvaluationContext):
    partition_key = _previous_day_partition(context)
    yield dg.RunRequest(run_key=partition_key, partition_key=partition_key)


@dg.schedule(
    cron_schedule="15 3 * * *",  # 03:15 — lk_visitors funnel (bronze → sensor → silver → gold)
    job=lk_visitors_bronze_new_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def lk_visitors_bronze_new_schedule(context: dg.ScheduleEvaluationContext):
    partition_key = _previous_day_partition(context)
    yield dg.RunRequest(run_key=partition_key, partition_key=partition_key)


# bt_transactions_new: last run (03:00). Only runs if both silver jobs (projects + spots) succeeded for that partition.
@dg.schedule(
    cron_schedule="0 3 * * *",  # 03:00 — after all bronze (spots 00:10 … lk_matches 02:45)
    job=gold_bt_transactions_new_job,
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def bt_transactions_new_schedule(context: dg.ScheduleEvaluationContext):
    partition_key = _previous_day_partition(context)
    if not _both_silver_jobs_succeeded_for_partition(context.instance, partition_key):
        context.log.info(
            f"Skipping bt_transactions_new for partition {partition_key}: "
            "both lk_projects_silver_new and lk_spots_silver_new must have succeeded."
        )
        return
    yield dg.RunRequest(
        run_key=partition_key, partition_key=partition_key, job_name=gold_bt_transactions_new_job.name
    )
