"""Assets for the Visitors to Leads (VTL) pipeline."""
from dagster import asset, AssetExecutionContext
import pandas as pd

from dagster_pipeline.defs.visitors_to_leads.config import VTLConfig
from dagster_pipeline.defs.visitors_to_leads.main import (
    get_data,
    create_final_matches_all,
    create_final_matches_all_complete,
)
from dagster_pipeline.defs.visitors_to_leads.utils.get_conversations import (
    fetch_conversations,
    group_conversations,
)
from dagster_pipeline.defs.visitors_to_leads.utils.phone_utils import normalize_phone
from dagster_pipeline.defs.visitors_to_leads.utils.lead_events_processing import process_lead_events
from dagster_pipeline.defs.visitors_to_leads.utils.match_chats_to_leads import match_chats_to_lead_events
from dagster_pipeline.defs.visitors_to_leads.utils.s3_upload import (
    save_to_local,
    upload_dataframe_to_s3,
    trigger_table_replacement,
)


@asset
def vtl_clients():
    """Fetch clients data from MySQL production database."""
    clients_df = get_data("clients")
    clients_df["phone_clean"] = clients_df["phone_number"].apply(
        lambda x: normalize_phone(x) if pd.notnull(x) and str(x).strip() != "" else None
    )
    return clients_df


@asset
def vtl_lead_events():
    """Fetch lead events from BigQuery."""
    return get_data("lead_events")


@asset
def vtl_conversations(config: VTLConfig):
    """Fetch and group conversations from Chatbot database."""
    chats_raw = fetch_conversations(
        start_date=config.start_date,
        end_date=config.end_date
    )
    chats_grouped = group_conversations(chats_raw, gap_hours=24)
    return chats_grouped.sort_values("conversation_start").drop_duplicates(
        subset=["phone_clean"], keep="first"
    )


@asset
def vtl_processed_lead_events(config: VTLConfig, vtl_lead_events):
    """Process lead events: optionally filter out Spot2 emails and extract first events."""
    if config.include_spot2_emails:
        lead_events_filtered = vtl_lead_events.copy()
    else:
        ids_spot2 = vtl_lead_events[
            vtl_lead_events["email"].str.lower().str.contains("@spot2", na=False)
        ]["user_pseudo_id"].unique()
        lead_events_filtered = vtl_lead_events[
            ~vtl_lead_events["user_pseudo_id"].isin(ids_spot2)
        ].copy()

    # Add phone_clean before processing
    if "phone" in lead_events_filtered.columns:
        lead_events_filtered["phone_clean"] = lead_events_filtered["phone"].apply(
            lambda x: normalize_phone(x) if pd.notnull(x) and str(x).strip() != "" else None
        )

    df_first, _ = process_lead_events(
        lead_events_filtered, exclude_spot2_emails=not config.include_spot2_emails
    )
    return {"df_first": df_first, "lead_events_filtered": lead_events_filtered}


@asset
def vtl_matched(
    config: VTLConfig,
    vtl_clients,
    vtl_processed_lead_events,
    vtl_conversations,
):
    """Match conversations to lead events."""
    df_first = vtl_processed_lead_events["df_first"]
    lead_events_filtered = vtl_processed_lead_events["lead_events_filtered"]

    df_first_wpp = df_first[df_first["event_name"] == "clientRequestedWhatsappForm"].copy()

    matched_df, _, _ = match_chats_to_lead_events(
        df_grouped=vtl_conversations,
        df_first=df_first_wpp,
        start_date=config.start_date,
        end_date=config.end_date,
        time_buffer_seconds=config.time_buffer_seconds,
        lead_events_all=lead_events_filtered,
        try_subsequent_events=config.try_subsequent_events,
        max_time_diff_minutes=config.max_time_diff_minutes,
        clients_df=vtl_clients,
        deduplicate_matches=config.deduplicate_matches,
    )

    return matched_df


@asset
def vtl_final_output(
    context: AssetExecutionContext,
    config: VTLConfig,
    vtl_matched,
    vtl_processed_lead_events,
):
    """Create final output, upload to S3, and trigger table replacement."""
    df_first = vtl_processed_lead_events["df_first"]

    final_matches_all = create_final_matches_all(
        matched=vtl_matched,
        df_first=df_first,
        start_date=config.start_date,
        end_date=config.end_date,
        keep_all_client_matches=config.keep_all_client_matches,
    )

    result = create_final_matches_all_complete(
        df_first=df_first,
        final_matches_all=final_matches_all,
    )

    # Convert datetime columns to consistent types
    datetime_cols = [
        "lead_date", "lead_min_at", "event_datetime", 
        "event_datetime_first", "conversation_start"
    ]
    for col in datetime_cols:
        if col in result.columns:
            result[col] = pd.to_datetime(result[col], errors="coerce")

    # Add derived columns required for the target table
    result["with_match"] = result["match_source"].apply(
        lambda x: "with_match" if x is not None and x != "no_match" else "without_match"
    )
    result["year_month_first"] = (
        result["event_datetime_first"].dt.to_period("M").astype(str)
    )

    # Save locally (optional)
    if config.save_local:
        filepath = save_to_local(result, config.output_dir, config.output_format)
        context.log.info(f"Saved to local: {filepath}")

    # Upload to S3 and trigger replacement (optional)
    if config.upload_to_s3:
        context.log.info(f"Uploading {len(result)} rows to S3...")
        main_uri, backup_uri = upload_dataframe_to_s3(result)
        context.log.info(f"Uploaded to {main_uri}")
        if backup_uri:
            context.log.info(f"Backup created at {backup_uri}")
        context.log.info("Triggering table replacement...")
        api_result = trigger_table_replacement()
        context.log.info(f"Table replacement result: {api_result}")

    return result
