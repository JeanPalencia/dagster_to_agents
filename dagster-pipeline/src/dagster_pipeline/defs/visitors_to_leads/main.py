"""
Main pipeline to match chats to lead events and create final_matches_all_complete.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Optional

from dagster_pipeline.defs.visitors_to_leads.utils.database import get_engine, query_bigquery
from dagster_pipeline.defs.visitors_to_leads.utils.get_conversations import (
    fetch_conversations,
    group_conversations,
)
from dagster_pipeline.defs.visitors_to_leads.utils.phone_utils import normalize_phone
from dagster_pipeline.defs.visitors_to_leads.utils.lead_events_processing import process_lead_events
from dagster_pipeline.defs.visitors_to_leads.utils.match_chats_to_leads import match_chats_to_lead_events
from dagster_pipeline.defs.visitors_to_leads.utils.dataframe_utils import safe_concat_multiple


# Path to queries directory (relative to this file)
QUERIES_DIR = Path(__file__).parent / "queries"


def get_data(kind: str) -> pd.DataFrame:
    """Load 'clients' or 'lead_events' from SQL query."""
    match kind:
        case "clients":
            query_path = QUERIES_DIR / "clients.sql"
            with open(query_path, "r", encoding="utf-8") as f:
                query = f.read()
                clients = pd.read_sql_query(query, get_engine("prod"))
                clients["all_3_null"] = (
                    clients.hs_utm_campaign.isna()
                    & clients.hs_utm_source.isna()
                    & clients.hs_utm_medium.isna()
                )
            return clients
        case "lead_events":
            query_path = QUERIES_DIR / "lead_events.sql"
            with open(query_path, "r", encoding="utf-8") as f:
                query = f.read()
                return query_bigquery(query)
        case _:
            raise ValueError(f"Unknown kind: {kind}")


def get_lead_date(row):
    """Determine the date when the user became a lead based on match_source."""
    lead_min_at = row.get("lead_min_at")
    if pd.notna(lead_min_at):
        return lead_min_at
    
    return row.get("event_datetime")


def create_final_matches_all(
    matched: pd.DataFrame,
    df_first: pd.DataFrame,
    start_date: str,
    end_date: str,
    keep_all_client_matches: bool = False,
) -> pd.DataFrame:
    """Create final_matches_all by combining eventos_l1, matches_chat, matches_sin_chat, and wpp_desktop_no_match."""
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)  # End of day (23:59:59)

    df_first_filtered = df_first[
        (df_first["event_datetime"] >= start_dt) & (df_first["event_datetime"] <= end_dt)
    ].copy()

    eventos_l1_cols = ["user_pseudo_id", "event_name", "channel", "event_datetime"]
    if "campaign_name" in df_first_filtered.columns:
        eventos_l1_cols.append("campaign_name")
    
    eventos_l1 = (
        df_first_filtered[
            df_first_filtered["event_name"]
            .str.lower()
            .str.startswith("clientsubmitformblog")
        ]
        .sort_values("event_datetime")
        .drop_duplicates(subset=["user_pseudo_id"], keep="first")[eventos_l1_cols]
    )
    eventos_l1["match_source"] = "l1_no_required_match"

    matched_filtered = matched[
        (
            (
                matched["conversation_start"].notna()
                & (matched["conversation_start"] >= start_dt)
                & (matched["conversation_start"] <= end_dt)
            )
            | (
                matched["conversation_start"].isna()
                & matched["event_datetime"].notna()
                & (matched["event_datetime"] >= start_dt)
                & (matched["event_datetime"] <= end_dt)
            )
        )
    ].copy()

    all_desired_columns = [
        "user_pseudo_id", "event_name", "channel", "match_source", "client_id",
        "event_datetime", "conversation_start", "source", "medium", "campaign_name",
        "phone_number", "email_clients", "lead_min_at", "lead_min_type", "lead_type",
        "lead_date", "page_location",
    ]

    matches_chat = matched_filtered[
        (matched_filtered["conversation_start"].notna())
        & (matched_filtered["user_pseudo_id"].notna())
    ]
    matches_sin_chat = matched_filtered[
        (matched_filtered["conversation_start"].isna())
        & (matched_filtered["event_datetime"].notna())
        & (matched_filtered["user_pseudo_id"].notna())
    ]

    matches_chat = matches_chat[
        [col for col in all_desired_columns if col in matches_chat.columns]
    ]
    matches_sin_chat = matches_sin_chat[
        [col for col in all_desired_columns if col in matches_sin_chat.columns]
    ]

    for col in all_desired_columns:
        if col not in eventos_l1.columns:
            eventos_l1[col] = None

    eventos_l1 = eventos_l1.reindex(columns=all_desired_columns)
    matches_chat = matches_chat.reindex(columns=all_desired_columns)
    matches_sin_chat = matches_sin_chat.reindex(columns=all_desired_columns)

    pre_matches = safe_concat_multiple(eventos_l1, matches_chat, matches_sin_chat)

    if not pre_matches.empty and pre_matches["user_pseudo_id"].notna().any():
        user_match_sources = (
            pre_matches[pre_matches["user_pseudo_id"].notna()]
            .groupby("user_pseudo_id")["match_source"]
            .agg(lambda x: set(x))
            .reset_index()
        )
        conflicting_user_ids = user_match_sources[
            user_match_sources["match_source"].apply(
                lambda x: "l1_no_required_match" in x and len(x) > 1
            )
        ]["user_pseudo_id"].unique()
    else:
        conflicting_user_ids = []

    pre_matches_filtered = pre_matches[
        ~(
            (pre_matches["user_pseudo_id"].isin(conflicting_user_ids))
            & (pre_matches["match_source"] == "l1_no_required_match")
        )
    ].copy()

    match_source_priority = [
        "first_event", "subsequent_event", "via_client_both", "via_client_email",
        "via_client_phone", "direct_client_match_email", "direct_client_match_phone",
    ]

    priority_dict = {source: idx for idx, source in enumerate(match_source_priority)}
    pre_matches_filtered["_priority"] = pre_matches_filtered["match_source"].map(
        lambda x: priority_dict.get(x, 999)
    )

    dedup_subset = ["user_pseudo_id", "client_id"] if keep_all_client_matches else ["user_pseudo_id"]
    pre_matches_filtered = (
        pre_matches_filtered.sort_values(["user_pseudo_id", "_priority"])
        .drop_duplicates(subset=dedup_subset, keep="first")
        .drop(columns=["_priority"])
        .reset_index(drop=True)
    )

    pre_matches_user_ids = set(pre_matches["user_pseudo_id"].dropna().unique())

    wpp_desktop_no_match = df_first_filtered[
        (df_first_filtered["event_name"] == "clientRequestedWhatsappForm")
        & (df_first_filtered["device_category"] == "desktop")
        & (~df_first_filtered["user_pseudo_id"].isin(pre_matches_user_ids))
    ][["user_pseudo_id", "event_name", "channel"]].copy()

    wpp_desktop_no_match["match_source"] = "wpp_desktop_without_match"

    if not matched.empty and "client_id" in matched.columns:
        client_id_lookup = (
            matched[["user_pseudo_id", "client_id"]]
            .drop_duplicates(subset=["user_pseudo_id"])
            .set_index("user_pseudo_id")["client_id"]
        )
        wpp_desktop_no_match["client_id"] = wpp_desktop_no_match["user_pseudo_id"].map(
            client_id_lookup
        )
    else:
        wpp_desktop_no_match["client_id"] = None

    wpp_cols_to_merge = ["user_pseudo_id", "event_datetime"]
    for col in ["source", "medium", "campaign_name"]:
        if col in df_first_filtered.columns:
            wpp_cols_to_merge.append(col)

    wpp_desktop_no_match = wpp_desktop_no_match.merge(
        df_first_filtered[wpp_cols_to_merge], on="user_pseudo_id", how="left"
    )

    for col in all_desired_columns:
        if col not in wpp_desktop_no_match.columns:
            wpp_desktop_no_match[col] = None

    wpp_desktop_no_match = wpp_desktop_no_match.reindex(columns=all_desired_columns)

    final_matches_all = safe_concat_multiple(pre_matches_filtered, wpp_desktop_no_match)

    final_matches_all["lead_date"] = final_matches_all.apply(get_lead_date, axis=1)

    return final_matches_all


def create_final_matches_all_complete(
    df_first: pd.DataFrame,
    final_matches_all: pd.DataFrame
) -> pd.DataFrame:
    """Create final_matches_all_complete by merging df_first (all users) with final_matches_all."""
    df_first_base = df_first[["user_pseudo_id", "event_datetime", "event_name", "channel"]].copy()
    df_first_base = df_first_base.rename(
        columns={
            "event_datetime": "event_datetime_first",
            "event_name": "event_name_first",
            "channel": "channel_first"
        }
    )

    columns_from_final = [
        "match_source", "client_id", "conversation_start", "event_datetime",
        "event_name", "channel", "source", "medium", "campaign_name", "phone_number",
        "email_clients", "lead_min_at", "lead_min_type", "lead_type", "lead_date",
        "page_location",
    ]

    available_columns = [
        col for col in columns_from_final if col in final_matches_all.columns
    ]

    final_matches_all_complete = df_first_base.merge(
        final_matches_all[["user_pseudo_id"] + available_columns],
        on="user_pseudo_id",
        how="left",
    )

    final_matches_all_complete["match_source"] = final_matches_all_complete[
        "match_source"
    ].fillna("no_match")

    return final_matches_all_complete


def run_pipeline(
    start_date: str,
    end_date: str,
    time_buffer_seconds: int = 60,
    try_subsequent_events: bool = True,
    max_time_diff_minutes: int = 1440,
    deduplicate_matches: bool = True,
    keep_all_client_matches: bool = False,
    include_spot2_emails: bool = True,
) -> Dict[str, pd.DataFrame]:
    """Main pipeline function that orchestrates the entire matching process."""
    print("Loading data from queries...")
    clients = get_data("clients")
    clients["phone_clean"] = clients["phone_number"].apply(
        lambda x: normalize_phone(x) if pd.notnull(x) and str(x).strip() != "" else None
    )

    lead_events = get_data("lead_events")

    print("Processing lead events...")
    if not include_spot2_emails:
        ids_spot2_mail = lead_events["email"].str.lower().str.contains("@spot2", na=False)
        ids_spot2_mail = lead_events.loc[ids_spot2_mail, "user_pseudo_id"].unique()
        lead_events_filtered = lead_events[
            ~lead_events["user_pseudo_id"].isin(ids_spot2_mail)
        ].copy()
    else:
        lead_events_filtered = lead_events.copy()

    df_first, result = process_lead_events(
        lead_events_filtered, exclude_spot2_emails=not include_spot2_emails
    )

    print("Loading and processing chats...")
    chats_crudos = fetch_conversations()
    chats_agrupados = group_conversations(chats_crudos, gap_hours=24)
    first_global_chat = chats_agrupados.sort_values(
        "conversation_start", ascending=True
    ).drop_duplicates(subset=["phone_clean"], keep="first")

    df_first_wpp_a_mirar = df_first[
        df_first["event_name"].isin(["clientRequestedWhatsappForm"])
    ].copy()

    print("Executing matching...")
    matched, chats_prepared, lead_events_prepared = match_chats_to_lead_events(
        df_grouped=first_global_chat,
        df_first=df_first_wpp_a_mirar,
        start_date=start_date,
        end_date=end_date,
        time_buffer_seconds=time_buffer_seconds,
        lead_events_all=lead_events_filtered,
        try_subsequent_events=try_subsequent_events,
        max_time_diff_minutes=max_time_diff_minutes,
        clients_df=clients,
        deduplicate_matches=deduplicate_matches
    )

    print("Creating final_matches_all...")
    final_matches_all = create_final_matches_all(
        matched=matched,
        df_first=df_first,
        start_date=start_date,
        end_date=end_date,
        keep_all_client_matches=keep_all_client_matches,
    )

    print("Creating final_matches_all_complete...")
    final_matches_all_complete = create_final_matches_all_complete(
        df_first=df_first,
        final_matches_all=final_matches_all
    )

    print("Pipeline completed successfully!")

    return {
        "matched": matched,
        "final_matches_all_complete": final_matches_all_complete
    }

