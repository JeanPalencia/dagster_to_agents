"""Assets for the LK Visitors to Leads pipeline (lk_matches_visitors_to_leads branch only)."""
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source
from dagster_pipeline.defs.lk_matches_visitors_to_leads.config import VTLConfig
from dagster_pipeline.defs.lk_matches_visitors_to_leads.main import (
    get_data,
    create_final_matches_all,
    create_final_matches_all_complete,
)
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.get_conversations import (
    fetch_conversations,
    group_conversations,
)
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.phone_utils import normalize_phone
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.lead_events_processing import process_lead_events
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.match_chats_to_leads import match_chats_to_lead_events
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.s3_upload import (
    save_to_local,
    upload_dataframe_to_s3,
    trigger_table_replacement,
    resolve_table_and_s3_prefix,
)
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.crm_offline_mat import (
    MAT_OUTPUT_COLUMNS,
    append_missing_bt_cohorts_as_crm_offline,
    apply_crm_offline_synthetic_to_mat,
)
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.identity import build_visitor_identity


def _lk_vtl_emit_error_metadata(
    context: AssetExecutionContext, exc: BaseException
) -> Iterator[dg.MaterializeResult]:
    """Error metadata for lk_vtl (outside pipeline_asset_error_handling)."""
    context.log.exception(
        "lk_vtl asset failed; error metadata was yielded and the exception is re-raised so the step is FAILED"
    )
    meta: dict[str, Any] = {
        "handled": dg.MetadataValue.bool(True),
        "error_type": dg.MetadataValue.text(type(exc).__name__),
        "error_message": dg.MetadataValue.text(str(exc)),
        "asset_key": dg.MetadataValue.text(context.asset_key.to_user_string()),
    }
    pk = getattr(context, "partition_key", None)
    if pk is not None:
        meta["partition_key"] = dg.MetadataValue.text(str(pk))
    yield dg.MaterializeResult(metadata=meta)


@asset
def lk_vtl_clients(context: AssetExecutionContext):
    """MySQL lookup table: used only to match chat conversations (phone/email → client_id). Does not drive lead/cohort counts; those come from bt_lds_leads."""
    try:
        clients_df = get_data("clients")
        clients_df["phone_clean"] = clients_df["phone_number"].apply(
            lambda x: normalize_phone(x) if pd.notnull(x) and str(x).strip() != "" else None
        )
        yield dg.Output(clients_df)
    except Exception as exc:
        yield from _lk_vtl_emit_error_metadata(context, exc)
        raise


# Queries dir for bt_lds_leads (next to this package)
_QUERIES_DIR = Path(__file__).resolve().parent / "queries"


@asset
def lk_vtl_bt_lds_leads(context: AssetExecutionContext):
    """Primary leads and cohorts source for LK_MAT: new and reactivated from GeoSpot (bt_lds_lead_spots_governance + lk_leads_governance). One row per (lead_id, lead_cohort_dt). MAT lead/cohort counts come only from here."""
    try:
        query_path = _QUERIES_DIR / "bt_lds_leads.sql"
        query = query_path.read_text(encoding="utf-8")
        df_pl = query_bronze_source(
            query, source_type="geospot_postgres", context=context
        )
        df = df_pl.to_pandas() if hasattr(df_pl, "to_pandas") else pd.DataFrame(df_pl)
        df["lead_cohort_dt"] = pd.to_datetime(df["lead_cohort_dt"], errors="coerce")
        df["lead_created_date"] = pd.to_datetime(df["lead_created_date"], errors="coerce")
        # Normalize phone for merge with lead_events (source may use lead_full_phone_number)
        phone_col = "lead_phone_number" if "lead_phone_number" in df.columns else None
        if phone_col:
            df["lead_phone_clean"] = df[phone_col].apply(
                lambda x: normalize_phone(x) if pd.notnull(x) and str(x).strip() != "" else None
            )
        else:
            df["lead_phone_clean"] = None
        yield dg.Output(df)
    except Exception as exc:
        yield from _lk_vtl_emit_error_metadata(context, exc)
        raise


@asset
def lk_vtl_lk_lead_events(context: AssetExecutionContext):
    """Fetch lead events from BigQuery: raw_events (event_datetime, event_name) joined to funnel_with_channel (attribution). One row per event. LK pipeline."""
    try:
        df = get_data("lead_events_lk")
        if "event_datetime" in df.columns:
            ser = pd.to_datetime(df["event_datetime"])
            if getattr(ser.dtype, "tz", None) is not None:
                df = df.copy()
                df["event_datetime"] = pd.to_datetime(
                    ser.apply(
                        lambda t: t.replace(tzinfo=None) if pd.notna(t) and getattr(t, "tzinfo", None) else t
                    )
                )
        yield dg.Output(df)
    except Exception as exc:
        yield from _lk_vtl_emit_error_metadata(context, exc)
        raise


@asset
def lk_vtl_conversations(context: AssetExecutionContext, config: VTLConfig):
    """Fetch and group conversations from Chatbot database (pipeline LK)."""
    try:
        chats_raw = fetch_conversations(
            start_date=config.start_date,
            end_date=config.end_date
        )
        chats_grouped = group_conversations(chats_raw, gap_hours=24)
        yield dg.Output(
            chats_grouped.sort_values("conversation_start").drop_duplicates(
                subset=["phone_clean"], keep="first"
            )
        )
    except Exception as exc:
        yield from _lk_vtl_emit_error_metadata(context, exc)
        raise


@asset
def lk_vtl_identity(
    context: AssetExecutionContext,
    lk_vtl_lk_lead_events,
    lk_vtl_clients,
):
    """Build visitor identity map: user_pseudo_id -> canonical_visitor_id via Union-Find on shared phone/email contacts."""
    try:
        identity_df = build_visitor_identity(lk_vtl_lk_lead_events, lk_vtl_clients)
        resolved = identity_df[identity_df["identity_match_method"] != "self"]
        multi = identity_df[identity_df["identity_cluster_size"] > 1]
        context.log.info(
            f"Identity resolution: {len(identity_df)} users, "
            f"{len(resolved)} with contact bridge, "
            f"{multi['canonical_visitor_id'].nunique()} multi-user clusters "
            f"(max size {identity_df['identity_cluster_size'].max()})"
        )
        yield dg.Output(identity_df)
    except Exception as exc:
        yield from _lk_vtl_emit_error_metadata(context, exc)
        raise


@asset
def lk_vtl_lk_processed_lead_events(
    context: AssetExecutionContext, config: VTLConfig, lk_vtl_lk_lead_events
):
    """Process LK lead events: filter Spot2, first events. Pipeline LK."""
    try:
        if config.include_spot2_emails:
            lead_events_filtered = lk_vtl_lk_lead_events.copy()
        else:
            ids_spot2 = lk_vtl_lk_lead_events[
                lk_vtl_lk_lead_events["email"].str.lower().str.contains("@spot2", na=False)
            ]["user_pseudo_id"].unique()
            lead_events_filtered = lk_vtl_lk_lead_events[
                ~lk_vtl_lk_lead_events["user_pseudo_id"].isin(ids_spot2)
            ].copy()

        if "phone" in lead_events_filtered.columns:
            lead_events_filtered["phone_clean"] = lead_events_filtered["phone"].apply(
                lambda x: normalize_phone(x) if pd.notnull(x) and str(x).strip() != "" else None
            )

        df_first, _ = process_lead_events(
            lead_events_filtered, exclude_spot2_emails=not config.include_spot2_emails
        )
        yield dg.Output({"df_first": df_first, "lead_events_filtered": lead_events_filtered})
    except Exception as exc:
        yield from _lk_vtl_emit_error_metadata(context, exc)
        raise


@asset
def lk_vtl_lk_matched(
    context: AssetExecutionContext,
    config: VTLConfig,
    lk_vtl_clients,
    lk_vtl_lk_processed_lead_events,
    lk_vtl_conversations,
):
    """Match conversations to LK lead events. Uses lk_vtl_clients only as lookup (phone/email → client_id) to enrich chat matches; the lead list does not come from clients."""
    try:
        df_first = lk_vtl_lk_processed_lead_events["df_first"]
        lead_events_filtered = lk_vtl_lk_processed_lead_events["lead_events_filtered"]

        df_first_wpp = df_first[df_first["event_name"] == "clientRequestedWhatsappForm"].copy()

        matched_df, _, _ = match_chats_to_lead_events(
            df_grouped=lk_vtl_conversations,
            df_first=df_first_wpp,
            start_date=config.start_date,
            end_date=config.end_date,
            time_buffer_seconds=config.time_buffer_seconds,
            lead_events_all=lead_events_filtered,
            try_subsequent_events=config.try_subsequent_events,
            max_time_diff_minutes=config.max_time_diff_minutes,
            clients_df=lk_vtl_clients,
            deduplicate_matches=config.deduplicate_matches,
        )

        yield dg.Output(matched_df)
    except Exception as exc:
        yield from _lk_vtl_emit_error_metadata(context, exc)
        raise


def _result_to_mat(
    df: pd.DataFrame,
    matched_df: pd.DataFrame,
    from_reactivados: bool = False,
) -> pd.DataFrame:
    """Build a DataFrame with MAT_OUTPUT_COLUMNS from main result or from merged reactivated rows."""
    if df.empty:
        return pd.DataFrame(columns=MAT_OUTPUT_COLUMNS)
    df = df.copy()
    lead_id_col = "lead_id" if "lead_id" in df.columns else "client_id"
    # Unify ID type so chat merge does not fail (int vs float: 123 != 123.0 in join)
    id_series = df[lead_id_col] if lead_id_col in df.columns else df.get("client_id")
    df["_lid"] = pd.to_numeric(id_series, errors="coerce")

    chat_cols = ["user_pseudo_id", "client_id", "conversation_start", "match_source"]
    chat_subset = matched_df[[c for c in chat_cols if c in matched_df.columns]].drop_duplicates(
        subset=["user_pseudo_id", "client_id"], keep="first"
    )
    chat_subset = chat_subset.copy()
    chat_subset["_cid"] = pd.to_numeric(chat_subset["client_id"], errors="coerce")

    df = df.merge(
        chat_subset.rename(columns={"client_id": "_client_id"}),
        left_on=["user_pseudo_id", "_lid"],
        right_on=["user_pseudo_id", "_cid"],
        how="left",
        suffixes=("", "_chat"),
    )
    # Keep match_source and conversation_start from df (result) if merge did not populate them
    if "match_source_chat" in df.columns:
        df["match_source"] = df["match_source"].fillna(df["match_source_chat"])
    if "conversation_start_chat" in df.columns:
        df["conversation_start"] = df["conversation_start"].fillna(df["conversation_start_chat"])
    for c in ["_lid", "_cid", "_client_id", "match_source_chat", "conversation_start_chat"]:
        if c in df.columns:
            df = df.drop(columns=[c], errors="ignore")
    df["mat_with_match"] = df["match_source"].apply(
        lambda x: "with_match" if pd.notna(x) and x != "no_match" else "without_match"
    )
    df["mat_match_source"] = df["match_source"].fillna("no_match")
    lead_id_vals = df[lead_id_col] if lead_id_col in df.columns else df.get("client_id")
    lead_created_dt = df["lead_created_date"] if from_reactivados and "lead_created_date" in df.columns else df.get("lead_created_dt")
    mat = pd.DataFrame({
        "vis_user_pseudo_id": df["user_pseudo_id"],
        "mat_event_datetime": df["event_datetime"],
        "mat_event_name": df["event_name"],
        "mat_match_source": df["mat_match_source"],
        "lead_id": lead_id_vals.astype("float64"),
        "conv_start_date": df["conversation_start"] if "conversation_start" in df.columns else None,
        "mat_channel": df["channel"],
        "mat_source": df["source"],
        "mat_medium": df["medium"],
        "mat_campaign_name": df["campaign_name"],
        "lead_phone_number": df.get("lead_phone_number", df.get("phone_number")),
        "lead_email": df.get("lead_email", df.get("email_clients")),
        "lead_sector": df["lead_sector"] if "lead_sector" in df.columns else pd.Series([None] * len(df), index=df.index),
        "lead_fecha_cohort_dt": df["lead_cohort_dt"] if "lead_cohort_dt" in df.columns else df.get("lead_fecha_cohort_dt"),
        "lead_created_dt": pd.to_datetime(lead_created_dt, errors="coerce") if lead_created_dt is not None else None,
        "lead_cohort_type": df["lead_cohort_type"] if "lead_cohort_type" in df.columns else ("Reactivated" if from_reactivados else "New"),
        "lead_max_type": df.get("lead_max_type", df.get("lead_type")),
        "mat_with_match": df["mat_with_match"],
        "mat_page_location": df["page_location"],
        "mat_entry_point": df["entry_point"].fillna("Other") if "entry_point" in df.columns else "Other",
        "mat_conversion_point": df["conversion_point"].fillna("Sin Conversión") if "conversion_point" in df.columns else "Sin Conversión",
        "mat_traffic_type": df["traffic_type"].fillna("Unassigned") if "traffic_type" in df.columns else "Unassigned",
    })
    for col in MAT_OUTPUT_COLUMNS:
        if col not in mat.columns:
            mat[col] = None
    mat = apply_crm_offline_synthetic_to_mat(mat, df, lead_id_col)
    return mat[MAT_OUTPUT_COLUMNS]


@asset
def lk_vtl_lk_final_output(
    context: AssetExecutionContext,
    config: VTLConfig,
    lk_vtl_clients,
    lk_vtl_bt_lds_leads,
    lk_vtl_lk_matched,
    lk_vtl_lk_processed_lead_events,
    lk_vtl_identity,
):
    """Output for LK_MAT_MATCHES_VISITORS_TO_LEADS. Same core as main (create_final_matches_all + create_final_matches_all_complete); appends reactivated rows from bt_lds_leads."""
    try:
        df_first = lk_vtl_lk_processed_lead_events["df_first"]
        lead_events_filtered = lk_vtl_lk_processed_lead_events["lead_events_filtered"].copy()
    
        # --- 1) Original main logic (lk_matches_visitors_to_leads) ---
        final_matches_all = create_final_matches_all(
            matched=lk_vtl_lk_matched,
            df_first=df_first,
            start_date=config.start_date,
            end_date=config.end_date,
            keep_all_client_matches=config.keep_all_client_matches,
        )
        result = create_final_matches_all_complete(
            df_first=df_first,
            final_matches_all=final_matches_all,
        )
    
        datetime_cols = [
            "lead_date", "lead_min_at", "event_datetime",
            "event_datetime_first", "conversation_start",
        ]
        for col in datetime_cols:
            if col in result.columns:
                result[col] = pd.to_datetime(result[col], errors="coerce")
    
        result["with_match"] = result["match_source"].apply(
            lambda x: "with_match" if x is not None and x != "no_match" else "without_match"
        )
        result["year_month_first"] = (
            result["event_datetime_first"].dt.to_period("M").astype(str)
        )
    
        funnel_cols = ["user_pseudo_id", "entry_point", "conversion_point", "traffic_type", "segunda_bifurcacion", "user_sospechoso"]
        cols_from_first = [c for c in funnel_cols if c in df_first.columns]
        first_lk = df_first[cols_from_first].drop_duplicates(subset=["user_pseudo_id"], keep="first")
        result = result.merge(first_lk, on="user_pseudo_id", how="left")
        result["entry_point"] = result["entry_point"].fillna("Other")
        result["conversion_point"] = result["conversion_point"].fillna("Sin Conversión")
        if "traffic_type" in result.columns:
            result["traffic_type"] = result["traffic_type"].fillna("Unassigned")
    
        # --- 2) Build MAT from result (rows with lead only; lead_fecha_cohort_dt from lead_min_at/lead_date) ---
        result_with_lead = result[result["client_id"].notna()].copy()
        if result_with_lead.empty:
            mat_main = pd.DataFrame(columns=MAT_OUTPUT_COLUMNS)
        else:
            result_with_lead["lead_fecha_cohort_dt"] = (
                result_with_lead["lead_min_at"]
                .fillna(result_with_lead["lead_date"])
                .fillna(result_with_lead["event_datetime_first"])
            )
            result_with_lead = result_with_lead[result_with_lead["lead_fecha_cohort_dt"].notna()]
    
            clients_for_merge = lk_vtl_clients[["client_id", "created_at"]].copy()
            clients_for_merge = clients_for_merge.rename(columns={"created_at": "lead_created_dt"})
            result_with_lead = result_with_lead.merge(
                clients_for_merge, on="client_id", how="left"
            )
            result_with_lead["lead_created_dt"] = pd.to_datetime(
                result_with_lead["lead_created_dt"], errors="coerce"
            )
    
            # Enrich with lead_cohort_type and lead_sector from bt_lds_leads (match lead_id + lead_fecha_cohort_dt)
            bt = lk_vtl_bt_lds_leads.copy()
            bt["lead_cohort_dt_norm"] = pd.to_datetime(bt["lead_cohort_dt"], errors="coerce").dt.normalize()
            result_with_lead["lead_fecha_norm"] = pd.to_datetime(result_with_lead["lead_fecha_cohort_dt"], errors="coerce").dt.normalize()
            bt_sub = bt[["lead_id", "lead_cohort_dt_norm", "lead_cohort_type", "lead_sector", "lead_created_date"]].drop_duplicates(
                subset=["lead_id", "lead_cohort_dt_norm"], keep="first"
            )
            result_with_lead = result_with_lead.merge(
                bt_sub.rename(columns={
                    "lead_cohort_type": "_bt_cohort_type",
                    "lead_sector": "_bt_sector",
                    "lead_created_date": "_bt_created",
                }),
                left_on=["client_id", "lead_fecha_norm"],
                right_on=["lead_id", "lead_cohort_dt_norm"],
                how="left",
            )
            result_with_lead["lead_cohort_type"] = result_with_lead["_bt_cohort_type"].fillna("New")
            result_with_lead["lead_sector"] = result_with_lead["_bt_sector"]
            result_with_lead["lead_created_dt"] = result_with_lead["lead_created_dt"].fillna(
                pd.to_datetime(result_with_lead["_bt_created"], errors="coerce")
            )
            for c in ["lead_cohort_dt_norm", "_bt_cohort_type", "_bt_sector", "_bt_created", "lead_fecha_norm"]:
                if c in result_with_lead.columns:
                    result_with_lead = result_with_lead.drop(columns=[c], errors="ignore")
            if "lead_id" in result_with_lead.columns and result_with_lead["lead_id"].isna().any():
                result_with_lead["lead_id"] = result_with_lead["lead_id"].fillna(result_with_lead["client_id"])
    
            # One row per (lead_id, cohort): same client_id may have multiple leads/properties same day.
            # Prefer rows with a real match (avoid dropping matched rows).
            result_with_lead["_has_match"] = (
                result_with_lead["match_source"].notna()
                & (result_with_lead["match_source"].astype(str).str.strip() != "no_match")
            )
            result_with_lead = (
                result_with_lead.sort_values("_has_match", ascending=False)
                .drop_duplicates(subset=["lead_id", "lead_fecha_cohort_dt"], keep="first")
                .drop(columns=["_has_match"], errors="ignore")
                .reset_index(drop=True)
            )
    
            mat_main = _result_to_mat(result_with_lead, lk_vtl_lk_matched)
    
        # --- 3) Append reactivated from bt_lds_leads (match lead_events by contact + event_date = lead_cohort_dt) ---
        reactivados = lk_vtl_bt_lds_leads[
            lk_vtl_bt_lds_leads["lead_cohort_type"].astype(str).str.strip().str.lower() == "reactivated"
        ].copy()
        if not reactivados.empty and not lead_events_filtered.empty:
            lead_events_filtered = lead_events_filtered.copy()
            lead_events_filtered["event_date_dt"] = pd.to_datetime(
                lead_events_filtered["event_date"].astype(str), format="%Y%m%d", errors="coerce"
            ).dt.normalize()
            reactivados["lead_cohort_date"] = pd.to_datetime(reactivados["lead_cohort_dt"], errors="coerce").dt.normalize()
            if "lead_email_norm" not in reactivados.columns:
                reactivados["lead_email_norm"] = reactivados["lead_email"].fillna("").astype(str).str.strip().str.lower()
    
            by_phone_r = lead_events_filtered[lead_events_filtered["phone_clean"].notna()].merge(
                reactivados,
                left_on=["phone_clean", "event_date_dt"],
                right_on=["lead_phone_clean", "lead_cohort_date"],
                how="inner",
                suffixes=("", "_r"),
            )
            le_email = lead_events_filtered[
                lead_events_filtered["email"].fillna("").astype(str).str.strip().str.len() > 0
            ].copy()
            le_email["email_norm"] = le_email["email"].fillna("").astype(str).str.strip().str.lower()
            by_email_r = le_email.merge(
                reactivados,
                left_on=["email_norm", "event_date_dt"],
                right_on=["lead_email_norm", "lead_cohort_date"],
                how="inner",
                suffixes=("", "_r"),
            )
            merged_r = pd.concat([by_phone_r, by_email_r], ignore_index=True)
            if not merged_r.empty:
                merged_r = merged_r.sort_values("event_datetime").drop_duplicates(
                    subset=["lead_id", "lead_cohort_dt"], keep="first"
                ).reset_index(drop=True)
                if not mat_main.empty and "lead_id" in mat_main.columns and "lead_fecha_cohort_dt" in mat_main.columns:
                    keys_main = set(
                        zip(
                            mat_main["lead_id"].astype(str),
                            pd.to_datetime(mat_main["lead_fecha_cohort_dt"]).dt.normalize().astype(str),
                        )
                    )
                    merged_r["_key"] = list(
                        zip(
                            merged_r["lead_id"].astype(str),
                            pd.to_datetime(merged_r["lead_cohort_dt"]).dt.normalize().astype(str),
                        )
                    )
                    merged_r = merged_r[~merged_r["_key"].isin(keys_main)].drop(columns=["_key"], errors="ignore")
                if not merged_r.empty:
                    mat_react = _result_to_mat(merged_r, lk_vtl_lk_matched, from_reactivados=True)
                    mat_main = pd.concat([mat_main, mat_react], ignore_index=True)
    
        mat_result = append_missing_bt_cohorts_as_crm_offline(mat_main, lk_vtl_bt_lds_leads)
    
        if not mat_result.empty and "lead_id" in mat_result.columns and "lead_fecha_cohort_dt" in mat_result.columns:
            mat_result = (
                mat_result.sort_values(
                    ["lead_id", "lead_fecha_cohort_dt", "mat_event_datetime"],
                    ascending=[True, True, False],
                )
                .drop_duplicates(subset=["lead_id", "lead_fecha_cohort_dt"], keep="first")
                .reset_index(drop=True)
            )
    
        # Identity resolution: JOIN canonical_visitor_id
        # Drop placeholder column from _result_to_mat before merge to avoid suffix conflict
        mat_result = mat_result.drop(columns=["mat_canonical_visitor_id"], errors="ignore")
        identity_map = lk_vtl_identity[["user_pseudo_id", "canonical_visitor_id"]].drop_duplicates(
            subset=["user_pseudo_id"], keep="first"
        )
        mat_result = mat_result.merge(
            identity_map,
            left_on="vis_user_pseudo_id",
            right_on="user_pseudo_id",
            how="left",
        ).drop(columns=["user_pseudo_id"], errors="ignore")
        mat_result["mat_canonical_visitor_id"] = mat_result["canonical_visitor_id"].fillna(
            mat_result["vis_user_pseudo_id"]
        )
        mat_result = mat_result.drop(columns=["canonical_visitor_id"], errors="ignore")

        resolved_count = (mat_result["mat_canonical_visitor_id"] != mat_result["vis_user_pseudo_id"]).sum()
        context.log.info(
            f"Identity resolution: {resolved_count} rows with mat_canonical_visitor_id != vis_user_pseudo_id"
        )

        # Single output: new lk_mat_matches_visitors_to_leads (MAT) table
        prefix = (getattr(config, "table_name_prefix", None) or "").strip()
        mat_table, mat_s3_key, mat_backup = resolve_table_and_s3_prefix(prefix)
    
        if config.save_local:
            filepath_mat = save_to_local(
                mat_result, config.output_dir, config.output_format,
                table_basename=mat_table,
            )
            context.log.info(f"Saved MAT to local: {filepath_mat}")
    
        if config.upload_to_s3:
            context.log.info(f"Uploading {len(mat_result)} rows to S3 (MAT table {mat_table})...")
            main_mat, backup_mat = upload_dataframe_to_s3(
                mat_result, s3_key=mat_s3_key, create_backup=True, backup_prefix=mat_backup
            )
            context.log.info(f"Uploaded MAT to {main_mat}")
            if backup_mat:
                context.log.info(f"Backup MAT at {backup_mat}")
            context.log.info(f"Triggering table replacement ({mat_table})...")
            api_mat = trigger_table_replacement(s3_key=mat_s3_key, table_name=mat_table)
            context.log.info(f"Table replacement MAT: {api_mat}")
    
        yield dg.Output(mat_result)
    except Exception as exc:
        yield from _lk_vtl_emit_error_metadata(context, exc)
        raise
