"""
Test: VTL con identity resolution — valida que la lógica MAT produce
resultados correctos y reporta estadísticas de canonical_visitor_id.

Ejecuta el pipeline en memoria (sin S3/GeoSpot) y valida las 23 columnas MAT.

Ejecutar con: uv run python lakehouse-sdk/ad_hoc/conversion_visitante_lead_2026-04-08/test_vtl_v1_vs_v2.py
"""

import sys
import pandas as pd

from dagster_pipeline.defs.lk_matches_visitors_to_leads.main import get_data
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.phone_utils import normalize_phone
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.lead_events_processing import process_lead_events
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.match_chats_to_leads import match_chats_to_lead_events
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.get_conversations import (
    fetch_conversations,
    group_conversations,
)
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.crm_offline_mat import (
    MAT_OUTPUT_COLUMNS,
    append_missing_bt_cohorts_as_crm_offline,
    apply_crm_offline_synthetic_to_mat,
)
from dagster_pipeline.defs.lk_matches_visitors_to_leads.main import (
    create_final_matches_all,
    create_final_matches_all_complete,
)
from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source
from dagster_pipeline.defs.lk_matches_visitors_to_leads.utils.identity import build_visitor_identity

from pathlib import Path

QUERIES_DIR = Path(__file__).resolve().parents[2] / "dagster-pipeline/src/dagster_pipeline/defs/lk_matches_visitors_to_leads/queries"
# Fallback if running from dagster-pipeline
if not QUERIES_DIR.exists():
    QUERIES_DIR = Path(__file__).resolve().parents[0] / "../../dagster-pipeline/src/dagster_pipeline/defs/lk_matches_visitors_to_leads/queries"


START_DATE = "2025-01-01"
END_DATE = pd.Timestamp.now().strftime("%Y-%m-%d")


def fetch_bronze():
    """Fetch all bronze data once (shared between v1 and v2)."""
    print("Fetching bronze data...")

    print("  clients (MySQL)...")
    clients_df = get_data("clients")
    clients_df["phone_clean"] = clients_df["phone_number"].apply(
        lambda x: normalize_phone(x) if pd.notnull(x) and str(x).strip() != "" else None
    )

    print("  lead_events (BigQuery)...")
    lead_events = get_data("lead_events_lk")
    if "event_datetime" in lead_events.columns:
        ser = pd.to_datetime(lead_events["event_datetime"])
        if getattr(ser.dtype, "tz", None) is not None:
            lead_events = lead_events.copy()
            lead_events["event_datetime"] = pd.to_datetime(
                ser.apply(lambda t: t.replace(tzinfo=None) if pd.notna(t) and getattr(t, "tzinfo", None) else t)
            )

    print("  bt_lds_leads (GeoSpot)...")
    query_path = Path(__file__).resolve().parents[2] / "dagster-pipeline/src/dagster_pipeline/defs/lk_matches_visitors_to_leads/queries/bt_lds_leads.sql"
    if not query_path.exists():
        # Try relative from dagster-pipeline
        query_path = Path("/home/luis/Spot2/dagster/dagster-pipeline/src/dagster_pipeline/defs/lk_matches_visitors_to_leads/queries/bt_lds_leads.sql")
    query = query_path.read_text(encoding="utf-8")
    df_pl = query_bronze_source(query, source_type="geospot_postgres")
    bt_lds = df_pl.to_pandas() if hasattr(df_pl, "to_pandas") else pd.DataFrame(df_pl)
    bt_lds["lead_cohort_dt"] = pd.to_datetime(bt_lds["lead_cohort_dt"], errors="coerce")
    bt_lds["lead_created_date"] = pd.to_datetime(bt_lds["lead_created_date"], errors="coerce")
    phone_col = "lead_phone_number" if "lead_phone_number" in bt_lds.columns else None
    if phone_col:
        bt_lds["lead_phone_clean"] = bt_lds[phone_col].apply(
            lambda x: normalize_phone(x) if pd.notnull(x) and str(x).strip() != "" else None
        )
    else:
        bt_lds["lead_phone_clean"] = None

    print("  conversations (Chatbot)...")
    chats_raw = fetch_conversations(start_date=START_DATE, end_date=END_DATE)
    chats_grouped = group_conversations(chats_raw, gap_hours=24)
    conversations = chats_grouped.sort_values("conversation_start").drop_duplicates(
        subset=["phone_clean"], keep="first"
    )

    print(f"  Bronze: clients={len(clients_df)}, events={len(lead_events)}, "
          f"bt_lds={len(bt_lds)}, conversations={len(conversations)}")
    return clients_df, lead_events, bt_lds, conversations


def run_silver(lead_events, clients_df, conversations):
    """Run shared silver logic (same for v1 and v2)."""
    lead_events_filtered = lead_events.copy()
    if "phone" in lead_events_filtered.columns:
        lead_events_filtered["phone_clean"] = lead_events_filtered["phone"].apply(
            lambda x: normalize_phone(x) if pd.notnull(x) and str(x).strip() != "" else None
        )
    df_first, _ = process_lead_events(lead_events_filtered, exclude_spot2_emails=False)

    df_first_wpp = df_first[df_first["event_name"] == "clientRequestedWhatsappForm"].copy()
    matched_df, _, _ = match_chats_to_lead_events(
        df_grouped=conversations,
        df_first=df_first_wpp,
        start_date=START_DATE,
        end_date=END_DATE,
        time_buffer_seconds=60,
        lead_events_all=lead_events_filtered,
        try_subsequent_events=True,
        max_time_diff_minutes=1440,
        clients_df=clients_df,
        deduplicate_matches=True,
    )
    return df_first, lead_events_filtered, matched_df


def run_gold(df_first, lead_events_filtered, matched_df, clients_df, bt_lds):
    """Run gold logic (identical between v1 and v2, returns MAT DataFrame)."""
    # Importing _result_to_mat from v1 assets
    from dagster_pipeline.defs.lk_matches_visitors_to_leads.assets import _result_to_mat

    final_matches_all = create_final_matches_all(
        matched=matched_df, df_first=df_first,
        start_date=START_DATE, end_date=END_DATE,
        keep_all_client_matches=True,
    )
    result = create_final_matches_all_complete(
        df_first=df_first, final_matches_all=final_matches_all,
    )

    datetime_cols = ["lead_date", "lead_min_at", "event_datetime", "event_datetime_first", "conversation_start"]
    for col in datetime_cols:
        if col in result.columns:
            result[col] = pd.to_datetime(result[col], errors="coerce")

    result["with_match"] = result["match_source"].apply(
        lambda x: "with_match" if x is not None and x != "no_match" else "without_match"
    )
    result["year_month_first"] = result["event_datetime_first"].dt.to_period("M").astype(str)

    funnel_cols = ["user_pseudo_id", "entry_point", "conversion_point", "traffic_type", "segunda_bifurcacion", "user_sospechoso"]
    cols_from_first = [c for c in funnel_cols if c in df_first.columns]
    first_lk = df_first[cols_from_first].drop_duplicates(subset=["user_pseudo_id"], keep="first")
    result = result.merge(first_lk, on="user_pseudo_id", how="left")
    result["entry_point"] = result["entry_point"].fillna("Other")
    result["conversion_point"] = result["conversion_point"].fillna("Sin Conversión")
    if "traffic_type" in result.columns:
        result["traffic_type"] = result["traffic_type"].fillna("Unassigned")

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
        clients_for_merge = clients_df[["client_id", "created_at"]].copy()
        clients_for_merge = clients_for_merge.rename(columns={"created_at": "lead_created_dt"})
        result_with_lead = result_with_lead.merge(clients_for_merge, on="client_id", how="left")
        result_with_lead["lead_created_dt"] = pd.to_datetime(result_with_lead["lead_created_dt"], errors="coerce")

        bt = bt_lds.copy()
        bt["lead_cohort_dt_norm"] = pd.to_datetime(bt["lead_cohort_dt"], errors="coerce").dt.normalize()
        result_with_lead["lead_fecha_norm"] = pd.to_datetime(result_with_lead["lead_fecha_cohort_dt"], errors="coerce").dt.normalize()
        bt_sub = bt[["lead_id", "lead_cohort_dt_norm", "lead_cohort_type", "lead_sector", "lead_created_date"]].drop_duplicates(
            subset=["lead_id", "lead_cohort_dt_norm"], keep="first"
        )
        result_with_lead = result_with_lead.merge(
            bt_sub.rename(columns={"lead_cohort_type": "_bt_cohort_type", "lead_sector": "_bt_sector", "lead_created_date": "_bt_created"}),
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
        mat_main = _result_to_mat(result_with_lead, matched_df)

    # Reactivated
    reactivados = bt_lds[bt_lds["lead_cohort_type"].astype(str).str.strip().str.lower() == "reactivated"].copy()
    if not reactivados.empty and not lead_events_filtered.empty:
        lead_events_filtered_copy = lead_events_filtered.copy()
        lead_events_filtered_copy["event_date_dt"] = pd.to_datetime(
            lead_events_filtered_copy["event_date"].astype(str), format="%Y%m%d", errors="coerce"
        ).dt.normalize()
        reactivados["lead_cohort_date"] = pd.to_datetime(reactivados["lead_cohort_dt"], errors="coerce").dt.normalize()
        if "lead_email_norm" not in reactivados.columns:
            reactivados["lead_email_norm"] = reactivados["lead_email"].fillna("").astype(str).str.strip().str.lower()

        by_phone_r = lead_events_filtered_copy[lead_events_filtered_copy["phone_clean"].notna()].merge(
            reactivados, left_on=["phone_clean", "event_date_dt"], right_on=["lead_phone_clean", "lead_cohort_date"],
            how="inner", suffixes=("", "_r"),
        )
        le_email = lead_events_filtered_copy[
            lead_events_filtered_copy["email"].fillna("").astype(str).str.strip().str.len() > 0
        ].copy()
        le_email["email_norm"] = le_email["email"].fillna("").astype(str).str.strip().str.lower()
        by_email_r = le_email.merge(
            reactivados, left_on=["email_norm", "event_date_dt"], right_on=["lead_email_norm", "lead_cohort_date"],
            how="inner", suffixes=("", "_r"),
        )
        merged_r = pd.concat([by_phone_r, by_email_r], ignore_index=True)
        if not merged_r.empty:
            merged_r = merged_r.sort_values("event_datetime").drop_duplicates(
                subset=["lead_id", "lead_cohort_dt"], keep="first"
            ).reset_index(drop=True)
            if not mat_main.empty:
                keys_main = set(zip(
                    mat_main["lead_id"].astype(str),
                    pd.to_datetime(mat_main["lead_fecha_cohort_dt"]).dt.normalize().astype(str),
                ))
                merged_r["_key"] = list(zip(
                    merged_r["lead_id"].astype(str),
                    pd.to_datetime(merged_r["lead_cohort_dt"]).dt.normalize().astype(str),
                ))
                merged_r = merged_r[~merged_r["_key"].isin(keys_main)].drop(columns=["_key"], errors="ignore")
            if not merged_r.empty:
                mat_react = _result_to_mat(merged_r, matched_df, from_reactivados=True)
                mat_main = pd.concat([mat_main, mat_react], ignore_index=True)

    mat_result = append_missing_bt_cohorts_as_crm_offline(mat_main, bt_lds)
    if not mat_result.empty and "lead_id" in mat_result.columns:
        mat_result = (
            mat_result.sort_values(["lead_id", "lead_fecha_cohort_dt", "mat_event_datetime"], ascending=[True, True, False])
            .drop_duplicates(subset=["lead_id", "lead_fecha_cohort_dt"], keep="first")
            .reset_index(drop=True)
        )
    return mat_result


ORIGINAL_22_COLUMNS = [c for c in MAT_OUTPUT_COLUMNS if c != "mat_canonical_visitor_id"]


def compare(v1: pd.DataFrame, v2: pd.DataFrame):
    print("\n" + "=" * 70)
    print("COMPARACIÓN VTL: base vs base+identity")
    print("=" * 70)
    all_pass = True

    # 1. Row count
    print(f"\n1. CONTEO DE FILAS")
    print(f"   base: {len(v1)}")
    print(f"   base+identity: {len(v2)}")
    if len(v1) == len(v2):
        print("   PASS — Misma cantidad de filas")
    else:
        print("   FAIL — Cantidad de filas difiere")
        all_pass = False

    # 2. Compare original 22 columns (without canonical_visitor_id)
    print(f"\n2. COMPARACIÓN DE LAS 22 COLUMNAS MAT ORIGINALES")
    key = ["lead_id", "lead_fecha_cohort_dt"]
    v1_sorted = v1[ORIGINAL_22_COLUMNS].sort_values(key).reset_index(drop=True)
    v2_sorted = v2[ORIGINAL_22_COLUMNS].sort_values(key).reset_index(drop=True)

    for col in ORIGINAL_22_COLUMNS:
        c1 = v1_sorted[col].fillna("__NULL__").astype(str)
        c2 = v2_sorted[col].fillna("__NULL__").astype(str)
        diff_count = (c1 != c2).sum()
        if diff_count == 0:
            print(f"   PASS — {col}: 100% idéntico")
        else:
            pct = (1 - diff_count / len(v1_sorted)) * 100
            print(f"   FAIL — {col}: {diff_count} diferencias ({pct:.1f}% coincide)")
            all_pass = False

    # 3. Identity resolution stats
    print(f"\n3. ESTADÍSTICAS DE IDENTITY RESOLUTION")
    if "mat_canonical_visitor_id" in v2.columns:
        resolved = v2[v2["mat_canonical_visitor_id"] != v2["vis_user_pseudo_id"]]
        print(f"   Total filas: {len(v2)}")
        print(f"   Con identidad resuelta (canonical != vis): {len(resolved)} ({len(resolved)/len(v2)*100:.1f}%)")
        print(f"   Canonical IDs únicos: {v2['mat_canonical_visitor_id'].nunique()}")
        print(f"   vis_user_pseudo_id únicos: {v2['vis_user_pseudo_id'].nunique()}")
        reduction = v2['vis_user_pseudo_id'].nunique() - v2['mat_canonical_visitor_id'].nunique()
        print(f"   Reducción de identidades: {reduction} ({reduction/max(v2['vis_user_pseudo_id'].nunique(),1)*100:.1f}%)")
    else:
        print("   SKIP — mat_canonical_visitor_id no encontrado en v2")

    print("\n" + "=" * 70)
    if all_pass:
        print("RESULTADO: TODAS LAS VALIDACIONES PASARON")
    else:
        print("RESULTADO: HAY VALIDACIONES FALLIDAS")
    print("=" * 70)
    return all_pass


def main():
    clients_df, lead_events, bt_lds, conversations = fetch_bronze()

    print("\nRunning silver (shared)...")
    df_first, lead_events_filtered, matched_df = run_silver(lead_events, clients_df, conversations)
    print(f"  df_first: {len(df_first)}, matched: {len(matched_df)}")

    print("\nRunning gold v1...")
    v1_mat = run_gold(df_first, lead_events_filtered, matched_df, clients_df, bt_lds)
    print(f"  v1 MAT: {len(v1_mat)} rows")

    print("\nRunning identity resolution (v2)...")
    identity_df = build_visitor_identity(lead_events, clients_df)
    resolved = identity_df[identity_df["identity_match_method"] != "self"]
    multi = identity_df[identity_df["identity_cluster_size"] > 1]
    print(f"  Identity: {len(identity_df)} users, {len(resolved)} with contact bridge, "
          f"{multi['canonical_visitor_id'].nunique()} multi-user clusters")

    print("\nRunning gold v2 (same as v1 + identity join)...")
    v2_mat = run_gold(df_first, lead_events_filtered, matched_df, clients_df, bt_lds)
    # Drop the placeholder mat_canonical_visitor_id (all None from _result_to_mat) before merge
    v2_mat = v2_mat.drop(columns=["mat_canonical_visitor_id"], errors="ignore")
    identity_map = identity_df[["user_pseudo_id", "canonical_visitor_id"]].drop_duplicates(subset=["user_pseudo_id"], keep="first")
    v2_mat = v2_mat.merge(identity_map, left_on="vis_user_pseudo_id", right_on="user_pseudo_id", how="left").drop(columns=["user_pseudo_id"], errors="ignore")
    v2_mat["mat_canonical_visitor_id"] = v2_mat["canonical_visitor_id"].fillna(v2_mat["vis_user_pseudo_id"])
    v2_mat = v2_mat.drop(columns=["canonical_visitor_id"], errors="ignore")
    print(f"  v2 MAT: {len(v2_mat)} rows")

    passed = compare(v1_mat, v2_mat)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
