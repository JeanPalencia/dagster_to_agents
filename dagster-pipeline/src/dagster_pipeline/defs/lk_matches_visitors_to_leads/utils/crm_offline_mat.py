"""Sintéticos CRM_OFFLINE_* para lk_mat cuando no hay cookie GA4 (Metabase filtra NULL en vis_user_pseudo_id)."""

from __future__ import annotations

import pandas as pd

# Column order for LK_MAT_MATCHES_VISITORS_TO_LEADS (new table, PK: lead_id + lead_fecha_cohort_dt)
MAT_OUTPUT_COLUMNS = [
    "vis_user_pseudo_id",
    "mat_event_datetime",
    "mat_event_name",
    "mat_match_source",
    "lead_id",
    "conv_start_date",
    "mat_channel",
    "mat_source",
    "mat_medium",
    "mat_campaign_name",
    "lead_phone_number",
    "lead_email",
    "lead_sector",
    "lead_fecha_cohort_dt",
    "lead_created_dt",
    "lead_cohort_type",
    "lead_max_type",
    "mat_with_match",
    "mat_page_location",
    "mat_entry_point",
    "mat_conversion_point",
    "mat_traffic_type",
    "mat_canonical_visitor_id",
]

CRM_OFFLINE_PREFIX = "CRM_OFFLINE_"
CRM_OFFLINE_EVENT_NAME = "crm_project_created"
CRM_OFFLINE_CHANNEL = "Offline"
CRM_OFFLINE_ATTRIBUTION = "Unassigned"
CRM_OFFLINE_TRAFFIC_TYPE = "Unassigned"


def format_lead_id_for_crm_offline(lead_id) -> str:
    """Representación estable de lead_id para CONCAT(CRM_OFFLINE_, id)."""
    if pd.isna(lead_id):
        return "unknown"
    try:
        f = float(lead_id)
        if f == int(f):
            return str(int(f))
    except (TypeError, ValueError):
        pass
    s = str(lead_id).strip()
    if s.endswith(".0") and len(s) > 2 and s[:-2].replace("-", "").isdigit():
        return s[:-2]
    return s


def _crm_offline_row_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty or "user_pseudo_id" not in df.columns:
        return pd.Series(False, index=df.index)
    u = df["user_pseudo_id"]
    s = u.astype(str).str.strip()
    return u.isna() | s.isin(["", "nan", "None", "<NA>", "NaT"])


def cohort_key_lead_date(lead_id, cohort_dt) -> tuple[str, str]:
    lid = format_lead_id_for_crm_offline(lead_id)
    d = pd.to_datetime(cohort_dt, errors="coerce")
    if pd.isna(d):
        return (lid, "")
    return (lid, d.normalize().strftime("%Y-%m-%d"))


def apply_crm_offline_synthetic_to_mat(mat: pd.DataFrame, df: pd.DataFrame, lead_id_col: str) -> pd.DataFrame:
    """COALESCE sintético: cookie, evento y atribución para filas sin user_pseudo_id web."""
    if mat.empty or df.empty:
        return mat
    mask = _crm_offline_row_mask(df)
    if not mask.any():
        return mat
    out = mat.copy()
    lids = df.loc[mask, lead_id_col].map(format_lead_id_for_crm_offline)
    out.loc[mask, "vis_user_pseudo_id"] = CRM_OFFLINE_PREFIX + lids.astype(str)
    out.loc[mask, "mat_event_name"] = CRM_OFFLINE_EVENT_NAME
    off_dt = out.loc[mask, "mat_event_datetime"]
    cohort_f = out.loc[mask, "lead_fecha_cohort_dt"]
    out.loc[mask, "mat_event_datetime"] = pd.to_datetime(off_dt, errors="coerce").fillna(
        pd.to_datetime(cohort_f, errors="coerce")
    )
    out.loc[mask, "mat_channel"] = CRM_OFFLINE_CHANNEL
    out.loc[mask, "mat_source"] = CRM_OFFLINE_ATTRIBUTION
    out.loc[mask, "mat_medium"] = CRM_OFFLINE_ATTRIBUTION
    out.loc[mask, "mat_campaign_name"] = CRM_OFFLINE_ATTRIBUTION
    out.loc[mask, "mat_traffic_type"] = CRM_OFFLINE_TRAFFIC_TYPE
    out.loc[mask, "mat_match_source"] = out.loc[mask, "mat_match_source"].fillna("no_match")
    return out


def append_missing_bt_cohorts_as_crm_offline(mat_result: pd.DataFrame, bt_lds: pd.DataFrame) -> pd.DataFrame:
    """Añade filas MAT solo-CRM para cada (lead_id, lead_cohort_dt) en bt_lds que no esté ya en mat_result."""
    if bt_lds is None or bt_lds.empty:
        return mat_result
    bt = bt_lds.drop_duplicates(subset=["lead_id", "lead_cohort_dt"], keep="first").copy()
    if mat_result is None or mat_result.empty:
        existing: set[tuple[str, str]] = set()
    else:
        existing = {
            cohort_key_lead_date(r["lead_id"], r["lead_fecha_cohort_dt"])
            for _, r in mat_result.iterrows()
        }
    new_rows: list[dict] = []
    for _, row in bt.iterrows():
        k = cohort_key_lead_date(row.get("lead_id"), row.get("lead_cohort_dt"))
        if k[1] == "" or k in existing:
            continue
        existing.add(k)
        lid = row.get("lead_id")
        cohort_dt = pd.to_datetime(row.get("lead_cohort_dt"), errors="coerce")
        synthetic_vis = CRM_OFFLINE_PREFIX + format_lead_id_for_crm_offline(lid)
        new_rows.append(
            {
                "vis_user_pseudo_id": synthetic_vis,
                "mat_event_datetime": cohort_dt,
                "mat_event_name": CRM_OFFLINE_EVENT_NAME,
                "mat_match_source": "no_match",
                "lead_id": float(lid) if pd.notna(lid) else None,
                "conv_start_date": pd.NaT,
                "mat_channel": CRM_OFFLINE_CHANNEL,
                "mat_source": CRM_OFFLINE_ATTRIBUTION,
                "mat_medium": CRM_OFFLINE_ATTRIBUTION,
                "mat_campaign_name": CRM_OFFLINE_ATTRIBUTION,
                "lead_phone_number": row.get("lead_phone_number"),
                "lead_email": row.get("lead_email"),
                "lead_sector": row.get("lead_sector"),
                "lead_fecha_cohort_dt": cohort_dt,
                "lead_created_dt": pd.to_datetime(row.get("lead_created_date"), errors="coerce"),
                "lead_cohort_type": row.get("lead_cohort_type"),
                "lead_max_type": row.get("lead_max_type"),
                "mat_with_match": "without_match",
                "mat_page_location": None,
                "mat_entry_point": "Other",
                "mat_conversion_point": "Sin Conversión",
                "mat_traffic_type": CRM_OFFLINE_TRAFFIC_TYPE,
            }
        )
    if not new_rows:
        return mat_result
    add_df = pd.DataFrame(new_rows)
    if mat_result is None or mat_result.empty:
        return add_df[MAT_OUTPUT_COLUMNS]
    return pd.concat([mat_result, add_df], ignore_index=True)[MAT_OUTPUT_COLUMNS]
