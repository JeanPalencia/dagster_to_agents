"""
segment: conv_variables derived from project attributes (e.g. seniority_level from lk_projects).

Seniority (spot_1): SS, Top SS, Jr, Mid, Sr based on sector, modality, area and prices.
"""

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


def assign_seniority(
    sector: Optional[str],
    modality: Optional[str],
    area_sqm: Optional[float] = None,
    price_rent: Optional[float] = None,
    price_sale: Optional[float] = None,
) -> Optional[str]:
    """Assign seniority tier from sector, modality and price/area."""
    sector = str(sector).strip().lower() if pd.notna(sector) else ""
    modality = str(modality).strip().lower() if pd.notna(modality) else ""
    area_sqm = pd.to_numeric(area_sqm, errors="coerce")
    price_rent = pd.to_numeric(price_rent, errors="coerce")
    price_sale = pd.to_numeric(price_sale, errors="coerce")
    is_sale = modality in ("sale", "venta")
    if is_sale:
        p = price_sale
        if pd.isna(p) or p < 2_500_000:
            return "SS"
        if p < 5_000_000:
            return "Top SS"
        if p < 15_000_000:
            return "Jr"
        if p < 30_000_000:
            return "Mid"
        return "Sr"
    if modality not in ("rent", "renta"):
        return None
    if "industrial" in sector:
        a = area_sqm
        if pd.isna(a) or a < 400:
            return "SS"
        if a < 800:
            return "Top SS"
        if a < 2500:
            return "Jr"
        if a < 5000:
            return "Mid"
        return "Sr"
    if "office" in sector:
        a = area_sqm
        if pd.isna(a) or a < 50:
            return "SS"
        if a < 100:
            return "Top SS"
        if a < 300:
            return "Jr"
        if a < 500:
            return "Mid"
        return "Sr"
    if "retail" in sector:
        p = price_rent
        if pd.isna(p) or p < 30_000:
            return "SS"
        if p < 40_000:
            return "Top SS"
        if p < 80_000:
            return "Jr"
        if p < 150_000:
            return "Mid"
        return "Sr"
    if "land" in sector:
        p = price_rent
        if pd.isna(p) or p < 50_000:
            return "SS"
        if p < 100_000:
            return "Top SS"
        if p < 300_000:
            return "Jr"
        if p < 600_000:
            return "Mid"
        return "Sr"
    return None


def _single_or_mean(a: Any, b: Any) -> float:
    """
    Use the single non-null value if only min or max is present, or the mean if both.
    Spot2 often has only project_min_* or only project_max_* (sale, rent, square_space).
    """
    a, b = pd.to_numeric(a, errors="coerce"), pd.to_numeric(b, errors="coerce")
    if pd.isna(a) and pd.isna(b):
        return np.nan
    return float(np.nanmean([a, b]))


def _ticket_rent(row: pd.Series) -> float:
    """Precio renta: el que exista (min o max) o promedio si hay ambos."""
    return _single_or_mean(
        row.get("project_min_rent_price"), row.get("project_max_rent_price")
    )


def _ticket_sale(row: pd.Series) -> float:
    """Precio venta: el que exista (min o max) o promedio si hay ambos."""
    return _single_or_mean(
        row.get("project_min_sale_price"), row.get("project_max_sale_price")
    )


def _project_area_sqm(row: pd.Series) -> float:
    """Área m²: el que exista (min o max) o promedio si hay ambos."""
    return _single_or_mean(
        row.get("project_min_square_space"), row.get("project_max_square_space")
    )


def seniority_from_project_row(row: pd.Series) -> Optional[str]:
    """
    Compute seniority from a single lk_projects (or project) row.
    Expects columns: project_sector or spot_sector, project_min_rent_price, project_max_rent_price,
    project_min_sale_price, project_max_sale_price, project_min_square_space, project_max_square_space.
    """
    sector = row.get("project_sector") or row.get("spot_sector")
    if pd.isna(sector) or sector == "":
        return None
    ticket_r = _ticket_rent(row)
    ticket_s = _ticket_sale(row)
    area = _project_area_sqm(row)
    # Prefer rent if any rent price is present (even 0); else sale if present
    if pd.notna(ticket_r):
        modality = "rent"
        price_rent, price_sale = ticket_r, None
    elif pd.notna(ticket_s):
        modality = "sale"
        price_rent, price_sale = None, ticket_s
    else:
        return None
    return assign_seniority(
        sector, modality, area_sqm=area, price_rent=price_rent, price_sale=price_sale
    )


def _normalize_lead_id(value: Any) -> Optional[str]:
    """Compare lead_id as string to avoid int/str mismatch between conv and lk_projects."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value).strip()


def build_conv_seniority_map(
    conv_with_projects: pd.DataFrame,
    lk_projects: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a DataFrame with conv_id and seniority_level (segment) by linking
    conversations -> lk_projects by lead_id (no MySQL project_requirements).

    Per lead we keep a single project: the most advanced by funnel dates
    (project_created_at -> visit_created_date -> visit_confirmed_at -> visit_realized_at -> loi_date).
    Highest stage with non-null date wins. If tied, the most recent (project_updated_at DESC).
    lead_id is compared as string to avoid int/str mismatch.

    Returns:
        DataFrame with columns: conversation_id (conv_id), seniority_level.
    """
    df_conv = conv_with_projects.copy()
    df_lk = lk_projects.copy()

    if df_lk.empty:
        return pd.DataFrame({
            "conversation_id": df_conv["conversation_id"],
            "seniority_level": [None] * len(df_conv),
        })

    # Normalize lead_id in lk_projects for matching
    df_lk["_lead_id_norm"] = df_lk["lead_id"].apply(_normalize_lead_id)
    df_lk = df_lk[df_lk["_lead_id_norm"].notna()].copy()

    # Avance por funnel: 0=created, 1=visit_created, 2=visit_confirmed, 3=visit_realized, 4=loi_date
    def _funnel_rank(row: pd.Series) -> int:
        if pd.notna(row.get("project_funnel_loi_date")):
            return 4
        if pd.notna(row.get("project_funnel_visit_realized_at")):
            return 3
        if pd.notna(row.get("project_funnel_visit_confirmed_at")):
            return 2
        if pd.notna(row.get("project_funnel_visit_created_date")):
            return 1
        if pd.notna(row.get("project_created_at")):
            return 0
        return -1

    df_lk["_funnel_rank"] = df_lk.apply(_funnel_rank, axis=1)
    df_lk["project_updated_at"] = pd.to_datetime(df_lk["project_updated_at"], errors="coerce") if "project_updated_at" in df_lk.columns else pd.NaT

    # Orden: más avanzado primero (_funnel_rank desc), luego más reciente (updated_at desc). Nulls last.
    df_lk = df_lk.sort_values(
        by=["_funnel_rank", "project_updated_at"],
        ascending=[False, False],
        na_position="last",
    )
    # Una fila por lead: la primera tras el orden (más avanzada; si empate, más reciente)
    df_lk_by_lead = df_lk.drop_duplicates(subset=["_lead_id_norm"], keep="first")

    results = []
    for idx, row in df_conv.iterrows():
        conv_id = row["conversation_id"]
        lead_id = _normalize_lead_id(row.get("lead_id"))
        seniority_level = None
        if lead_id:
            match = df_lk_by_lead[df_lk_by_lead["_lead_id_norm"] == lead_id]
            if not match.empty:
                project_row = match.iloc[0]
                seniority_level = seniority_from_project_row(project_row)
        results.append({"conversation_id": conv_id, "seniority_level": seniority_level})

    return pd.DataFrame(results)


def get_seniority_variable(seniority_level: Optional[str]) -> Optional[Dict[str, Any]]:
    """Return conv_variable dict for segment.seniority_level, or None if level is None/empty."""
    if seniority_level is None or (isinstance(seniority_level, str) and not seniority_level.strip()):
        return None
    return {
        "conv_variable_category": "segment",
        "conv_variable_name": "seniority_level",
        "conv_variable_value": str(seniority_level).strip(),
    }
