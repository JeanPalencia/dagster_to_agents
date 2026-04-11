"""
Diagnóstico: GFG (model_lead_matcheados) vs FinN FIXED (Projects Funnel FIXED)
Valida que la corrección del join por (lead_id, fecha_cohort) elimina las
diferencias de canal encontradas en el diagnóstico anterior.
"""

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "dagster-pipeline" / "src"))
from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

SQL_DIR = Path(__file__).parent
GFG_SQL = SQL_DIR / "model_lead_matcheados.sql"
FINN_ORIG_SQL = SQL_DIR / "Query: Projects Funnel.sql"
FINN_FIXED_SQL = SQL_DIR / "Query: Projects Funnel FIXED.sql"

TARGET_MONTH = "2025-12"
ORGANIC_CHANNELS = {"Direct", "Organic Search", "Organic LLMs", "Organic Social"}


def run_geospot_query(sql: str, label: str) -> pd.DataFrame:
    print(f"\n  Ejecutando {label}...")
    df_polars = query_bronze_source(query=sql, source_type="geospot_postgres")
    df = df_polars.to_pandas()
    print(f"  -> {len(df)} filas")
    return df


def load_gfg() -> pd.DataFrame:
    with open(GFG_SQL, "r", encoding="utf-8") as f:
        sql = f.read()
    df = run_geospot_query(sql, "GFG (model_lead_matcheados)")
    df["lead_date"] = pd.to_datetime(df["lead_date"], errors="coerce")
    df["month"] = df["lead_date"].dt.to_period("M").astype(str)
    return df


def load_finn(sql_path: Path, label: str) -> pd.DataFrame:
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    df = run_geospot_query(sql, label)
    df["fecha_cohort"] = pd.to_datetime(df["fecha_cohort"], errors="coerce")
    df["primera_fecha_lead"] = pd.to_datetime(df["primera_fecha_lead"], errors="coerce")
    df["month"] = df["fecha_cohort"].dt.to_period("M").astype(str)

    df["fc_date"] = df["fecha_cohort"].dt.date
    df["pf_date"] = df["primera_fecha_lead"].dt.date
    df["cohort_type_derived"] = "New"
    mask_react = df["project_created_at"].notna() & (df["fc_date"] != df["pf_date"])
    df.loc[mask_react, "cohort_type_derived"] = "Reactivated"
    return df


def compare_pair(gfg_all: pd.DataFrame, finn_all: pd.DataFrame, label: str):
    print("\n" + "=" * 70)
    print(f"COMPARACIÓN: GFG vs {label} — New, {TARGET_MONTH}")
    print("=" * 70)

    gfg = gfg_all[
        (gfg_all["month"] == TARGET_MONTH) & (gfg_all["cohort_type"] == "New")
    ].copy()
    finn = finn_all[
        (finn_all["month"] == TARGET_MONTH) & (finn_all["cohort_type_derived"] == "New")
    ].copy()

    gfg_leads = gfg.drop_duplicates(subset=["lead_id"], keep="first").set_index("lead_id")[["channel"]]
    finn_leads = finn.drop_duplicates(subset=["lead_id"], keep="first").set_index("lead_id")[["channel"]]

    print(f"\n  GFG  distinct leads: {len(gfg_leads)}")
    print(f"  {label} distinct leads: {len(finn_leads)}")

    # Distribución por canal
    print(f"\n--- Distribución por canal ---")
    gfg_ch = gfg_leads["channel"].value_counts().rename("GFG")
    finn_ch = finn_leads["channel"].value_counts().rename(label)
    comp = pd.concat([gfg_ch, finn_ch], axis=1).fillna(0).astype(int)
    comp["Diff"] = comp["GFG"] - comp[label]
    comp = comp.sort_values("GFG", ascending=False)
    print(comp.to_string())

    # Canales orgánicos
    gfg_org = gfg_leads[gfg_leads["channel"].isin(ORGANIC_CHANNELS)]
    finn_org = finn_leads[finn_leads["channel"].isin(ORGANIC_CHANNELS)]
    print(f"\n--- Canales orgánicos ---")
    print(f"  GFG:  {len(gfg_org)}")
    print(f"  {label}: {len(finn_org)}")
    print(f"  Diferencia: {len(gfg_org) - len(finn_org)}")

    # Leads con canal distinto
    common = set(gfg_leads.index) & set(finn_leads.index)
    canal_diff = []
    for lid in common:
        ch_g = gfg_leads.loc[lid, "channel"]
        ch_f = finn_leads.loc[lid, "channel"]
        if ch_g != ch_f:
            canal_diff.append({"lead_id": lid, "canal_GFG": ch_g, f"canal_{label}": ch_f})

    print(f"\n--- Leads con canal DISTINTO: {len(canal_diff)} ---")
    if canal_diff:
        df_diff = pd.DataFrame(canal_diff).sort_values("lead_id")
        print(df_diff.to_string(index=False))
    else:
        print("  NINGUNO — Los canales coinciden perfectamente")

    return len(canal_diff)


def main():
    print("=" * 70)
    print("TEST: GFG vs FinN ORIGINAL vs FinN FIXED")
    print("=" * 70)

    gfg = load_gfg()
    finn_orig = load_finn(FINN_ORIG_SQL, "FinN (original)")
    finn_fixed = load_finn(FINN_FIXED_SQL, "FinN (FIXED)")

    diff_orig = compare_pair(gfg, finn_orig, "FinN_orig")
    diff_fixed = compare_pair(gfg, finn_fixed, "FinN_FIXED")

    print("\n" + "=" * 70)
    print("RESUMEN FINAL")
    print("=" * 70)
    print(f"  Leads con canal distinto (GFG vs FinN original): {diff_orig}")
    print(f"  Leads con canal distinto (GFG vs FinN FIXED):    {diff_fixed}")
    if diff_fixed == 0:
        print("\n  RESULTADO: La corrección elimina todas las diferencias de canal.")
    elif diff_fixed < diff_orig:
        print(f"\n  RESULTADO: La corrección reduce las diferencias de {diff_orig} a {diff_fixed}.")
    else:
        print(f"\n  RESULTADO: Aún quedan {diff_fixed} diferencias. Requiere análisis adicional.")
    print("=" * 70)


if __name__ == "__main__":
    main()
