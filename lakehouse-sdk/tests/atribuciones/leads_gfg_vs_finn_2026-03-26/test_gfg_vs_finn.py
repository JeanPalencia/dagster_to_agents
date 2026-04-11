"""
Diagnóstico: GFG (model_lead_matcheados) vs FinN (Projects Funnel)
Compara leads "New" en diciembre 2025, enfocado en diferencias de canal.

Hipótesis: FinN usa DISTINCT ON (lead_id) para asignar UN canal global,
mientras GFG usa ROW_NUMBER por (lead_id, lead_fecha_cohort_dt) y tiene
lógica CRM_OFFLINE→Offline / L3→Outbound.
"""

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "dagster-pipeline" / "src"))
from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

SQL_DIR = Path(__file__).parent
GFG_SQL = SQL_DIR / "model_lead_matcheados.sql"
FINN_SQL = SQL_DIR / "Query: Projects Funnel.sql"

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


def load_finn() -> pd.DataFrame:
    with open(FINN_SQL, "r", encoding="utf-8") as f:
        sql = f.read()
    df = run_geospot_query(sql, "FinN (Projects Funnel)")
    df["fecha_cohort"] = pd.to_datetime(df["fecha_cohort"], errors="coerce")
    df["primera_fecha_lead"] = pd.to_datetime(df["primera_fecha_lead"], errors="coerce")
    df["month"] = df["fecha_cohort"].dt.to_period("M").astype(str)

    df["fc_date"] = df["fecha_cohort"].dt.date
    df["pf_date"] = df["primera_fecha_lead"].dt.date
    df["cohort_type_derived"] = "New"
    mask_react = (
        df["project_created_at"].notna()
        & (df["fc_date"] != df["pf_date"])
    )
    df.loc[mask_react, "cohort_type_derived"] = "Reactivated"
    return df


def load_mat_for_leads(lead_ids: list) -> pd.DataFrame:
    if not lead_ids:
        return pd.DataFrame()
    ids_str = ", ".join(f"'{lid}'" for lid in lead_ids)
    sql = f"""
    SELECT lead_id, lead_fecha_cohort_dt, lead_cohort_type,
           mat_channel, mat_traffic_type, mat_event_datetime,
           vis_user_pseudo_id, mat_with_match
    FROM lk_mat_matches_visitors_to_leads
    WHERE lead_id IN ({ids_str})
    ORDER BY lead_id, mat_event_datetime DESC
    """
    return run_geospot_query(sql, f"MAT detalle ({len(lead_ids)} leads)")


def compare(gfg_all: pd.DataFrame, finn_all: pd.DataFrame):
    print("\n" + "=" * 70)
    print("FILTRADO: Cohort New, Diciembre 2025")
    print("=" * 70)

    gfg = gfg_all[
        (gfg_all["month"] == TARGET_MONTH) & (gfg_all["cohort_type"] == "New")
    ].copy()
    finn = finn_all[
        (finn_all["month"] == TARGET_MONTH)
        & (finn_all["cohort_type_derived"] == "New")
    ].copy()

    gfg_dedup = gfg.drop_duplicates(subset=["lead_id"], keep="first")
    finn_dedup = finn.drop_duplicates(subset=["lead_id"], keep="first")

    gfg_leads = gfg_dedup.set_index("lead_id")[["channel"]]
    finn_leads = finn_dedup.set_index("lead_id")[["channel"]]

    print(f"\n  GFG  distinct leads (New, dic-2025): {len(gfg_leads)}")
    print(f"  FinN distinct leads (New, dic-2025): {len(finn_leads)}")

    # --- Distribución por canal ---
    print("\n--- Distribución por canal (DISTINCT lead_id) ---")
    gfg_ch = gfg_leads["channel"].value_counts().rename("GFG")
    finn_ch = finn_leads["channel"].value_counts().rename("FinN")
    comp = pd.concat([gfg_ch, finn_ch], axis=1).fillna(0).astype(int)
    comp["Diff"] = comp["GFG"] - comp["FinN"]
    comp = comp.sort_values("GFG", ascending=False)
    print(comp.to_string())

    # --- Solo canales orgánicos ---
    print(f"\n--- Solo canales: {ORGANIC_CHANNELS} ---")
    gfg_org = gfg_leads[gfg_leads["channel"].isin(ORGANIC_CHANNELS)]
    finn_org = finn_leads[finn_leads["channel"].isin(ORGANIC_CHANNELS)]
    print(f"  GFG:  {len(gfg_org)} leads")
    print(f"  FinN: {len(finn_org)} leads")
    print(f"  Diferencia: {len(gfg_org) - len(finn_org)}")

    # --- Leads exclusivos en canales orgánicos ---
    gfg_ids = set(gfg_org.index)
    finn_ids = set(finn_org.index)

    solo_gfg = gfg_ids - finn_ids
    solo_finn = finn_ids - gfg_ids
    en_ambos = gfg_ids & finn_ids

    print(f"\n  En ambos (mismos canales orgánicos): {len(en_ambos)}")
    print(f"  Solo en GFG (canales orgánicos):      {len(solo_gfg)}")
    print(f"  Solo en FinN (canales orgánicos):      {len(solo_finn)}")

    # --- Detalle: leads solo en GFG (canales orgánicos) ---
    if solo_gfg:
        print(f"\n--- Leads en canales orgánicos en GFG pero NO en FinN ---")
        for lid in sorted(solo_gfg):
            ch_gfg = gfg_leads.loc[lid, "channel"]
            if lid in finn_leads.index:
                ch_finn = finn_leads.loc[lid, "channel"]
                print(f"  {lid}: GFG={ch_gfg}, FinN={ch_finn} (está en FinN pero canal distinto)")
            else:
                print(f"  {lid}: GFG={ch_gfg}, FinN=(no aparece)")

    # --- Detalle: leads solo en FinN (canales orgánicos) ---
    if solo_finn:
        print(f"\n--- Leads en canales orgánicos en FinN pero NO en GFG ---")
        for lid in sorted(solo_finn):
            ch_finn = finn_leads.loc[lid, "channel"]
            if lid in gfg_leads.index:
                ch_gfg = gfg_leads.loc[lid, "channel"]
                print(f"  {lid}: FinN={ch_finn}, GFG={ch_gfg} (está en GFG pero canal distinto)")
            else:
                print(f"  {lid}: FinN={ch_finn}, GFG=(no aparece)")

    # --- Leads en ambos pero con canal distinto (todos los canales) ---
    all_common = set(gfg_leads.index) & set(finn_leads.index)
    canal_diff = []
    for lid in all_common:
        ch_g = gfg_leads.loc[lid, "channel"]
        ch_f = finn_leads.loc[lid, "channel"]
        if ch_g != ch_f:
            canal_diff.append({"lead_id": lid, "canal_GFG": ch_g, "canal_FinN": ch_f})

    print(f"\n--- Leads en ambos pero con canal DISTINTO: {len(canal_diff)} ---")
    if canal_diff:
        df_diff = pd.DataFrame(canal_diff).sort_values("lead_id")
        print(df_diff.to_string(index=False))

        diff_ids = [r["lead_id"] for r in canal_diff]
        mat = load_mat_for_leads(diff_ids)
        if not mat.empty:
            print("\n--- Detalle MAT para leads con canal distinto ---")
            print("    (muestra todas las filas de cada lead en MAT)")
            for lid in diff_ids:
                rows = mat[mat["lead_id"] == lid]
                if rows.empty:
                    print(f"\n  {lid}: sin filas en MAT")
                    continue
                ch_g = [r for r in canal_diff if r["lead_id"] == lid][0]
                print(f"\n  {lid} (GFG={ch_g['canal_GFG']}, FinN={ch_g['canal_FinN']}):")
                for _, r in rows.iterrows():
                    upid = str(r.get("vis_user_pseudo_id", ""))[:35]
                    print(
                        f"    cohort={r['lead_cohort_type']:<12s} "
                        f"fecha_cohort={r['lead_fecha_cohort_dt']}  "
                        f"channel={str(r['mat_channel']):<20s} "
                        f"event_dt={r['mat_event_datetime']}  "
                        f"upid={upid}"
                    )

    # --- Leads que cruzan de orgánico ↔ no-orgánico ---
    print("\n--- Leads que cambian de grupo (orgánico ↔ no-orgánico) ---")
    crossover = []
    for lid in all_common:
        ch_g = gfg_leads.loc[lid, "channel"]
        ch_f = finn_leads.loc[lid, "channel"]
        in_org_g = ch_g in ORGANIC_CHANNELS
        in_org_f = ch_f in ORGANIC_CHANNELS
        if in_org_g != in_org_f:
            crossover.append({
                "lead_id": lid,
                "canal_GFG": ch_g,
                "en_org_GFG": in_org_g,
                "canal_FinN": ch_f,
                "en_org_FinN": in_org_f,
            })
    if crossover:
        df_cross = pd.DataFrame(crossover).sort_values("lead_id")
        print(df_cross.to_string(index=False))
    else:
        print("  Ninguno")

    print("\n" + "=" * 70)
    print("FIN DEL DIAGNÓSTICO")
    print("=" * 70)


def main():
    print("=" * 70)
    print("DIAGNÓSTICO: GFG vs FinN — Canal en cohort New, Diciembre 2025")
    print("=" * 70)

    gfg = load_gfg()
    finn = load_finn()
    compare(gfg, finn)


if __name__ == "__main__":
    main()
