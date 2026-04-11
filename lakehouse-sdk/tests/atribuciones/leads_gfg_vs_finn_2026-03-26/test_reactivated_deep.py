"""
Investigación profunda: diferencias en Reactivated entre GFG y FinN FIXED.

Compara a nivel (lead_id, fecha_cohort_date) en lugar de solo lead_id,
para determinar si las diferencias son reales o un artefacto de la
deduplicación por lead_id cuando un lead tiene múltiples proyectos.
"""

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "dagster-pipeline" / "src"))
from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

SQL_DIR = Path(__file__).parent
GFG_SQL = SQL_DIR / "model_lead_matcheados.sql"
FINN_FIXED_SQL = SQL_DIR / "Query: Projects Funnel FIXED.sql"

TARGET_MONTHS = {"2025-12", "2026-01", "2026-02", "2026-03"}


def run_q(sql: str, label: str) -> pd.DataFrame:
    print(f"  Ejecutando {label}...")
    df = query_bronze_source(query=sql, source_type="geospot_postgres").to_pandas()
    print(f"  -> {len(df)} filas")
    return df


def load_gfg() -> pd.DataFrame:
    with open(GFG_SQL, "r", encoding="utf-8") as f:
        sql = f.read()
    df = run_q(sql, "GFG")
    df["lead_date"] = pd.to_datetime(df["lead_date"], errors="coerce")
    df["fecha_cohort_date"] = df["lead_date"].dt.date
    df["month"] = df["lead_date"].dt.to_period("M").astype(str)
    df["cohort_type"] = df["cohort_type"].astype(str).str.strip()
    return df


def load_finn() -> pd.DataFrame:
    with open(FINN_FIXED_SQL, "r", encoding="utf-8") as f:
        sql = f.read()
    df = run_q(sql, "FinN FIXED")
    df["fecha_cohort"] = pd.to_datetime(df["fecha_cohort"], errors="coerce")
    df["primera_fecha_lead"] = pd.to_datetime(df["primera_fecha_lead"], errors="coerce")
    df["fecha_cohort_date"] = df["fecha_cohort"].dt.date
    df["month"] = df["fecha_cohort"].dt.to_period("M").astype(str)
    df["pf_date"] = df["primera_fecha_lead"].dt.date
    df["cohort_type"] = "New"
    mask = df["project_created_at"].notna() & (df["fecha_cohort_date"] != df["pf_date"])
    df.loc[mask, "cohort_type"] = "Reactivated"
    return df


def load_mat_for_leads(lead_ids: list) -> pd.DataFrame:
    if not lead_ids:
        return pd.DataFrame()
    ids_str = ", ".join(f"'{lid}'" for lid in lead_ids)
    sql = f"""
    SELECT lead_id, lead_fecha_cohort_dt, lead_cohort_type,
           mat_channel, mat_event_datetime, vis_user_pseudo_id
    FROM lk_mat_matches_visitors_to_leads
    WHERE lead_id IN ({ids_str})
    ORDER BY lead_id, lead_fecha_cohort_dt
    """
    return run_q(sql, f"MAT ({len(lead_ids)} leads)")


def main():
    print("=" * 80)
    print("INVESTIGACIÓN PROFUNDA: Reactivated — GFG vs FinN FIXED")
    print("=" * 80)

    gfg_all = load_gfg()
    finn_all = load_finn()

    # --- Parte 1: ¿Cuántos Reactivated tiene cada lead por mes? ---
    print("\n" + "=" * 80)
    print("PARTE 1: Leads con múltiples Reactivated en el mismo mes")
    print("=" * 80)

    for month in sorted(TARGET_MONTHS):
        gfg_r = gfg_all[(gfg_all["month"] == month) & (gfg_all["cohort_type"] == "Reactivated")]
        finn_r = finn_all[(finn_all["month"] == month) & (finn_all["cohort_type"] == "Reactivated")]

        gfg_multi = gfg_r.groupby("lead_id").size()
        finn_multi = finn_r.groupby("lead_id").size()

        gfg_with_multi = (gfg_multi > 1).sum()
        finn_with_multi = (finn_multi > 1).sum()

        print(f"\n  {month}: GFG {len(gfg_multi)} leads ({gfg_with_multi} con >1 fila), "
              f"FinN {len(finn_multi)} leads ({finn_with_multi} con >1 fila)")

        if finn_with_multi > 0:
            multi_ids = finn_multi[finn_multi > 1].index.tolist()
            for lid in multi_ids[:5]:
                rows_f = finn_r[finn_r["lead_id"] == lid][["lead_id", "fecha_cohort_date", "channel", "project_id"]].values
                rows_g = gfg_r[gfg_r["lead_id"] == lid][["lead_id", "fecha_cohort_date", "channel"]].values
                print(f"    lead {lid}:")
                print(f"      FinN: {[(str(r[1]), r[2], r[3]) for r in rows_f]}")
                print(f"      GFG:  {[(str(r[1]), r[2]) for r in rows_g]}")

    # --- Parte 2: Comparar a nivel (lead_id, fecha_cohort_date) ---
    print("\n" + "=" * 80)
    print("PARTE 2: Comparación a nivel (lead_id, fecha_cohort_date)")
    print("=" * 80)

    all_diff_leads = set()

    for month in sorted(TARGET_MONTHS):
        gfg_r = gfg_all[(gfg_all["month"] == month) & (gfg_all["cohort_type"] == "Reactivated")].copy()
        finn_r = finn_all[(finn_all["month"] == month) & (finn_all["cohort_type"] == "Reactivated")].copy()

        gfg_keys = set(zip(gfg_r["lead_id"].astype(str), gfg_r["fecha_cohort_date"].astype(str)))
        finn_keys = set(zip(finn_r["lead_id"].astype(str), finn_r["fecha_cohort_date"].astype(str)))

        solo_gfg = gfg_keys - finn_keys
        solo_finn = finn_keys - gfg_keys
        common_keys = gfg_keys & finn_keys

        # Para keys en common, comparar canal
        gfg_r["_key_str"] = gfg_r["lead_id"].astype(str) + "|" + gfg_r["fecha_cohort_date"].astype(str)
        finn_r["_key_str"] = finn_r["lead_id"].astype(str) + "|" + finn_r["fecha_cohort_date"].astype(str)

        gfg_map = gfg_r.drop_duplicates(subset=["_key_str"], keep="first").set_index("_key_str")["channel"].to_dict()
        finn_map = finn_r.drop_duplicates(subset=["_key_str"], keep="first").set_index("_key_str")["channel"].to_dict()

        canal_diff = []
        for k in common_keys:
            k_str = f"{k[0]}|{k[1]}"
            ch_g = gfg_map.get(k_str, "?")
            ch_f = finn_map.get(k_str, "?")
            if ch_g != ch_f:
                canal_diff.append({"key": k, "canal_GFG": ch_g, "canal_FinN": ch_f})

        ok = len(solo_gfg) == 0 and len(solo_finn) == 0 and len(canal_diff) == 0

        print(f"\n  {month} Reactivated:")
        print(f"    GFG keys:  {len(gfg_keys)}")
        print(f"    FinN keys: {len(finn_keys)}")
        print(f"    Comunes:   {len(common_keys)}")
        print(f"    Solo GFG:  {len(solo_gfg)}")
        print(f"    Solo FinN: {len(solo_finn)}")
        print(f"    Canal diff (en comunes): {len(canal_diff)}")
        print(f"    Estado: {'OK' if ok else 'DIFF'}")

        if solo_gfg:
            print(f"    --- Keys solo en GFG ---")
            for k in sorted(solo_gfg)[:10]:
                print(f"      lead={k[0]}, fecha={k[1]}")
                all_diff_leads.add(k[0])

        if solo_finn:
            print(f"    --- Keys solo en FinN ---")
            for k in sorted(solo_finn)[:10]:
                print(f"      lead={k[0]}, fecha={k[1]}")
                all_diff_leads.add(k[0])

        if canal_diff:
            print(f"    --- Canal distinto ---")
            for d in sorted(canal_diff, key=lambda x: x["key"])[:10]:
                print(f"      {d['key']}: GFG={d['canal_GFG']}, FinN={d['canal_FinN']}")
                all_diff_leads.add(d["key"][0])

    # --- Parte 3: MAT detalle para leads problemáticos ---
    if all_diff_leads:
        print("\n" + "=" * 80)
        print(f"PARTE 3: Detalle MAT para {len(all_diff_leads)} leads con diferencias")
        print("=" * 80)

        mat = load_mat_for_leads(list(all_diff_leads)[:30])
        if not mat.empty:
            for lid in sorted(all_diff_leads)[:15]:
                lid_f = float(lid)
                rows = mat[mat["lead_id"] == lid_f]
                if rows.empty:
                    print(f"\n  {lid}: sin filas en MAT")
                    continue
                print(f"\n  Lead {lid} — filas en MAT:")
                for _, r in rows.iterrows():
                    upid = str(r.get("vis_user_pseudo_id", ""))[:30]
                    print(
                        f"    type={str(r['lead_cohort_type']):<12s} "
                        f"fecha_cohort={r['lead_fecha_cohort_dt']}  "
                        f"channel={str(r['mat_channel']):<20s} "
                        f"upid={upid}"
                    )

                gfg_rows = gfg_all[
                    (gfg_all["lead_id"].astype(str) == lid)
                    & (gfg_all["cohort_type"] == "Reactivated")
                ][["lead_id", "fecha_cohort_date", "channel", "month"]]
                finn_rows = finn_all[
                    (finn_all["lead_id"].astype(str) == lid)
                    & (finn_all["cohort_type"] == "Reactivated")
                ][["lead_id", "fecha_cohort_date", "channel", "month", "project_id"]]

                if not gfg_rows.empty:
                    print(f"    GFG Reactivated entries:")
                    for _, r in gfg_rows.iterrows():
                        print(f"      fecha={r['fecha_cohort_date']}, channel={r['channel']}, month={r['month']}")
                if not finn_rows.empty:
                    print(f"    FinN Reactivated entries:")
                    for _, r in finn_rows.iterrows():
                        print(f"      fecha={r['fecha_cohort_date']}, channel={r['channel']}, "
                              f"month={r['month']}, project_id={r['project_id']}")

    print("\n" + "=" * 80)
    print("FIN DE LA INVESTIGACIÓN")
    print("=" * 80)


if __name__ == "__main__":
    main()
