"""
Test general: GFG vs FinN FIXED — Dic 2025 a Mar 2026, New + Reactivated.
Compara lead_id, fecha cohort y canal por mes y tipo de cohort.
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
    df["month"] = df["lead_date"].dt.to_period("M").astype(str)
    df["cohort_type"] = df["cohort_type"].astype(str).str.strip()
    return df


def load_finn() -> pd.DataFrame:
    with open(FINN_FIXED_SQL, "r", encoding="utf-8") as f:
        sql = f.read()
    df = run_q(sql, "FinN FIXED")
    df["fecha_cohort"] = pd.to_datetime(df["fecha_cohort"], errors="coerce")
    df["primera_fecha_lead"] = pd.to_datetime(df["primera_fecha_lead"], errors="coerce")
    df["month"] = df["fecha_cohort"].dt.to_period("M").astype(str)

    df["fc_date"] = df["fecha_cohort"].dt.date
    df["pf_date"] = df["primera_fecha_lead"].dt.date
    df["cohort_type"] = "New"
    mask = df["project_created_at"].notna() & (df["fc_date"] != df["pf_date"])
    df.loc[mask, "cohort_type"] = "Reactivated"
    return df


def dedup_gfg(df: pd.DataFrame, month: str, cohort: str) -> pd.DataFrame:
    sub = df[(df["month"] == month) & (df["cohort_type"] == cohort)].copy()
    return sub.drop_duplicates(subset=["lead_id"], keep="first")


def dedup_finn(df: pd.DataFrame, month: str, cohort: str) -> pd.DataFrame:
    sub = df[(df["month"] == month) & (df["cohort_type"] == cohort)].copy()
    return sub.drop_duplicates(subset=["lead_id"], keep="first")


def compare_group(gfg_sub: pd.DataFrame, finn_sub: pd.DataFrame, month: str, cohort: str):
    gfg_ids = set(gfg_sub["lead_id"])
    finn_ids = set(finn_sub["lead_id"])

    solo_gfg = gfg_ids - finn_ids
    solo_finn = finn_ids - gfg_ids
    common = gfg_ids & finn_ids

    gfg_idx = gfg_sub.set_index("lead_id")
    finn_idx = finn_sub.set_index("lead_id")

    canal_diff = []
    fecha_diff = []
    for lid in common:
        ch_g = gfg_idx.loc[lid, "channel"]
        ch_f = finn_idx.loc[lid, "channel"]
        if ch_g != ch_f:
            canal_diff.append({"lead_id": lid, "canal_GFG": ch_g, "canal_FinN": ch_f})

        fd_g = gfg_idx.loc[lid, "lead_date"]
        fd_f = finn_idx.loc[lid, "fecha_cohort"]
        if pd.notna(fd_g) and pd.notna(fd_f):
            fd_g_date = pd.Timestamp(fd_g).date()
            fd_f_date = pd.Timestamp(fd_f).date()
            if fd_g_date != fd_f_date:
                fecha_diff.append({
                    "lead_id": lid,
                    "fecha_GFG": str(fd_g_date),
                    "fecha_FinN": str(fd_f_date),
                })

    ok = len(solo_gfg) == 0 and len(solo_finn) == 0 and len(canal_diff) == 0 and len(fecha_diff) == 0

    print(f"\n  {month} | {cohort:<12s} | GFG={len(gfg_ids):>4d}  FinN={len(finn_ids):>4d}  "
          f"solo_GFG={len(solo_gfg):>3d}  solo_FinN={len(solo_finn):>3d}  "
          f"canal_diff={len(canal_diff):>3d}  fecha_diff={len(fecha_diff):>3d}  "
          f"{'OK' if ok else 'DIFF'}")

    if solo_gfg and len(solo_gfg) <= 10:
        for lid in sorted(solo_gfg):
            ch = gfg_idx.loc[lid, "channel"] if lid in gfg_idx.index else "?"
            print(f"    solo_GFG: {lid} (channel={ch})")
    elif solo_gfg:
        print(f"    solo_GFG: {len(solo_gfg)} leads (primeros 5: {sorted(solo_gfg)[:5]})")

    if solo_finn and len(solo_finn) <= 10:
        for lid in sorted(solo_finn):
            ch = finn_idx.loc[lid, "channel"] if lid in finn_idx.index else "?"
            print(f"    solo_FinN: {lid} (channel={ch})")
    elif solo_finn:
        print(f"    solo_FinN: {len(solo_finn)} leads (primeros 5: {sorted(solo_finn)[:5]})")

    if canal_diff and len(canal_diff) <= 15:
        for r in sorted(canal_diff, key=lambda x: x["lead_id"]):
            print(f"    canal_diff: {r['lead_id']} GFG={r['canal_GFG']} FinN={r['canal_FinN']}")
    elif canal_diff:
        print(f"    canal_diff: {len(canal_diff)} leads")
        df_cd = pd.DataFrame(canal_diff)
        pairs = df_cd.groupby(["canal_GFG", "canal_FinN"]).size().reset_index(name="count")
        pairs = pairs.sort_values("count", ascending=False)
        for _, p in pairs.iterrows():
            print(f"      {p['canal_GFG']} -> {p['canal_FinN']}: {p['count']}")

    if fecha_diff and len(fecha_diff) <= 10:
        for r in sorted(fecha_diff, key=lambda x: x["lead_id"]):
            print(f"    fecha_diff: {r['lead_id']} GFG={r['fecha_GFG']} FinN={r['fecha_FinN']}")
    elif fecha_diff:
        print(f"    fecha_diff: {len(fecha_diff)} leads (primeros 5)")
        for r in sorted(fecha_diff, key=lambda x: x["lead_id"])[:5]:
            print(f"      {r['lead_id']} GFG={r['fecha_GFG']} FinN={r['fecha_FinN']}")

    return {
        "month": month, "cohort": cohort,
        "gfg_count": len(gfg_ids), "finn_count": len(finn_ids),
        "solo_gfg": len(solo_gfg), "solo_finn": len(solo_finn),
        "canal_diff": len(canal_diff), "fecha_diff": len(fecha_diff),
    }


def main():
    print("=" * 80)
    print("TEST GENERAL: GFG vs FinN FIXED — Dic 2025 a Mar 2026, New + Reactivated")
    print("=" * 80)

    gfg = load_gfg()
    finn = load_finn()

    results = []
    for month in sorted(TARGET_MONTHS):
        for cohort in ["New", "Reactivated"]:
            g = dedup_gfg(gfg, month, cohort)
            f = dedup_finn(finn, month, cohort)
            r = compare_group(g, f, month, cohort)
            results.append(r)

    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    df_r = pd.DataFrame(results)
    print(df_r.to_string(index=False))

    total_canal = df_r["canal_diff"].sum()
    total_fecha = df_r["fecha_diff"].sum()
    total_solo_gfg = df_r["solo_gfg"].sum()
    total_solo_finn = df_r["solo_finn"].sum()

    print(f"\n  Total diferencias canal:      {total_canal}")
    print(f"  Total diferencias fecha:      {total_fecha}")
    print(f"  Total leads solo en GFG:      {total_solo_gfg}")
    print(f"  Total leads solo en FinN:     {total_solo_finn}")

    if total_canal == 0 and total_fecha == 0 and total_solo_gfg == 0 and total_solo_finn == 0:
        print("\n  RESULTADO: Coincidencia perfecta en todos los meses y cohorts.")
    else:
        print(f"\n  RESULTADO: Hay diferencias que requieren análisis.")
    print("=" * 80)


if __name__ == "__main__":
    main()
