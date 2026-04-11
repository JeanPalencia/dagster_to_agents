"""
Test: PoP Cohorts DISAGG vs FIXED

Ejecuta ambas queries, reconstruye los totales de la DISAGG agrupando
en Python (replicando la logica COUNTD de Metabase) y compara columna
por columna con la FIXED.

- Leads: siempre COUNTD(lead_id) donde flag=1  (igual en ambos modos)
- Proyecto+: COUNTD(entity_id) donde flag=1     (lead_id en By Lead,
                                                  project_id en By Project)
"""

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "dagster-pipeline" / "src"))
from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

TEST_DIR = Path(__file__).resolve().parent
FIXED_SQL = TEST_DIR / "Query: PoP Cohorts FIXED.sql"
DISAGG_SQL = TEST_DIR / "Query: PoP Cohorts DISAGG.sql"

GROUP_COLS = [
    "comparison_type",
    "current_period_start", "current_period_end",
    "previous_period_start", "previous_period_end",
    "cohort_type", "spot_sector", "lead_max_type",
    "channel", "traffic_type", "campaign_name",
    "mode",
]

METRIC_COLS = [
    "leads_current", "leads_prev",
    "projects_current", "projects_prev",
    "scheduled_visits_current", "scheduled_visits_prev",
    "completed_visits_current", "completed_visits_prev",
    "lois_current", "lois_prev",
    "won_current", "won_prev",
]

LEAD_METRICS = {"leads_current", "leads_prev"}


def run_query(path: Path, label: str) -> pd.DataFrame:
    sql = path.read_text()
    print(f"\n  Ejecutando {label}...")
    df = query_bronze_source(query=sql, source_type="geospot_postgres").to_pandas()
    print(f"  -> {len(df):,} filas")
    return df


def reconstruct_from_disagg(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa la DISAGG replicando la logica COUNTD de Metabase."""
    records = []

    for keys, grp in df.groupby(GROUP_COLS, dropna=False):
        row = dict(zip(GROUP_COLS, keys))

        for metric in METRIC_COLS:
            subset = grp[grp[metric] == 1]
            if metric in LEAD_METRICS:
                row[metric] = subset["lead_id"].nunique()
            else:
                row[metric] = subset["entity_id"].nunique()

        records.append(row)

    return pd.DataFrame(records)


def compare(df_fixed: pd.DataFrame, df_reconstructed: pd.DataFrame) -> int:
    """Compara ambos DataFrames y reporta diferencias.

    En Metabase, leads_current/leads_prev se calculan SIEMPRE desde
    mode='By Lead' (la tarjeta esta fija a ese mode). Por tanto,
    las metricas de leads solo se comparan en mode='By Lead' y las
    demas metricas se comparan en ambos modes.
    """
    total_diffs = 0

    # --- 1) leads_current / leads_prev: solo By Lead ---
    lead_dims = [c for c in GROUP_COLS if c != "mode"]
    df_f_lead = df_fixed[df_fixed["mode"] == "By Lead"].copy()
    df_r_lead = df_reconstructed[df_reconstructed["mode"] == "By Lead"].copy()

    merged_lead = df_f_lead.merge(
        df_r_lead,
        on=lead_dims + ["mode"],
        how="outer",
        suffixes=("_fixed", "_disagg"),
        indicator=True,
    )

    only_f = merged_lead[merged_lead["_merge"] == "left_only"]
    only_r = merged_lead[merged_lead["_merge"] == "right_only"]
    both_l = merged_lead[merged_lead["_merge"] == "both"]

    if len(only_f) > 0:
        print(f"\n  ALERTA: {len(only_f)} grupos By Lead solo en FIXED")
        total_diffs += len(only_f)
    if len(only_r) > 0:
        print(f"\n  ALERTA: {len(only_r)} grupos By Lead solo en DISAGG")
        total_diffs += len(only_r)

    for metric in LEAD_METRICS:
        col_f = f"{metric}_fixed"
        col_d = f"{metric}_disagg"
        diffs = both_l[both_l[col_f] != both_l[col_d]]
        if len(diffs) > 0:
            total_diffs += len(diffs)
            print(f"\n  DIFERENCIA en {metric} (By Lead): {len(diffs)} grupos")
            print(diffs[lead_dims + [col_f, col_d]].head(5).to_string(index=False))
        else:
            print(f"  {metric} (By Lead): OK")

    # --- 2) project+ metricas: ambos modes ---
    project_metrics = [m for m in METRIC_COLS if m not in LEAD_METRICS]

    merged_all = df_fixed.merge(
        df_reconstructed,
        on=GROUP_COLS,
        how="outer",
        suffixes=("_fixed", "_disagg"),
        indicator=True,
    )

    only_fixed = merged_all[merged_all["_merge"] == "left_only"]
    only_disagg = merged_all[merged_all["_merge"] == "right_only"]
    both = merged_all[merged_all["_merge"] == "both"]

    # Grupos solo en FIXED/DISAGG pero descartando By Project sin actividad de proyecto
    only_fixed_relevant = only_fixed[
        ~((only_fixed["mode"] == "By Project")
          & (only_fixed[[f"{m}_fixed" for m in project_metrics if f"{m}_fixed" in only_fixed.columns]].fillna(0) == 0).all(axis=1))
    ] if len(only_fixed) > 0 else only_fixed

    if len(only_fixed_relevant) > 0:
        print(f"\n  ALERTA: {len(only_fixed_relevant)} grupos solo en FIXED (con metricas de proyecto)")
        print(only_fixed_relevant[GROUP_COLS].head(10).to_string(index=False))
        total_diffs += len(only_fixed_relevant)
    elif len(only_fixed) > 0:
        print(f"\n  INFO: {len(only_fixed)} grupos solo en FIXED pero sin metricas de proyecto (esperado, ignorados)")

    if len(only_disagg) > 0:
        print(f"\n  ALERTA: {len(only_disagg)} grupos solo en DISAGG")
        total_diffs += len(only_disagg)

    for metric in project_metrics:
        col_f = f"{metric}_fixed"
        col_d = f"{metric}_disagg"
        diffs = both[both[col_f] != both[col_d]]
        if len(diffs) > 0:
            total_diffs += len(diffs)
            print(f"\n  DIFERENCIA en {metric}: {len(diffs)} grupos")
            print(diffs[GROUP_COLS + [col_f, col_d]].head(5).to_string(index=False))
        else:
            print(f"  {metric}: OK")

    return total_diffs


def main():
    print("=" * 60)
    print("Test: PoP Cohorts DISAGG vs FIXED")
    print("=" * 60)

    df_fixed = run_query(FIXED_SQL, "FIXED")
    df_disagg = run_query(DISAGG_SQL, "DISAGG")

    print(f"\n  FIXED:  {len(df_fixed):,} filas (grupos pre-agregados)")
    print(f"  DISAGG: {len(df_disagg):,} filas (entidades individuales)")

    # Filtrar filas con todos los conteos en 0 de FIXED (provienen del dims LEFT JOIN)
    zero_mask = (df_fixed[METRIC_COLS] == 0).all(axis=1)
    df_fixed_nonzero = df_fixed[~zero_mask].copy()
    print(f"\n  FIXED sin filas vacias: {len(df_fixed_nonzero):,} grupos")

    print("\n--- Reconstruyendo totales desde DISAGG ---")
    df_reconstructed = reconstruct_from_disagg(df_disagg)
    print(f"  -> {len(df_reconstructed):,} grupos reconstruidos")

    print("\n--- Comparando ---")
    total_diffs = compare(df_fixed_nonzero, df_reconstructed)

    print("\n" + "=" * 60)
    if total_diffs == 0:
        print("  RESULTADO: 0 diferencias. La DISAGG es equivalente a la FIXED.")
    else:
        print(f"  RESULTADO: {total_diffs} diferencias encontradas. Revisar.")
    print("=" * 60)


if __name__ == "__main__":
    main()
