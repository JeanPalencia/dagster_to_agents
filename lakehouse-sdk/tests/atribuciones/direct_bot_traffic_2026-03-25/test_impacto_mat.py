"""
Test de impacto: verifica si la corrección v4 (bot filtering) afecta leads en
la tabla lk_mat_matches_visitors_to_leads.

Estrategia simplificada (equivalente a correr el pipeline completo porque
`channel` es metadata carry-along, no afecta la lógica de matching):

1. Ejecuta lead_events_from_funnel_lk.sql con v3 (producción) y v4 (corregida)
2. Compara distribución de canales en los lead_events
3. Cruza user_pseudo_id reclasificados con tabla MAT actual en GeoSpot
"""

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[4]

sys.path.insert(0, str(REPO_ROOT / "dagster-pipeline" / "src"))
from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

LEAD_EVENTS_SQL = (
    REPO_ROOT
    / "dagster-pipeline"
    / "src"
    / "dagster_pipeline"
    / "defs"
    / "lk_matches_visitors_to_leads"
    / "queries"
    / "lead_events_from_funnel_lk.sql"
)

V4_SQL = REPO_ROOT / "lakehouse-sdk" / "sql" / "old" / " funnel_with_channel_v4.sql"


def load_lead_events_v3() -> pd.DataFrame:
    """Ejecuta lead_events_from_funnel_lk.sql tal cual (lee de producción v3)."""
    with open(LEAD_EVENTS_SQL, "r", encoding="utf-8") as f:
        sql = f.read()
    return query_bronze_source(sql, source_type="bigquery").to_pandas()


def load_lead_events_v4() -> pd.DataFrame:
    """Ejecuta lead_events reemplazando funnel_with_channel por la query v4."""
    with open(LEAD_EVENTS_SQL, "r", encoding="utf-8") as f:
        lead_sql = f.read()
    with open(V4_SQL, "r", encoding="utf-8") as f:
        v4_sql = f.read()

    v4_as_subquery = f"({v4_sql})"
    modified_sql = lead_sql.replace(
        "`analitics_spot2.funnel_with_channel`",
        v4_as_subquery,
    )
    return query_bronze_source(modified_sql, source_type="bigquery").to_pandas()


def load_mat_table() -> pd.DataFrame:
    """Lee la tabla MAT actual de GeoSpot."""
    sql = """
    SELECT
        vis_user_pseudo_id,
        mat_channel,
        mat_source,
        mat_medium,
        lead_id,
        lead_fecha_cohort_dt,
        mat_event_name,
        mat_match_source
    FROM lk_mat_matches_visitors_to_leads
    """
    return query_bronze_source(sql, source_type="geospot_postgres").to_pandas()


def run_test():
    print("=" * 70)
    print("TEST: Impacto de v4 en lead_events y tabla MAT")
    print("=" * 70)

    # --- 1. Lead events v3 ---
    print("\n[1/4] Ejecutando lead_events con v3 (producción)...")
    df_v3 = load_lead_events_v3()
    print(f"  Filas v3: {len(df_v3):,}")

    # --- 2. Lead events v4 ---
    print("\n[2/4] Ejecutando lead_events con v4 (corregida)...")
    df_v4 = load_lead_events_v4()
    print(f"  Filas v4: {len(df_v4):,}")

    # --- 3. Comparar ---
    print("\n" + "=" * 70)
    print("COMPARACIÓN LEAD_EVENTS: v3 vs v4")
    print("=" * 70)

    print(f"\n  Total filas v3: {len(df_v3):,}")
    print(f"  Total filas v4: {len(df_v4):,}")
    print(f"  Diferencia:     {len(df_v4) - len(df_v3):,}")

    ch_v3 = df_v3.groupby("channel").size().reset_index(name="v3")
    ch_v4 = df_v4.groupby("channel").size().reset_index(name="v4")
    ch_merged = ch_v3.merge(ch_v4, on="channel", how="outer").fillna(0)
    ch_merged["v3"] = ch_merged["v3"].astype(int)
    ch_merged["v4"] = ch_merged["v4"].astype(int)
    ch_merged["delta"] = ch_merged["v4"] - ch_merged["v3"]
    ch_merged = ch_merged.sort_values("v3", ascending=False)

    print("\n--- Distribución de canal en lead_events ---")
    print(ch_merged.to_string(index=False))

    canales_con_cambio = ch_merged[ch_merged["delta"] != 0]
    if canales_con_cambio.empty:
        print("\nOK: No hay cambios en canales de lead_events.")
    else:
        print(f"\nCanales que cambian: {len(canales_con_cambio)}")

    # Identificar user_pseudo_ids reclasificados
    if "user_pseudo_id" in df_v3.columns and "user_pseudo_id" in df_v4.columns:
        merged_users = df_v3[["user_pseudo_id", "channel"]].drop_duplicates().merge(
            df_v4[["user_pseudo_id", "channel"]].drop_duplicates(),
            on="user_pseudo_id",
            suffixes=("_v3", "_v4"),
        )
        reclasificados = merged_users[merged_users["channel_v3"] != merged_users["channel_v4"]]
        print(f"\n  Usuarios con canal diferente: {reclasificados['user_pseudo_id'].nunique():,}")

        if not reclasificados.empty:
            print("\n--- Cambios de canal (top 20) ---")
            cambio_resumen = (
                reclasificados.groupby(["channel_v3", "channel_v4"])
                .agg(usuarios=("user_pseudo_id", "nunique"))
                .sort_values("usuarios", ascending=False)
            )
            print(cambio_resumen.to_string())

            bot_users = reclasificados[reclasificados["channel_v4"] == "Bot/Spam"][
                "user_pseudo_id"
            ].unique()
            print(f"\n  Usuarios reclasificados a Bot/Spam: {len(bot_users):,}")
        else:
            bot_users = []
    else:
        bot_users = []

    # --- 4. Cruzar con tabla MAT ---
    print("\n" + "=" * 70)
    print("CRUCE CON TABLA MAT (GeoSpot)")
    print("=" * 70)

    print("\n[3/4] Leyendo tabla MAT de GeoSpot...")
    df_mat = load_mat_table()
    print(f"  Filas MAT: {len(df_mat):,}")
    print(f"  Leads únicos: {df_mat['lead_id'].nunique():,}")

    if len(bot_users) > 0:
        mat_con_bot = df_mat[df_mat["vis_user_pseudo_id"].isin(bot_users)]
        leads_afectados = mat_con_bot["lead_id"].nunique()
        print(f"\n  Leads con visitante reclasificado a Bot/Spam: {leads_afectados}")

        if leads_afectados > 0:
            print("\n--- Leads afectados ---")
            print(
                mat_con_bot[
                    ["lead_id", "vis_user_pseudo_id", "mat_channel", "mat_source",
                     "mat_event_name", "mat_match_source", "lead_fecha_cohort_dt"]
                ]
                .drop_duplicates(subset=["lead_id"])
                .sort_values("lead_id")
                .to_string(index=False)
            )

            print("\n--- Canales actuales de los leads afectados ---")
            print(
                mat_con_bot.groupby("mat_channel")
                .agg(leads=("lead_id", "nunique"))
                .sort_values("leads", ascending=False)
                .to_string()
            )
        else:
            print("  OK: Ningún lead tiene un bot como visitante matcheado.")
    else:
        print("\n  No hay usuarios reclasificados a Bot/Spam en lead_events.")
        print("  OK: Sin impacto en la tabla MAT.")

    # --- Veredicto ---
    print("\n" + "=" * 70)
    print("VEREDICTO FINAL")
    print("=" * 70)

    filas_ok = len(df_v3) == len(df_v4)
    leads_ok = len(bot_users) == 0 or (
        len(bot_users) > 0 and df_mat[df_mat["vis_user_pseudo_id"].isin(bot_users)]["lead_id"].nunique() == 0
    )

    if filas_ok and leads_ok:
        print("v4 ES SEGURA: mismas filas, sin impacto en leads.")
    elif filas_ok and not leads_ok:
        n = df_mat[df_mat["vis_user_pseudo_id"].isin(bot_users)]["lead_id"].nunique()
        print(f"REVISAR: {n} leads tienen visitante bot. Ver detalle arriba.")
    else:
        print(f"ALERTA: diferencia de filas ({len(df_v4) - len(df_v3):,}). Investigar.")


if __name__ == "__main__":
    run_test()
