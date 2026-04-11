"""
Test end-to-end: funnel_with_channel v3 (actual) vs v4 (con reglas bot ampliadas).

Ejecuta ambas queries acotadas a marzo 2026, compara distribución de canales
y valida que la única diferencia sea la reclasificación de bots a "Bot/Spam".
"""

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[3] / "dagster-pipeline" / "src"),
)
from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "old"
V3_PATH = SQL_DIR / " funnel_with_channel_v3.sql"
V4_PATH = SQL_DIR / " funnel_with_channel_v4.sql"

DATE_FROM = "20260301"
DATE_TO = "20260325"


def limit_dates(sql: str) -> str:
    """Reemplaza el rango de fechas para acotar a marzo 2026."""
    sql = sql.replace("event_date >= '20250201'", f"event_date >= '{DATE_FROM}'")
    sql = re.sub(
        r"event_date\s*<\s*FORMAT_DATE\('%Y%m%d',\s*CURRENT_DATE\(\)\)",
        f"event_date <= '{DATE_TO}'",
        sql,
    )
    return sql


def run_query(path: Path, label: str) -> pd.DataFrame:
    print(f"\n{'='*70}")
    print(f"Ejecutando {label}: {path.name}")
    print(f"{'='*70}")

    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    sql = limit_dates(sql)
    df = query_bronze_source(sql, source_type="bigquery").to_pandas()
    print(f"  Filas: {len(df):,}")
    print(f"  Columnas: {list(df.columns)}")
    return df


def compare(df_v3: pd.DataFrame, df_v4: pd.DataFrame):
    print("\n" + "=" * 70)
    print("COMPARACIÓN: v3 vs v4")
    print("=" * 70)

    # --- 1. Sesiones por canal ---
    print("\n--- 1. SESIONES POR CANAL ---")
    ch_v3 = df_v3.groupby("Channel").size().reset_index(name="v3_sesiones")
    ch_v4 = df_v4.groupby("Channel").size().reset_index(name="v4_sesiones")
    merged = ch_v3.merge(ch_v4, on="Channel", how="outer").fillna(0)
    merged["v3_sesiones"] = merged["v3_sesiones"].astype(int)
    merged["v4_sesiones"] = merged["v4_sesiones"].astype(int)
    merged["delta"] = merged["v4_sesiones"] - merged["v3_sesiones"]
    merged = merged.sort_values("v3_sesiones", ascending=False)
    print(merged.to_string(index=False))

    total_v3 = merged["v3_sesiones"].sum()
    total_v4 = merged["v4_sesiones"].sum()
    print(f"\nTotal v3: {total_v3:,} | Total v4: {total_v4:,} | Delta: {total_v4 - total_v3:,}")

    # --- 2. Validar que el total no cambió ---
    print("\n--- 2. VALIDACIÓN: TOTAL DE FILAS ---")
    if total_v3 == total_v4:
        print("OK: El total de filas es idéntico. No se eliminaron ni agregaron registros.")
    else:
        print(f"*** ALERTA: Total difiere. v3={total_v3:,} vs v4={total_v4:,} ***")

    # --- 3. Verificar que Bot/Spam solo existe en v4 ---
    print("\n--- 3. CANAL Bot/Spam ---")
    bot_v4 = merged[merged["Channel"] == "Bot/Spam"]
    if not bot_v4.empty:
        bot_count = bot_v4["v4_sesiones"].values[0]
        print(f"OK: v4 tiene {bot_count:,} sesiones en 'Bot/Spam'.")
    else:
        print("*** ALERTA: No se encontró canal 'Bot/Spam' en v4 ***")

    bot_v3 = merged[merged["Channel"] == "Bot/Spam"]["v3_sesiones"]
    if bot_v3.empty or bot_v3.values[0] == 0:
        print("OK: v3 no tiene canal 'Bot/Spam' (esperado).")
    else:
        print("*** ALERTA: v3 tiene sesiones en 'Bot/Spam' (inesperado) ***")

    # --- 4. Verificar que la diferencia en Direct + Unassigned = Bot/Spam ---
    print("\n--- 4. BALANCE: Direct + Unassigned perdidos = Bot/Spam ganados ---")
    def get_canal(df_m, canal):
        row = df_m[df_m["Channel"] == canal]
        return row.iloc[0] if not row.empty else pd.Series({"v3_sesiones": 0, "v4_sesiones": 0, "delta": 0})

    direct_delta = get_canal(merged, "Direct")["delta"]
    unassigned_delta = get_canal(merged, "Unassigned")["delta"]
    bot_delta = get_canal(merged, "Bot/Spam")["delta"]
    perdidos = direct_delta + unassigned_delta
    print(f"  Direct delta:     {direct_delta:+,}")
    print(f"  Unassigned delta: {unassigned_delta:+,}")
    print(f"  Bot/Spam delta:   {bot_delta:+,}")
    print(f"  Perdidos (Direct + Unassigned): {perdidos:,}")
    print(f"  Ganados (Bot/Spam):             {bot_delta:,}")

    if abs(perdidos + bot_delta) == 0:
        print("OK: Balance perfecto. Todo lo que salió de Direct/Unassigned fue a Bot/Spam.")
    else:
        diff = perdidos + bot_delta
        print(f"*** ALERTA: Desbalance de {diff:,} sesiones. Investigar. ***")

    # --- 5. Validar que los demás canales no cambiaron ---
    print("\n--- 5. CANALES SIN CAMBIOS (delta debe ser 0) ---")
    otros = merged[~merged["Channel"].isin(["Direct", "Unassigned", "Bot/Spam"])]
    cambios = otros[otros["delta"] != 0]
    if cambios.empty:
        print("OK: Todos los demás canales tienen exactamente las mismas sesiones.")
    else:
        print("*** ALERTA: Canales con cambios inesperados ***")
        print(cambios.to_string(index=False))

    # --- 6. Traffic_type ---
    print("\n--- 6. TRAFFIC_TYPE ---")
    tt_v3 = df_v3.groupby("Traffic_type").size().reset_index(name="v3")
    tt_v4 = df_v4.groupby("Traffic_type").size().reset_index(name="v4")
    tt_merged = tt_v3.merge(tt_v4, on="Traffic_type", how="outer").fillna(0)
    tt_merged["v3"] = tt_merged["v3"].astype(int)
    tt_merged["v4"] = tt_merged["v4"].astype(int)
    tt_merged["delta"] = tt_merged["v4"] - tt_merged["v3"]
    tt_merged = tt_merged.sort_values("v3", ascending=False)
    print(tt_merged.to_string(index=False))

    # --- 7. user_sospechoso flag ---
    print("\n--- 7. FLAG user_sospechoso ---")
    sosp_v3 = df_v3["user_sospechoso"].sum()
    sosp_v4 = df_v4["user_sospechoso"].sum()
    print(f"  v3 user_sospechoso=1: {sosp_v3:,}")
    print(f"  v4 user_sospechoso=1: {sosp_v4:,}")
    print(f"  Nuevos sospechosos en v4: {sosp_v4 - sosp_v3:,}")

    if "Channel" in df_v4.columns:
        bot_spam_sosp = df_v4[df_v4["Channel"] == "Bot/Spam"]["user_sospechoso"].sum()
        bot_spam_total = len(df_v4[df_v4["Channel"] == "Bot/Spam"])
        print(f"  Bot/Spam con user_sospechoso=1: {bot_spam_sosp:,} de {bot_spam_total:,}")
        if bot_spam_sosp == bot_spam_total:
            print("  OK: Todos los Bot/Spam tienen user_sospechoso=1.")
        else:
            print("  *** ALERTA: Hay Bot/Spam sin user_sospechoso=1 ***")

    # --- 8. Comparar con test anterior ---
    print("\n--- 8. COMPARACIÓN CON TEST ANTERIOR ---")
    print("  Test anterior (compare_scraping_rules): 6,005 sesiones bot, 5,522 usuarios")
    if not bot_v4.empty:
        bot_sessions = bot_v4["v4_sesiones"].values[0]
        bot_users = df_v4[df_v4["Channel"] == "Bot/Spam"]["user"].nunique()
        print(f"  Este test (v4 Bot/Spam):               {bot_sessions:,} sesiones, {bot_users:,} usuarios")
        print(f"  Nota: las cifras pueden diferir ligeramente porque este test")
        print(f"  incluye el bot viejo (regla 129) que ya estaba en v3.")

    # --- Veredicto ---
    print("\n" + "=" * 70)
    print("VEREDICTO FINAL")
    print("=" * 70)
    issues = []
    if total_v3 != total_v4:
        issues.append("Total de filas difiere")
    if not cambios.empty:
        issues.append("Canales inesperados cambiaron")
    if abs(perdidos + bot_delta) != 0:
        issues.append("Desbalance Direct/Unassigned vs Bot/Spam")

    if not issues:
        print("QUERY v4 VALIDADA: los resultados son consistentes.")
        print("- Total de filas idéntico")
        print("- Solo Direct y Unassigned pierden sesiones")
        print("- Todo lo perdido va a Bot/Spam")
        print("- Demás canales intactos")
    else:
        print(f"PROBLEMAS DETECTADOS: {', '.join(issues)}")


if __name__ == "__main__":
    df_v3 = run_query(V3_PATH, "v3 (producción actual)")
    df_v4 = run_query(V4_PATH, "v4 (con reglas bot ampliadas)")
    compare(df_v3, df_v4)
