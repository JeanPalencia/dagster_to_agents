"""
Genera gráfico de sesiones diarias por canal Direct vs Bot/Spam en marzo 2026.
Permite verificar visualmente que Direct volvió a niveles normales.
"""

import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[4] / "dagster-pipeline" / "src"),
)
from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

OUTPUT_DIR = Path(__file__).parent


def fetch_daily_data() -> pd.DataFrame:
    query = """
    SELECT
      event_date,
      Channel,
      COUNT(*) AS sesiones
    FROM `analitics_spot2.funnel_with_channel`
    WHERE event_date >= '2026-02-15'
    GROUP BY event_date, Channel
    ORDER BY event_date
    """
    return query_bronze_source(query, source_type="bigquery").to_pandas()


def plot_direct_vs_bot(df: pd.DataFrame):
    canales_interes = ["Direct", "Bot/Spam"]
    df_filtrado = df[df["Channel"].isin(canales_interes)].copy()
    df_filtrado["event_date"] = pd.to_datetime(df_filtrado["event_date"])

    pivot = df_filtrado.pivot_table(
        index="event_date", columns="Channel", values="sesiones", fill_value=0
    )

    if "Bot/Spam" not in pivot.columns:
        pivot["Bot/Spam"] = 0

    fig, ax = plt.subplots(figsize=(16, 6))

    ax.bar(pivot.index, pivot.get("Bot/Spam", 0), label="Bot/Spam", color="#e74c3c", alpha=0.7)
    ax.bar(pivot.index, pivot["Direct"], bottom=pivot.get("Bot/Spam", 0),
           label="Direct (limpio)", color="#3498db", alpha=0.8)

    ax.set_title("Sesiones diarias: Direct vs Bot/Spam (feb-mar 2026)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Sesiones")
    ax.legend(loc="upper left")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    plt.xticks(rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    output_path = OUTPUT_DIR / "grafico_direct_diario.png"
    fig.savefig(output_path, dpi=150)
    print(f"Gráfico guardado en: {output_path}")
    plt.close()


def plot_all_channels(df: pd.DataFrame):
    top_canales = ["Paid Search", "Organic Search", "Direct", "Bot/Spam",
                   "Paid Social", "Referral", "Organic LLMs"]
    df_filtrado = df[df["Channel"].isin(top_canales)].copy()
    df_filtrado["event_date"] = pd.to_datetime(df_filtrado["event_date"])

    pivot = df_filtrado.pivot_table(
        index="event_date", columns="Channel", values="sesiones", fill_value=0
    )

    fig, ax = plt.subplots(figsize=(16, 7))
    colors = {
        "Paid Search": "#2ecc71",
        "Organic Search": "#27ae60",
        "Direct": "#3498db",
        "Bot/Spam": "#e74c3c",
        "Paid Social": "#9b59b6",
        "Referral": "#f39c12",
        "Organic LLMs": "#1abc9c",
    }

    for canal in top_canales:
        if canal in pivot.columns:
            ax.plot(pivot.index, pivot[canal], label=canal,
                    color=colors.get(canal, "#95a5a6"), linewidth=2 if canal in ["Direct", "Bot/Spam"] else 1)

    ax.set_title("Sesiones diarias por canal (feb-mar 2026)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Sesiones")
    ax.legend(loc="upper left", fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    plt.xticks(rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    output_path = OUTPUT_DIR / "grafico_canales_diario.png"
    fig.savefig(output_path, dpi=150)
    print(f"Gráfico guardado en: {output_path}")
    plt.close()


def main():
    print("Consultando datos diarios de funnel_with_channel...")
    df = fetch_daily_data()
    print(f"Filas: {len(df):,}")

    print("\n--- Sesiones Direct por día (marzo) ---")
    direct = df[(df["Channel"] == "Direct") & (df["event_date"] >= "2026-03-01")].sort_values("event_date")
    print(direct[["event_date", "sesiones"]].to_string(index=False))

    bot = df[(df["Channel"] == "Bot/Spam")].sort_values("event_date")
    if not bot.empty:
        print("\n--- Sesiones Bot/Spam por día ---")
        print(bot[["event_date", "sesiones"]].to_string(index=False))

    print("\nGenerando gráficos...")
    plot_direct_vs_bot(df)
    plot_all_channels(df)
    print("Listo.")


if __name__ == "__main__":
    main()
