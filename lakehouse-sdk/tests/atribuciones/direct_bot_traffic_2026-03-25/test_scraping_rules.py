"""
Test comparativo: CTE scraping actual vs reglas ampliadas refinadas.

Ejecuta compare_scraping_rules.sql contra BigQuery y analiza los nuevos
usuarios que serían marcados como sospechosos, buscando falsos positivos.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[3] / "dagster-pipeline" / "src"),
)
from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

QUERY_PATH = Path(__file__).parent / "compare_scraping_rules.sql"


def run_comparison():
    print("=" * 70)
    print("TEST: Comparación CTE scraping actual vs ampliado refinado")
    print("=" * 70)

    with open(QUERY_PATH, "r", encoding="utf-8") as f:
        query = f.read()

    print("\nEjecutando query en BigQuery (solo marzo 2026)...")
    df = query_bronze_source(query, source_type="bigquery").to_pandas()
    print(f"Filas retornadas (nuevos eventos marcados): {len(df)}")

    if df.empty:
        print("\nNo hay nuevos marcados. Las reglas ampliadas no capturan nada nuevo.")
        return

    usuarios = df["user_pseudo_id"].nunique()
    print(f"Usuarios únicos nuevamente marcados: {usuarios}")

    # --- 1. Distribución por regla ---
    print("\n" + "=" * 70)
    print("1. DISTRIBUCIÓN POR REGLA")
    print("=" * 70)
    por_regla = (
        df.groupby("regla_bot")
        .agg(
            usuarios=("user_pseudo_id", "nunique"),
            sesiones=("user_pseudo_id", "count"),
        )
        .sort_values("usuarios", ascending=False)
    )
    print(por_regla.to_string())

    # --- 2. Canales afectados (falsos positivos = canales no Direct) ---
    print("\n" + "=" * 70)
    print("2. CANALES AFECTADOS POR REGLA (buscar canales != Direct/Unassigned)")
    print("=" * 70)
    por_canal = (
        df.groupby(["regla_bot", "channel_group"])
        .agg(usuarios=("user_pseudo_id", "nunique"))
        .sort_values(["regla_bot", "usuarios"], ascending=[True, False])
    )
    print(por_canal.to_string())

    falsos_canal = df[
        ~df["channel_group"].isin(["Direct", "Unassigned", None])
        & df["channel_group"].notna()
    ]
    if not falsos_canal.empty:
        print(f"\n*** ALERTA: {falsos_canal['user_pseudo_id'].nunique()} usuarios "
              f"en canales NO Direct/Unassigned serían marcados ***")
        print(falsos_canal[["regla_bot", "channel_group", "user_pseudo_id", "page_location"]]
              .drop_duplicates(subset=["user_pseudo_id"])
              .head(20)
              .to_string(index=False))
    else:
        print("\nOK: Todos los nuevos marcados están en canales Direct/Unassigned.")

    # --- 3. Pages afectadas ---
    print("\n" + "=" * 70)
    print("3. PAGES MÁS FRECUENTES POR REGLA")
    print("=" * 70)
    for regla in df["regla_bot"].unique():
        sub = df[df["regla_bot"] == regla]
        pages = (
            sub.groupby("page_location")
            .agg(eventos=("user_pseudo_id", "count"), usuarios=("user_pseudo_id", "nunique"))
            .sort_values("usuarios", ascending=False)
            .head(10)
        )
        print(f"\n--- {regla} ---")
        print(pages.to_string())

    # --- 4. Geo ---
    print("\n" + "=" * 70)
    print("4. DISTRIBUCIÓN GEOGRÁFICA POR REGLA")
    print("=" * 70)
    por_geo = (
        df.groupby(["regla_bot", "country", "city"])
        .agg(usuarios=("user_pseudo_id", "nunique"))
        .sort_values(["regla_bot", "usuarios"], ascending=[True, False])
    )
    for regla in df["regla_bot"].unique():
        print(f"\n--- {regla} ---")
        sub = por_geo.loc[regla] if regla in por_geo.index.get_level_values(0) else pd.DataFrame()
        print(sub.head(10).to_string())

    # --- 5. Engagement (falsos positivos = engagement alto) ---
    print("\n" + "=" * 70)
    print("5. ENGAGEMENT POR REGLA (engagement alto = posible falso positivo)")
    print("=" * 70)
    eng = (
        df.groupby("regla_bot")["engagement_ms"]
        .describe(percentiles=[0.25, 0.5, 0.75, 0.95])
        .round(0)
    )
    print(eng.to_string())

    # --- 6. Eventos de conversión en usuarios marcados ---
    print("\n" + "=" * 70)
    print("6. VERIFICACIÓN: ¿Algún usuario marcado tiene eventos de conversión?")
    print("=" * 70)
    marcados_ids = df["user_pseudo_id"].unique().tolist()
    sample_ids = marcados_ids[:200]
    ids_str = ", ".join([f"'{uid}'" for uid in sample_ids])

    conv_query = f"""
    SELECT
      user_pseudo_id,
      event_name,
      COUNT(*) AS total
    FROM `analytics_276054961.events_*`
    WHERE event_date BETWEEN '20260301' AND '20260325'
      AND user_pseudo_id IN ({ids_str})
      AND event_name IN (
        'clientRequestedWhatsappForm',
        'clientRequestedContactLead',
        'clientClickedRegisterForm',
        'clientSubmittedRegisterUser',
        'clientSubmittedRegisterForm',
        'lead_form_submission',
        'clientSearchedSpotSearch'
      )
    GROUP BY user_pseudo_id, event_name
    ORDER BY total DESC
    """
    conv_df = query_bronze_source(conv_query, source_type="bigquery").to_pandas()
    if conv_df.empty:
        print("OK: Ninguno de los usuarios marcados (muestra) generó eventos de conversión.")
    else:
        print(f"*** ALERTA: {conv_df['user_pseudo_id'].nunique()} usuarios marcados "
              f"tienen eventos de conversión ***")
        print(conv_df.to_string(index=False))

    # --- 7. Tabla comparativa de canales ANTES vs DESPUÉS ---
    print("\n" + "=" * 70)
    print("7. DISTRIBUCIÓN DE CANALES: ANTES vs DESPUÉS DEL FILTRADO")
    print("=" * 70)

    channel_query = """
    WITH scraping_ids AS (
      SELECT DISTINCT user_pseudo_id
      FROM `analytics_276054961.events_*`
      WHERE event_date BETWEEN '20260301' AND '20260325'
        AND event_name IN ('session_start', 'first_visit')
        AND (
          -- Regla existente
          (device.web_info.browser_version = '129.0.6668.71'
           AND session_traffic_source_last_click.cross_channel_campaign.default_channel_group IN ('Direct', 'Unassigned')
           AND event_name = 'first_visit'
           AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1) LIKE 'https://spot2.mx/%')
          -- Regla 1: Opera 128
          OR (device.web_info.browser = 'Opera'
              AND device.web_info.browser_version = '128.0.0.0'
              AND device.category = 'desktop'
              AND device.operating_system = 'Windows'
              AND session_traffic_source_last_click.cross_channel_campaign.default_channel_group IN ('Direct', 'Unassigned'))
          -- Regla 2: Chrome 146 US login
          OR (device.web_info.browser = 'Chrome'
              AND device.web_info.browser_version = '146.0.7680.80'
              AND device.category = 'desktop'
              AND device.operating_system = 'Windows'
              AND geo.country = 'United States'
              AND session_traffic_source_last_click.cross_channel_campaign.default_channel_group IN ('Direct', 'Unassigned')
              AND ((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1) LIKE '%signup=login%'
                OR (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1) LIKE '%signup=verify-otp%'))
          -- Regla 3: Mobile Android US staging
          OR (device.category = 'mobile'
              AND device.web_info.browser = 'Chrome'
              AND device.operating_system = 'Android'
              AND geo.country = 'United States'
              AND ((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1) LIKE '%vercel.app%'
                OR (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1) LIKE '%gamma.spot2.mx%'))
        )
    ),
    todas_sesiones AS (
      SELECT
        e.user_pseudo_id,
        COALESCE(
          e.session_traffic_source_last_click.cross_channel_campaign.default_channel_group,
          CASE WHEN e.traffic_source.source = '(direct)' THEN 'Direct' ELSE 'Unassigned' END
        ) AS canal,
        CASE WHEN s.user_pseudo_id IS NOT NULL THEN 1 ELSE 0 END AS es_bot
      FROM `analytics_276054961.events_*` e
      LEFT JOIN scraping_ids s ON e.user_pseudo_id = s.user_pseudo_id
      WHERE e.event_date BETWEEN '20260301' AND '20260325'
        AND e.event_name = 'session_start'
    )
    SELECT
      canal,
      COUNT(*) AS sesiones_antes,
      COUNTIF(es_bot = 0) AS sesiones_despues,
      COUNTIF(es_bot = 1) AS sesiones_bot,
      COUNT(DISTINCT user_pseudo_id) AS usuarios_antes,
      COUNT(DISTINCT CASE WHEN es_bot = 0 THEN user_pseudo_id END) AS usuarios_despues,
      COUNT(DISTINCT CASE WHEN es_bot = 1 THEN user_pseudo_id END) AS usuarios_bot
    FROM todas_sesiones
    GROUP BY canal
    ORDER BY sesiones_antes DESC
    """
    print("Ejecutando query de canales en BigQuery...")
    ch_df = query_bronze_source(channel_query, source_type="bigquery").to_pandas()

    ch_df["delta_sesiones"] = ch_df["sesiones_despues"] - ch_df["sesiones_antes"]
    ch_df["pct_antes"] = (100.0 * ch_df["sesiones_antes"] / ch_df["sesiones_antes"].sum()).round(2)
    ch_df["pct_despues"] = (100.0 * ch_df["sesiones_despues"] / ch_df["sesiones_despues"].sum()).round(2)
    ch_df["delta_pct"] = (ch_df["pct_despues"] - ch_df["pct_antes"]).round(2)

    print("\n--- SESIONES POR CANAL ---")
    print(ch_df[["canal", "sesiones_antes", "sesiones_despues", "sesiones_bot",
                 "pct_antes", "pct_despues", "delta_pct"]].to_string(index=False))

    print("\n--- USUARIOS POR CANAL ---")
    print(ch_df[["canal", "usuarios_antes", "usuarios_despues", "usuarios_bot"]].to_string(index=False))

    total_antes = ch_df["sesiones_antes"].sum()
    total_despues = ch_df["sesiones_despues"].sum()
    total_bot = ch_df["sesiones_bot"].sum()
    print(f"\nTotal sesiones antes: {total_antes:,}")
    print(f"Total sesiones después: {total_despues:,}")
    print(f"Sesiones eliminadas (bot): {total_bot:,} ({100*total_bot/total_antes:.1f}%)")

    # --- Resumen final ---
    print("\n" + "=" * 70)
    print("RESUMEN FINAL")
    print("=" * 70)
    print(f"Total usuarios nuevamente marcados: {usuarios}")
    print(f"Por regla:")
    for _, row in por_regla.iterrows():
        print(f"  {row.name}: {row['usuarios']} usuarios, {row['sesiones']} sesiones")
    falsos_count = falsos_canal["user_pseudo_id"].nunique() if not falsos_canal.empty else 0
    print(f"Falsos positivos por canal: {falsos_count}")
    conv_count = conv_df["user_pseudo_id"].nunique() if not conv_df.empty else 0
    print(f"Usuarios con conversiones: {conv_count}")
    print(f"\nVeredicto: {'REGLAS SEGURAS' if falsos_count == 0 and conv_count == 0 else 'REVISAR REGLAS'}")


if __name__ == "__main__":
    run_comparison()
