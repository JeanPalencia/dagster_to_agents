"""Alertas de observabilidad: anomalías de tráfico web vía Google Chat."""

import requests
from dagster import AssetExecutionContext, asset

from dagster_pipeline.defs.lk_visitors.assets import lk_funnel_with_channel
from dagster_pipeline.defs.lk_visitors.utils.database import query_bigquery

GOOGLE_CHAT_WEBHOOK = "https://chat.googleapis.com/v1/spaces/AAQAaTks1_A/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=JH1NWr0rmtPY4QFQ5-dHuzm0nki8RyxGiSpNAwOxC2o"

TRAFFIC_ANOMALY_SQL = """
WITH daily_traffic AS (
    SELECT
        event_date,
        primera_bifurcacion AS canal,
        COUNT(DISTINCT user) AS visitantes
    FROM `analitics_spot2.funnel_with_channel`
    WHERE event_date >= DATE_SUB(CURRENT_DATE('America/Mexico_City'), INTERVAL 31 DAY)
      AND event_date < DATE_SUB(CURRENT_DATE('America/Mexico_City'), INTERVAL 1 DAY)
    GROUP BY 1, 2
),
stats_historicas AS (
    SELECT
        canal,
        AVG(visitantes) AS media_movil_30d,
        STDDEV_SAMP(visitantes) AS stddev_30d
    FROM daily_traffic
    GROUP BY 1
),
trafico_ayer AS (
    SELECT
        primera_bifurcacion AS canal,
        COUNT(DISTINCT user) AS trafico_ayer
    FROM `analitics_spot2.funnel_with_channel`
    WHERE event_date = DATE_SUB(CURRENT_DATE('America/Mexico_City'), INTERVAL 1 DAY)
    GROUP BY 1
)
SELECT
    COALESCE(t.canal, s.canal) AS canal,
    COALESCE(t.trafico_ayer, 0) AS trafico_ayer,
    CAST(ROUND(s.media_movil_30d, 0) AS INT64) AS media_movil,
    CAST(ROUND(s.media_movil_30d + (1.5 * IFNULL(s.stddev_30d, 0)), 0) AS INT64) AS limite_superior,
    CAST(GREATEST(ROUND(s.media_movil_30d - (1.5 * IFNULL(s.stddev_30d, 0)), 0), 0) AS INT64) AS limite_inferior
FROM trafico_ayer t
FULL OUTER JOIN stats_historicas s ON t.canal = s.canal
WHERE (COALESCE(t.trafico_ayer, 0) > CAST(ROUND(s.media_movil_30d + (1.5 * IFNULL(s.stddev_30d, 0)), 0) AS INT64) -- Rompe techo
   OR COALESCE(t.trafico_ayer, 0) < CAST(GREATEST(ROUND(s.media_movil_30d - (1.5 * IFNULL(s.stddev_30d, 0)), 0), 0) AS INT64)) -- Rompe piso
   AND s.media_movil_30d > 5 
   AND COALESCE(t.canal, s.canal) NOT IN ('Other')
ORDER BY trafico_ayer DESC
"""


@asset(
    deps=[lk_funnel_with_channel],
    description="Alertas de tráfico con bandas tipo Bollinger (media móvil 30d, 1.5σ) vía Google Chat.",
)
def traffic_anomaly_alerts(context: AssetExecutionContext) -> None:
    df = query_bigquery(TRAFFIC_ANOMALY_SQL)

    if df.empty:
        context.log.info(
            "Tráfico normal: no se detectaron canales con anomalía (DataFrame vacío tras filtros)."
        )
        return

    lines = [
        "🚨 ALERTA DE ANOMALÍA DE TRÁFICO 🚨",
        "",
    ]
    for _, row in df.iterrows():
        canal = row["canal"]
        trafico_ayer = int(row["trafico_ayer"])
        limite_inferior = int(row["limite_inferior"])
        limite_superior = int(row["limite_superior"])
        if trafico_ayer > limite_superior:
            emoji = "📈"
        else:
            emoji = "📉"
        lines.append(
            f"• *{canal}*: {trafico_ayer} visitas. (Normal: {limite_inferior} a {limite_superior}) {emoji}"
        )

    mensaje = "\n".join(lines)
    requests.post(GOOGLE_CHAT_WEBHOOK, json={"text": mensaje})
    context.log.info(
        f"Alertas enviadas a Google Chat: {len(df)} canal(es) fuera de bandas (30d, 1.5σ)."
    )
