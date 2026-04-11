-- Compara el CTE scraping actual vs el ampliado con reglas refinadas.
-- Solo retorna filas donde la clasificación cambió (nuevos marcados).
-- Acotado a marzo 2026 para controlar costos de escaneo.

WITH sesiones AS (
  SELECT
    user_pseudo_id,
    event_date,
    event_name,
    device.category AS device_category,
    device.web_info.browser AS browser,
    device.web_info.browser_version AS browser_version,
    device.operating_system AS os,
    geo.country AS country,
    geo.city AS city,
    session_traffic_source_last_click.cross_channel_campaign.default_channel_group AS channel_group,
    traffic_source.source AS traffic_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1) AS page_location,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec' LIMIT 1) AS engagement_ms
  FROM `analytics_276054961.events_*`
  WHERE event_date BETWEEN '20260301' AND '20260325'
    AND event_name IN ('session_start', 'first_visit')
),

-- Regla actual: solo browser_version viejo
scraping_actual AS (
  SELECT DISTINCT user_pseudo_id
  FROM sesiones
  WHERE channel_group IN ('Direct', 'Unassigned')
    AND event_name = 'first_visit'
    AND page_location LIKE 'https://spot2.mx/%'
    AND browser_version = '129.0.6668.71'
),

-- Reglas ampliadas refinadas
scraping_ampliado AS (
  SELECT DISTINCT user_pseudo_id,
    CASE
      -- Regla existente
      WHEN browser_version = '129.0.6668.71'
        AND channel_group IN ('Direct', 'Unassigned')
        AND event_name = 'first_visit'
        AND page_location LIKE 'https://spot2.mx/%'
        THEN 'bot_viejo_129'

      -- Regla 1: Opera 128 en Windows Desktop + canal Direct/Unassigned
      WHEN browser = 'Opera'
        AND browser_version = '128.0.0.0'
        AND device_category = 'desktop'
        AND os = 'Windows'
        AND channel_group IN ('Direct', 'Unassigned')
        THEN 'bot_opera_128'

      -- Regla 2: Chrome Desktop Windows US + browser_version específica del bot + login/OTP
      WHEN browser = 'Chrome'
        AND browser_version = '146.0.7680.80'
        AND device_category = 'desktop'
        AND os = 'Windows'
        AND country = 'United States'
        AND channel_group IN ('Direct', 'Unassigned')
        AND (page_location LIKE '%signup=login%' OR page_location LIKE '%signup=verify-otp%')
        THEN 'bot_chrome_us_login'

      -- Regla 3: Mobile Chrome Android US en URLs de staging
      WHEN device_category = 'mobile'
        AND browser = 'Chrome'
        AND os = 'Android'
        AND country = 'United States'
        AND (page_location LIKE '%vercel.app%' OR page_location LIKE '%gamma.spot2.mx%')
        THEN 'bot_mobile_us_staging'

      ELSE NULL
    END AS regla_bot
  FROM sesiones
),

-- Solo los user_pseudo_id que serían NUEVAMENTE marcados (no estaban en el CTE actual)
nuevos_marcados AS (
  SELECT
    sa.user_pseudo_id,
    sa.regla_bot
  FROM scraping_ampliado sa
  LEFT JOIN scraping_actual sc ON sa.user_pseudo_id = sc.user_pseudo_id
  WHERE sa.regla_bot IS NOT NULL
    AND sc.user_pseudo_id IS NULL
)

SELECT
  s.user_pseudo_id,
  s.event_date,
  nm.regla_bot,
  s.channel_group,
  s.traffic_source,
  s.browser,
  s.browser_version,
  s.os,
  s.device_category,
  s.country,
  s.city,
  s.page_location,
  s.engagement_ms
FROM sesiones s
INNER JOIN nuevos_marcados nm ON s.user_pseudo_id = nm.user_pseudo_id
WHERE s.event_name = 'session_start'
ORDER BY nm.regla_bot, s.event_date, s.user_pseudo_id
