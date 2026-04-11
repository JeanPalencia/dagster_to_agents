-- q_browse_sessions.sql
-- Sesiones de browse pages a granularidad (user_pseudo_id, ga_session_id, event_date).
-- Incluye TODAS las sesiones, no solo la primera del día.
--
-- Patrones reutilizados de:
--   - funnel_with_channel_v5.sql: scraping (4 reglas), canal (12 reglas), traffic_type
--   - lead_events_from_funnel_lk.sql: subconsultas escalares, triple fallback source/medium/campaign
--
-- Ventana: 6 meses cerrados + mes actual (dinámica)

-- CTE 1: Detección de bots/scraping (mismas 4 reglas que funnel_v5)
WITH scraping AS (
  SELECT DISTINCT user_pseudo_id
  FROM `analytics_276054961.events_*`,
    UNNEST(event_params) AS ep
  WHERE
    (session_traffic_source_last_click.cross_channel_campaign.default_channel_group IN ('Direct', 'Unassigned')
      AND event_name = 'first_visit'
      AND ep.key = 'page_location'
      AND ep.value.string_value LIKE 'https://spot2.mx/%'
      AND device.web_info.browser_version = '129.0.6668.71')
    OR (device.web_info.browser = 'Opera'
      AND device.web_info.browser_version = '128.0.0.0'
      AND device.category = 'desktop'
      AND device.operating_system = 'Windows'
      AND session_traffic_source_last_click.cross_channel_campaign.default_channel_group IN ('Direct', 'Unassigned')
      AND event_date BETWEEN '20260316' AND '20260325')
    OR (device.web_info.browser = 'Chrome'
      AND device.web_info.browser_version = '146.0.7680.80'
      AND device.category = 'desktop'
      AND device.operating_system = 'Windows'
      AND geo.country = 'United States'
      AND session_traffic_source_last_click.cross_channel_campaign.default_channel_group IN ('Direct', 'Unassigned')
      AND ep.key = 'page_location'
      AND (ep.value.string_value LIKE '%signup=login%' OR ep.value.string_value LIKE '%signup=verify-otp%')
      AND event_date BETWEEN '20260316' AND '20260325')
    OR (device.category = 'mobile'
      AND device.web_info.browser = 'Chrome'
      AND device.operating_system = 'Android'
      AND geo.country = 'United States'
      AND ep.key = 'page_location'
      AND (ep.value.string_value LIKE '%vercel.app%' OR ep.value.string_value LIKE '%gamma.spot2.mx%'))
),

-- CTE 2: Eventos crudos de page_view en browse pages (subconsultas escalares)
browse_page_views AS (
  SELECT
    event_date,
    DATETIME(TIMESTAMP_MICROS(event_timestamp), "America/Mexico_City") AS event_datetime,
    user_pseudo_id,

    -- ga_session_id como STRING + flag
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING) AS ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) IS NOT NULL AS has_session_id,

    -- Página
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1) AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer' LIMIT 1) AS page_referrer,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title' LIMIT 1) AS page_title,

    -- Métricas de sesión GA4
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number' LIMIT 1) AS ga_session_number,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged' LIMIT 1) AS session_engaged,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec' LIMIT 1) AS engagement_time_msec,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances' LIMIT 1) AS entrances,

    -- Source/Medium/Campaign con triple fallback
    CASE
      WHEN session_traffic_source_last_click.cross_channel_campaign.source IS NULL
        OR TRIM(COALESCE(session_traffic_source_last_click.cross_channel_campaign.source, '')) IN ('', '(not set)')
      THEN traffic_source.source
      ELSE session_traffic_source_last_click.cross_channel_campaign.source
    END AS source,
    CASE
      WHEN session_traffic_source_last_click.cross_channel_campaign.medium IS NULL
        OR TRIM(COALESCE(session_traffic_source_last_click.cross_channel_campaign.medium, '')) IN ('', '(not set)')
      THEN traffic_source.medium
      ELSE session_traffic_source_last_click.cross_channel_campaign.medium
    END AS medium,
    CASE
      WHEN session_traffic_source_last_click.cross_channel_campaign.campaign_name IS NULL
        OR TRIM(COALESCE(session_traffic_source_last_click.cross_channel_campaign.campaign_name, '')) IN ('', '(not set)')
      THEN traffic_source.name
      ELSE session_traffic_source_last_click.cross_channel_campaign.campaign_name
    END AS campaign_name,

    -- UTMs manuales (collected_traffic_source)
    collected_traffic_source.manual_source AS source_collected,
    collected_traffic_source.manual_medium AS medium_collected,
    collected_traffic_source.manual_campaign_name AS campaign_name_collected,

    -- Canal GA4 nativo
    session_traffic_source_last_click.cross_channel_campaign.default_channel_group AS ga4_channel_group,

    -- Dispositivo y geo
    device.category AS device_category,
    device.web_info.browser AS browser,
    device.operating_system AS operating_system,
    geo.country AS country,

    -- Contacto (si está en el evento)
    (SELECT COALESCE(
        SAFE_CAST(value.int_value AS STRING),
        NULLIF(TRIM(value.string_value), '')
      )
      FROM UNNEST(event_params)
      WHERE key = 'phone'
        AND (value.int_value IS NOT NULL OR (value.string_value IS NOT NULL AND TRIM(value.string_value) != ''))
      LIMIT 1
    ) AS phone,
    (SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'email'
        AND value.string_value IS NOT NULL AND TRIM(value.string_value) != ''
      LIMIT 1
    ) AS email

  FROM `analytics_276054961.events_*`
  WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d',
          DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH))
    AND _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    AND event_name = 'page_view'
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1),
      r'https://spot2\.mx/(bodegas|coworking|locales-comerciales|naves-industriales|oficinas|renta|terrenos|venta)(/|$|\?)'
    )
),

-- CTE 3: Agregación a nivel sesión
session_agg AS (
  SELECT
    event_date,
    user_pseudo_id,
    ga_session_id,
    has_session_id,

    -- Timestamps
    MIN(event_datetime) AS session_start,
    MAX(event_datetime) AS session_end,
    DATETIME_DIFF(MAX(event_datetime), MIN(event_datetime), SECOND) AS session_duration_sec,

    -- Métricas de browse
    COUNT(*) AS browse_page_views,
    COUNT(DISTINCT page_location) AS distinct_browse_pages,
    ARRAY_AGG(page_location ORDER BY event_datetime LIMIT 1)[SAFE_OFFSET(0)] AS first_browse_page,
    ARRAY_AGG(page_location ORDER BY event_datetime DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_browse_page,
    STRING_AGG(DISTINCT page_location, ', ') AS all_browse_pages,

    -- Session engagement
    MAX(ga_session_number) AS ga_session_number,
    MAX(session_engaged) AS session_engaged,
    SUM(engagement_time_msec) AS total_engagement_time_msec,

    -- Atribución (del primer evento de la sesión)
    ARRAY_AGG(source ORDER BY event_datetime LIMIT 1)[SAFE_OFFSET(0)] AS source,
    ARRAY_AGG(medium ORDER BY event_datetime LIMIT 1)[SAFE_OFFSET(0)] AS medium,
    ARRAY_AGG(campaign_name ORDER BY event_datetime LIMIT 1)[SAFE_OFFSET(0)] AS campaign_name,
    ARRAY_AGG(source_collected ORDER BY event_datetime LIMIT 1)[SAFE_OFFSET(0)] AS source_collected,
    ARRAY_AGG(medium_collected ORDER BY event_datetime LIMIT 1)[SAFE_OFFSET(0)] AS medium_collected,
    ARRAY_AGG(campaign_name_collected ORDER BY event_datetime LIMIT 1)[SAFE_OFFSET(0)] AS campaign_name_collected,
    ARRAY_AGG(ga4_channel_group ORDER BY event_datetime LIMIT 1)[SAFE_OFFSET(0)] AS ga4_channel_group,
    ARRAY_AGG(page_referrer ORDER BY event_datetime LIMIT 1)[SAFE_OFFSET(0)] AS page_referrer,

    -- Dispositivo y geo (consistente por sesión)
    ANY_VALUE(device_category) AS device_category,
    ANY_VALUE(browser) AS browser,
    ANY_VALUE(operating_system) AS operating_system,
    ANY_VALUE(country) AS country,

    -- Contacto (cualquier no-null en la sesión)
    MAX(phone) AS phone,
    MAX(email) AS email

  FROM browse_page_views
  GROUP BY event_date, user_pseudo_id, ga_session_id, has_session_id
),

-- CTE 4: Eventos de conversión por sesión (18 eventos canónicos)
conversion_in_session AS (
  SELECT
    user_pseudo_id,
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS STRING) AS ga_session_id,
    event_date,
    COUNT(*) AS conversion_event_count,
    STRING_AGG(DISTINCT event_name, ', ') AS conversion_event_names
  FROM `analytics_276054961.events_*`
  WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d',
          DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH))
    AND _TABLE_SUFFIX <= FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    AND event_name IN (
      'clientRequestedWhatsappForm',
      'clientRequestedContactLead',
      'clientClickedRegisterForm',
      'clientSubmittedRegisterUser',
      'clientClickedRegisterUser',
      'clientSubmittedRegisterForm',
      'lead_form_submission',
      'clientSubmitFormBlogRetail',
      'clientSubmitFormBlogOficinas',
      'clientSubmitFormBlogIndustrial',
      'clientClickedLeadPopup',
      'spotMapSearch',
      'clientClickedSendInfoModalConsulting',
      'clientSubmitStep1Industrial',
      'clientSubmitStep1Retail',
      'clientSubmitStep1Oficinas',
      'clientSubmittedBudgetFormBP',
      'clientSearchedSpotSearch'
    )
  GROUP BY user_pseudo_id, ga_session_id, event_date
),

-- CTE 5: Clasificación de canal (12 reglas de funnel_v5)
with_channel AS (
  SELECT
    s.*,

    -- is_scraping flag
    CASE WHEN scr.user_pseudo_id IS NOT NULL THEN 1 ELSE 0 END AS is_scraping,

    -- Conversión en sesión
    COALESCE(c.conversion_event_count, 0) AS conversion_event_count,
    c.conversion_event_names,
    COALESCE(c.conversion_event_count, 0) > 0 AS has_conversion_in_session,

    -- Canal (12 reglas, misma prioridad que funnel_v5)
    CASE
      WHEN scr.user_pseudo_id IS NOT NULL THEN 'Bot/Spam'
      WHEN REGEXP_CONTAINS(LOWER(s.source),
        r'(chatgpt|perplexity|claude|bard|gemini|anthropic|openai|llm|ai\s*chat|ai\s*assistant)'
      ) THEN 'Organic LLMs'
      WHEN COALESCE(TRIM(s.medium), '') = ''
        OR ((LOWER(s.source) LIKE '%l.wl.co%' OR LOWER(s.source) LIKE 't.co%'
          OR LOWER(s.source) LIKE '%github.com%' OR LOWER(s.source) LIKE '%statics.teams.cdn.office.net%')
          AND s.medium = 'referral')
      THEN 'Nulo-Vacío'
      WHEN (s.campaign_name = '(organic)' AND LOWER(s.source) NOT IN ('adwords') AND LOWER(s.medium) NOT IN ('spot'))
        OR s.medium = 'organic'
        OR ((COALESCE(s.source, '') = '') AND (COALESCE(s.medium, '') = '') AND LOWER(s.campaign_name) NOT LIKE '%comunicado%')
        OR ((LOWER(s.source) LIKE '%search.yam.com%' OR LOWER(s.source) LIKE '%copilot.microsoft.com%'
          OR LOWER(s.source) LIKE '%search.google.com' OR LOWER(s.source) LIKE '%msn.com')
          AND s.medium = 'referral')
        OR (REGEXP_CONTAINS(LOWER(s.source), r'(google|bing|yahoo|duckduckgo|ecosia|search|adwords)')
          AND s.medium NOT IN ('cpc', 'spot', 'paid', 'referral')
          AND REGEXP_CONTAINS(LOWER(s.medium), r'(cpc,|spot,|paid,|(not set))') = FALSE)
        OR (REGEXP_CONTAINS(LOWER(s.source), r'(search.yahoo.com)') AND s.medium = 'referral')
      THEN 'Organic Search'
      WHEN LOWER(s.source) = 'mail'
        OR (REGEXP_CONTAINS(LOWER(s.source),
          r'(gmail|outlook|yahoo|hotmail|protonmail|icloud|zoho|mail\.ru|yandex|aol|live\.com|office365|exchange|mailchimp|sendgrid|mailgun|postmark|amazonses|sendinblue|brevo|active_campaign|active_campaing)')
          AND REGEXP_CONTAINS(LOWER(s.source), r'(search.)') = FALSE)
      THEN 'Mail'
      WHEN LOWER(s.campaign_name) LIKE '%_display_%' OR LOWER(s.campaign_name) LIKE '%_disp_%'
      THEN 'Display'
      WHEN s.source = '(direct)' THEN 'Direct'
      WHEN (s.campaign_name = '(referral)' OR LOWER(s.medium) LIKE '%referral%')
        AND NOT REGEXP_CONTAINS(LOWER(s.source),
          r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)')
      THEN 'Referral'
      WHEN LOWER(s.campaign_name) LIKE '%cross-network%'
        OR LOWER(s.campaign_name) LIKE '%pmax%'
        OR LOWER(s.source) LIKE '%syndicatedsearch.goog%'
        OR (LOWER(s.source) LIKE '%google%' AND s.medium IN ('', 'cross-network'))
        OR (LOWER(s.source) LIKE '%nova.taboolanews.com%' AND s.medium = 'referral')
      THEN 'Cross-network'
      WHEN LOWER(s.campaign_name) LIKE '%youtube%'
        OR REGEXP_CONTAINS(LOWER(s.campaign_name), r'yt_')
        OR REGEXP_CONTAINS(LOWER(s.campaign_name), r'_yt')
      THEN 'Paid Video'
      WHEN (s.medium IN ('cpc', 'spot', 'paid')
        AND (REGEXP_CONTAINS(LOWER(s.source), r'(google|bing|yahoo|duckduckgo|ecosia|search\.)')
          OR LOWER(s.campaign_name) LIKE '%search%' OR LOWER(s.campaign_name) LIKE '%srch%'))
        OR (LOWER(s.source) = 'adwords' AND LOWER(s.medium) = '(not set)')
      THEN 'Paid Search'
      WHEN s.medium IN ('cpc', 'paid', 'paid_social')
        AND REGEXP_CONTAINS(LOWER(s.source),
          r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)')
      THEN 'Paid Social'
      WHEN s.medium IN ('social', 'rss')
        OR (REGEXP_CONTAINS(LOWER(s.source),
          r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)')
          AND s.medium NOT IN ('cpc', 'paid')
          AND REGEXP_CONTAINS(LOWER(s.campaign_name), r'(meta)') = FALSE)
      THEN 'Organic Social'
      ELSE 'Unassigned'
    END AS channel

  FROM session_agg s
  LEFT JOIN scraping scr ON s.user_pseudo_id = scr.user_pseudo_id
  LEFT JOIN conversion_in_session c
    ON s.user_pseudo_id = c.user_pseudo_id
    AND s.ga_session_id = c.ga_session_id
    AND s.event_date = c.event_date
)

-- SELECT final: agregar traffic_type y flags
SELECT
  *,
  CASE
    WHEN channel = 'Bot/Spam' THEN 'Bot/Spam'
    WHEN LOWER(campaign_name) LIKE '%demandgen%' THEN 'Paid'
    WHEN channel = 'Organic Search' THEN 'Organic'
    WHEN channel = 'Display' THEN 'Paid'
    WHEN channel = 'Direct' THEN 'Organic'
    WHEN channel = 'Referral' THEN 'Organic'
    WHEN channel = 'Cross-network' THEN 'Paid'
    WHEN channel = 'Paid Video' THEN 'Paid'
    WHEN channel = 'Paid Search' THEN 'Paid'
    WHEN channel = 'Paid Social' THEN 'Paid'
    WHEN channel = 'Organic Social' THEN 'Organic'
    WHEN channel = 'Organic LLMs' THEN 'Organic'
    WHEN channel = 'Mail' THEN 'Organic'
    ELSE 'Unassigned'
  END AS traffic_type,
  CASE
    WHEN first_browse_page LIKE '%localhost%'
      OR first_browse_page LIKE '%staging%'
      OR first_browse_page LIKE '%vercel.app%'
    THEN 1 ELSE 0
  END AS is_technical_waste
FROM with_channel
ORDER BY session_start DESC
