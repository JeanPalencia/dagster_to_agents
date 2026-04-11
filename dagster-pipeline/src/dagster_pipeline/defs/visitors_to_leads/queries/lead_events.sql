-- Query to capture specific lead events with user and timestamp
WITH target_events AS (
  SELECT -- User identification
    user_pseudo_id,
    -- Event details
    event_name,
    event_date,
    event_timestamp,
    -- Convert timestamp to readable datetime in Mexico City timezone (precision: seconds)
    DATETIME(
      TIMESTAMP_MICROS(event_timestamp),
      "America/Mexico_City"
    ) AS event_datetime,
    -- Extract email if available (only non-null and non-empty)
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'email'
        AND value.string_value IS NOT NULL
        AND TRIM(value.string_value) != ''
      LIMIT 1
    ) AS email,
    -- Extract phone: BigQuery guarda teléfonos en int_value o string_value; unificar a STRING
    (
      SELECT COALESCE(
          SAFE_CAST(value.int_value AS STRING),
          NULLIF(TRIM(value.string_value), '')
        )
      FROM UNNEST(event_params)
      WHERE key = 'phone'
        AND (
          value.int_value IS NOT NULL
          OR (value.string_value IS NOT NULL AND TRIM(value.string_value) != '')
        )
      LIMIT 1
    ) AS phone,
    -- Extract from if available (only non-null and non-empty)
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'from'
        AND value.string_value IS NOT NULL
        AND TRIM(value.string_value) != ''
      LIMIT 1
    ) AS from_param,
    -- Extract spotId if available (int or string value)
    (
      SELECT COALESCE(
          CAST(value.int_value AS STRING),
          value.string_value
        )
      FROM UNNEST(event_params)
      WHERE key = 'spotId'
        AND (
          value.int_value IS NOT NULL
          OR (
            value.string_value IS NOT NULL
            AND TRIM(value.string_value) != ''
          )
        )
      LIMIT 1
    ) AS spotId,
    -- Extract page location if available
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'page_location'
      LIMIT 1
    ) AS page_location,
    -- Extract page referrer if available
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'page_referrer'
      LIMIT 1
    ) AS page_referrer,
    -- Traffic source information
    session_traffic_source_last_click.cross_channel_campaign.source AS source,
    session_traffic_source_last_click.cross_channel_campaign.medium AS medium,
    session_traffic_source_last_click.cross_channel_campaign.campaign_name AS campaign_name,
    -- Device information
    device.category AS device_category
  FROM `analytics_276054961.events_*`
  WHERE -- Filter by target event names
    event_name IN (
      'clientRequestedWhatsappForm', -- WhatsApp
      'clientSubmitFormBlogRetail', -- Landing de Retail
      'clientSubmitFormBlogOficinas', -- Landing de Oficinas
      'clientSubmitFormBlogIndustrial', -- Landing de Industrial
      'clientRequestedContactLead', -- Botón Contactar
      'clientClickedLeadPopup', -- PoPup 100 segundos
      'spotMapSearch', -- PoPup 100 segundos
      'clientClickedRegisterForm', -- Login Usuarios
      'clientClickedSendInfoModalConsulting', -- Modal Solicitar Consultoría
      'clientSubmitStep1Industrial', -- Evento Landing Industrial
      'clientSubmitStep1Retail', -- Evento Landing Retail
      'clientSubmitStep1Oficinas', -- Evento Landing Oficinas
      'clientSubmittedBudgetFormBP' -- Botón consulta Superficie necesitada
    )
    AND event_date >= '20250201'
),

events_with_channel AS (
  SELECT event_date,
    event_datetime,
    user_pseudo_id,
    email,
    phone,
    from_param,
    spotId,
    event_name,
    page_location,
    page_referrer,
    source,
    medium,
    campaign_name,
    device_category,
    -- Channel classification based on source, medium, and campaign_name
    CASE
      -- 1️⃣ LLMs PRIMERO (ChatGPT, Gemini, etc. — llegan frecuentemente con medium vacío)
      WHEN REGEXP_CONTAINS(LOWER(source),
        r'(chatgpt|perplexity|claude|bard|gemini|anthropic|openai|llm|ai\s*chat|ai\s*assistant)'
      ) THEN 'Organic LLMs'

      -- 2️⃣ Vacíos o nulos (ahora ya sin LLMs atrapados)
      WHEN COALESCE(TRIM(medium), '') = ''
       or ((LOWER(source) like '%l.wl.co%' or LOWER(source) like 't.co%' or LOWER(source) like '%github.com%' 
        or  LOWER(source) like '%statics.teams.cdn.office.net%') and medium = 'referral')
       THEN 'Nulo-Vacío' 

      -- 3️⃣ Búsqueda orgánica
      WHEN (campaign_name = '(organic)' and lower(source) not in ('adwords') and lower(medium) not in ('spot')) 
        OR medium = 'organic'
        OR (
          (COALESCE(source, '') = '')
          AND (COALESCE(medium, '') = '')
          AND LOWER(campaign_name) NOT LIKE '%comunicado%'
        )
        or ((LOWER(source) like '%search.yam.com%' or LOWER(source) like '%copilot.microsoft.com%' 
         or LOWER(source) like '%search.google.com' or LOWER(source) like '%msn.com' ) and medium = 'referral')
        or(
        
         REGEXP_CONTAINS(LOWER(source),
          r'(google|bing|yahoo|duckduckgo|ecosia|search|adwords)') and medium not in  ('cpc', 'spot', 'paid','referral')
          and REGEXP_CONTAINS(LOWER(medium),
          r'(cpc,|spot,|paid,|(not set))')= false)

         or (REGEXP_CONTAINS(LOWER(source),r'(search.yahoo.com)')  and medium = 'referral')
               THEN 'Organic Search'

      -- 4️⃣ Correo electrónico
      WHEN LOWER(source) = 'mail'
        OR (REGEXP_CONTAINS(LOWER(source),
          r'(gmail|outlook|yahoo|hotmail|protonmail|icloud|zoho|mail\.ru|yandex|aol|live\.com|office365|exchange|mailchimp|sendgrid|mailgun|postmark|amazonses|sendinblue|brevo|active_campaign|active_campaing)') and REGEXP_CONTAINS(LOWER(source), r'(search.)') = false
        )
      THEN 'Mail'

      -- 5️⃣ Display / banners
      WHEN LOWER(campaign_name) LIKE '%_display_%' 
        or LOWER(campaign_name) LIKE '%_disp_%' 
       THEN 'Display'

      -- 6️⃣ Directo
      WHEN source = '(direct)' THEN 'Direct'

      -- 7️⃣ Referidos
     WHEN (campaign_name = '(referral)'
      OR LOWER(medium) LIKE '%referral%')
       AND NOT REGEXP_CONTAINS(
        LOWER(source),
        r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)'
      ) THEN 'Referral'

      -- 8️⃣ Cross-network (PMAX, etc.)
      WHEN LOWER(campaign_name) LIKE '%cross-network%'
        OR LOWER(campaign_name) LIKE '%pmax%'
        OR LOWER(source) LIKE '%syndicatedsearch.goog%'
        OR (LOWER(source) LIKE '%google%' AND medium IN ('', 'cross-network'))
        or (LOWER(source) LIKE '%nova.taboolanews.com%' and medium = 'referral')
      THEN 'Cross-network' 

      -- 9️⃣ Video (YouTube, etc.)
      WHEN LOWER(campaign_name) LIKE '%youtube%'
        OR REGEXP_CONTAINS(LOWER(campaign_name), r'yt_')

      THEN 'Paid Video'

      -- 🔟 Búsqueda paga
      WHEN medium IN ('cpc', 'spot', 'paid')
        AND (REGEXP_CONTAINS(LOWER(source),
          r'(google|bing|yahoo|duckduckgo|ecosia|search\.)'
        )
        OR LOWER(campaign_name) LIKE '%search%' OR LOWER(campaign_name) LIKE '%srch%') 
        or (LOWER(source) = 'adwords' and lower(medium) = '(not set)')
      THEN 'Paid Search'

      -- 11️⃣ Social pago
      WHEN medium IN ('cpc', 'paid','paid_social')
        AND REGEXP_CONTAINS(LOWER(source),
          r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)'
        )
      THEN 'Paid Social'

      -- 12️⃣ Social orgánico
      WHEN medium IN ('social', 'rss')
        OR (
          REGEXP_CONTAINS(LOWER(source),
            r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)'
          )
          AND medium NOT IN ('cpc', 'paid')
          and REGEXP_CONTAINS(LOWER(campaign_name),r'(meta)') = false
          
        )
      THEN 'Organic Social'

      ELSE 'Unassigned'
    END AS channel
  FROM target_events
)
SELECT event_date,
  event_datetime,
  user_pseudo_id,
  email,
  phone,
  from_param,
  spotId,
  event_name,
  page_location,
  page_referrer,
  source,
  medium,
  campaign_name,
  device_category,
  channel,
  -- Traffic type classification based on channel and campaign_name
  CASE
    -- Demand campaign_name -> Paid (nueva regla)
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
  END AS traffic_type
FROM events_with_channel
ORDER BY event_datetime DESC