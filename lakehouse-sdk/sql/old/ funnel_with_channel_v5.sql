--- Aca tomo a todos los usuarios
--- Me fijo en el dia cual fue la primera interacción que tuvo con nosotros, por donde vino....  
with pre_users_sin_exluir as (
  select event_date,
    user_pseudo_id,
    min(event_timestamp) as event_timestamp_day
  FROM `analytics_276054961.events_*`,
    UNNEST(event_params) AS ep
  WHERE event_date >= '20250201'
    and event_date < FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    AND ep.key IN ('ga_session_id')
  GROUP BY event_date,
    user_pseudo_id
),
--excluimos lo que tenemos sospechas que son scraping
scraping as (
  select distinct user_pseudo_id
  FROM `analytics_276054961.events_*`,
    UNNEST(event_params) AS ep
  where
    -- Regla original: bot viejo browser 129
    (session_traffic_source_last_click.cross_channel_campaign.default_channel_group in ('Direct', 'Unassigned')
      and event_name = 'first_visit'
      and ep.key = 'page_location'
      and ep.value.string_value like 'https://spot2.mx/%'
      and device.web_info.browser_version = '129.0.6668.71')
    -- Regla 1: Opera 128 Windows Desktop (login/OTP brute force) — acotada a marzo 16-25
    OR (device.web_info.browser = 'Opera'
      and device.web_info.browser_version = '128.0.0.0'
      and device.category = 'desktop'
      and device.operating_system = 'Windows'
      and session_traffic_source_last_click.cross_channel_campaign.default_channel_group in ('Direct', 'Unassigned')
      and event_date between '20260316' and '20260325')
    -- Regla 2: Chrome 146.0.7680.80 Desktop Windows US en login/OTP — acotada a marzo 16-25
    OR (device.web_info.browser = 'Chrome'
      and device.web_info.browser_version = '146.0.7680.80'
      and device.category = 'desktop'
      and device.operating_system = 'Windows'
      and geo.country = 'United States'
      and session_traffic_source_last_click.cross_channel_campaign.default_channel_group in ('Direct', 'Unassigned')
      and ep.key = 'page_location'
      and (ep.value.string_value like '%signup=login%' or ep.value.string_value like '%signup=verify-otp%')
      and event_date between '20260316' and '20260325')
    -- Regla 3: Mobile Chrome Android US en URLs de staging
    OR (device.category = 'mobile'
      and device.web_info.browser = 'Chrome'
      and device.operating_system = 'Android'
      and geo.country = 'United States'
      and ep.key = 'page_location'
      and (ep.value.string_value like '%vercel.app%' or ep.value.string_value like '%gamma.spot2.mx%'))
),
pre_users as (
  select a11.*,
    case
      when a12.user_pseudo_id is null then 0
      else 1
    end as user_sospechoso
  from pre_users_sin_exluir a11
    left join scraping a12 on a11.user_pseudo_id = a12.user_pseudo_id
),
users AS (
  SELECT a11.event_date,
    a11.user_pseudo_id,
    -- Fuente con Fallback
    CASE
      WHEN a11.session_traffic_source_last_click.cross_channel_campaign.source IS NULL
      OR TRIM(
        COALESCE(
          a11.session_traffic_source_last_click.cross_channel_campaign.source,
          ''
        )
      ) IN ('', '(not set)') THEN a11.traffic_source.source
      ELSE a11.session_traffic_source_last_click.cross_channel_campaign.source
    END AS source,
    a11.collected_traffic_source.manual_source as source_collected,
    -- Medio con Fallback
    CASE
      WHEN a11.session_traffic_source_last_click.cross_channel_campaign.medium IS NULL
      OR TRIM(
        COALESCE(
          a11.session_traffic_source_last_click.cross_channel_campaign.medium,
          ''
        )
      ) IN ('', '(not set)') THEN a11.traffic_source.medium
      ELSE a11.session_traffic_source_last_click.cross_channel_campaign.medium
    END AS medium,
    a11.collected_traffic_source.manual_medium as medium_collected,
    -- Campaña con Fallback
    CASE
      WHEN a11.session_traffic_source_last_click.cross_channel_campaign.campaign_name IS NULL
      OR TRIM(
        COALESCE(
          a11.session_traffic_source_last_click.cross_channel_campaign.campaign_name,
          ''
        )
      ) IN ('', '(not set)') THEN a11.traffic_source.name
      ELSE a11.session_traffic_source_last_click.cross_channel_campaign.campaign_name
    END AS campaign_name,
    a11.collected_traffic_source.manual_campaign_name as campaign_name_collected,
    a11.session_traffic_source_last_click.cross_channel_campaign.default_channel_group as ga4_channel_group,
    a12.user_sospechoso,
    MIN(
      CASE
        WHEN ep.key = 'ga_session_id' THEN CONCAT(
          a11.user_pseudo_id,
          "-",
          CAST(ep.value.int_value AS STRING)
        )
      END
    ) AS id_sesion,
    -- Punto de entrada: la primera página por sesión
    STRING_AGG(
      DISTINCT CASE
        WHEN ep.key = 'page_location' THEN ep.value.string_value
      END,
      ', '
    ) AS page_locations
  FROM `analytics_276054961.events_*` a11,
    UNNEST(event_params) AS ep
    inner join pre_users a12 on a11.event_date = a12.event_date
    and a11.user_pseudo_id = a12.user_pseudo_id
    and a11.event_timestamp = a12.event_timestamp_day
  WHERE a11.event_date >= '20250201'
    and a11.event_date < FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    AND ep.key IN ('ga_session_id', 'page_location')
  GROUP BY event_date,
    user_pseudo_id,
    source,
    medium,
    campaign_name,
    session_traffic_source_last_click.cross_channel_campaign.default_channel_group,
    collected_traffic_source.manual_source,
    collected_traffic_source.manual_medium,
    collected_traffic_source.manual_campaign_name,
    user_sospechoso
),
-- Acá le asigno a mis usuarios su primera bifurcación del funnel
primera_bifurcacion AS (
  SELECT event_date,
    user_pseudo_id,
    id_sesion,
    page_locations,
    CASE
      WHEN page_locations LIKE '%localhost%'
      OR page_locations LIKE '%staging%'
      OR page_locations LIKE '%vercel.app%'
      OR page_locations LIKE '%signup=login%'
      OR page_locations LIKE '%signup=verify-otp%' THEN 'Technical Waste'
      WHEN
      (
        page_locations like '%https://spot2.mx/landing-industrial%'
        or page_locations like '%https://spot2.mx/landing-oficinas%'
        or page_locations like '%https://spot2.mx/landing-oficinas%'
        or page_locations like '%https://spot2.mx/landings/industrial%'
        or page_locations like '%https://spot2.mx/landings/oficinas%'
        or page_locations like '%https://spot2.mx/landings/locales-comerciales%'
      )
      or (
        (
          page_locations LIKE 'https://spot2.mx/oficinas%'
          or page_locations LIKE 'https://spot2.mx/locales-comerciales%'
          or page_locations LIKE 'https://spot2.mx/naves-industriales%'
          or page_locations LIKE 'https://spot2.mx/terrenos%'
        )
        and event_date <= '20250813'
      ) THEN 'Landing'
      WHEN (
        page_locations like '%https://blog.spot2.mx/%'
        or page_locations like '%https://spot2.mx/blog%'
      ) THEN 'Blog'
      WHEN (
        page_locations like '%news.spot2.mx%'
        or page_locations like '%/noticias%'
      ) THEN 'News & PR'
      WHEN page_locations like '%https://spot2.mx/publica-tu-espacio%' THEN 'Supply'
      WHEN (
        page_locations LIKE 'https://spot2.mx/oficinas%'
        or page_locations LIKE 'https://spot2.mx/locales-comerciales%'
        or page_locations LIKE 'https://spot2.mx/naves-industriales%'
        or page_locations LIKE 'https://spot2.mx/terrenos%'
        or page_locations LIKE 'https://spot2.mx/renta%'
        or page_locations LIKE 'https://spot2.mx/venta%'
        or page_locations LIKE 'https://spot2.mx/bodegas%'
        or page_locations LIKE 'https://spot2.mx/coworking%'
      )
      and event_date >= '20250813' THEN 'Browse Pages'
      when page_locations LIKE 'https://spot2.mx/buscar%' THEN 'Search'
      when page_locations LIKE 'https://spot2.mx'
      OR page_locations LIKE 'https://spot2.mx/'
      or page_locations LIKE 'https://spot2.mx/?%' THEN 'HomePage'
      when page_locations LIKE 'https://spot2.mx/spots/%' THEN 'Spot Details'
      WHEN page_locations LIKE '%/v2/proyectos/%'
      OR page_locations LIKE '/v1/proyectos%' THEN 'Projects'
      WHEN page_locations LIKE '%/desarrolladores%' THEN 'Developers B2B'
      WHEN page_locations LIKE '%/v1/spots%'
      OR page_locations LIKE '%estadisticas%'
      OR page_locations LIKE '%/crear%' THEN 'Inventory Management'
      WHEN page_locations LIKE '%valua-tu-propiedad%' THEN 'Valuation Tool'
      WHEN page_locations LIKE '%www.colossu.com/%' THEN 'Colossu'
      ELSE 'Other'
    END AS primera_bifurcacion
  FROM users
),
conversion_metadata AS (
  SELECT user_pseudo_id,
    event_date,
    MAX(
      CASE
        WHEN ep.key = 'from' THEN ep.value.string_value
      END
    ) as conversion_from,
    MAX(
      CASE
        WHEN ep.key = 'page_location' THEN ep.value.string_value
      END
    ) as conversion_url
  FROM `analytics_276054961.events_*`,
    UNNEST(event_params) AS ep
  WHERE event_name IN (
      'clientSearchedSpotSearch',
      'clientRequestedWhatsappForm',
      'clientRequestedContactLead',
      'clientClickedRegisterForm',
      'clientSubmittedRegisterUser',
      'clientClickedRegisterUser',
      'clientSubmittedRegisterForm',
      'lead_form_submission',
      -- LOS QUE FALTABAN:
      'clientSubmitFormBlogRetail',
      'clientSubmitFormBlogOficinas',
      'clientSubmitFormBlogIndustrial',
      'clientClickedLeadPopup',
      'spotMapSearch',
      'clientClickedSendInfoModalConsulting',
      'clientSubmitStep1Industrial',
      'clientSubmitStep1Retail',
      'clientSubmitStep1Oficinas',
      'clientSubmittedBudgetFormBP'
    )
    AND event_date >= '20250201'
  GROUP BY 1,
    2
),
-- Acá sus sesiones con su respectiva demanda
spot2_demand as (
  SELECT user_pseudo_id,
    count(*) as demand
  FROM `analytics_276054961.events_*`,
    UNNEST(event_params) AS ep
  WHERE event_name not IN (
      -- Eventos nativos de GA4 (automáticos o Enhanced Measurement)
      'add_to_cart',
      'begin_checkout',
      'click',
      'file_download',
      'first_visit',
      'form_start',
      'form_submit',
      'page_view',
      'purchase',
      'remove_from_cart',
      'scroll',
      'session_start',
      'user_engagement',
      'video_complete',
      'video_progress',
      'video_start',
      'view_cart',
      'view_item',
      'view_search_results',
      --- Eventos de Cookies
      'clientAcceptedDefaultCookies',
      'clientConsentGranted',
      -- Eventos personalizados Supply
      'clientAcceptedQaLimitForm',
      'clientClickedAppraiserFooterLanding',
      'clientClickedBrokerPostYourSpot',
      'clientClickedContactDevLanding',
      'clientClickedDeveloperPostYourSpot',
      'clientClickedLogInHeaderLanding',
      'clientClickedMySpotsMenu',
      'clientClickedOwnerPostYourSpot',
      'clientClickedPostMySpotLandig',
      'clientClickedPostSpotBrokerLanding',
      'clientClickedPostSpotFooterLanding',
      'clientClickedPostSpotMenu',
      'clientClickedPostSpotOwnerLanding',
      'clientClientClickedPostSpotMenu',
      'clientRejectedQaLimitForm',
      'clickedAppraiserOwnerLanding',
      'clickedBrokerPostYourSpot',
      'clickedContactDevLanding',
      'clickedDeveloperPostYourSpot',
      'clickedOwnerPostYourSpot',
      'clickedPostSpotBrokerLanding',
      'clickedPostSpotOwnerLanding'
    )
    or (
      event_name in ('page_view')
      and (
        CASE
          WHEN ep.key = 'page_location' THEN ep.value.string_value
        END like 'https://spot2.mx/spots/%'
      )
    )
    and event_date >= '20250201'
    and event_date < FORMAT_DATE('%Y%m%d', CURRENT_DATE())
  group by 1
),
-- Acá sus sesiones con su respectiva demanda
spot2_supply AS (
  SELECT user_pseudo_id,
    count(*) as supply
  FROM `analytics_276054961.events_*`,
    UNNEST(event_params) AS ep
  WHERE event_name in -- Eventos personalizados Supply
    (
      'clientAcceptedQaLimitForm',
      'clientClickedAppraiserFooterLanding',
      'clientClickedBrokerPostYourSpot',
      'clientClickedContactDevLanding',
      'clientClickedDeveloperPostYourSpot',
      'clientClickedLogInHeaderLanding',
      'clientClickedMySpotsMenu',
      'clientClickedOwnerPostYourSpot',
      'clientClickedPostMySpotLandig',
      'clientClickedPostSpotBrokerLanding',
      'clientClickedPostSpotFooterLanding',
      'clientClickedPostSpotMenu',
      'clientClickedPostSpotOwnerLanding',
      'clientClientClickedPostSpotMenu',
      'clientRejectedQaLimitForm',
      'clickedAppraiserOwnerLanding',
      'clickedBrokerPostYourSpot',
      'clickedContactDevLanding',
      'clickedDeveloperPostYourSpot',
      'clickedOwnerPostYourSpot',
      'clickedPostSpotBrokerLanding',
      'clickedPostSpotOwnerLanding'
    )
    and event_date >= '20250201'
    and event_date < FORMAT_DATE('%Y%m%d', CURRENT_DATE())
  GROUP BY 1
),
spot2_engagement as (
  SELECT user_pseudo_id
  FROM `analytics_276054961.events_*`,
    UNNEST(event_params) AS ep
  WHERE event_name in ('user_engagement')
),
-- Asignacion de sesion por evento
segundo_arbol AS (
  SELECT a11.user_pseudo_id,
    a11.primera_bifurcacion,
    CASE
      WHEN a12.user_pseudo_id IS NOT NULL
      AND a13.user_pseudo_id IS NULL THEN 'Demand'
      WHEN a12.user_pseudo_id IS NULL
      AND a13.user_pseudo_id IS NOT NULL THEN 'Supply'
      WHEN a12.user_pseudo_id IS NOT NULL
      AND a13.user_pseudo_id IS NOT NULL THEN 'Hybrid'
      ELSE 'Sin Interaccion'
    END AS segunda_bifurcacion,
    CASE
      when a14.user_pseudo_id IS NOT NULL then 1
      else 0
    end as flag_engagement
  FROM primera_bifurcacion a11
    LEFT JOIN spot2_demand a12 ON a11.user_pseudo_id = a12.user_pseudo_id
    LEFT JOIN spot2_supply a13 ON a11.user_pseudo_id = a13.user_pseudo_id
    LEFT JOIN spot2_engagement a14 on a11.user_pseudo_id = a14.user_pseudo_id
  group by 1,
    2,
    3,
    4
),
details as (
  select a11.event_date,
    a11.user_pseudo_id,
    a11.source,
    a11.medium,
    a11.campaign_name,
    a11.source_collected,
    a11.medium_collected,
    a11.campaign_name_collected,
    a11.ga4_channel_group,
    a12.primera_bifurcacion,
    a12.page_locations,
    a13.segunda_bifurcacion,
    a13.flag_engagement,
    a11.user_sospechoso
  FROM users a11
    left join primera_bifurcacion a12 on a11.user_pseudo_id = a12.user_pseudo_id
    and a11.event_date = a12.event_date
    left join segundo_arbol a13 on a11.user_pseudo_id = a13.user_pseudo_id
    and a11.event_date = a12.event_date
  group by 1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14
),
phone AS (
  SELECT DISTINCT user_pseudo_id,
    -- Intentamos obtenerlo de int_value, si no, convertimos el string_value a entero
    COALESCE(
      (
        SELECT value.int_value
        FROM UNNEST(event_params)
        WHERE key = 'phone'
        LIMIT 1
      ), SAFE_CAST(
        (
          SELECT value.string_value
          FROM UNNEST(event_params)
          WHERE key = 'phone'
          LIMIT 1
        ) AS INT64
      )
    ) AS phone
  FROM `spot2-mx-ga4-bq.analytics_276054961.events_*`
  WHERE EXISTS (
      SELECT 1
      FROM UNNEST(event_params)
      WHERE key = 'phone'
        AND (
          value.string_value IS NOT NULL
          OR value.int_value IS NOT NULL
        )
    )
),
email as (
  SELECT DISTINCT user_pseudo_id,
    (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'email'
      LIMIT 1
    ) AS email
  FROM `spot2-mx-ga4-bq.analytics_276054961.events_*`
  WHERE (
      SELECT value.string_value
      FROM UNNEST(event_params)
      WHERE key = 'email'
      LIMIT 1
    ) IS NOT NULL
  order by user_pseudo_id desc
),
total as (
  SELECT PARSE_DATE(
      '%Y-%m-%d',
      FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', a11.event_date))
    ) AS event_date,
    primera_bifurcacion,
    segunda_bifurcacion,
    a11.user_pseudo_id AS user,
    user_sospechoso,
    a12.email,
    a13.phone,
    source,
    source_collected,
    medium,
    a11.medium_collected,
    a11.campaign_name,
    a11.campaign_name_collected,
    ga4_channel_group,
    m.conversion_from,
    CASE
      WHEN (
        m.conversion_url like '%https://spot2.mx/landing-industrial%'
        or m.conversion_url like '%https://spot2.mx/landing-oficinas%'
        or m.conversion_url like '%https://spot2.mx/landings/industrial%'
        or m.conversion_url like '%https://spot2.mx/landings/oficinas%'
        or m.conversion_url like '%https://spot2.mx/landings/locales-comerciales%'
      )
      or (
        (
          m.conversion_url LIKE 'https://spot2.mx/oficinas%'
          or m.conversion_url LIKE 'https://spot2.mx/locales-comerciales%'
          or m.conversion_url LIKE 'https://spot2.mx/naves-industriales%'
          or m.conversion_url LIKE 'https://spot2.mx/terrenos%'
        )
        and a11.event_date <= '20250813'
      ) THEN 'Landing'
      WHEN (
        m.conversion_url like '%https://blog.spot2.mx/%'
        or m.conversion_url like '%https://spot2.mx/blog%'
      ) THEN 'Blog'
      WHEN (
        m.conversion_url like '%news.spot2.mx%'
        or m.conversion_url like '%/noticias%'
      ) THEN 'News & PR'
      WHEN m.conversion_url like '%https://spot2.mx/publica-tu-espacio%' THEN 'Supply'
      WHEN (
        m.conversion_url LIKE 'https://spot2.mx/oficinas%'
        or m.conversion_url LIKE 'https://spot2.mx/locales-comerciales%'
        or m.conversion_url LIKE 'https://spot2.mx/naves-industriales%'
        or m.conversion_url LIKE 'https://spot2.mx/terrenos%'
        or m.conversion_url LIKE 'https://spot2.mx/renta%'
        or m.conversion_url LIKE 'https://spot2.mx/venta%'
        or m.conversion_url LIKE 'https://spot2.mx/bodegas%'
        or m.conversion_url LIKE 'https://spot2.mx/coworking%'
      )
      and a11.event_date >= '20250813' THEN 'Browse Pages'
      WHEN m.conversion_url LIKE 'https://spot2.mx/buscar%' THEN 'Search'
      WHEN m.conversion_url LIKE 'https://spot2.mx'
      OR m.conversion_url LIKE 'https://spot2.mx/'
      or m.conversion_url LIKE 'https://spot2.mx/?%' THEN 'HomePage'
      WHEN m.conversion_url LIKE 'https://spot2.mx/spots/%' THEN 'Spot Details'
      WHEN m.conversion_url IS NULL THEN 'Sin Conversión'
      WHEN m.conversion_url LIKE '%/v2/proyectos/%'
      OR m.conversion_url LIKE '/v1/proyectos%' THEN 'Projects'
      WHEN m.conversion_url LIKE '%/desarrolladores%' THEN 'Developers B2B'
      WHEN m.conversion_url LIKE '%/v1/spots%'
      OR m.conversion_url LIKE '%estadisticas%'
      OR m.conversion_url LIKE '%/crear%' THEN 'Inventory Management'
      WHEN m.conversion_url LIKE '%valua-tu-propiedad%' THEN 'Valuation Tool'
      WHEN m.conversion_url LIKE '%signup=login%'
      OR m.conversion_url LIKE '%signup=verify-otp%'
      OR m.conversion_url LIKE '%localhost%'
      OR m.conversion_url LIKE '%staging%'
      OR m.conversion_url LIKE '%vercel.app%' THEN 'Technical Waste'
      ELSE 'Other'
    END AS conversion_point,
    CASE
      WHEN user_sospechoso = 1 THEN 'Bot/Spam'
      -- 1️⃣ LLMs PRIMERO (ChatGPT, Gemini, etc. — llegan frecuentemente con medium vacío)
      WHEN REGEXP_CONTAINS(
        LOWER(source),
        r'(chatgpt|perplexity|claude|bard|gemini|anthropic|openai|llm|ai\s*chat|ai\s*assistant)'
      ) THEN 'Organic LLMs'
      -- 2️⃣ Vacíos o nulos (ahora ya sin LLMs atrapados)
      WHEN COALESCE(TRIM(medium), '') = ''
      or (
        (
          LOWER(source) like '%l.wl.co%'
          or LOWER(source) like 't.co%'
          or LOWER(source) like '%github.com%'
          or LOWER(source) like '%statics.teams.cdn.office.net%'
        )
        and medium = 'referral'
      ) THEN 'Nulo-Vacío'
      -- 3️⃣ Búsqueda orgánica
      WHEN (
        campaign_name = '(organic)'
        and lower(source) not in ('adwords')
        and lower(medium) not in ('spot')
      )
      OR medium = 'organic'
      OR (
        (COALESCE(source, '') = '')
        AND (COALESCE(medium, '') = '')
        AND LOWER(campaign_name) NOT LIKE '%comunicado%'
      )
      or (
        (
          LOWER(source) like '%search.yam.com%'
          or LOWER(source) like '%copilot.microsoft.com%'
          or LOWER(source) like '%search.google.com'
          or LOWER(source) like '%msn.com'
        )
        and medium = 'referral'
      )
      or(
        REGEXP_CONTAINS(
          LOWER(source),
          r'(google|bing|yahoo|duckduckgo|ecosia|search|adwords)'
        )
        and medium not in ('cpc', 'spot', 'paid', 'referral')
        and REGEXP_CONTAINS(
          LOWER(medium),
          r'(cpc,|spot,|paid,|(not set))'
        ) = false
      )
      or (
        REGEXP_CONTAINS(LOWER(source), r'(search.yahoo.com)')
        and medium = 'referral'
      ) THEN 'Organic Search'
      -- 4️⃣ Correo electrónico
      WHEN LOWER(source) = 'mail'
      OR (
        REGEXP_CONTAINS(
          LOWER(source),
          r'(gmail|outlook|yahoo|hotmail|protonmail|icloud|zoho|mail\.ru|yandex|aol|live\.com|office365|exchange|mailchimp|sendgrid|mailgun|postmark|amazonses|sendinblue|brevo|active_campaign|active_campaing)'
        )
        and REGEXP_CONTAINS(LOWER(source), r'(search.)') = false
      ) THEN 'Mail' -- 5️⃣ Display / banners
      WHEN LOWER(campaign_name) LIKE '%_display_%'
      or LOWER(campaign_name) LIKE '%_disp_%' THEN 'Display' -- 6️⃣ Directo
      WHEN source = '(direct)' THEN 'Direct' -- 7️⃣ Referidos
      WHEN (
        campaign_name = '(referral)'
        OR LOWER(medium) LIKE '%referral%'
      )
      AND NOT REGEXP_CONTAINS(
        LOWER(source),
        r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)'
      ) THEN 'Referral' -- 8️⃣ Cross-network (PMAX, etc.)
      WHEN LOWER(campaign_name) LIKE '%cross-network%'
      OR LOWER(campaign_name) LIKE '%pmax%'
      OR LOWER(source) LIKE '%syndicatedsearch.goog%'
      OR (
        LOWER(source) LIKE '%google%'
        AND medium IN ('', 'cross-network')
      )
      or (
        LOWER(source) LIKE '%nova.taboolanews.com%'
        and medium = 'referral'
      ) THEN 'Cross-network' -- 9️⃣ Video (YouTube, etc.)
      WHEN LOWER(campaign_name) LIKE '%youtube%'
      OR REGEXP_CONTAINS(LOWER(campaign_name), r'yt_')
      OR REGEXP_CONTAINS(LOWER(campaign_name), r'_yt') THEN 'Paid Video' -- 🔟 Búsqueda paga
      WHEN medium IN ('cpc', 'spot', 'paid')
      AND (
        REGEXP_CONTAINS(
          LOWER(source),
          r'(google|bing|yahoo|duckduckgo|ecosia|search\.)'
        )
        OR LOWER(campaign_name) LIKE '%search%'
        OR LOWER(campaign_name) LIKE '%srch%'
      )
      or (
        LOWER(source) = 'adwords'
        and lower(medium) = '(not set)'
      ) THEN 'Paid Search' -- 11️⃣ Social pago
      WHEN medium IN ('cpc', 'paid', 'paid_social')
      AND REGEXP_CONTAINS(
        LOWER(source),
        r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)'
      ) THEN 'Paid Social' -- 12️⃣ Social orgánico
      WHEN medium IN ('social', 'rss')
      OR (
        REGEXP_CONTAINS(
          LOWER(source),
          r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)'
        )
        AND medium NOT IN ('cpc', 'paid')
        and REGEXP_CONTAINS(LOWER(campaign_name), r'(meta)') = false
      ) THEN 'Organic Social'
      ELSE 'Unassigned'
    END AS Channel,
    a11.flag_engagement,
    a11.page_locations
  FROM details a11
    left join email a12 on a11.user_pseudo_id = a12.user_pseudo_id
    left join conversion_metadata m on a11.user_pseudo_id = m.user_pseudo_id
    AND a11.event_date = m.event_date
    left join phone a13 on a11.user_pseudo_id = a13.user_pseudo_id
)
select *,
  CASE
    WHEN channel = 'Bot/Spam' THEN 'Bot/Spam'
    -- Demand traffic_source_name -> Paid (nueva regla)
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
  END AS `Traffic_type`
from total
order by event_date desc