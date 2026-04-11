-- Una sola fila por (user, event_date) para que el JOIN no duplique eventos cuando

-- el mismo usuario tiene varias filas (p. ej. dos canales) el mismo día en funnel_with_channel.

WITH funnel_raw AS (

  SELECT

    user,

    event_date AS funnel_event_date,

    Channel,

    Traffic_type,

    primera_bifurcacion,

    conversion_point,

    segunda_bifurcacion,

    user_sospechoso

  FROM `analitics_spot2.funnel_with_channel`

),

funnel_base AS (

  SELECT

    user,

    funnel_event_date,

    ANY_VALUE(Channel) AS Channel,

    ANY_VALUE(Traffic_type) AS Traffic_type,

    ANY_VALUE(primera_bifurcacion) AS primera_bifurcacion,

    ANY_VALUE(conversion_point) AS conversion_point,

    ANY_VALUE(segunda_bifurcacion) AS segunda_bifurcacion,

    ANY_VALUE(user_sospechoso) AS user_sospechoso

  FROM funnel_raw

  GROUP BY user, funnel_event_date

),



-- PASO 1: Filtrar agresivamente ANTES de desanidar nada

filtered_events AS (

  SELECT *

  FROM `spot2-mx-ga4-bq.analytics_276054961.events_*`

  WHERE _TABLE_SUFFIX >= '20250101'

    AND event_name IN (

      'clientRequestedWhatsappForm', 'clientSubmitFormBlogRetail',

      'clientSubmitFormBlogOficinas', 'clientSubmitFormBlogIndustrial',

      'clientRequestedContactLead', 'clientClickedLeadPopup',

      'spotMapSearch', 'clientClickedRegisterForm',

      'clientClickedSendInfoModalConsulting', 'clientSubmitStep1Industrial',

      'clientSubmitStep1Retail', 'clientSubmitStep1Oficinas',

      'clientSubmittedBudgetFormBP'

    )

),



-- PASO 2: Extraer variables con Subconsultas Escalares

raw_events AS (

  SELECT

    event_date,

    DATETIME(TIMESTAMP_MICROS(event_timestamp), "America/Mexico_City") AS event_datetime,

    user_pseudo_id,

    event_name,

    device.category AS device_category,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'email' LIMIT 1) AS email,

    (SELECT COALESCE(CAST(value.int_value AS STRING), value.string_value) FROM UNNEST(event_params) WHERE key = 'phone' LIMIT 1) AS phone,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1) AS page_location,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer' LIMIT 1) AS page_referrer,

    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'from' LIMIT 1) AS from_param,

    (SELECT COALESCE(CAST(value.int_value AS STRING), value.string_value) FROM UNNEST(event_params) WHERE key = 'spotId' LIMIT 1) AS spotId,

    CASE

      WHEN session_traffic_source_last_click.cross_channel_campaign.source IS NULL

           OR TRIM(COALESCE(session_traffic_source_last_click.cross_channel_campaign.source, '')) = ''

           OR TRIM(COALESCE(session_traffic_source_last_click.cross_channel_campaign.source, '')) = '(not set)'

      THEN traffic_source.source

      ELSE session_traffic_source_last_click.cross_channel_campaign.source

    END AS source,

    CASE

      WHEN session_traffic_source_last_click.cross_channel_campaign.medium IS NULL

           OR TRIM(COALESCE(session_traffic_source_last_click.cross_channel_campaign.medium, '')) = ''

           OR TRIM(COALESCE(session_traffic_source_last_click.cross_channel_campaign.medium, '')) = '(not set)'

      THEN traffic_source.medium

      ELSE session_traffic_source_last_click.cross_channel_campaign.medium

    END AS medium,

    CASE

      WHEN session_traffic_source_last_click.cross_channel_campaign.campaign_name IS NULL

           OR TRIM(COALESCE(session_traffic_source_last_click.cross_channel_campaign.campaign_name, '')) = ''

           OR TRIM(COALESCE(session_traffic_source_last_click.cross_channel_campaign.campaign_name, '')) = '(not set)'

      THEN traffic_source.name

      ELSE session_traffic_source_last_click.cross_channel_campaign.campaign_name

    END AS campaign_name

  FROM filtered_events

)



-- PASO 3: Cruce con funnel (canal y traffic_type vienen ya calculados en funnel_with_channel)

SELECT

  r.event_date,

  r.event_datetime,

  r.user_pseudo_id,

  r.email,

  r.phone,

  r.from_param,

  r.spotId,

  r.event_name,

  r.page_location,

  r.page_referrer,

  r.source,

  r.medium,

  r.campaign_name,

  r.device_category,

  COALESCE(f.Channel, 'Unassigned') AS channel,

  COALESCE(f.Traffic_type, 'Unassigned') AS traffic_type,

  COALESCE(f.primera_bifurcacion, 'Other') AS entry_point,

  COALESCE(f.conversion_point, 'Sin Conversión') AS conversion_point,

  f.segunda_bifurcacion,

  f.user_sospechoso

FROM raw_events r

LEFT JOIN funnel_base f

  ON r.user_pseudo_id = f.user

  AND PARSE_DATE('%Y%m%d', r.event_date) = f.funnel_event_date

ORDER BY r.event_datetime DESC;

