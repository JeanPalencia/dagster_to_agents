-- Diagnostico rapido de inconsistencias de flow entre lk_leads_v2 y lk_leads_governance
-- Caso puntual (ejemplo): lead_id = 19169

-- 1) Comparar flow final entre legacy y governance en Geospot
SELECT 'governance' AS src, lead_id, lead_adv_funnel_flow, lead_rec_funnel_flow
FROM lk_leads_governance
WHERE lead_id = 19169
UNION ALL
SELECT 'legacy_v2' AS src, lead_id, lead_adv_funnel_flow, lead_rec_funnel_flow
FROM lk_leads_v2
WHERE lead_id = 19169;

-- 2) Revisar base en Spot2 Platform (clients)
SELECT
  id AS lead_id,
  lead0_at,
  lead1_at,
  lead2_at,
  lead3_at,
  lead4_at,
  updated_at
FROM spot2_service.clients
WHERE id = 19169;

-- 3) Revisar eventos de flow en histories (Spot2 Platform)
SELECT
  id,
  client_id,
  event_name,
  created_at,
  CASE
    WHEN event_name IN ('whatsappInteraction', 'contactInteraction') THEN 'L2'
    WHEN event_name = 'accountCreation' THEN 'L0'
    WHEN event_name IN ('officeLandingCreation', 'retailLandingCreation', 'industrialLandingCreation') THEN 'L1'
    WHEN event_name IN ('levelsContactWhatsappInteraction', 'levelsContactEmailInteraction', 'levelsContactPhoneInteraction') THEN 'Levels Brokers'
    WHEN event_name IN ('chatbotConfirmSpotInterest', 'chatbotProfilingCompleted') THEN 'Bot interested'
  END AS flow_label
FROM spot2_service.client_event_histories
WHERE client_id = 19169
  AND event_name IN (
    'whatsappInteraction',
    'contactInteraction',
    'accountCreation',
    'officeLandingCreation',
    'retailLandingCreation',
    'industrialLandingCreation',
    'levelsContactWhatsappInteraction',
    'levelsContactEmailInteraction',
    'levelsContactPhoneInteraction',
    'chatbotConfirmSpotInterest',
    'chatbotProfilingCompleted'
  )
  AND created_at >= '2025-08-01'
ORDER BY created_at, id;

-- 4) Detectar clientes con empate en el primer timestamp de flow (potenciales inconsistencias)
WITH flow_events AS (
  SELECT
    client_id,
    id,
    event_name,
    created_at
  FROM spot2_service.client_event_histories
  WHERE event_name IN (
    'whatsappInteraction',
    'contactInteraction',
    'accountCreation',
    'officeLandingCreation',
    'retailLandingCreation',
    'industrialLandingCreation',
    'levelsContactWhatsappInteraction',
    'levelsContactEmailInteraction',
    'levelsContactPhoneInteraction',
    'chatbotConfirmSpotInterest',
    'chatbotProfilingCompleted'
  )
    AND created_at >= '2025-08-01'
),
first_ts AS (
  SELECT client_id, MIN(created_at) AS first_created_at
  FROM flow_events
  GROUP BY client_id
)
SELECT
  fe.client_id,
  fe.created_at AS first_created_at,
  COUNT(*) AS events_at_first_ts,
  GROUP_CONCAT(fe.event_name ORDER BY fe.id SEPARATOR ', ') AS events
FROM flow_events fe
JOIN first_ts ft
  ON fe.client_id = ft.client_id
 AND fe.created_at = ft.first_created_at
GROUP BY fe.client_id, fe.created_at
HAVING COUNT(*) > 1
ORDER BY fe.created_at DESC;
