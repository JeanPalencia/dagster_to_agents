WITH clientes AS (
  SELECT
     DATE(LEAST(
      COALESCE(lead0_at, '9999-12-31'),
      COALESCE(lead1_at, '9999-12-31'),
      COALESCE(lead2_at, '9999-12-31'),
      COALESCE(lead3_at, '9999-12-31'),
      COALESCE(lead4_at, '9999-12-31')
    )) AS fecha_cliente,
    c.id AS client_id,
    CASE spot_type_id
      WHEN 13 THEN 'retail'
      WHEN 11 THEN 'office'
      WHEN 9 THEN 'industrial'
      WHEN 15 THEN 'land'
      ELSE 'retail'
    END AS sector_lead,
    CASE
      WHEN lead4_at IS NOT NULL THEN 'L4'
      WHEN lead3_at IS NOT NULL THEN 'L3'
      WHEN lead2_at IS NOT NULL THEN 'L2'
      WHEN lead1_at IS NOT NULL THEN 'L1'
      WHEN lead0_at IS NOT NULL THEN 'L0'
      ELSE 'others'
    END AS lead_type, 
    tenant_type, 
    c.user_id,
    CASE 
		when hs_utm_campaign like '%BOFU%' or hs_utm_campaign like '%MOFU%' 
		or hs_utm_campaign like '%TOFU%' or hs_utm_campaign like '%LOFU%' 
		or hs_utm_campaign like '%Conversiones%'
		or hs_utm_campaign like '%BrandTerms%'
		then 'Paid'
		when c.origin in (6,7,8) then 'Paid'
		else 'Organic'
    END AS campaing,
	profiling_completed_at
  FROM clients AS c 
  LEFT JOIN profiles AS p ON c.user_id = p.user_id
  WHERE 
    (c.lead0_at IS NOT NULL 
      OR c.lead1_at IS NOT NULL
      OR c.lead2_at IS NOT NULL
      OR c.lead3_at IS NOT NULL
      OR c.lead4_at IS NOT NULL)
    AND DATE(LEAST(
      COALESCE(lead0_at, '9999-12-31'),
      COALESCE(lead1_at, '9999-12-31'),
      COALESCE(lead2_at, '9999-12-31'),
      COALESCE(lead3_at, '9999-12-31'),
      COALESCE(lead4_at, '9999-12-31')
    )) >= '2024-01-01'
    AND c.deleted_at IS NULL 
    AND (c.email NOT REGEXP '@spot2\\.mx$' OR c.email IS NULL)
),

proyectos AS (
  SELECT
    DATE(a11.created_at) AS fecha_creacion_proyecto,
    a11.id AS project_id,
    a12.client_id,
    a12.sector_lead,
    a12.lead_type,
    a12.campaing, 
    p.tenant_type,
    a12.user_id,
    CASE
      WHEN a11.last_spot_stage = 1 THEN 'search'
      WHEN a11.last_spot_stage = 2 THEN 'visit'
      WHEN a11.last_spot_stage = 3 THEN 'intention letter'
      WHEN a11.last_spot_stage = 4 THEN 'documentation'
      WHEN a11.last_spot_stage = 5 THEN 'contract'
      WHEN a11.last_spot_stage = 6 THEN 'won'
      WHEN a11.last_spot_stage = 7 THEN 'reject'
      ELSE 'Unknown'
    END AS project_status,
    CASE
      WHEN a11.enable = 0 THEN
        CASE
          WHEN a11.reason_id = 1 THEN 'ganado'
          WHEN a11.reason_id = 2 THEN 'dejo de responder'
          WHEN a11.reason_id = 3 THEN 'ya encontro'
          WHEN a11.reason_id = 4 THEN 'cancelo requerimiento'
          WHEN a11.reason_id = 5 THEN 'espacios no cumplen requerimientos'
          WHEN a11.reason_id = 6 THEN 'otro'
          WHEN a11.reason_id = 7 THEN 'proyecto eliminado'
          WHEN a11.reason_id = 8 THEN 'bajo presupuesto'
          WHEN a11.reason_id = 9 THEN 'falta documentacion'
          WHEN a11.reason_id = 10 THEN 'standy'
        END
    END AS deactivation_reason
  FROM project_requirements a11
  JOIN clientes a12 ON a11.client_id = a12.client_id
  LEFT JOIN profiles p ON a12.user_id = p.user_id
  WHERE DATE(a11.created_at) BETWEEN '2024-01-01' AND CURRENT_DATE()
),

pre_visitas_created AS (
  SELECT 
    ca.project_requirement_id AS project_id,
    MIN(DATE(ca.created_at)) AS fecha_visita_creada,
    MAX(ca.id) AS calendar_appointment_id,
    (SELECT last_date_status 
     FROM calendar_appointments ca2 
     WHERE ca2.project_requirement_id = ca.project_requirement_id 
     ORDER BY ca2.created_at DESC 
     LIMIT 1) AS last_date_status
  FROM calendar_appointments ca
  LEFT JOIN calendar_appointment_dates cad ON ca.id = cad.calendar_appointment_id
  GROUP BY ca.project_requirement_id
),

pre_visitas_realized AS (
  SELECT 
    project_requirement_id AS project_id,
    MIN(cad.date) AS fecha_visita_realizada
  FROM calendar_appointments ca 
  JOIN calendar_appointment_dates cad ON ca.id = cad.calendar_appointment_id
  WHERE cad.status = 4 
  GROUP BY project_requirement_id
),

pre_visitas_confirmed AS (
  SELECT 
    project_requirement_id AS project_id,
    MIN(cad.date) AS fecha_visita_confirmada
  FROM calendar_appointments ca 
  JOIN calendar_appointment_dates cad ON ca.id = cad.calendar_appointment_id
  WHERE cad.status IN (3, 4, 6, 7, 8, 9, 18, 19)
  GROUP BY project_requirement_id
),

pre_lois_created AS (
  SELECT
    JSON_EXTRACT(properties, '$.project_id') AS project_id,
    MIN(DATE(created_at)) AS fecha_loi
  FROM activity_log
  WHERE DATE(created_at) BETWEEN '2024-01-01' AND CURRENT_DATE()
    AND event IN ('projectSpotIntentionLetter', 'projectSpotDocumentation', 'projectSpotContract', 'projectSpotWon')
  GROUP BY JSON_EXTRACT(properties, '$.project_id')
),

pre_contratos_created AS (
  SELECT
    JSON_EXTRACT(properties, '$.project_id') AS project_id,
    MIN(DATE(created_at)) AS fecha_contrato
  FROM activity_log
  WHERE DATE(created_at) BETWEEN '2024-01-01' AND CURRENT_DATE()
    AND event IN ('projectSpotContract', 'projectSpotWon')
  GROUP BY JSON_EXTRACT(properties, '$.project_id')
),

transactions_created AS (
  SELECT
    a11.id AS project_id,
    DATE(a11.won_date) AS fecha_transaccion
  FROM project_requirements a11
  WHERE DATE(a11.won_date) BETWEEN '2024-01-01' AND CURRENT_DATE()
),

-- Subquery for flujo and event_name (from the second query)
flujo_data AS (
  SELECT 
    client_id,
    event_name,
    CASE
      WHEN event_name IN ('whatsappInteraction', 'contactInteraction') THEN 'L2'
      WHEN event_name IN ('accountCreation') THEN 'L0'
      WHEN event_name IN ('officeLandingCreation', 'retailLandingCreation', 'industrialLandingCreation') THEN 'L1'
      WHEN event_name IN ('levelsContactWhatsappInteraction', 'levelsContactEmailInteraction', 'levelsContactPhoneInteraction') THEN 'Levels Brokers'
      WHEN event_name IN ('chatbotConfirmSpotInterest', 'chatbotProfilingCompleted') THEN 'Bot interested'
    END AS flujo
  FROM (
    SELECT 
      client_id,
      event_name,
      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created_at ASC, id ASC) AS rn
    FROM client_event_histories
    WHERE event_name IN (
      'whatsappInteraction', 'contactInteraction',
      'accountCreation',
      'officeLandingCreation', 'retailLandingCreation', 'industrialLandingCreation',
      'levelsContactWhatsappInteraction', 'levelsContactEmailInteraction', 'levelsContactPhoneInteraction',
      'chatbotConfirmSpotInterest', 'chatbotProfilingCompleted'
    )
    AND created_at >= '2025-08-01'
  ) t
  WHERE rn = 1
),

total AS (
  SELECT
    c.fecha_cliente,
    p.fecha_creacion_proyecto,
    CASE WHEN vc.project_id IS NOT NULL THEN p.fecha_creacion_proyecto END AS fecha_visita_creada_por_proyecto,
    vc.fecha_visita_creada,
    vc.calendar_appointment_id,
    CASE
      WHEN vc.last_date_status = 1 THEN 'NewAppointmentDate'
      WHEN vc.last_date_status = 2 THEN 'ToConfirm'
      WHEN vc.last_date_status = 3 THEN 'AgentConfirmed'
      WHEN vc.last_date_status = 4 THEN 'Visited'
      WHEN vc.last_date_status = 5 THEN 'Rejected'
      WHEN vc.last_date_status = 6 THEN 'Canceled'
      WHEN vc.last_date_status = 7 THEN 'ClientConfirmed'
      WHEN vc.last_date_status = 8 THEN 'ClientCanceled'
      WHEN vc.last_date_status = 9 THEN 'ContactConfirmed'
      WHEN vc.last_date_status = 10 THEN 'ContactRejected'
      WHEN vc.last_date_status = 11 THEN 'OfferNotAvailableRejected'
      WHEN vc.last_date_status = 12 THEN 'ScheduleNotAvailableRejected'
      WHEN vc.last_date_status = 13 THEN 'UnsuitableSpotRejected'
      WHEN vc.last_date_status = 14 THEN 'NoContactAnswerRejected'
      WHEN vc.last_date_status = 15 THEN 'NoClientAnswerRejected'
      WHEN vc.last_date_status = 16 THEN 'ScheduleErrorRejected'
      WHEN vc.last_date_status = 17 THEN 'NotSharedComissionRejected'
      WHEN vc.last_date_status = 18 THEN 'ContactCanceled'
      WHEN vc.last_date_status = 19 THEN 'RescheduledCanceled'
      ELSE 'Unknown'
    END AS visit_status,
    CASE WHEN vr.project_id IS NOT NULL THEN p.fecha_creacion_proyecto END AS fecha_visita_realizada_por_proyecto,
    vr.fecha_visita_realizada,
    CASE WHEN vcf.project_id IS NOT NULL THEN p.fecha_creacion_proyecto END AS fecha_visita_confirmada_por_proyecto,
    vcf.fecha_visita_confirmada,
    CASE WHEN l.project_id IS NOT NULL THEN p.fecha_creacion_proyecto END AS fecha_loi_por_proyecto,
    l.fecha_loi,
    CASE WHEN ct.project_id IS NOT NULL THEN p.fecha_creacion_proyecto END AS fecha_contrato_por_proyecto,
    ct.fecha_contrato,
    CASE WHEN t.project_id IS NOT NULL THEN p.fecha_creacion_proyecto END AS fecha_transaccion_por_proyecto,
    t.fecha_transaccion,
    p.sector_lead,
    p.lead_type,
    p.tenant_type,
    p.client_id,
    p.campaing, 
    p.project_id,
    p.project_status,
    p.deactivation_reason,
    f.flujo,
    f.event_name,
	c.profiling_completed_at
  FROM proyectos p
  LEFT JOIN clientes c ON p.client_id = c.client_id
  LEFT JOIN pre_visitas_created vc ON p.project_id = vc.project_id
  LEFT JOIN pre_visitas_realized vr ON p.project_id = vr.project_id
  LEFT JOIN pre_visitas_confirmed vcf ON p.project_id = vcf.project_id
  LEFT JOIN pre_lois_created l ON p.project_id = l.project_id
  LEFT JOIN pre_contratos_created ct ON p.project_id = ct.project_id
  LEFT JOIN transactions_created t ON p.project_id = t.project_id
  LEFT JOIN flujo_data f ON p.client_id = f.client_id
  UNION ALL
  SELECT
    c.fecha_cliente,
    NULL AS fecha_creacion_proyecto,
    NULL AS fecha_visita_creada_por_proyecto,
    NULL AS fecha_visita_creada,
    NULL AS calendar_appointment_id,
    NULL AS visit_status,
    NULL AS fecha_visita_realizada_por_proyecto,
    NULL AS fecha_visita_realizada,
    NULL AS fecha_visita_confirmada_por_proyecto,
    NULL AS fecha_visita_confirmada,
    NULL AS fecha_loi_por_proyecto,
    NULL AS fecha_loi,
    NULL AS fecha_contrato_por_proyecto,
    NULL AS fecha_contrato,
    NULL AS fecha_transaccion_por_proyecto,
    NULL AS fecha_transaccion,
    c.sector_lead,
    c.lead_type,
    c.tenant_type,
    c.client_id,
    c.campaing, 
    NULL AS project_id,
    NULL AS project_status,
    NULL AS deactivation_reason,
    f.flujo,
    f.event_name,
	c.profiling_completed_at
  FROM clientes c
  LEFT JOIN proyectos p ON c.client_id = p.client_id
  LEFT JOIN flujo_data f ON c.client_id = f.client_id
  WHERE p.project_id IS NULL
)

SELECT 
  fecha_cliente,
  fecha_creacion_proyecto,
  fecha_visita_creada,
  fecha_visita_creada_por_proyecto,
  calendar_appointment_id,
  visit_status,
  fecha_visita_realizada,
  fecha_visita_realizada_por_proyecto,
  fecha_visita_confirmada,
  fecha_visita_confirmada_por_proyecto,
  fecha_loi,
  fecha_loi_por_proyecto,
  fecha_contrato,
  fecha_contrato_por_proyecto,
  fecha_transaccion,
  fecha_transaccion_por_proyecto,
  sector_lead,
  lead_type,
  tenant_type,
  client_id,
  campaing, 
  project_id,
  project_status,
  deactivation_reason,
  flujo,
  event_name,
  profiling_completed_at
FROM total
WHERE fecha_cliente IS NOT NULL OR project_id IS NOT NULL
ORDER BY COALESCE(fecha_creacion_proyecto, fecha_cliente) ASC, project_id, client_id;