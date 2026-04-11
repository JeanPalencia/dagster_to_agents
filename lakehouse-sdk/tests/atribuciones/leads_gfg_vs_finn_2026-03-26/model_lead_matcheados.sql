WITH leads_totales AS (
    SELECT
      lead_id,
      spot_sector,
      LEAST(
        COALESCE(lead_lead0_at::date, '9999-12-31'::date),
        COALESCE(lead_lead1_at::date, '9999-12-31'::date),
        COALESCE(lead_lead2_at::date, '9999-12-31'::date),
        COALESCE(lead_lead3_at::date, '9999-12-31'::date),
        COALESCE(lead_lead4_at::date, '9999-12-31'::date)
      ) AS primera_fecha,
      CASE
        WHEN lead_l4 THEN 'L4'
        WHEN lead_l3 THEN 'L3'
        WHEN lead_l2 THEN 'L2'
        WHEN lead_l1 THEN 'L1'
        ELSE 'L0'
      END AS nivel_lead
    FROM lk_leads
    WHERE lead_deleted_at IS NULL
      AND (lead_l0 OR lead_l1 OR lead_l2 OR lead_l3 OR lead_l4)
      AND (lead_domain NOT IN ('spot2.mx') OR lead_domain IS NULL)
),
mat_deduplicado AS (
    SELECT
        *,
	ROW_NUMBER() OVER(
          PARTITION BY lead_id, lead_fecha_cohort_dt::date
          ORDER BY mat_event_datetime DESC
        ) as fila_unica
    FROM lk_mat_matches_visitors_to_leads
)
SELECT
    COALESCE(mat.lead_fecha_cohort_dt::date, lk.primera_fecha) AS lead_date,
    lk.lead_id,

    -- ID Único: Usamos lead_id solo para que el Journey no cuente doble
    lk.lead_id AS lead_id_unico,

    lk.nivel_lead,
    COALESCE(mat.lead_sector, lk.spot_sector) AS sector,
    COALESCE(mat.lead_cohort_type, 'New') AS cohort_type,

    CASE
        WHEN lk.nivel_lead = 'L3' THEN COALESCE(mat.mat_entry_point, 'L3')
        ELSE mat.mat_entry_point
    END AS entry_point,

    COALESCE(mat.mat_conversion_point, 'Other') AS conversion_point,
    'Sin Interaccion' AS intencion_funnel,

    CASE
        WHEN mat.vis_user_pseudo_id LIKE 'CRM_OFFLINE_%' THEN 'Offline' -- Forzamos canal Offline
        WHEN lk.nivel_lead = 'L3' THEN COALESCE(mat.mat_channel, 'Outbound')
        ELSE COALESCE(mat.mat_channel, 'Unassigned')
    END AS channel,

    CASE
      WHEN lk.nivel_lead = 'L3' THEN 'Outbound'
      WHEN mat.mat_traffic_type IS NOT NULL AND mat.mat_traffic_type <> '' THEN mat.mat_traffic_type
      ELSE 'Unassigned'
    END AS traffic_type,

    CASE
        WHEN lk.nivel_lead = 'L3' THEN COALESCE(mat.mat_campaign_name, 'Outbound')
        ELSE mat.mat_campaign_name
    END AS campaign_name,

    -- AQUÍ ESTÁ EL TRUCO PARA EL MATCHING:
    -- Si es CRM_OFFLINE, lo dejamos como NULL para que el conteo de "matcheados" no lo sume
    CASE
        WHEN mat.vis_user_pseudo_id LIKE 'CRM_OFFLINE_%' THEN NULL
        WHEN lk.nivel_lead = 'L3' THEN COALESCE(mat.vis_user_pseudo_id, lk.lead_id || 'L3')
        ELSE mat.vis_user_pseudo_id
    END AS user_pseudo_id,

    mat.mat_event_name AS tipo_evento,
    NULL AS user_sospechoso,
    mat.mat_page_location AS page_location

FROM leads_totales lk
LEFT JOIN mat_deduplicado mat
    ON lk.lead_id = mat.lead_id
    AND mat.fila_unica = 1
ORDER BY lead_date DESC;

