-- PoP-Cohorts Disaggregated
-- Filas individuales con flags 1/0 en lugar de conteos pre-agregados.
-- El dashboard reconstruye las metricas con:
--   Leads:      distinct(case([leads_current] = 1, [lead_id]))
--   Proyecto+:  distinct(case([flag] = 1, [entity_id]))
-- donde entity_id = lead_id (By Lead) o project_id (By Project).

WITH mat_lead_attrs AS (
    SELECT
        lead_id,
        lead_fecha_cohort_dt::date AS cohort_date,
        COALESCE(mat_channel, 'Unassigned')       AS channel,
        COALESCE(mat_traffic_type, 'Unassigned')   AS traffic_type,
        COALESCE(mat_campaign_name, 'Unassigned')  AS campaign_name
    FROM (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY lead_id, lead_fecha_cohort_dt::date
                ORDER BY mat_event_datetime DESC
            ) AS rn
        FROM lk_mat_matches_visitors_to_leads
    ) sub
    WHERE rn = 1
),

leads_base AS (
  SELECT
    lk.lead_id,
    LEAST(
      COALESCE(lk.lead_lead0_at::timestamp, TIMESTAMP '9999-12-31'),
      COALESCE(lk.lead_lead1_at::timestamp, TIMESTAMP '9999-12-31'),
      COALESCE(lk.lead_lead2_at::timestamp, TIMESTAMP '9999-12-31'),
      COALESCE(lk.lead_lead3_at::timestamp, TIMESTAMP '9999-12-31'),
      COALESCE(lk.lead_lead4_at::timestamp, TIMESTAMP '9999-12-31')
    ) AS primera_fecha_lead,
    COALESCE(lk.spot_sector, 'Retail') AS spot_sector,
    CASE
      WHEN lk.lead_lead4_at IS NOT NULL THEN 'L4'
      WHEN lk.lead_lead3_at IS NOT NULL THEN 'L3'
      WHEN lk.lead_lead2_at IS NOT NULL THEN 'L2'
      WHEN lk.lead_lead1_at IS NOT NULL THEN 'L1'
      WHEN lk.lead_lead0_at IS NOT NULL THEN 'L0'
      ELSE NULL
    END AS lead_max_type
  FROM lk_leads lk
  WHERE
    (lk.lead_l0 OR lk.lead_l1 OR lk.lead_l2 OR lk.lead_l3 OR lk.lead_l4)
    AND (lk.lead_domain NOT IN ('spot2.mx') OR lk.lead_domain IS NULL)
    AND lk.lead_deleted_at IS NULL
),

base_nuevos AS (
    SELECT
        lb.lead_id,
        lb.primera_fecha_lead,
        lb.spot_sector,
        lb.lead_max_type,
        NULL::integer   AS project_id,
        NULL::timestamp AS project_created_at,
        NULL::date      AS visita_solicitada_at,
        NULL::timestamp AS visita_realizada_at,
        NULL::date      AS project_funnel_loi_date,
        NULL::date      AS project_won_date,
        lb.primera_fecha_lead AS fecha_cohort
    FROM leads_base lb
    WHERE lb.primera_fecha_lead >= TIMESTAMP '2021-01-01'
      AND lb.primera_fecha_lead < CURRENT_DATE
),

rows_proyectos AS (
    SELECT
        lb.lead_id,
        lb.primera_fecha_lead,
        lb.spot_sector,
        lb.lead_max_type,
        p.project_id,
        p.project_created_at,
        p.project_funnel_visit_created_date AS visita_solicitada_at,
        p.project_funnel_visit_realized_at  AS visita_realizada_at,
        p.project_funnel_loi_date,
        p.project_won_date,
        CASE
            WHEN p.project_created_at IS NOT NULL
             AND p.project_created_at >= lb.primera_fecha_lead + INTERVAL '30 days'
                THEN p.project_created_at
            ELSE lb.primera_fecha_lead
        END AS fecha_cohort
    FROM leads_base lb
    LEFT JOIN lk_projects p USING (lead_id)
    WHERE lb.primera_fecha_lead >= TIMESTAMP '2021-01-01'
      AND lb.primera_fecha_lead < CURRENT_DATE
      AND p.project_id IS NOT NULL
),

universo_base AS (
    SELECT * FROM base_nuevos
    UNION ALL
    SELECT * FROM rows_proyectos
),

universo_enriquecido AS (
    SELECT
        ub.*,
        DATE(ub.fecha_cohort) AS cohort_date_real,
        DATE_TRUNC('month', ub.fecha_cohort)::date   AS mes_date,
        DATE_TRUNC('quarter', ub.fecha_cohort)::date AS quarter_date,
        CASE
            WHEN DATE(ub.fecha_cohort) = DATE(ub.primera_fecha_lead) THEN 'Nuevo'
            ELSE 'Reactivado'
        END AS cohort_type,
        COALESCE(m.channel, 'Unassigned') AS channel,
        COALESCE(m.traffic_type, 'Unassigned') AS traffic_type,
        COALESCE(m.campaign_name, 'Unassigned') AS campaign_name
    FROM universo_base ub
    LEFT JOIN mat_lead_attrs m
        ON m.lead_id = ub.lead_id
       AND m.cohort_date = DATE(ub.fecha_cohort)
),

params AS (
    SELECT
        (CURRENT_DATE - INTERVAL '1 day')::date AS cutoff_date,
        DATE_TRUNC('quarter', (CURRENT_DATE - INTERVAL '1 day'))::date AS curr_q_start,
        (DATE_TRUNC('quarter', (CURRENT_DATE - INTERVAL '1 day')) - INTERVAL '3 months')::date AS prev_q_start,
        DATE_TRUNC('month', (CURRENT_DATE - INTERVAL '1 day'))::date AS curr_m_start,
        (DATE_TRUNC('month', (CURRENT_DATE - INTERVAL '1 day')) - INTERVAL '1 month')::date AS prev_m_start
),

comparison_windows AS (
    SELECT
        'Quarter over Quarter'::text AS comparison_type,
        p.curr_q_start AS curr_start,
        p.cutoff_date  AS curr_end,
        p.prev_q_start AS prev_start,
        (p.prev_q_start + (p.cutoff_date - p.curr_q_start))::date AS prev_end
    FROM params p
    WHERE p.cutoff_date >= p.curr_q_start

    UNION ALL

    SELECT
        'Month over Month'::text AS comparison_type,
        p.curr_m_start AS curr_start,
        p.cutoff_date  AS curr_end,
        p.prev_m_start AS prev_start,
        LEAST(
            (p.prev_m_start + (p.cutoff_date - p.curr_m_start))::date,
            (DATE_TRUNC('month', p.prev_m_start) + INTERVAL '1 month - 1 day')::date
        ) AS prev_end
    FROM params p
    WHERE p.cutoff_date >= p.curr_m_start
),

range_bounds AS (
    SELECT
        MIN(prev_start) AS min_start,
        MAX(curr_end)   AS max_end
    FROM comparison_windows
),

universo_relevante AS (
    SELECT ue.*
    FROM universo_enriquecido ue
    CROSS JOIN range_bounds rb
    WHERE ue.cohort_date_real BETWEEN rb.min_start AND rb.max_end
)

/* ---- Salida desagregada ---- */

/* By Lead: todas las filas, entity_id = lead_id */
SELECT
    cw.comparison_type,
    cw.curr_start  AS current_period_start,
    cw.curr_end    AS current_period_end,
    cw.prev_start  AS previous_period_start,
    cw.prev_end    AS previous_period_end,

    ue.lead_id,
    ue.project_id,
    ue.lead_id     AS entity_id,
    'By Lead'::text AS mode,

    ue.cohort_type,
    ue.lead_max_type,
    ue.spot_sector,
    ue.channel,
    ue.traffic_type,
    ue.campaign_name,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
         THEN 1 ELSE 0 END  AS leads_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
         THEN 1 ELSE 0 END  AS leads_prev,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
          AND ue.project_created_at IS NOT NULL
          AND ue.project_created_at::date <= cw.curr_end
         THEN 1 ELSE 0 END  AS projects_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
          AND ue.project_created_at IS NOT NULL
          AND ue.project_created_at::date <= cw.prev_end
         THEN 1 ELSE 0 END  AS projects_prev,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
          AND ue.visita_solicitada_at IS NOT NULL
          AND ue.visita_solicitada_at <= cw.curr_end
         THEN 1 ELSE 0 END  AS scheduled_visits_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
          AND ue.visita_solicitada_at IS NOT NULL
          AND ue.visita_solicitada_at <= cw.prev_end
         THEN 1 ELSE 0 END  AS scheduled_visits_prev,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
          AND ue.visita_realizada_at IS NOT NULL
          AND ue.visita_realizada_at::date <= cw.curr_end
         THEN 1 ELSE 0 END  AS completed_visits_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
          AND ue.visita_realizada_at IS NOT NULL
          AND ue.visita_realizada_at::date <= cw.prev_end
         THEN 1 ELSE 0 END  AS completed_visits_prev,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
          AND ue.project_funnel_loi_date IS NOT NULL
          AND ue.project_funnel_loi_date <= cw.curr_end
         THEN 1 ELSE 0 END  AS lois_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
          AND ue.project_funnel_loi_date IS NOT NULL
          AND ue.project_funnel_loi_date <= cw.prev_end
         THEN 1 ELSE 0 END  AS lois_prev,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
          AND ue.project_won_date IS NOT NULL
          AND ue.project_won_date <= cw.curr_end
         THEN 1 ELSE 0 END  AS won_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
          AND ue.project_won_date IS NOT NULL
          AND ue.project_won_date <= cw.prev_end
         THEN 1 ELSE 0 END  AS won_prev

FROM universo_relevante ue
CROSS JOIN comparison_windows cw
WHERE ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
   OR ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end

UNION ALL

/* By Project: solo filas con proyecto, entity_id = project_id */
SELECT
    cw.comparison_type,
    cw.curr_start  AS current_period_start,
    cw.curr_end    AS current_period_end,
    cw.prev_start  AS previous_period_start,
    cw.prev_end    AS previous_period_end,

    ue.lead_id,
    ue.project_id,
    ue.project_id  AS entity_id,
    'By Project'::text AS mode,

    ue.cohort_type,
    ue.lead_max_type,
    ue.spot_sector,
    ue.channel,
    ue.traffic_type,
    ue.campaign_name,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
         THEN 1 ELSE 0 END  AS leads_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
         THEN 1 ELSE 0 END  AS leads_prev,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
          AND ue.project_created_at IS NOT NULL
          AND ue.project_created_at::date <= cw.curr_end
         THEN 1 ELSE 0 END  AS projects_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
          AND ue.project_created_at IS NOT NULL
          AND ue.project_created_at::date <= cw.prev_end
         THEN 1 ELSE 0 END  AS projects_prev,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
          AND ue.visita_solicitada_at IS NOT NULL
          AND ue.visita_solicitada_at <= cw.curr_end
         THEN 1 ELSE 0 END  AS scheduled_visits_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
          AND ue.visita_solicitada_at IS NOT NULL
          AND ue.visita_solicitada_at <= cw.prev_end
         THEN 1 ELSE 0 END  AS scheduled_visits_prev,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
          AND ue.visita_realizada_at IS NOT NULL
          AND ue.visita_realizada_at::date <= cw.curr_end
         THEN 1 ELSE 0 END  AS completed_visits_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
          AND ue.visita_realizada_at IS NOT NULL
          AND ue.visita_realizada_at::date <= cw.prev_end
         THEN 1 ELSE 0 END  AS completed_visits_prev,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
          AND ue.project_funnel_loi_date IS NOT NULL
          AND ue.project_funnel_loi_date <= cw.curr_end
         THEN 1 ELSE 0 END  AS lois_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
          AND ue.project_funnel_loi_date IS NOT NULL
          AND ue.project_funnel_loi_date <= cw.prev_end
         THEN 1 ELSE 0 END  AS lois_prev,

    CASE WHEN ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
          AND ue.project_won_date IS NOT NULL
          AND ue.project_won_date <= cw.curr_end
         THEN 1 ELSE 0 END  AS won_current,
    CASE WHEN ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
          AND ue.project_won_date IS NOT NULL
          AND ue.project_won_date <= cw.prev_end
         THEN 1 ELSE 0 END  AS won_prev

FROM universo_relevante ue
CROSS JOIN comparison_windows cw
WHERE ue.project_id IS NOT NULL
  AND (ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
    OR ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end)

ORDER BY comparison_type, mode, lead_id, project_id;
