-- PoP-Cohorts with Attribution Segmenters (FIXED)
-- FIX: El canal se asigna por (lead_id, fecha_cohort) en lugar de un canal
-- global por lead_id. Esto respeta el canal con el que llegó el lead en cada
-- cohort (New vs Reactivated) en vez de usar siempre el evento más reciente.

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
),

dims AS (
    SELECT DISTINCT
        cw.comparison_type,
        cw.curr_start, cw.curr_end, cw.prev_start, cw.prev_end,
        ue.cohort_type,
        ue.spot_sector,
        ue.lead_max_type,
        ue.channel,
        ue.traffic_type,
        ue.campaign_name
    FROM comparison_windows cw
    JOIN universo_relevante ue
      ON (ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end)
      OR (ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end)
),

/* --- Agregaciones por Lead --- */
curr_lead_first AS (
    SELECT
        cw.comparison_type,
        cw.curr_start, cw.curr_end, cw.prev_start, cw.prev_end,
        ue.lead_id,
        ue.cohort_type,
        ue.spot_sector,
        ue.lead_max_type,
        ue.channel,
        ue.traffic_type,
        ue.campaign_name
    FROM comparison_windows cw
    JOIN universo_relevante ue
      ON ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
curr_lead_flags AS (
    SELECT
        f.comparison_type,
        f.curr_start, f.curr_end, f.prev_start, f.prev_end,
        f.lead_id,
        f.cohort_type,
        f.spot_sector,
        f.lead_max_type,
        f.channel,
        f.traffic_type,
        f.campaign_name,
        BOOL_OR(ue.project_created_at IS NOT NULL AND ue.project_created_at::date <= f.curr_end) AS has_project,
        BOOL_OR(ue.visita_solicitada_at IS NOT NULL AND ue.visita_solicitada_at <= f.curr_end) AS has_scheduled_visit,
        BOOL_OR(ue.visita_realizada_at IS NOT NULL AND ue.visita_realizada_at::date <= f.curr_end) AS has_completed_visit,
        BOOL_OR(ue.project_funnel_loi_date IS NOT NULL AND ue.project_funnel_loi_date <= f.curr_end) AS has_loi,
        BOOL_OR(ue.project_won_date IS NOT NULL AND ue.project_won_date <= f.curr_end) AS has_won
    FROM curr_lead_first f
    JOIN universo_relevante ue
      ON ue.lead_id       = f.lead_id
     AND ue.cohort_type   = f.cohort_type
     AND ue.spot_sector   = f.spot_sector
     AND ue.lead_max_type = f.lead_max_type
     AND ue.channel       = f.channel
     AND ue.traffic_type  = f.traffic_type
     AND ue.campaign_name = f.campaign_name
     AND ue.cohort_date_real BETWEEN f.curr_start AND f.curr_end
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
agg_curr_lead AS (
    SELECT
        comparison_type,
        curr_start, curr_end, prev_start, prev_end,
        cohort_type, spot_sector, lead_max_type,
        channel, traffic_type, campaign_name,
        COUNT(*) AS leads_current,
        SUM(has_project::int)         AS projects_current,
        SUM(has_scheduled_visit::int) AS scheduled_visits_current,
        SUM(has_completed_visit::int) AS completed_visits_current,
        SUM(has_loi::int)             AS lois_current,
        SUM(has_won::int)             AS won_current
    FROM curr_lead_flags
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),

prev_lead_first AS (
    SELECT
        cw.comparison_type,
        cw.curr_start, cw.curr_end, cw.prev_start, cw.prev_end,
        ue.lead_id,
        ue.cohort_type,
        ue.spot_sector,
        ue.lead_max_type,
        ue.channel,
        ue.traffic_type,
        ue.campaign_name
    FROM comparison_windows cw
    JOIN universo_relevante ue
      ON ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
prev_lead_flags AS (
    SELECT
        f.comparison_type,
        f.curr_start, f.curr_end, f.prev_start, f.prev_end,
        f.lead_id,
        f.cohort_type,
        f.spot_sector,
        f.lead_max_type,
        f.channel,
        f.traffic_type,
        f.campaign_name,
        BOOL_OR(ue.project_created_at IS NOT NULL AND ue.project_created_at::date <= f.prev_end) AS has_project,
        BOOL_OR(ue.visita_solicitada_at IS NOT NULL AND ue.visita_solicitada_at <= f.prev_end) AS has_scheduled_visit,
        BOOL_OR(ue.visita_realizada_at IS NOT NULL AND ue.visita_realizada_at::date <= f.prev_end) AS has_completed_visit,
        BOOL_OR(ue.project_funnel_loi_date IS NOT NULL AND ue.project_funnel_loi_date <= f.prev_end) AS has_loi,
        BOOL_OR(ue.project_won_date IS NOT NULL AND ue.project_won_date <= f.prev_end) AS has_won
    FROM prev_lead_first f
    JOIN universo_relevante ue
      ON ue.lead_id       = f.lead_id
     AND ue.cohort_type   = f.cohort_type
     AND ue.spot_sector   = f.spot_sector
     AND ue.lead_max_type = f.lead_max_type
     AND ue.channel       = f.channel
     AND ue.traffic_type  = f.traffic_type
     AND ue.campaign_name = f.campaign_name
     AND ue.cohort_date_real BETWEEN f.prev_start AND f.prev_end
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
agg_prev_lead AS (
    SELECT
        comparison_type,
        curr_start, curr_end, prev_start, prev_end,
        cohort_type, spot_sector, lead_max_type,
        channel, traffic_type, campaign_name,
        COUNT(*) AS leads_prev,
        SUM(has_project::int)         AS projects_prev,
        SUM(has_scheduled_visit::int) AS scheduled_visits_prev,
        SUM(has_completed_visit::int) AS completed_visits_prev,
        SUM(has_loi::int)             AS lois_prev,
        SUM(has_won::int)             AS won_prev
    FROM prev_lead_flags
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),

/* --- Agregaciones por Proyecto --- */
curr_proj_first AS (
    SELECT
        cw.comparison_type,
        cw.curr_start, cw.curr_end, cw.prev_start, cw.prev_end,
        ue.project_id,
        ue.cohort_type,
        ue.spot_sector,
        ue.lead_max_type,
        ue.channel,
        ue.traffic_type,
        ue.campaign_name
    FROM comparison_windows cw
    JOIN universo_relevante ue
      ON ue.project_id IS NOT NULL
     AND ue.cohort_date_real BETWEEN cw.curr_start AND cw.curr_end
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
curr_proj_flags AS (
    SELECT
        f.comparison_type,
        f.curr_start, f.curr_end, f.prev_start, f.prev_end,
        f.project_id,
        f.cohort_type,
        f.spot_sector,
        f.lead_max_type,
        f.channel,
        f.traffic_type,
        f.campaign_name,
        BOOL_OR(ue.project_created_at IS NOT NULL AND ue.project_created_at::date <= f.curr_end) AS has_project,
        BOOL_OR(ue.visita_solicitada_at IS NOT NULL AND ue.visita_solicitada_at <= f.curr_end) AS has_scheduled_visit,
        BOOL_OR(ue.visita_realizada_at IS NOT NULL AND ue.visita_realizada_at::date <= f.curr_end) AS has_completed_visit,
        BOOL_OR(ue.project_funnel_loi_date IS NOT NULL AND ue.project_funnel_loi_date <= f.curr_end) AS has_loi,
        BOOL_OR(ue.project_won_date IS NOT NULL AND ue.project_won_date <= f.curr_end) AS has_won
    FROM curr_proj_first f
    JOIN universo_relevante ue
      ON ue.project_id    = f.project_id
     AND ue.cohort_type   = f.cohort_type
     AND ue.spot_sector   = f.spot_sector
     AND ue.lead_max_type = f.lead_max_type
     AND ue.channel       = f.channel
     AND ue.traffic_type  = f.traffic_type
     AND ue.campaign_name = f.campaign_name
     AND ue.cohort_date_real BETWEEN f.curr_start AND f.curr_end
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
agg_curr_project AS (
    SELECT
        comparison_type,
        curr_start, curr_end, prev_start, prev_end,
        cohort_type, spot_sector, lead_max_type,
        channel, traffic_type, campaign_name,
        SUM(has_project::int)         AS projects_current,
        SUM(has_scheduled_visit::int) AS scheduled_visits_current,
        SUM(has_completed_visit::int) AS completed_visits_current,
        SUM(has_loi::int)             AS lois_current,
        SUM(has_won::int)             AS won_current
    FROM curr_proj_flags
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),

prev_proj_first AS (
    SELECT
        cw.comparison_type,
        cw.curr_start, cw.curr_end, cw.prev_start, cw.prev_end,
        ue.project_id,
        ue.cohort_type,
        ue.spot_sector,
        ue.lead_max_type,
        ue.channel,
        ue.traffic_type,
        ue.campaign_name
    FROM comparison_windows cw
    JOIN universo_relevante ue
      ON ue.project_id IS NOT NULL
     AND ue.cohort_date_real BETWEEN cw.prev_start AND cw.prev_end
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
prev_proj_flags AS (
    SELECT
        f.comparison_type,
        f.curr_start, f.curr_end, f.prev_start, f.prev_end,
        f.project_id,
        f.cohort_type,
        f.spot_sector,
        f.lead_max_type,
        f.channel,
        f.traffic_type,
        f.campaign_name,
        BOOL_OR(ue.project_created_at IS NOT NULL AND ue.project_created_at::date <= f.prev_end) AS has_project,
        BOOL_OR(ue.visita_solicitada_at IS NOT NULL AND ue.visita_solicitada_at <= f.prev_end) AS has_scheduled_visit,
        BOOL_OR(ue.visita_realizada_at IS NOT NULL AND ue.visita_realizada_at::date <= f.prev_end) AS has_completed_visit,
        BOOL_OR(ue.project_funnel_loi_date IS NOT NULL AND ue.project_funnel_loi_date <= f.prev_end) AS has_loi,
        BOOL_OR(ue.project_won_date IS NOT NULL AND ue.project_won_date <= f.prev_end) AS has_won
    FROM prev_proj_first f
    JOIN universo_relevante ue
      ON ue.project_id    = f.project_id
     AND ue.cohort_type   = f.cohort_type
     AND ue.spot_sector   = f.spot_sector
     AND ue.lead_max_type = f.lead_max_type
     AND ue.channel       = f.channel
     AND ue.traffic_type  = f.traffic_type
     AND ue.campaign_name = f.campaign_name
     AND ue.cohort_date_real BETWEEN f.prev_start AND f.prev_end
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
agg_prev_project AS (
    SELECT
        comparison_type,
        curr_start, curr_end, prev_start, prev_end,
        cohort_type, spot_sector, lead_max_type,
        channel, traffic_type, campaign_name,
        SUM(has_project::int)         AS projects_prev,
        SUM(has_scheduled_visit::int) AS scheduled_visits_prev,
        SUM(has_completed_visit::int) AS completed_visits_prev,
        SUM(has_loi::int)             AS lois_prev,
        SUM(has_won::int)             AS won_prev
    FROM prev_proj_flags
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)

/* Salida Final */
SELECT
    d.comparison_type,
    d.curr_start AS current_period_start,
    d.curr_end   AS current_period_end,
    d.prev_start AS previous_period_start,
    d.prev_end   AS previous_period_end,
    d.cohort_type,
    d.lead_max_type,
    d.spot_sector,
    d.channel,
    d.traffic_type,
    d.campaign_name,
    'By Lead'::text AS mode,
    COALESCE(cl.leads_current, 0)            AS leads_current,
    COALESCE(cl.projects_current, 0)         AS projects_current,
    COALESCE(cl.scheduled_visits_current, 0) AS scheduled_visits_current,
    COALESCE(cl.completed_visits_current, 0) AS completed_visits_current,
    COALESCE(cl.lois_current, 0)             AS lois_current,
    COALESCE(cl.won_current, 0)              AS won_current,
    COALESCE(pl.leads_prev, 0)               AS leads_prev,
    COALESCE(pl.projects_prev, 0)            AS projects_prev,
    COALESCE(pl.scheduled_visits_prev, 0)    AS scheduled_visits_prev,
    COALESCE(pl.completed_visits_prev, 0)    AS completed_visits_prev,
    COALESCE(pl.lois_prev, 0)                AS lois_prev,
    COALESCE(pl.won_prev, 0)                 AS won_prev
FROM dims d
LEFT JOIN agg_curr_lead cl ON cl.comparison_type = d.comparison_type AND cl.curr_start = d.curr_start AND cl.curr_end = d.curr_end AND cl.cohort_type = d.cohort_type AND cl.spot_sector = d.spot_sector AND cl.lead_max_type = d.lead_max_type AND cl.channel = d.channel AND cl.traffic_type = d.traffic_type AND cl.campaign_name = d.campaign_name
LEFT JOIN agg_prev_lead pl ON pl.comparison_type = d.comparison_type AND pl.prev_start = d.prev_start AND pl.prev_end = d.prev_end AND pl.cohort_type = d.cohort_type AND pl.spot_sector = d.spot_sector AND pl.lead_max_type = d.lead_max_type AND pl.channel = d.channel AND pl.traffic_type = d.traffic_type AND pl.campaign_name = d.campaign_name

UNION ALL

SELECT
    d.comparison_type,
    d.curr_start AS current_period_start,
    d.curr_end   AS current_period_end,
    d.prev_start AS previous_period_start,
    d.prev_end   AS previous_period_end,
    d.cohort_type,
    d.lead_max_type,
    d.spot_sector,
    d.channel,
    d.traffic_type,
    d.campaign_name,
    'By Project'::text AS mode,
    COALESCE(cl.leads_current, 0) AS leads_current,
    COALESCE(cp.projects_current, 0)         AS projects_current,
    COALESCE(cp.scheduled_visits_current, 0) AS scheduled_visits_current,
    COALESCE(cp.completed_visits_current, 0) AS completed_visits_current,
    COALESCE(cp.lois_current, 0)             AS lois_current,
    COALESCE(cp.won_current, 0)              AS won_current,
    COALESCE(pl.leads_prev, 0) AS leads_prev,
    COALESCE(pp.projects_prev, 0)            AS projects_prev,
    COALESCE(pp.scheduled_visits_prev, 0)    AS scheduled_visits_prev,
    COALESCE(pp.completed_visits_prev, 0)    AS completed_visits_prev,
    COALESCE(pp.lois_prev, 0)                AS lois_prev,
    COALESCE(pp.won_prev, 0)                 AS won_prev
FROM dims d
LEFT JOIN agg_curr_lead cl ON cl.comparison_type = d.comparison_type AND cl.curr_start = d.curr_start AND cl.curr_end = d.curr_end AND cl.cohort_type = d.cohort_type AND cl.spot_sector = d.spot_sector AND cl.lead_max_type = d.lead_max_type AND cl.channel = d.channel AND cl.traffic_type = d.traffic_type AND cl.campaign_name = d.campaign_name
LEFT JOIN agg_prev_lead pl ON pl.comparison_type = d.comparison_type AND pl.prev_start = d.prev_start AND pl.prev_end = d.prev_end AND pl.cohort_type = d.cohort_type AND pl.spot_sector = d.spot_sector AND pl.lead_max_type = d.lead_max_type AND pl.channel = d.channel AND pl.traffic_type = d.traffic_type AND pl.campaign_name = d.campaign_name
LEFT JOIN agg_curr_project cp ON cp.comparison_type = d.comparison_type AND cp.curr_start = d.curr_start AND cp.curr_end = d.curr_end AND cp.cohort_type = d.cohort_type AND cp.spot_sector = d.spot_sector AND cp.lead_max_type = d.lead_max_type AND cp.channel = d.channel AND cp.traffic_type = d.traffic_type AND cp.campaign_name = d.campaign_name
LEFT JOIN agg_prev_project pp ON pp.comparison_type = d.comparison_type AND pp.prev_start = d.prev_start AND pp.prev_end = d.prev_end AND pp.cohort_type = d.cohort_type AND pp.spot_sector = d.spot_sector AND pp.lead_max_type = d.lead_max_type AND pp.channel = d.channel AND pp.traffic_type = d.traffic_type AND pp.campaign_name = d.campaign_name

ORDER BY comparison_type, current_period_end DESC, cohort_type, mode, lead_max_type, spot_sector, channel, traffic_type, campaign_name;
