-- Projects-Funnel with Attribution Segmenters (FIXED)
-- FIX: El canal se asigna por (lead_id, fecha_cohort) en lugar de un canal
-- global por lead_id. Esto respeta el canal con el que llegó el lead en cada
-- cohort (New vs Reactivated) en vez de usar siempre el evento más reciente.

WITH params AS (
    SELECT (CURRENT_DATE - INTERVAL '1 day')::date AS cutoff_date
),

mat_lead_attrs AS (
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

leads_calculados AS (
    SELECT
        COALESCE(lk.spot_sector, 'Retail') AS spot_sector,
        lk.lead_id,
        lk.lead_l0, lk.lead_l1, lk.lead_l2, lk.lead_l3, lk.lead_l4,
        lk.lead_domain,
        CASE
            WHEN lk.lead_lead4_at IS NOT NULL THEN 'L4'
            WHEN lk.lead_lead3_at IS NOT NULL THEN 'L3'
            WHEN lk.lead_lead2_at IS NOT NULL THEN 'L2'
            WHEN lk.lead_lead1_at IS NOT NULL THEN 'L1'
            WHEN lk.lead_lead0_at IS NOT NULL THEN 'L0'
            ELSE NULL
        END AS lead_max_type,
        LEAST(
            COALESCE(lk.lead_lead0_at::timestamp, TIMESTAMP '9999-12-31'),
            COALESCE(lk.lead_lead1_at::timestamp, TIMESTAMP '9999-12-31'),
            COALESCE(lk.lead_lead2_at::timestamp, TIMESTAMP '9999-12-31'),
            COALESCE(lk.lead_lead3_at::timestamp, TIMESTAMP '9999-12-31'),
            COALESCE(lk.lead_lead4_at::timestamp, TIMESTAMP '9999-12-31')
        ) AS primera_fecha_lead,
        lk.agent_id
    FROM lk_leads lk
    WHERE lk.lead_deleted_at IS NULL
),

universo_base AS (
    SELECT
        a11.lead_id,
        COALESCE(a12.spot_sector, a11.spot_sector) AS sector,
        COALESCE(a12.lead_max_type, a11.lead_max_type) AS lead_type_max,
        a11.primera_fecha_lead,

        CASE
            WHEN a12.project_created_at IS NOT NULL
             AND a12.project_created_at >= a11.primera_fecha_lead + INTERVAL '30 days'
                THEN a12.project_created_at
            ELSE a11.primera_fecha_lead
        END AS fecha_cohort,

        a12.project_id,
        a12.project_created_at,

        CASE WHEN a12.project_funnel_visit_created_date <= p.cutoff_date THEN a12.project_funnel_visit_created_date END AS project_funnel_visit_created_date,
        a12.project_funnel_visit_confirmed_at,
        CASE WHEN a12.project_funnel_visit_realized_at::date <= p.cutoff_date THEN a12.project_funnel_visit_realized_at END AS project_funnel_visit_realized_at,
        CASE WHEN a12.project_funnel_loi_date <= p.cutoff_date THEN a12.project_funnel_loi_date END AS project_funnel_loi_date,
        CASE WHEN a12.project_funnel_contract_date <= p.cutoff_date THEN a12.project_funnel_contract_date END AS project_funnel_contract_date,
        CASE WHEN a12.project_won_date <= p.cutoff_date THEN a12.project_won_date END AS project_won_date,

        project_disable_reason, project_funnel_visit_status, project_updated_date, project_enable,
        project_last_spot_stage, agent_id

    FROM leads_calculados a11
    CROSS JOIN params p
    LEFT JOIN lk_projects a12
      ON a12.lead_id = a11.lead_id
     AND a12.project_created_at::date <= p.cutoff_date

    WHERE
        (a11.lead_l0 OR a11.lead_l1 OR a11.lead_l2 OR a11.lead_l3 OR a11.lead_l4)
        AND (a11.lead_domain NOT IN ('spot2.mx') OR a11.lead_domain IS NULL)
        AND a11.primera_fecha_lead::date >= DATE '2021-01-01'
        AND a11.primera_fecha_lead::date <= p.cutoff_date
),

total AS (
    SELECT
        lead_id, sector, lead_type_max, primera_fecha_lead, fecha_cohort,
        project_id, project_created_at,
        project_funnel_visit_created_date,
        project_funnel_visit_confirmed_at,
        project_funnel_visit_realized_at,
        project_funnel_loi_date,
        project_funnel_contract_date,
        project_won_date, project_disable_reason, project_funnel_visit_status, project_updated_date, project_enable,
        project_last_spot_stage, agent_id
    FROM universo_base
    WHERE project_id IS NOT NULL

    UNION ALL

    SELECT DISTINCT ON (lead_id)
        lead_id, sector, lead_type_max, primera_fecha_lead,
        primera_fecha_lead AS fecha_cohort,
        NULL::integer AS project_id,
        NULL::timestamp AS project_created_at,
        NULL::date AS project_funnel_visit_created_date,
        NULL::timestamp AS project_funnel_visit_confirmed_at,
        NULL::timestamp AS project_funnel_visit_realized_at,
        NULL::date AS project_funnel_loi_date,
        NULL::date AS project_funnel_contract_date,
        NULL::date AS project_won_date,
        NULL::text as project_disable_reason,
        NULL::text as project_funnel_visit_status,
        NULL::date as project_updated_date,
        NULL::text as project_enable,
        NULL::text as project_last_spot_stage, agent_id
    FROM universo_base u1
    WHERE NOT EXISTS (
        SELECT 1
        FROM universo_base u2
        WHERE u2.lead_id = u1.lead_id
          AND DATE_TRUNC('month', u2.fecha_cohort) = DATE_TRUNC('month', u1.primera_fecha_lead)
          AND u2.project_id IS NOT NULL
    )
)

SELECT
    t.*,
    COALESCE(m.channel, 'Unassigned') AS channel,
    COALESCE(m.traffic_type, 'Unassigned') AS traffic_type,
    COALESCE(m.campaign_name, 'Unassigned') AS campaign_name
FROM total t
CROSS JOIN params p
LEFT JOIN mat_lead_attrs m
    ON m.lead_id = t.lead_id
   AND m.cohort_date = DATE(t.fecha_cohort)
WHERE DATE(t.fecha_cohort) <= p.cutoff_date;
