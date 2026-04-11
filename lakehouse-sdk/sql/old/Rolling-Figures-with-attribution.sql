-- Rolling-Figures with Attribution Segmenters
-- Adds channel, traffic_type, campaign_name from lk_mat_matches_visitors_to_leads
-- as additional dimensions. All original logic remains unchanged.

WITH mat_lead_attrs AS (
    SELECT DISTINCT ON (lead_id)
        lead_id,
        COALESCE(mat_channel, 'Unassigned')       AS channel,
        COALESCE(mat_traffic_type, 'Unassigned')   AS traffic_type,
        COALESCE(mat_campaign_name, 'Unassigned')  AS campaign_name
    FROM lk_mat_matches_visitors_to_leads
    ORDER BY lead_id, mat_event_datetime DESC
),

base_leads AS (
    SELECT 
        lk.lead_id,
        coalesce(lk.spot_sector, 'Retail') as spot_sector,
        CASE
            WHEN lk.lead_lead4_at IS NOT NULL THEN 'L4'
            WHEN lk.lead_lead3_at IS NOT NULL THEN 'L3'
            WHEN lk.lead_lead2_at IS NOT NULL THEN 'L2'
            WHEN lk.lead_lead1_at IS NOT NULL THEN 'L1'
            WHEN lk.lead_lead0_at IS NOT NULL THEN 'L0'
            ELSE NULL
        END AS lead_max_type,
        CAST(LEAST(
            COALESCE(lk.lead_lead0_at, '9999-12-31'),
            COALESCE(lk.lead_lead1_at, '9999-12-31'),
            COALESCE(lk.lead_lead2_at, '9999-12-31'),
            COALESCE(lk.lead_lead3_at, '9999-12-31'),
            COALESCE(lk.lead_lead4_at, '9999-12-31')
        ) AS DATE) AS fecha_lead,
        COALESCE(m.channel, 'Unassigned')      AS channel,
        COALESCE(m.traffic_type, 'Unassigned') AS traffic_type,
        COALESCE(m.campaign_name, 'Unassigned') AS campaign_name
    FROM lk_leads lk
    LEFT JOIN mat_lead_attrs m USING (lead_id)
    WHERE (lk.lead_l0 OR lk.lead_l1 OR lk.lead_l2 OR lk.lead_l3 OR lk.lead_l4) 
      AND (lk.lead_domain NOT IN ('spot2.mx') OR lk.lead_domain IS NULL)
      AND lk.lead_deleted_at IS NULL
      AND lk.lead_created_date < CURRENT_DATE
),

leads_agg AS (
    SELECT fecha_lead AS fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name,
           COUNT(DISTINCT lead_id) AS total_leads
    FROM base_leads
    GROUP BY 1,2,3,4,5,6
),

projects_base AS (
    SELECT 
        p.project_id,
        p.project_created_at,
        p.project_funnel_visit_created_date,
        p.project_funnel_visit_confirmed_at,
        p.project_funnel_visit_realized_at,
        p.project_funnel_loi_date,
        p.project_funnel_contract_date,
        p.project_won_date,
        l.lead_max_type, 
        l.spot_sector,
        l.channel,
        l.traffic_type,
        l.campaign_name
    FROM lk_projects p
    INNER JOIN base_leads l ON p.lead_id = l.lead_id 
    WHERE date(project_created_at) < CURRENT_DATE
),

projects_agg AS (
    SELECT CAST(project_created_at AS DATE) AS fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name,
           COUNT(DISTINCT project_id) AS total_projects
    FROM projects_base GROUP BY 1,2,3,4,5,6
),

request_visit AS (
    SELECT CAST(project_funnel_visit_created_date AS DATE) AS fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name,
           COUNT(DISTINCT project_id) AS total_req_visit
    FROM projects_base GROUP BY 1,2,3,4,5,6
),

confirmed_visits AS (
    SELECT CAST(project_funnel_visit_confirmed_at AS DATE) AS fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name,
           COUNT(DISTINCT project_id) AS total_conf_visit
    FROM projects_base GROUP BY 1,2,3,4,5,6
),

completed_visits AS (
    SELECT CAST(project_funnel_visit_realized_at AS DATE) AS fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name,
           COUNT(DISTINCT project_id) AS total_comp_visit
    FROM projects_base GROUP BY 1,2,3,4,5,6
),

lois AS (
    SELECT CAST(project_funnel_loi_date AS DATE) AS fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name,
           COUNT(DISTINCT project_id) AS total_lois
    FROM projects_base GROUP BY 1,2,3,4,5,6
),

contract AS (
    SELECT CAST(project_funnel_contract_date AS DATE) AS fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name,
           COUNT(DISTINCT project_id) AS total_contracts
    FROM projects_base GROUP BY 1,2,3,4,5,6
),

won AS (
    SELECT CAST(project_won_date AS DATE) AS fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name,
           COUNT(DISTINCT project_id) AS total_won
    FROM projects_base GROUP BY 1,2,3,4,5,6
),

universo_dimensiones AS (
    SELECT fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name FROM leads_agg 
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name FROM projects_agg
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name FROM request_visit
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name FROM confirmed_visits
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name FROM completed_visits
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name FROM lois
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name FROM contract
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector, channel, traffic_type, campaign_name FROM won
)

SELECT 
    ud.fecha,
    ud.lead_max_type,
    ud.spot_sector,
    ud.channel,
    ud.traffic_type,
    ud.campaign_name,
    COALESCE(l.total_leads, 0) AS leads,
    COALESCE(p.total_projects, 0) AS projects,
    COALESCE(rv.total_req_visit, 0) AS request_visit,
    COALESCE(cv.total_conf_visit, 0) AS confirmed_visit,
    COALESCE(cp.total_comp_visit, 0) AS completed_visit,
    COALESCE(lo.total_lois, 0) AS lois,
    COALESCE(ct.total_contracts, 0) AS contract,
    COALESCE(w.total_won, 0) AS won
FROM universo_dimensiones ud
LEFT JOIN leads_agg l ON ud.fecha = l.fecha AND ud.lead_max_type = l.lead_max_type AND ud.spot_sector = l.spot_sector AND ud.channel = l.channel AND ud.traffic_type = l.traffic_type AND ud.campaign_name = l.campaign_name
LEFT JOIN projects_agg p ON ud.fecha = p.fecha AND ud.lead_max_type = p.lead_max_type AND ud.spot_sector = p.spot_sector AND ud.channel = p.channel AND ud.traffic_type = p.traffic_type AND ud.campaign_name = p.campaign_name
LEFT JOIN request_visit rv ON ud.fecha = rv.fecha AND ud.lead_max_type = rv.lead_max_type AND ud.spot_sector = rv.spot_sector AND ud.channel = rv.channel AND ud.traffic_type = rv.traffic_type AND ud.campaign_name = rv.campaign_name
LEFT JOIN confirmed_visits cv ON ud.fecha = cv.fecha AND ud.lead_max_type = cv.lead_max_type AND ud.spot_sector = cv.spot_sector AND ud.channel = cv.channel AND ud.traffic_type = cv.traffic_type AND ud.campaign_name = cv.campaign_name
LEFT JOIN completed_visits cp ON ud.fecha = cp.fecha AND ud.lead_max_type = cp.lead_max_type AND ud.spot_sector = cp.spot_sector AND ud.channel = cp.channel AND ud.traffic_type = cp.traffic_type AND ud.campaign_name = cp.campaign_name
LEFT JOIN lois lo ON ud.fecha = lo.fecha AND ud.lead_max_type = lo.lead_max_type AND ud.spot_sector = lo.spot_sector AND ud.channel = lo.channel AND ud.traffic_type = lo.traffic_type AND ud.campaign_name = lo.campaign_name
LEFT JOIN contract ct ON ud.fecha = ct.fecha AND ud.lead_max_type = ct.lead_max_type AND ud.spot_sector = ct.spot_sector AND ud.channel = ct.channel AND ud.traffic_type = ct.traffic_type AND ud.campaign_name = ct.campaign_name
LEFT JOIN won w ON ud.fecha = w.fecha AND ud.lead_max_type = w.lead_max_type AND ud.spot_sector = w.spot_sector AND ud.channel = w.channel AND ud.traffic_type = w.traffic_type AND ud.campaign_name = w.campaign_name
WHERE ud.fecha IS NOT NULL 
ORDER BY ud.fecha DESC, ud.spot_sector, ud.lead_max_type, ud.channel, ud.traffic_type, ud.campaign_name;
