WITH base_leads AS (
    SELECT 
        lead_id,
        coalesce(spot_sector, 'Retail') as spot_sector,
        CASE
            WHEN lead_lead4_at IS NOT NULL THEN 'L4'
            WHEN lead_lead3_at IS NOT NULL THEN 'L3'
            WHEN lead_lead2_at IS NOT NULL THEN 'L2'
            WHEN lead_lead1_at IS NOT NULL THEN 'L1'
            WHEN lead_lead0_at IS NOT NULL THEN 'L0'
            ELSE NULL
        END AS lead_max_type,
        CAST(LEAST(
            COALESCE(lead_lead0_at, '9999-12-31'),
            COALESCE(lead_lead1_at , '9999-12-31'),
            COALESCE(lead_lead2_at , '9999-12-31'),
            COALESCE(lead_lead3_at , '9999-12-31'),
            COALESCE(lead_lead4_at , '9999-12-31')
        ) AS DATE) AS fecha_lead
    FROM lk_leads
    WHERE (lead_l0  OR lead_l1  OR lead_l2  OR lead_l3  OR lead_l4 ) 
      AND (lead_domain NOT IN ('spot2.mx') OR lead_domain IS NULL) and lead_deleted_at is null and lead_created_date < CURRENT_DATE
	  
	  
),

leads_agg AS (
    SELECT fecha_lead AS fecha, lead_max_type, spot_sector, COUNT(DISTINCT lead_id) AS total_leads
    FROM base_leads
    GROUP BY 1,2,3
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
        l.spot_sector 
    FROM lk_projects p
    INNER JOIN base_leads l ON p.lead_id = l.lead_id 
	   where date(project_created_at) < CURRENT_DATE
),

projects_agg AS (
    SELECT CAST(project_created_at AS DATE) AS fecha, lead_max_type, spot_sector, COUNT(DISTINCT project_id) AS total_projects
    FROM projects_base GROUP BY 1,2,3
),

request_visit AS (
    SELECT CAST(project_funnel_visit_created_date AS DATE) AS fecha, lead_max_type, spot_sector, COUNT(DISTINCT project_id) AS total_req_visit
    FROM projects_base GROUP BY 1,2,3
),

confirmed_visits AS (
    SELECT CAST(project_funnel_visit_confirmed_at AS DATE) AS fecha, lead_max_type, spot_sector, COUNT(DISTINCT project_id) AS total_conf_visit
    FROM projects_base GROUP BY 1,2,3
),

completed_visits AS (
    SELECT CAST(project_funnel_visit_realized_at AS DATE) AS fecha, lead_max_type, spot_sector, COUNT(DISTINCT project_id) AS total_comp_visit
    FROM projects_base GROUP BY 1,2,3
),

lois AS (
    SELECT CAST(project_funnel_loi_date AS DATE) AS fecha, lead_max_type, spot_sector, COUNT(DISTINCT project_id) AS total_lois
    FROM projects_base GROUP BY 1,2,3
),

contract AS (
    SELECT CAST(project_funnel_contract_date AS DATE) AS fecha, lead_max_type, spot_sector, COUNT(DISTINCT project_id) AS total_contracts
    FROM projects_base GROUP BY 1,2,3
),

won AS (
    SELECT CAST(project_won_date AS DATE) AS fecha, lead_max_type, spot_sector, COUNT(DISTINCT project_id) AS total_won
    FROM projects_base GROUP BY 1,2,3
),

universo_dimensiones AS (
    SELECT fecha, lead_max_type, spot_sector FROM leads_agg 
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector FROM projects_agg
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector FROM request_visit
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector FROM confirmed_visits
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector FROM completed_visits
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector FROM lois
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector FROM contract
    UNION DISTINCT SELECT fecha, lead_max_type, spot_sector FROM won
)

SELECT 
    ud.fecha,
    ud.lead_max_type,
    ud.spot_sector,
    COALESCE(l.total_leads, 0) AS leads,
    COALESCE(p.total_projects, 0) AS projects,
    COALESCE(rv.total_req_visit, 0) AS request_visit,
    COALESCE(cv.total_conf_visit, 0) AS confirmed_visit,
    COALESCE(cp.total_comp_visit, 0) AS completed_visit,
    COALESCE(lo.total_lois, 0) AS lois,
    COALESCE(ct.total_contracts, 0) AS contract,
    COALESCE(w.total_won, 0) AS won
FROM universo_dimensiones ud
LEFT JOIN leads_agg l ON ud.fecha = l.fecha AND ud.lead_max_type = l.lead_max_type AND ud.spot_sector = l.spot_sector
LEFT JOIN projects_agg p ON ud.fecha = p.fecha AND ud.lead_max_type = p.lead_max_type AND ud.spot_sector = p.spot_sector
LEFT JOIN request_visit rv ON ud.fecha = rv.fecha AND ud.lead_max_type = rv.lead_max_type AND ud.spot_sector = rv.spot_sector
LEFT JOIN confirmed_visits cv ON ud.fecha = cv.fecha AND ud.lead_max_type = cv.lead_max_type AND ud.spot_sector = cv.spot_sector
LEFT JOIN completed_visits cp ON ud.fecha = cp.fecha AND ud.lead_max_type = cp.lead_max_type AND ud.spot_sector = cp.spot_sector
LEFT JOIN lois lo ON ud.fecha = lo.fecha AND ud.lead_max_type = lo.lead_max_type AND ud.spot_sector = lo.spot_sector
LEFT JOIN contract ct ON ud.fecha = ct.fecha AND ud.lead_max_type = ct.lead_max_type AND ud.spot_sector = ct.spot_sector
LEFT JOIN won w ON ud.fecha = w.fecha AND ud.lead_max_type = w.lead_max_type AND ud.spot_sector = w.spot_sector
WHERE ud.fecha IS NOT NULL 
ORDER BY ud.fecha DESC, ud.spot_sector, ud.lead_max_type;