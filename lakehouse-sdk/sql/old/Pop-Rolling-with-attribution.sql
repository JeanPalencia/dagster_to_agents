-- Pop-Rolling with Attribution Segmenters
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
        COALESCE(lk.spot_sector, 'Retail') as spot_sector,
        CASE
            WHEN lk.lead_lead4_at IS NOT NULL THEN 'L4'
            WHEN lk.lead_lead3_at IS NOT NULL THEN 'L3'
            WHEN lk.lead_lead2_at IS NOT NULL THEN 'L2'
            WHEN lk.lead_lead1_at IS NOT NULL THEN 'L1'
            WHEN lk.lead_lead0_at IS NOT NULL THEN 'L0'
            ELSE NULL
        END AS lead_max_type,
        CAST(LEAST(
            COALESCE(lk.lead_lead0_at::timestamp, '9999-12-31'::timestamp),
            COALESCE(lk.lead_lead1_at::timestamp, '9999-12-31'::timestamp),
            COALESCE(lk.lead_lead2_at::timestamp, '9999-12-31'::timestamp),
            COALESCE(lk.lead_lead3_at::timestamp, '9999-12-31'::timestamp),
            COALESCE(lk.lead_lead4_at::timestamp, '9999-12-31'::timestamp)
        ) AS DATE) AS fecha_lead,
        COALESCE(m.channel, 'Unassigned')      AS channel,
        COALESCE(m.traffic_type, 'Unassigned') AS traffic_type,
        COALESCE(m.campaign_name, 'Unassigned') AS campaign_name
    FROM lk_leads lk
    LEFT JOIN mat_lead_attrs m USING (lead_id)
    WHERE 
        (lk.lead_l0 OR lk.lead_l1 OR lk.lead_l2 OR lk.lead_l3 OR lk.lead_l4)  
        AND (lk.lead_domain NOT IN ('spot2.mx') OR lk.lead_domain IS NULL) 
        AND lk.lead_deleted_at IS NULL
        AND lk.lead_created_date < CURRENT_DATE
),

projects_base AS (
    SELECT 
        p.project_id,
        p.lead_id,
        CAST(p.project_created_at AS DATE) as project_created_at,
        CAST(p.project_funnel_visit_created_date AS DATE) as project_funnel_visit_created_date,
        CAST(p.project_funnel_visit_realized_at AS DATE) as project_funnel_visit_realized_at,
        CAST(p.project_funnel_loi_date AS DATE) as project_funnel_loi_date,
        CAST(p.project_won_date AS DATE) as project_won_date
    FROM lk_projects p
    WHERE p.project_created_at < CURRENT_DATE
),

periods AS (
    -- Quarter over Quarter
    SELECT 
        'Quarter over Quarter' AS comparison_type,
        DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1 day')::date AS current_period_start,
        (CURRENT_DATE - INTERVAL '1 day')::date AS current_period_end,
        (DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1 day') - INTERVAL '3 months')::date AS previous_period_start,
        (CURRENT_DATE - INTERVAL '1 day' - INTERVAL '3 months')::date AS previous_period_end
    UNION ALL
    -- Month over Month
    SELECT 
        'Month over Month' AS comparison_type,
        DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 day')::date AS current_period_start,
        (CURRENT_DATE - INTERVAL '1 day')::date AS current_period_end,
        (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 day') - INTERVAL '1 month')::date AS previous_period_start,
        (CURRENT_DATE - INTERVAL '1 day' - INTERVAL '1 month')::date AS previous_period_end
),

metrics_combined AS (
    SELECT 
        per.comparison_type,
        per.current_period_start,
        per.current_period_end,
        per.previous_period_start,
        per.previous_period_end,
        bl.spot_sector,
        bl.lead_max_type,
        bl.channel,
        bl.traffic_type,
        bl.campaign_name,
        -- LEADS
        COUNT(DISTINCT CASE WHEN bl.fecha_lead BETWEEN per.current_period_start AND per.current_period_end THEN bl.lead_id END) AS leads_current,
        COUNT(DISTINCT CASE WHEN bl.fecha_lead BETWEEN per.previous_period_start AND per.previous_period_end THEN bl.lead_id END) AS leads_prev,
        -- PROJECTS
        COUNT(DISTINCT CASE WHEN pb.project_created_at BETWEEN per.current_period_start AND per.current_period_end THEN pb.project_id END) AS projects_current,
        COUNT(DISTINCT CASE WHEN pb.project_created_at BETWEEN per.previous_period_start AND per.previous_period_end THEN pb.project_id END) AS projects_prev,
        -- VISITS REQUESTED
        COUNT(DISTINCT CASE WHEN pb.project_funnel_visit_created_date BETWEEN per.current_period_start AND per.current_period_end THEN pb.project_id END) AS request_visit_current,
        COUNT(DISTINCT CASE WHEN pb.project_funnel_visit_created_date BETWEEN per.previous_period_start AND per.previous_period_end THEN pb.project_id END) AS request_visit_prev,
        -- VISITS COMPLETED
        COUNT(DISTINCT CASE WHEN pb.project_funnel_visit_realized_at BETWEEN per.current_period_start AND per.current_period_end THEN pb.project_id END) AS completed_visit_current,
        COUNT(DISTINCT CASE WHEN pb.project_funnel_visit_realized_at BETWEEN per.previous_period_start AND per.previous_period_end THEN pb.project_id END) AS completed_visit_prev,
        -- LOIS
        COUNT(DISTINCT CASE WHEN pb.project_funnel_loi_date BETWEEN per.current_period_start AND per.current_period_end THEN pb.project_id END) AS lois_current,
        COUNT(DISTINCT CASE WHEN pb.project_funnel_loi_date BETWEEN per.previous_period_start AND per.previous_period_end THEN pb.project_id END) AS lois_prev,
        -- WON
        COUNT(DISTINCT CASE WHEN pb.project_won_date BETWEEN per.current_period_start AND per.current_period_end THEN pb.project_id END) AS won_current,
        COUNT(DISTINCT CASE WHEN pb.project_won_date BETWEEN per.previous_period_start AND per.previous_period_end THEN pb.project_id END) AS won_prev
    FROM periods per
    CROSS JOIN base_leads bl
    LEFT JOIN projects_base pb ON bl.lead_id = pb.lead_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT * FROM metrics_combined
WHERE (leads_current > 0 OR leads_prev > 0
    OR projects_current > 0 OR projects_prev > 0
    OR request_visit_current > 0 OR request_visit_prev > 0
    OR completed_visit_current > 0 OR completed_visit_prev > 0
    OR lois_current > 0 OR lois_prev > 0
    OR won_current > 0 OR won_prev > 0)
ORDER BY comparison_type DESC, spot_sector, lead_max_type, channel, traffic_type, campaign_name;
