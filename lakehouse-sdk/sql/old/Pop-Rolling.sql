WITH base_leads AS (
    SELECT 
        lead_id,
        COALESCE(spot_sector, 'Retail') as spot_sector,
        CASE
            WHEN lead_lead4_at IS NOT NULL THEN 'L4'
            WHEN lead_lead3_at IS NOT NULL THEN 'L3'
            WHEN lead_lead2_at IS NOT NULL THEN 'L2'
            WHEN lead_lead1_at IS NOT NULL THEN 'L1'
            WHEN lead_lead0_at IS NOT NULL THEN 'L0'
            ELSE NULL
        END AS lead_max_type,
        -- Aseguramos que el LEAST maneje timestamps antes del casting a DATE
        CAST(LEAST(
            COALESCE(lead_lead0_at::timestamp, '9999-12-31'::timestamp),
            COALESCE(lead_lead1_at::timestamp, '9999-12-31'::timestamp),
            COALESCE(lead_lead2_at::timestamp, '9999-12-31'::timestamp),
            COALESCE(lead_lead3_at::timestamp, '9999-12-31'::timestamp),
            COALESCE(lead_lead4_at::timestamp, '9999-12-31'::timestamp)
        ) AS DATE) AS fecha_lead
    FROM lk_leads
    WHERE 
        -- CORRECCIÓN: Se eliminó el "= 1" porque las columnas son booleanas
        (lead_l0 OR lead_l1 OR lead_l2 OR lead_l3 OR lead_l4)  
        AND (lead_domain NOT IN ('spot2.mx') OR lead_domain IS NULL) 
        AND lead_deleted_at IS NULL
        AND lead_created_date < CURRENT_DATE
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
    GROUP BY 1,2,3,4,5,6,7
)

SELECT * FROM metrics_combined
WHERE (leads_current > 0 OR leads_prev > 0 OR projects_current > 0 OR projects_prev > 0)
ORDER BY comparison_type DESC, spot_sector, lead_max_type;