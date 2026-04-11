-- Rolling-Projects with Attribution Segmenters
-- Adds channel, traffic_type, campaign_name from lk_mat_matches_visitors_to_leads
-- as additional columns. All original logic remains unchanged.

WITH mat_lead_attrs AS (
    SELECT DISTINCT ON (lead_id)
        lead_id,
        COALESCE(mat_channel, 'Unassigned')       AS channel,
        COALESCE(mat_traffic_type, 'Unassigned')   AS traffic_type,
        COALESCE(mat_campaign_name, 'Unassigned')  AS campaign_name
    FROM lk_mat_matches_visitors_to_leads
    ORDER BY lead_id, mat_event_datetime DESC
)

SELECT a11.project_id, project_created_date, a12.lead_id,
    CAST(LEAST(
        COALESCE(lead_lead0_at, '9999-12-31'),
        COALESCE(lead_lead1_at, '9999-12-31'),
        COALESCE(lead_lead2_at, '9999-12-31'),
        COALESCE(lead_lead3_at, '9999-12-31'),
        COALESCE(lead_lead4_at, '9999-12-31')
    ) AS DATE) AS lead_creation_date,
    a11.spot_sector,
    a11.lead_max_type,
    COALESCE(m.channel, 'Unassigned')       AS channel,
    COALESCE(m.traffic_type, 'Unassigned')  AS traffic_type,
    COALESCE(m.campaign_name, 'Unassigned') AS campaign_name
FROM lk_projects a11
    JOIN lk_leads a12 ON a11.lead_id = a12.lead_id
    LEFT JOIN mat_lead_attrs m ON a12.lead_id = m.lead_id
WHERE (lead_l0 OR lead_l1 OR lead_l2 OR lead_l3 OR lead_l4)
    AND (lead_domain NOT IN ('spot2.mx') OR lead_domain IS NULL)
ORDER BY project_created_date DESC
