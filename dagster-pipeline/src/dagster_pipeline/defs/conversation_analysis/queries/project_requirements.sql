-- Query to get project requirements with calendar appointments
-- Used for scheduled_visit and created_project tags

SELECT 
    pr.id as project_id, 
    pr.client_id as lead_id, 
    ca.created_at as visit_created_at,
    ca.updated_at as visit_updated_at,
    pr.created_at as project_created_at,
    ca.origin as calendar_origin, 
    pr.origin as project_origin
FROM project_requirements pr
LEFT JOIN calendar_appointments ca ON ca.project_requirement_id = pr.id
WHERE ca.origin = 2


