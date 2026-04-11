-- Conversaciones con proyecto (lk_projects), visita_chatbot (scheduled_visit) y seniority_level (segment) desde bt_conv_conversations.
SELECT
  c.conv_start_date::date AS conv_start_date,
  c.lead_id AS lead_id,
  p.project_id,
  p.project_name AS proyecto,
  p.project_funnel_flow AS funnel_proyecto,
  p.project_funnel_visit_created_date,
  p.project_funnel_visit_confirmed_at,
  p.project_funnel_visit_realized_at AS project_funnel_visit_completed_at,
  p.project_funnel_loi_date,
  p.project_funnel_contract_date,
  CASE
    WHEN COALESCE(
      (SELECT NULLIF(TRIM(elem->>'conv_variable_value'), '')::int
       FROM jsonb_array_elements(c.conv_variables) elem
       WHERE elem->>'conv_variable_name' = 'scheduled_visit'
       LIMIT 1), 0) > 0
    THEN 'chatbot_visit'
    ELSE 'no_chatbot_visit'
  END AS visita_chatbot,
  (SELECT NULLIF(TRIM(elem->>'conv_variable_value'), '')
   FROM jsonb_array_elements(c.conv_variables) elem
   WHERE elem->>'conv_variable_name' = 'seniority_level'
   LIMIT 1) AS seniority_level
FROM public.bt_conv_conversations c
LEFT JOIN (
  SELECT DISTINCT ON (lead_id)
    lead_id,
    project_id,
    project_name,
    project_funnel_flow,
    project_funnel_visit_created_date,
    project_funnel_visit_confirmed_at,
    project_funnel_visit_realized_at,
    project_funnel_loi_date,
    project_funnel_contract_date
  FROM public.lk_projects
  WHERE lead_id IS NOT NULL
  ORDER BY lead_id, project_updated_at DESC NULLS LAST
) p ON p.lead_id::text = c.lead_id
WHERE c.conv_start_date IS NOT NULL
ORDER BY c.conv_start_date::date, c.lead_id;
