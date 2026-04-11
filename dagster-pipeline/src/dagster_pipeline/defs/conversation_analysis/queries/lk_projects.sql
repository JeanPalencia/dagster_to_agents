-- Columns needed for seniority (segment.seniority_level).
-- Avance del proyecto por fechas del funnel (orden): project_created_at,
-- project_funnel_visit_created_date, project_funnel_visit_confirmed_at,
-- project_funnel_visit_realized_at, project_funnel_loi_date.
-- Por lead: proyecto más avanzado (máxima etapa con fecha no nula); si empate, project_updated_at más reciente.
SELECT
    project_id,
    lead_id,
    project_created_at,
    project_funnel_visit_created_date,
    project_funnel_visit_confirmed_at,
    project_funnel_visit_realized_at,
    project_funnel_loi_date,
    project_updated_at,
    spot_sector,
    project_min_square_space,
    project_max_square_space,
    project_min_rent_price,
    project_max_rent_price,
    project_min_sale_price,
    project_max_sale_price
FROM lk_projects
WHERE project_id IS NOT NULL
  AND lead_id IS NOT NULL;
