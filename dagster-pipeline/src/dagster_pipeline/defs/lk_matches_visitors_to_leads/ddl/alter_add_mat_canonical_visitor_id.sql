-- Agregar columna mat_canonical_visitor_id a la tabla MAT en GeoSpot.
-- Ejecutar UNA VEZ antes de desplegar el flujo con identity resolution.
-- Si el visitor no tiene resolución de identidad, el valor será igual a vis_user_pseudo_id.

ALTER TABLE lk_mat_matches_visitors_to_leads
ADD COLUMN IF NOT EXISTS mat_canonical_visitor_id VARCHAR(255);

COMMENT ON COLUMN lk_mat_matches_visitors_to_leads.mat_canonical_visitor_id
IS 'Identidad canónica del visitante. Resuelto por Union-Find sobre phone/email compartidos entre user_pseudo_ids y clients. Si no hay resolución, es igual a vis_user_pseudo_id.';

CREATE INDEX IF NOT EXISTS idx_mat_canonical_visitor_id
ON lk_mat_matches_visitors_to_leads (mat_canonical_visitor_id);
