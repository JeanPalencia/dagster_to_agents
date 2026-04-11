-- DDL de referencia para la tabla lk_mat_matches_visitors_to_leads.
-- La ingesta S3 → Postgres la hace el servicio data-lake-house (Geospot API).
-- Este archivo es la fuente de verdad del esquema que genera el pipeline; el backend
-- debe crear/actualizar la tabla en RDS para que coincida con el CSV.
--
-- Si la tabla ya existe pero le falta mat_traffic_type, ejecutar solo:
--   ALTER TABLE lk_mat_matches_visitors_to_leads ADD COLUMN IF NOT EXISTS mat_traffic_type VARCHAR(255);

CREATE TABLE IF NOT EXISTS lk_mat_matches_visitors_to_leads (
    vis_user_pseudo_id       VARCHAR(255),
    mat_event_datetime       TIMESTAMP,
    mat_event_name           VARCHAR(255),
    mat_match_source         VARCHAR(255),
    lead_id                  DOUBLE PRECISION,
    conv_start_date          TIMESTAMP,
    mat_channel              VARCHAR(255),
    mat_source               VARCHAR(255),
    mat_medium               VARCHAR(255),
    mat_campaign_name        VARCHAR(255),
    lead_phone_number        VARCHAR(255),
    lead_email               VARCHAR(255),
    lead_sector              VARCHAR(255),
    lead_fecha_cohort_dt     TIMESTAMP,
    lead_created_dt          TIMESTAMP,
    lead_cohort_type         VARCHAR(64),
    lead_max_type            VARCHAR(16),
    mat_with_match           VARCHAR(32),
    mat_page_location        TEXT,
    mat_entry_point          VARCHAR(255),
    mat_conversion_point      VARCHAR(255),
    mat_traffic_type         VARCHAR(255)
);

-- Para añadir solo la columna nueva si la tabla ya existía sin ella:
-- ALTER TABLE lk_mat_matches_visitors_to_leads ADD COLUMN IF NOT EXISTS mat_traffic_type VARCHAR(255);
