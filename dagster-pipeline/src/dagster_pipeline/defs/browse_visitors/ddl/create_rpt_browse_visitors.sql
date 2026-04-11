-- DDL: rpt_browse_visitors
-- Tabla de visitantes únicos de browse pages en spot2.mx
-- Una fila por canonical_visitor_id con métricas agregadas de sesiones

CREATE TABLE IF NOT EXISTS rpt_browse_visitors (
    canonical_visitor_id    VARCHAR(255) NOT NULL,
    total_sessions          INTEGER      NOT NULL DEFAULT 0,
    total_page_views        INTEGER      NOT NULL DEFAULT 0,
    first_session_date      DATE,
    last_session_date       DATE,
    days_active             INTEGER      NOT NULL DEFAULT 0,
    primary_channel         VARCHAR(100),
    primary_traffic_type    VARCHAR(50),
    primary_source          VARCHAR(255),
    primary_medium          VARCHAR(255),
    sectors_visited         TEXT,
    is_converted            BOOLEAN      NOT NULL DEFAULT FALSE,
    lead_id                 BIGINT,
    lead_sector             VARCHAR(255),
    lead_cohort_type        VARCHAR(100),
    is_scraping             BOOLEAN      NOT NULL DEFAULT FALSE,

    -- Audit fields (added by gold layer)
    aud_inserted_at         TIMESTAMP,
    aud_inserted_date       DATE,
    aud_updated_at          TIMESTAMP,
    aud_updated_date        DATE,
    aud_job                 VARCHAR(100)
);

COMMENT ON TABLE rpt_browse_visitors
IS 'Visitantes únicos de browse pages en spot2.mx. Una fila por canonical_visitor_id con métricas agregadas de sesiones y flag de conversión a lead.';

COMMENT ON COLUMN rpt_browse_visitors.canonical_visitor_id
IS 'ID canónico del visitante. Si convirtió a lead, viene de mat_canonical_visitor_id (Union-Find); si no, es su user_pseudo_id de GA4.';

COMMENT ON COLUMN rpt_browse_visitors.total_sessions
IS 'Número total de sesiones de browse pages del visitante en la ventana de 6 meses.';

COMMENT ON COLUMN rpt_browse_visitors.total_page_views
IS 'Suma de page_views en browse pages a través de todas las sesiones.';

COMMENT ON COLUMN rpt_browse_visitors.first_session_date
IS 'Fecha de la primera sesión de browse registrada.';

COMMENT ON COLUMN rpt_browse_visitors.last_session_date
IS 'Fecha de la última sesión de browse registrada.';

COMMENT ON COLUMN rpt_browse_visitors.days_active
IS 'Número de días distintos con actividad de browse.';

COMMENT ON COLUMN rpt_browse_visitors.primary_channel
IS 'Canal de atribución más frecuente del visitante (12 reglas de canal).';

COMMENT ON COLUMN rpt_browse_visitors.primary_traffic_type
IS 'Tipo de tráfico más frecuente (Organic, Paid, Bot/Spam, Unassigned).';

COMMENT ON COLUMN rpt_browse_visitors.sectors_visited
IS 'Lista de sectores únicos visitados (bodegas, coworking, oficinas, etc.), separados por coma.';

COMMENT ON COLUMN rpt_browse_visitors.is_converted
IS 'TRUE si el visitante tiene un match confirmado en lk_mat_matches_visitors_to_leads.';

COMMENT ON COLUMN rpt_browse_visitors.lead_id
IS 'ID del lead en el CRM, si convirtió (NULL si no convirtió).';

COMMENT ON COLUMN rpt_browse_visitors.lead_sector
IS 'Sector del lead en el CRM (NULL si no convirtió).';

COMMENT ON COLUMN rpt_browse_visitors.lead_cohort_type
IS 'Tipo de cohorte del lead en el CRM (NULL si no convirtió).';

COMMENT ON COLUMN rpt_browse_visitors.is_scraping
IS 'TRUE si el visitante fue identificado como bot/scraping.';

-- Indices
CREATE INDEX IF NOT EXISTS idx_rpt_bv_canonical_visitor_id
ON rpt_browse_visitors (canonical_visitor_id);

CREATE INDEX IF NOT EXISTS idx_rpt_bv_is_converted
ON rpt_browse_visitors (is_converted);

CREATE INDEX IF NOT EXISTS idx_rpt_bv_first_session_date
ON rpt_browse_visitors (first_session_date);

CREATE INDEX IF NOT EXISTS idx_rpt_bv_primary_channel
ON rpt_browse_visitors (primary_channel);
