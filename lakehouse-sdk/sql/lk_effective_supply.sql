-- =============================================================================
-- lk_effective_supply.sql
-- Table definition for lk_effective_supply (Effective Supply / Oferta Efectiva)
-- 
-- This table summarizes spot event counts (pivoted) with spot attributes:
-- - view_count: GA4 page views on spot detail pages
-- - contact_count: GA4 contact form clicks/requests
-- - project_count: Spots added to projects (from bt_lds_lead_spots)
--
-- Source pipeline: effective_supply
--   stg_bq_spot_contact_view_event_counts (BigQuery)
--   + stg_gs_bt_lds_spot_added (GeoSpot)
--   + stg_gs_lk_spots (GeoSpot)
--   → core_effective_supply_events (UNION)
--   → core_effective_supply (pivot + JOIN)
--   → gold_lk_effective_supply (+ audit)
--   → lk_effective_supply_to_s3
--   → lk_effective_supply_to_geospot
--
-- Schedule: Monthly (1st day of each month at 3:00 AM Mexico City)
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply (
    -- =========================================================================
    -- Auto-incremental primary key
    -- =========================================================================
    id                          SERIAL PRIMARY KEY,

    -- =========================================================================
    -- Spot identifier
    -- =========================================================================
    spot_id                     BIGINT,

    -- =========================================================================
    -- Event counts (pivoted from event types)
    -- =========================================================================
    view_count                  INTEGER NOT NULL DEFAULT 0,
    contact_count               INTEGER NOT NULL DEFAULT 0,
    project_count               INTEGER NOT NULL DEFAULT 0,

    -- =========================================================================
    -- Relevance scores (log-normalized, p95-clipped, range 0.0 to 1.0)
    -- Weights: view=0.1, contact=0.2, project=0.7
    -- =========================================================================
    view_score                  DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    contact_score               DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    project_score               DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    total_score                 DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    -- =========================================================================
    -- Top percentile flag (p80+ on marketable spots, ties included)
    -- =========================================================================
    is_top_p80_id               INTEGER NOT NULL DEFAULT 0,
    is_top_p80                  VARCHAR(3) NOT NULL DEFAULT 'No',

    -- =========================================================================
    -- Modality flags (binary: Rent & Sale belongs to both populations)
    -- =========================================================================
    is_for_rent_id              INTEGER NOT NULL DEFAULT 0,
    is_for_rent                 VARCHAR(3) NOT NULL DEFAULT 'No',
    is_for_sale_id              INTEGER NOT NULL DEFAULT 0,
    is_for_sale                 VARCHAR(3) NOT NULL DEFAULT 'No',

    -- =========================================================================
    -- Top percentile flags per modality population (p80+, ties included)
    -- =========================================================================
    is_top_p80_rent_id          INTEGER NOT NULL DEFAULT 0,
    is_top_p80_rent             VARCHAR(3) NOT NULL DEFAULT 'No',
    is_top_p80_sale_id          INTEGER NOT NULL DEFAULT 0,
    is_top_p80_sale             VARCHAR(3) NOT NULL DEFAULT 'No',

    -- =========================================================================
    -- Spot classification
    -- =========================================================================
    spot_sector_id              INTEGER,
    spot_sector                 VARCHAR(50),
    spot_type_id                INTEGER,
    spot_type                   VARCHAR(100),
    spot_modality_id            INTEGER,
    spot_modality               VARCHAR(50),

    -- =========================================================================
    -- Spot location
    -- =========================================================================
    spot_state_id               INTEGER,
    spot_state                  VARCHAR(100),
    spot_municipality_id        INTEGER,
    spot_municipality           VARCHAR(100),
    spot_zip_code_id            INTEGER,
    spot_zip_code               VARCHAR(20),
    spot_corridor_id            INTEGER,
    spot_corridor               VARCHAR(100),
    spot_latitude               DOUBLE PRECISION,
    spot_longitude              DOUBLE PRECISION,

    -- =========================================================================
    -- Spot characteristics
    -- =========================================================================
    spot_area_in_sqm            DOUBLE PRECISION,
    spot_price_total_mxn_rent   DOUBLE PRECISION,
    spot_price_total_mxn_sale   DOUBLE PRECISION,
    spot_price_sqm_mxn_rent    DOUBLE PRECISION,
    spot_price_sqm_mxn_sale    DOUBLE PRECISION,

    -- =========================================================================
    -- Audit fields
    -- =========================================================================
    aud_run_id                  INTEGER NOT NULL,
    aud_inserted_at             TIMESTAMP,
    aud_inserted_date           DATE,
    aud_updated_at              TIMESTAMP,
    aud_updated_date            DATE,
    aud_job                     VARCHAR(100)
);

-- =============================================================================
-- Indexes for query performance
-- =============================================================================

-- Sector filtering (common: filter by Industrial, Retail, Office, Flex)
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_sector 
    ON lk_effective_supply(spot_sector_id);

-- State filtering (common: geographic analysis)
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_state 
    ON lk_effective_supply(spot_state_id);

-- Municipality filtering (more granular geographic analysis)
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_municipality 
    ON lk_effective_supply(spot_municipality_id);

-- Modality filtering (rent vs sale)
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_modality 
    ON lk_effective_supply(spot_modality_id);

-- Composite index for common dashboard queries
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_sector_state 
    ON lk_effective_supply(spot_sector_id, spot_state_id);

-- View count sorting (find most viewed spots)
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_view_count 
    ON lk_effective_supply(view_count DESC);

-- Contact count sorting (find most contacted spots)
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_contact_count 
    ON lk_effective_supply(contact_count DESC);

-- Project count sorting (find most added-to-project spots)
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_project_count 
    ON lk_effective_supply(project_count DESC);

-- Total score ranking (find most relevant spots)
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_total_score 
    ON lk_effective_supply(total_score DESC);

-- Top p80 flag filtering (quick access to top 20% marketable spots)
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_is_top_p80 
    ON lk_effective_supply(is_top_p80_id);

-- Top p80 rent population filtering
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_is_top_p80_rent 
    ON lk_effective_supply(is_top_p80_rent_id);

-- Top p80 sale population filtering
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_is_top_p80_sale 
    ON lk_effective_supply(is_top_p80_sale_id);

-- Audit: filter by job run date
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_audit_date 
    ON lk_effective_supply(aud_inserted_date);

-- Audit: filter by run ID
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_aud_run_id ON lk_effective_supply(aud_run_id);
