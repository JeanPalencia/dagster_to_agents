-- =============================================================================
-- lk_projects_v2.sql
-- Table definition for lk_projects (version 2)
-- 
-- This table combines:
-- - Project data from stg_s2p_projects
-- - Lead attributes from stg_s2p_leads
-- - User profile data from stg_s2p_user_profiles
-- - Funnel metrics from core_project_funnel
--
-- Source asset: core_lk_projects → lk_projects_to_s3 → lk_projects_to_geospot
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_projects_v2 (
    -- =========================================================================
    -- Core identifiers
    -- =========================================================================
    project_id                              BIGINT PRIMARY KEY,
    project_name                            VARCHAR(255),
    lead_id                                 BIGINT,

    -- =========================================================================
    -- Lead attributes (from stg_s2p_leads)
    -- =========================================================================
    spot_sector                             VARCHAR(50),
    project_state_ids                       JSONB,
    lead_max_type                           VARCHAR(10),
    lead_campaign_type                      VARCHAR(50),
    lead_profiling_completed_at             TIMESTAMP,
    lead_project_funnel_relevant_id         INTEGER,

    -- =========================================================================
    -- User identifier
    -- =========================================================================
    user_id                                 BIGINT,

    -- =========================================================================
    -- User profile attributes (from stg_s2p_user_profiles)
    -- =========================================================================
    user_industria_role_id                  INTEGER,
    user_industria_role                     VARCHAR(100),

    -- =========================================================================
    -- Spot sector
    -- =========================================================================
    spot_sector_id                          INTEGER,

    -- =========================================================================
    -- Space requirements
    -- =========================================================================
    project_min_square_space                DOUBLE PRECISION,
    project_max_square_space                DOUBLE PRECISION,

    -- =========================================================================
    -- Currency
    -- =========================================================================
    project_currency_id                     INTEGER,
    spot_currency_sale_id                   INTEGER,
    spot_currency_sale                      VARCHAR(10),

    -- =========================================================================
    -- Pricing
    -- =========================================================================
    spot_price_sqm                          DOUBLE PRECISION,
    project_min_rent_price                  DOUBLE PRECISION,
    project_max_rent_price                  DOUBLE PRECISION,
    project_min_sale_price                  DOUBLE PRECISION,
    project_max_sale_price                  DOUBLE PRECISION,
    spot_price_sqm_mxn_sale_min             DOUBLE PRECISION,
    spot_price_sqm_mxn_sale_max             DOUBLE PRECISION,
    spot_price_sqm_mxn_rent_min             DOUBLE PRECISION,
    spot_price_sqm_mxn_rent_max             DOUBLE PRECISION,

    -- =========================================================================
    -- Terms
    -- =========================================================================
    project_rent_months                     INTEGER,
    project_commission                      DOUBLE PRECISION,

    -- =========================================================================
    -- Stage and status
    -- =========================================================================
    project_last_spot_stage_id              INTEGER,
    project_last_spot_stage                 VARCHAR(50),
    project_enable_id                       INTEGER,
    project_enable                          VARCHAR(10),
    project_disable_reason_id               INTEGER,
    project_disable_reason                  VARCHAR(200),

    -- =========================================================================
    -- Funnel relevance flags
    -- =========================================================================
    project_funnel_relevant_id              INTEGER,
    project_funnel_relevant                 VARCHAR(10),

    -- =========================================================================
    -- Won date
    -- =========================================================================
    project_won_date                        DATE,

    -- =========================================================================
    -- FUNNEL FIELDS (from core_project_funnel)
    -- =========================================================================

    -- Funnel: Lead date
    project_funnel_lead_date                DATE,

    -- Funnel: Visit created
    project_funnel_visit_created_date       DATE,
    project_funnel_visit_created_per_project_date DATE,
    project_funnel_visit_status             VARCHAR(100),

    -- Funnel: Visit realized
    project_funnel_visit_realized_at        TIMESTAMP,
    project_funnel_visit_realized_per_project_date DATE,

    -- Funnel: Visit confirmed
    project_funnel_visit_confirmed_at       TIMESTAMP,
    project_funnel_visit_confirmed_per_project_date DATE,

    -- Funnel: LOI
    project_funnel_loi_date                 DATE,
    project_funnel_loi_per_project_date     DATE,

    -- Funnel: Contract
    project_funnel_contract_date            DATE,
    project_funnel_contract_per_project_date DATE,

    -- Funnel: Transaction (won)
    project_funnel_transaction_date         DATE,
    project_funnel_transaction_per_project_date DATE,

    -- Funnel: Flow attribution
    project_funnel_flow                     VARCHAR(50),
    project_funnel_events                   JSONB,  -- JSON array of unique events per project

    -- =========================================================================
    -- Project timestamps
    -- =========================================================================
    project_created_at                      TIMESTAMP,
    project_created_date                    DATE,
    project_updated_at                      TIMESTAMP,
    project_updated_date                    DATE,

    -- =========================================================================
    -- Audit fields
    -- =========================================================================
    aud_inserted_at                         TIMESTAMP,
    aud_inserted_date                       DATE,
    aud_updated_at                          TIMESTAMP,
    aud_updated_date                        DATE,
    aud_job                                 VARCHAR(200)
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_lk_projects_v2_lead_id ON lk_projects_v2(lead_id);
CREATE INDEX IF NOT EXISTS idx_lk_projects_v2_user_id ON lk_projects_v2(user_id);
CREATE INDEX IF NOT EXISTS idx_lk_projects_v2_created_date ON lk_projects_v2(project_created_date);
