-- =============================================================================
-- lk_effective_supply_category_effects.sql
-- Table definition for ML category effects (Effective Supply)
--
-- Grain: market_universe x feature_name x category_id
-- Shows the lift (vs global average) of each feature value per universe.
--
-- Source pipeline: effective_supply/ml
--   ml_build_universes -> ml_category_effects -> gold -> publish
--
-- Schedule: Monthly (same as effective_supply)
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply_category_effects (
    -- =========================================================================
    -- Auto-incremental primary key
    -- =========================================================================
    id                          SERIAL PRIMARY KEY,

    -- =========================================================================
    -- Dimensions
    -- =========================================================================
    market_universe             VARCHAR(10) NOT NULL,   -- 'Rent' or 'Sale'
    feature_name                VARCHAR(50) NOT NULL,   -- e.g. 'estado', 'sector', 'precio_total'
    category_id                 VARCHAR(100) NOT NULL,  -- e.g. '9' (state ID) or 'Q3'
    category_name               VARCHAR(200),           -- e.g. 'Ciudad de México', '$50,000 – $120,000'

    -- =========================================================================
    -- Metrics
    -- =========================================================================
    support_n                   INTEGER NOT NULL,        -- number of spots in category
    p_top                       DOUBLE PRECISION NOT NULL, -- rate of is_top_p80 = 1
    lift                        DOUBLE PRECISION NOT NULL, -- p_top / p_top_global

    -- =========================================================================
    -- Model metadata
    -- =========================================================================
    model_version               VARCHAR(10) NOT NULL,   -- YYYYMM
    window_end_date             DATE,

    -- =========================================================================
    -- Audit fields
    -- =========================================================================
    aud_run_id                  INTEGER NOT NULL,
    aud_inserted_at             TIMESTAMP,
    aud_inserted_date           DATE,
    aud_updated_at              TIMESTAMP,
    aud_updated_date            DATE,
    aud_job                     VARCHAR(100),

    -- =========================================================================
    UNIQUE (market_universe, feature_name, category_id, model_version)
);

-- =============================================================================
-- Indexes for query performance
-- =============================================================================

-- Find top categories by lift within a universe
CREATE INDEX IF NOT EXISTS idx_lk_es_catfx_universe_lift
    ON lk_effective_supply_category_effects(market_universe, lift DESC);

-- Filter by feature within a universe
CREATE INDEX IF NOT EXISTS idx_lk_es_catfx_universe_feature
    ON lk_effective_supply_category_effects(market_universe, feature_name);

-- Filter by model version
CREATE INDEX IF NOT EXISTS idx_lk_es_catfx_version
    ON lk_effective_supply_category_effects(model_version);

-- Audit: filter by run ID
CREATE INDEX IF NOT EXISTS idx_lk_es_catfx_aud_run_id ON lk_effective_supply_category_effects(aud_run_id);
