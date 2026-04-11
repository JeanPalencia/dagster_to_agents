-- =============================================================================
-- lk_effective_supply_drivers.sql
-- Table definition for ML feature importance (Effective Supply)
--
-- Grain: market_universe x feature_name
-- Contains normalised permutation importance per feature per universe.
--
-- Source pipeline: effective_supply/ml
--   ml_train → ml_drivers → gold_lk_effective_supply_drivers
--
-- Schedule: Monthly (same as effective_supply)
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply_drivers (
    -- =========================================================================
    -- Auto-incremental primary key
    -- =========================================================================
    id                          SERIAL PRIMARY KEY,

    -- =========================================================================
    -- Universe + feature identifier
    -- =========================================================================
    market_universe             VARCHAR(10) NOT NULL,  -- 'Rent' or 'Sale'
    feature_name                VARCHAR(50) NOT NULL,

    -- =========================================================================
    -- Importance metrics
    -- =========================================================================
    importance                  DOUBLE PRECISION NOT NULL,  -- normalised 0–1
    importance_method           VARCHAR(30) NOT NULL,        -- permutation | catboost_builtin
    rank                        INTEGER NOT NULL,            -- 1 = most important

    -- =========================================================================
    -- Model metadata
    -- =========================================================================
    model_version               VARCHAR(10) NOT NULL,   -- YYYYMM
    window_end_date             DATE,
    model_variant               VARCHAR(30) NOT NULL,

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
    UNIQUE (market_universe, feature_name, model_version)
);

-- =============================================================================
-- Indexes for query performance
-- =============================================================================

-- Top features by importance within a universe
CREATE INDEX IF NOT EXISTS idx_lk_es_drivers_universe_rank
    ON lk_effective_supply_drivers(market_universe, rank);

-- Filter by model version
CREATE INDEX IF NOT EXISTS idx_lk_es_drivers_version
    ON lk_effective_supply_drivers(model_version);

-- Audit: filter by run ID
CREATE INDEX IF NOT EXISTS idx_lk_es_drivers_aud_run_id ON lk_effective_supply_drivers(aud_run_id);
