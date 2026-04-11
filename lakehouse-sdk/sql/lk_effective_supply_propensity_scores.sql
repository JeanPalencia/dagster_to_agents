-- =============================================================================
-- lk_effective_supply_propensity_scores.sql
-- Table definition for ML propensity scores (Effective Supply)
--
-- Grain: spot_id x market_universe
-- Each spot appears once per universe it belongs to (Rent and/or Sale).
-- Spots with modality "Rent & Sale" appear in both universes.
--
-- Source pipeline: effective_supply/ml
--   gold_lk_effective_supply → ml_build_universes → ml_feature_engineering
--   → ml_train → ml_score → gold_lk_effective_supply_propensity_scores
--
-- Schedule: Monthly (same as effective_supply)
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply_propensity_scores (
    -- =========================================================================
    -- Auto-incremental primary key
    -- =========================================================================
    id                          SERIAL PRIMARY KEY,

    -- =========================================================================
    -- Spot + universe identifier
    -- =========================================================================
    spot_id                     BIGINT NOT NULL,
    market_universe             VARCHAR(10) NOT NULL,  -- 'Rent' or 'Sale'

    -- =========================================================================
    -- Propensity score
    -- =========================================================================
    p_top_p80                   DOUBLE PRECISION NOT NULL,

    -- =========================================================================
    -- Model metadata
    -- =========================================================================
    model_version               VARCHAR(10) NOT NULL,   -- YYYYMM
    window_end_date             DATE,
    window_months               INTEGER NOT NULL DEFAULT 6,
    model_variant               VARCHAR(30) NOT NULL,   -- base, reg, no_muni, no_muni_no_corr

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
    UNIQUE (spot_id, market_universe, aud_run_id)
);

-- =============================================================================
-- Indexes for query performance
-- =============================================================================

-- Find top-scoring spots within a universe
CREATE INDEX IF NOT EXISTS idx_lk_es_propensity_universe_score
    ON lk_effective_supply_propensity_scores(market_universe, p_top_p80 DESC);

-- Lookup by spot_id across universes
CREATE INDEX IF NOT EXISTS idx_lk_es_propensity_spot
    ON lk_effective_supply_propensity_scores(spot_id);

-- Filter by model version
CREATE INDEX IF NOT EXISTS idx_lk_es_propensity_version
    ON lk_effective_supply_propensity_scores(model_version);

-- Audit date filtering
CREATE INDEX IF NOT EXISTS idx_lk_es_propensity_audit_date
    ON lk_effective_supply_propensity_scores(aud_inserted_date);

-- Audit: filter by run ID
CREATE INDEX IF NOT EXISTS idx_lk_es_propensity_aud_run_id ON lk_effective_supply_propensity_scores(aud_run_id);
