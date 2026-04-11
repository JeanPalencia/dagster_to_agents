-- =============================================================================
-- lk_effective_supply_rules.sql
-- Table definition for ML hierarchical rules (Effective Supply)
--
-- Grain: market_universe x rule_id
-- Hierarchical drill-down rules: estado-first, geo-aware (municipio and/or
-- corredor evaluated independently), with remaining features ordered by
-- CatBoost importance and pruned by 5% minimum lift increment.
--
-- Source pipeline: effective_supply/ml
--   ml_build_universes + ml_drivers -> ml_rules -> gold -> publish
--
-- Schedule: Monthly (same as effective_supply)
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply_rules (
    -- =========================================================================
    -- Auto-incremental primary key
    -- =========================================================================
    id                          SERIAL PRIMARY KEY,

    -- =========================================================================
    -- Dimensions
    -- =========================================================================
    market_universe             VARCHAR(10) NOT NULL,   -- 'Rent' or 'Sale'
    rule_id                     INTEGER NOT NULL,        -- sequential, 1 = highest p_top

    -- =========================================================================
    -- Rule columns (individual segmenters, NULL if not relevant)
    -- =========================================================================
    spot_state                  VARCHAR(100) NOT NULL,   -- always present (first split)
    spot_sector                 VARCHAR(100),            -- e.g. 'Retail', 'Industrial, Office'
    spot_type                   VARCHAR(100),            -- e.g. 'Single', 'Single, Subspace' (Marascuilo fusion)
    spot_municipality           VARCHAR(500),            -- Municipality names (coverage-based per-state encoding), NULL if corridor used
    spot_corridor               VARCHAR(500),            -- Corridor names (coverage-based per-state encoding), NULL if municipality used
    price_sqm_range             VARCHAR(100),            -- e.g. '$100 – $300' (value range)
    price_sqm_pctl_range        VARCHAR(20),             -- e.g. 'P25-P70' (percentile window)
    price_sqm_is_discriminant_id INTEGER NOT NULL DEFAULT 0, -- 1 = range improves lift >= 5%, 0 = context only
    price_sqm_is_discriminant   VARCHAR(3),              -- 'Yes' / 'No'
    area_range                  VARCHAR(100),            -- e.g. '100 – 500 m²' (value range)
    area_pctl_range             VARCHAR(20),             -- e.g. 'P5-P95' (percentile window)
    area_is_discriminant_id     INTEGER NOT NULL DEFAULT 0,  -- 1 = range improves lift >= 5%, 0 = context only
    area_is_discriminant        VARCHAR(3),              -- 'Yes' / 'No'

    -- =========================================================================
    -- Rule summary and metrics
    -- =========================================================================
    rule_text                   TEXT NOT NULL,            -- full human-readable rule path
    support_n                   INTEGER NOT NULL,         -- number of spots matching the rule
    p_top                       DOUBLE PRECISION NOT NULL, -- rate of is_top_p80 = 1 for matching spots
    lift                        DOUBLE PRECISION NOT NULL, -- p_top / p_top_global
    avg_score_percentile        DOUBLE PRECISION,          -- avg total_score percentile of matching spots (0-100)
    strength                    VARCHAR(20),               -- 'Very High' (>=3x), 'High' (>=2x), 'Moderate' (>=1.5x)

    -- =========================================================================
    -- Model metadata
    -- =========================================================================
    model_version               VARCHAR(10) NOT NULL,    -- YYYYMM
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
    UNIQUE (market_universe, rule_id, model_version)
);

-- =============================================================================
-- Indexes for query performance
-- =============================================================================

-- Top rules by p_top within a universe
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_universe_ptop
    ON lk_effective_supply_rules(market_universe, p_top DESC);

-- Top rules by lift
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_universe_lift
    ON lk_effective_supply_rules(market_universe, lift DESC);

-- Filter by state within a universe
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_state
    ON lk_effective_supply_rules(market_universe, spot_state);

-- Filter by strength category
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_strength
    ON lk_effective_supply_rules(market_universe, strength);

-- Filter by model version
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_version
    ON lk_effective_supply_rules(model_version);

-- Audit: filter by run ID
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_aud_run_id ON lk_effective_supply_rules(aud_run_id);
