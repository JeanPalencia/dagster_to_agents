-- =============================================================================
-- recreate_all_tables.sql
-- Drop and recreate all 6 effective_supply tables in GeoSpot (PostgreSQL).
-- Generated from individual SQL definitions in lakehouse-sdk/sql/.
--
-- WARNING: This will DELETE all data. Use only for full resets.
-- =============================================================================

-- ======================= DROP ALL TABLES ====================================
DROP TABLE IF EXISTS lk_effective_supply CASCADE;
DROP TABLE IF EXISTS lk_effective_supply_propensity_scores CASCADE;
DROP TABLE IF EXISTS lk_effective_supply_drivers CASCADE;
DROP TABLE IF EXISTS lk_effective_supply_category_effects CASCADE;
DROP TABLE IF EXISTS lk_effective_supply_rules CASCADE;
DROP TABLE IF EXISTS lk_effective_supply_model_metrics CASCADE;


-- =============================================================================
-- 1/6  lk_effective_supply
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply (
    id                          SERIAL PRIMARY KEY,

    spot_id                     BIGINT,

    view_count                  INTEGER NOT NULL DEFAULT 0,
    contact_count               INTEGER NOT NULL DEFAULT 0,
    project_count               INTEGER NOT NULL DEFAULT 0,

    view_score                  DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    contact_score               DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    project_score               DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    total_score                 DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    is_top_p80_id               INTEGER NOT NULL DEFAULT 0,
    is_top_p80                  VARCHAR(3) NOT NULL DEFAULT 'No',

    is_for_rent_id              INTEGER NOT NULL DEFAULT 0,
    is_for_rent                 VARCHAR(3) NOT NULL DEFAULT 'No',
    is_for_sale_id              INTEGER NOT NULL DEFAULT 0,
    is_for_sale                 VARCHAR(3) NOT NULL DEFAULT 'No',

    is_top_p80_rent_id          INTEGER NOT NULL DEFAULT 0,
    is_top_p80_rent             VARCHAR(3) NOT NULL DEFAULT 'No',
    is_top_p80_sale_id          INTEGER NOT NULL DEFAULT 0,
    is_top_p80_sale             VARCHAR(3) NOT NULL DEFAULT 'No',

    spot_sector_id              INTEGER,
    spot_sector                 VARCHAR(50),
    spot_type_id                INTEGER,
    spot_type                   VARCHAR(100),
    spot_modality_id            INTEGER,
    spot_modality               VARCHAR(50),

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

    spot_area_in_sqm            DOUBLE PRECISION,
    spot_price_total_mxn_rent   DOUBLE PRECISION,
    spot_price_total_mxn_sale   DOUBLE PRECISION,
    spot_price_sqm_mxn_rent    DOUBLE PRECISION,
    spot_price_sqm_mxn_sale    DOUBLE PRECISION,

    aud_run_id                  INTEGER NOT NULL,
    aud_inserted_at             TIMESTAMP,
    aud_inserted_date           DATE,
    aud_updated_at              TIMESTAMP,
    aud_updated_date            DATE,
    aud_job                     VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_sector
    ON lk_effective_supply(spot_sector_id);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_state
    ON lk_effective_supply(spot_state_id);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_municipality
    ON lk_effective_supply(spot_municipality_id);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_modality
    ON lk_effective_supply(spot_modality_id);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_sector_state
    ON lk_effective_supply(spot_sector_id, spot_state_id);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_view_count
    ON lk_effective_supply(view_count DESC);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_contact_count
    ON lk_effective_supply(contact_count DESC);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_project_count
    ON lk_effective_supply(project_count DESC);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_total_score
    ON lk_effective_supply(total_score DESC);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_is_top_p80
    ON lk_effective_supply(is_top_p80_id);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_is_top_p80_rent
    ON lk_effective_supply(is_top_p80_rent_id);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_is_top_p80_sale
    ON lk_effective_supply(is_top_p80_sale_id);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_audit_date
    ON lk_effective_supply(aud_inserted_date);
CREATE INDEX IF NOT EXISTS idx_lk_effective_supply_aud_run_id
    ON lk_effective_supply(aud_run_id);


-- =============================================================================
-- 2/6  lk_effective_supply_propensity_scores
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply_propensity_scores (
    id                          SERIAL PRIMARY KEY,

    spot_id                     BIGINT NOT NULL,
    market_universe             VARCHAR(10) NOT NULL,

    p_top_p80                   DOUBLE PRECISION NOT NULL,

    model_version               VARCHAR(10) NOT NULL,
    window_end_date             DATE,
    window_months               INTEGER NOT NULL DEFAULT 6,
    model_variant               VARCHAR(30) NOT NULL,

    aud_run_id                  INTEGER NOT NULL,
    aud_inserted_at             TIMESTAMP,
    aud_inserted_date           DATE,
    aud_updated_at              TIMESTAMP,
    aud_updated_date            DATE,
    aud_job                     VARCHAR(100),

    UNIQUE (spot_id, market_universe, aud_run_id)
);

CREATE INDEX IF NOT EXISTS idx_lk_es_propensity_universe_score
    ON lk_effective_supply_propensity_scores(market_universe, p_top_p80 DESC);
CREATE INDEX IF NOT EXISTS idx_lk_es_propensity_spot
    ON lk_effective_supply_propensity_scores(spot_id);
CREATE INDEX IF NOT EXISTS idx_lk_es_propensity_version
    ON lk_effective_supply_propensity_scores(model_version);
CREATE INDEX IF NOT EXISTS idx_lk_es_propensity_audit_date
    ON lk_effective_supply_propensity_scores(aud_inserted_date);
CREATE INDEX IF NOT EXISTS idx_lk_es_propensity_aud_run_id
    ON lk_effective_supply_propensity_scores(aud_run_id);


-- =============================================================================
-- 3/6  lk_effective_supply_drivers
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply_drivers (
    id                          SERIAL PRIMARY KEY,

    market_universe             VARCHAR(10) NOT NULL,
    feature_name                VARCHAR(50) NOT NULL,

    importance                  DOUBLE PRECISION NOT NULL,
    importance_method           VARCHAR(30) NOT NULL,
    rank                        INTEGER NOT NULL,

    model_version               VARCHAR(10) NOT NULL,
    window_end_date             DATE,
    model_variant               VARCHAR(30) NOT NULL,

    aud_run_id                  INTEGER NOT NULL,
    aud_inserted_at             TIMESTAMP,
    aud_inserted_date           DATE,
    aud_updated_at              TIMESTAMP,
    aud_updated_date            DATE,
    aud_job                     VARCHAR(100),

    UNIQUE (market_universe, feature_name, model_version)
);

CREATE INDEX IF NOT EXISTS idx_lk_es_drivers_universe_rank
    ON lk_effective_supply_drivers(market_universe, rank);
CREATE INDEX IF NOT EXISTS idx_lk_es_drivers_version
    ON lk_effective_supply_drivers(model_version);
CREATE INDEX IF NOT EXISTS idx_lk_es_drivers_aud_run_id
    ON lk_effective_supply_drivers(aud_run_id);


-- =============================================================================
-- 4/6  lk_effective_supply_category_effects
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply_category_effects (
    id                          SERIAL PRIMARY KEY,

    market_universe             VARCHAR(10) NOT NULL,
    feature_name                VARCHAR(50) NOT NULL,
    category_id                 VARCHAR(100) NOT NULL,
    category_name               VARCHAR(200),

    support_n                   INTEGER NOT NULL,
    p_top                       DOUBLE PRECISION NOT NULL,
    lift                        DOUBLE PRECISION NOT NULL,

    model_version               VARCHAR(10) NOT NULL,
    window_end_date             DATE,

    aud_run_id                  INTEGER NOT NULL,
    aud_inserted_at             TIMESTAMP,
    aud_inserted_date           DATE,
    aud_updated_at              TIMESTAMP,
    aud_updated_date            DATE,
    aud_job                     VARCHAR(100),

    UNIQUE (market_universe, feature_name, category_id, model_version)
);

CREATE INDEX IF NOT EXISTS idx_lk_es_catfx_universe_lift
    ON lk_effective_supply_category_effects(market_universe, lift DESC);
CREATE INDEX IF NOT EXISTS idx_lk_es_catfx_universe_feature
    ON lk_effective_supply_category_effects(market_universe, feature_name);
CREATE INDEX IF NOT EXISTS idx_lk_es_catfx_version
    ON lk_effective_supply_category_effects(model_version);
CREATE INDEX IF NOT EXISTS idx_lk_es_catfx_aud_run_id
    ON lk_effective_supply_category_effects(aud_run_id);


-- =============================================================================
-- 5/6  lk_effective_supply_rules
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply_rules (
    id                          SERIAL PRIMARY KEY,

    market_universe             VARCHAR(10) NOT NULL,
    rule_id                     INTEGER NOT NULL,

    spot_state                  VARCHAR(100) NOT NULL,
    spot_sector                 VARCHAR(100),
    spot_type                   VARCHAR(100),
    spot_municipality           VARCHAR(500),
    spot_corridor               VARCHAR(500),
    price_sqm_range             VARCHAR(100),
    price_sqm_pctl_range        VARCHAR(20),
    price_sqm_is_discriminant_id INTEGER NOT NULL DEFAULT 0,
    price_sqm_is_discriminant   VARCHAR(3),
    area_range                  VARCHAR(100),
    area_pctl_range             VARCHAR(20),
    area_is_discriminant_id     INTEGER NOT NULL DEFAULT 0,
    area_is_discriminant        VARCHAR(3),

    rule_text                   TEXT NOT NULL,
    support_n                   INTEGER NOT NULL,
    p_top                       DOUBLE PRECISION NOT NULL,
    lift                        DOUBLE PRECISION NOT NULL,
    avg_score_percentile        DOUBLE PRECISION,
    strength                    VARCHAR(20),

    model_version               VARCHAR(10) NOT NULL,
    window_end_date             DATE,

    aud_run_id                  INTEGER NOT NULL,
    aud_inserted_at             TIMESTAMP,
    aud_inserted_date           DATE,
    aud_updated_at              TIMESTAMP,
    aud_updated_date            DATE,
    aud_job                     VARCHAR(100),

    UNIQUE (market_universe, rule_id, model_version)
);

CREATE INDEX IF NOT EXISTS idx_lk_es_rules_universe_ptop
    ON lk_effective_supply_rules(market_universe, p_top DESC);
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_universe_lift
    ON lk_effective_supply_rules(market_universe, lift DESC);
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_state
    ON lk_effective_supply_rules(market_universe, spot_state);
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_strength
    ON lk_effective_supply_rules(market_universe, strength);
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_version
    ON lk_effective_supply_rules(model_version);
CREATE INDEX IF NOT EXISTS idx_lk_es_rules_aud_run_id
    ON lk_effective_supply_rules(aud_run_id);


-- =============================================================================
-- 6/6  lk_effective_supply_model_metrics
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply_model_metrics (
    id                          SERIAL PRIMARY KEY,

    market_universe             VARCHAR(10) NOT NULL,
    model_version               VARCHAR(10) NOT NULL,

    model_variant               VARCHAR(30) NOT NULL,
    gate_passed_id              INTEGER NOT NULL DEFAULT 0,
    gate_passed                 VARCHAR(3),

    auc_roc_random              DOUBLE PRECISION NOT NULL,
    pr_auc_random               DOUBLE PRECISION NOT NULL,
    brier_random                DOUBLE PRECISION NOT NULL,

    auc_roc_group               DOUBLE PRECISION NOT NULL,
    pr_auc_group                DOUBLE PRECISION NOT NULL,
    brier_group                 DOUBLE PRECISION NOT NULL,

    gap_auc                     DOUBLE PRECISION NOT NULL,
    best_iteration              INTEGER,

    window_end_date             DATE,
    window_months               INTEGER NOT NULL DEFAULT 6,

    aud_run_id                  INTEGER NOT NULL,
    aud_inserted_at             TIMESTAMP,
    aud_inserted_date           DATE,
    aud_updated_at              TIMESTAMP,
    aud_updated_date            DATE,
    aud_job                     VARCHAR(100),

    UNIQUE (market_universe, model_version)
);

CREATE INDEX IF NOT EXISTS idx_lk_es_metrics_universe_version
    ON lk_effective_supply_model_metrics(market_universe, model_version DESC);
CREATE INDEX IF NOT EXISTS idx_lk_es_metrics_version
    ON lk_effective_supply_model_metrics(model_version);
CREATE INDEX IF NOT EXISTS idx_lk_es_metrics_gate
    ON lk_effective_supply_model_metrics(gate_passed_id);
CREATE INDEX IF NOT EXISTS idx_lk_es_metrics_audit_date
    ON lk_effective_supply_model_metrics(aud_inserted_date);
CREATE INDEX IF NOT EXISTS idx_lk_es_metrics_aud_run_id
    ON lk_effective_supply_model_metrics(aud_run_id);
