-- =============================================================================
-- lk_effective_supply_model_metrics.sql
-- Table definition for ML model evaluation metrics (Effective Supply)
--
-- Grain: market_universe x model_version
-- Each row records the evaluation metrics for one CatBoost model training run.
-- This enables historical tracking of model quality across monthly executions.
--
-- Source pipeline: effective_supply/ml
--   ml_feature_engineering → ml_train → gold_lk_effective_supply_model_metrics
--
-- Schedule: Monthly (same as effective_supply)
-- =============================================================================

CREATE TABLE IF NOT EXISTS lk_effective_supply_model_metrics (
    -- =========================================================================
    -- Auto-incremental primary key
    -- =========================================================================
    id                          SERIAL PRIMARY KEY,

    -- =========================================================================
    -- Universe + version identifier
    -- =========================================================================
    market_universe             VARCHAR(10) NOT NULL,   -- 'Rent' or 'Sale'
    model_version               VARCHAR(10) NOT NULL,   -- YYYYMM

    -- =========================================================================
    -- Model configuration
    -- =========================================================================
    model_variant               VARCHAR(30) NOT NULL,   -- base, reg, no_muni, no_muni_no_corr
    gate_passed_id              INTEGER NOT NULL DEFAULT 0, -- 1 = QA gate passed, 0 = fallback used
    gate_passed                 VARCHAR(3),             -- 'Yes' / 'No'

    -- =========================================================================
    -- Random test set metrics (stratified 15% holdout)
    -- =========================================================================
    auc_roc_random              DOUBLE PRECISION NOT NULL,  -- AUC-ROC on random test
    pr_auc_random               DOUBLE PRECISION NOT NULL,  -- PR-AUC (precision-recall) on random test
    brier_random                DOUBLE PRECISION NOT NULL,  -- Brier score on random test (lower = better)

    -- =========================================================================
    -- Group holdout test set metrics (geographic stress test)
    -- =========================================================================
    auc_roc_group               DOUBLE PRECISION NOT NULL,  -- AUC-ROC on group holdout municipalities
    pr_auc_group                DOUBLE PRECISION NOT NULL,  -- PR-AUC on group holdout
    brier_group                 DOUBLE PRECISION NOT NULL,  -- Brier score on group holdout

    -- =========================================================================
    -- Derived metrics
    -- =========================================================================
    gap_auc                     DOUBLE PRECISION NOT NULL,  -- AUC_random - AUC_group (overfitting signal)
    best_iteration              INTEGER,                    -- CatBoost early-stopping iteration

    -- =========================================================================
    -- Training window metadata
    -- =========================================================================
    window_end_date             DATE,
    window_months               INTEGER NOT NULL DEFAULT 6,

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
    UNIQUE (market_universe, model_version)
);

-- =============================================================================
-- Indexes for query performance
-- =============================================================================

-- Historical trend by universe
CREATE INDEX IF NOT EXISTS idx_lk_es_metrics_universe_version
    ON lk_effective_supply_model_metrics(market_universe, model_version DESC);

-- Quick lookup by version across universes
CREATE INDEX IF NOT EXISTS idx_lk_es_metrics_version
    ON lk_effective_supply_model_metrics(model_version);

-- Filter by gate status
CREATE INDEX IF NOT EXISTS idx_lk_es_metrics_gate
    ON lk_effective_supply_model_metrics(gate_passed_id);

-- Audit date filtering
CREATE INDEX IF NOT EXISTS idx_lk_es_metrics_audit_date
    ON lk_effective_supply_model_metrics(aud_inserted_date);

-- Audit: filter by run ID
CREATE INDEX IF NOT EXISTS idx_lk_es_metrics_aud_run_id ON lk_effective_supply_model_metrics(aud_run_id);
