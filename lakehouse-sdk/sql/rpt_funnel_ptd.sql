-- =============================================================================
-- rpt_funnel_ptd.sql
-- Table definition for rpt_funnel_ptd (Funnel Period-to-Date Report)
--
-- Derived reporting table showing funnel metrics for the current period
-- (Month to Date and Quarter to Date) with Cohort and Rolling counting
-- methods. Includes OKR targets, Kaplan-Meier projections, PTD conversion
-- rates, and linear projections.
--
-- Source pipeline: funnel_ptd
--   Bronze: lk_leads, lk_projects, lk_okrs, bt_lds_lead_spots
--   STG: leads, projects, okrs, lead_spots
--   Core: current_counts, monitor, km_survival, projections, final
--   Gold: gold_fptd_period_to_date (+ audit)
--   Publish: rpt_funnel_ptd_to_s3 → rpt_funnel_ptd_to_geospot
--
-- Schedule: Daily (manual for now)
-- =============================================================================

CREATE TABLE IF NOT EXISTS rpt_funnel_ptd (
    -- =========================================================================
    -- Auto-incremental primary key
    -- =========================================================================
    id                              SERIAL PRIMARY KEY,

    -- =========================================================================
    -- Dimensions
    -- =========================================================================
    counting_method                 VARCHAR(10) NOT NULL,       -- Cohort / Rolling
    as_of_date                      DATE NOT NULL,              -- Reference date (yesterday)
    ptd_type                        VARCHAR(20) NOT NULL,       -- Month to Date / Quarter to Date
    period_start                    DATE NOT NULL,
    period_end                      DATE NOT NULL,
    cohort_type                     VARCHAR(15) NOT NULL,       -- New / Reactivated
    value_type                      VARCHAR(30) NOT NULL,       -- Actuals / ⚠️ Actuals + Forecast

    -- =========================================================================
    -- Current counts (events observed within the period)
    -- In Actuals rows these are integers; in Forecast rows
    -- scheduled_visits, completed_visits and lois include the KM
    -- projection (fractional). Rounding is left to the consumer.
    -- =========================================================================
    leads_current                   INTEGER NOT NULL DEFAULT 0,
    projects_current                INTEGER NOT NULL DEFAULT 0,
    scheduled_visits_current        DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    confirmed_visits_current        INTEGER NOT NULL DEFAULT 0,
    completed_visits_current        DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    lois_current                    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    won_current                     INTEGER NOT NULL DEFAULT 0,

    -- =========================================================================
    -- PTD targets (OKR * month_fraction for the period)
    -- =========================================================================
    leads_ptd                       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    projects_ptd                    DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    scheduled_visits_ptd            DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    confirmed_visits_ptd            DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    completed_visits_ptd            DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    lois_ptd                        DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    -- =========================================================================
    -- OKR totals (full month or quarter target)
    -- =========================================================================
    okr_leads_total                 DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    okr_projects_total              DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    okr_scheduled_visits_total      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    okr_confirmed_visits_total      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    okr_completed_visits_total      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    okr_lois_total                  DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    -- =========================================================================
    -- Kaplan-Meier projections (additional conversions expected)
    -- =========================================================================
    scheduled_visits_proj           DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    completed_visits_proj           DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    lois_proj                       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    n_projects_in_projection_scope  INTEGER NOT NULL DEFAULT 0,

    -- =========================================================================
    -- PTD conversion rates (current / ptd_target)
    -- =========================================================================
    leads_ptd_rate                  DOUBLE PRECISION,
    projects_ptd_rate               DOUBLE PRECISION,
    scheduled_visits_ptd_rate       DOUBLE PRECISION,
    confirmed_visits_ptd_rate       DOUBLE PRECISION,
    completed_visits_ptd_rate       DOUBLE PRECISION,
    lois_ptd_rate                   DOUBLE PRECISION,

    -- =========================================================================
    -- Linear projection counts (ptd_rate * okr_total for forecast rows,
    -- actual count for actuals rows)
    -- =========================================================================
    leads_count                     DOUBLE PRECISION,
    projects_count                  DOUBLE PRECISION,
    scheduled_visits_count          DOUBLE PRECISION,
    confirmed_visits_count          DOUBLE PRECISION,
    completed_visits_count          DOUBLE PRECISION,
    lois_count                      DOUBLE PRECISION,

    -- =========================================================================
    -- Audit fields
    -- =========================================================================
    aud_inserted_at                 TIMESTAMP,
    aud_inserted_date               DATE,
    aud_updated_at                  TIMESTAMP,
    aud_updated_date                DATE,
    aud_job                         VARCHAR(100)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_rpt_funnel_ptd_method
    ON rpt_funnel_ptd(counting_method);
CREATE INDEX IF NOT EXISTS idx_rpt_funnel_ptd_ptd_type
    ON rpt_funnel_ptd(ptd_type);
CREATE INDEX IF NOT EXISTS idx_rpt_funnel_ptd_period
    ON rpt_funnel_ptd(period_start, period_end);
CREATE INDEX IF NOT EXISTS idx_rpt_funnel_ptd_as_of
    ON rpt_funnel_ptd(as_of_date);

-- Comments
COMMENT ON TABLE rpt_funnel_ptd IS 'Funnel Period-to-Date report: MTD/QTD metrics with KM projections and linear forecasts';
COMMENT ON COLUMN rpt_funnel_ptd.counting_method IS 'Cohort (by cohort date) or Rolling (by event date)';
COMMENT ON COLUMN rpt_funnel_ptd.value_type IS 'Actuals = observed only; Actuals + Forecast = observed + KM projection';
COMMENT ON COLUMN rpt_funnel_ptd.leads_ptd_rate IS 'current / ptd_target — conversion rate relative to period OKR';
COMMENT ON COLUMN rpt_funnel_ptd.leads_count IS 'Linear projection: ptd_rate * okr_total (forecast) or actual count (actuals)';
