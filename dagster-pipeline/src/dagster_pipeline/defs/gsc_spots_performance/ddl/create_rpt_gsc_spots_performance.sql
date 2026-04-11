-- =============================================================================
-- rpt_gsc_spots_performance.sql
-- Report: SEO performance of /spots pages in Google Search Console.
--
-- One row per (spot_id, url, month, year) with impressions, clicks and
-- average position from GSC, enriched with spot attributes from lk_spots.
--
-- Source tables:
--   BigQuery: searchconsole_spot2mx.searchdata_url_impression
--   GeoSpot PG: lk_spots (spot_type, spot_sector, spot_status_full, spot_modality, spot_address)
--
-- Source pipeline: gsc_spots_performance
--   Bronze: raw_bq_gsc_url_impressions, raw_gs_lk_spots_gsp
--   STG: stg_bq_gsc_url_impressions, stg_gs_lk_spots_gsp
--   Core: core_gsc_spots_performance (regex spot_id extraction + JOIN)
--   Gold: gold_rpt_gsc_spots_performance (+ audit)
--   Publish: rpt_gsc_spots_performance_to_s3 -> rpt_gsc_spots_performance_to_geospot
--
-- Schedule: Daily at 1:00 PM America/Mexico_City
-- =============================================================================

CREATE TABLE IF NOT EXISTS rpt_gsc_spots_performance (
    -- =========================================================================
    -- Spot attributes (from lk_spots)
    -- =========================================================================
    spot_id                     INTEGER,
    spot_type                   TEXT,
    spot_sector                 TEXT,
    spot_status_full            TEXT,
    spot_modality               TEXT,
    spot_address                TEXT,

    -- =========================================================================
    -- GSC metrics
    -- =========================================================================
    url                         TEXT,
    impressions                 INTEGER,
    clicks                      INTEGER,
    avg_position                DOUBLE PRECISION,
    month                       INTEGER,
    year                        INTEGER,

    -- =========================================================================
    -- Audit fields
    -- =========================================================================
    aud_inserted_at             TIMESTAMP,
    aud_inserted_date           DATE,
    aud_updated_at              TIMESTAMP,
    aud_updated_date            DATE,
    aud_job                     TEXT
);

-- =============================================================================
-- Indexes
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_rpt_gsc_sp_spot_id
    ON rpt_gsc_spots_performance(spot_id);

CREATE INDEX IF NOT EXISTS idx_rpt_gsc_sp_month_year
    ON rpt_gsc_spots_performance(year, month);

CREATE INDEX IF NOT EXISTS idx_rpt_gsc_sp_spot_sector
    ON rpt_gsc_spots_performance(spot_sector);
