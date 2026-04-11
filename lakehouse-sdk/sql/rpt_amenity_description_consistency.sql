-- =============================================================================
-- rpt_amenity_description_consistency.sql
-- Report: consistency between tagged amenities and spot descriptions.
--
-- Classifies each spot by whether its AI-generated description mentions the
-- amenities that are tagged to it (all mentioned, partial omission, total
-- omission). Scope: Single and Subspace spots (spot_type_id IN 1, 3).
--
-- Source-derived fields (spot_type, spot_status_full, spot_description) are
-- intentionally excluded — they can be obtained via JOIN on lk_spots.spot_id.
--
-- Source tables (GeoSpot PG): lk_spots, bt_spot_amenities
-- Source asset: core_amenity_desc_consistency -> gold -> publish
-- =============================================================================

CREATE TABLE IF NOT EXISTS rpt_amenity_description_consistency (
    -- =========================================================================
    -- Spot identifier (JOIN with lk_spots for type, status, description)
    -- =========================================================================
    spot_id                     BIGINT NOT NULL,        -- Spot identifier

    -- =========================================================================
    -- Report-specific fields (adc = amenity_description_consistency)
    -- =========================================================================
    adc_tagged_amenities        TEXT,                   -- Comma-separated list of tagged amenity names
    adc_mentioned_amenities     TEXT,                   -- Comma-separated list of amenities found in description
    adc_omitted_amenities       TEXT,                   -- Comma-separated list of tagged amenities NOT in description
    adc_total_tagged            INTEGER,                -- Count of tagged amenities
    adc_total_mentioned         INTEGER,                -- Count of mentioned amenities
    adc_total_omitted           INTEGER,                -- Count of omitted amenities
    adc_mention_rate            DOUBLE PRECISION,       -- Ratio: mentioned / tagged (0.0 to 1.0)
    adc_category_id             INTEGER NOT NULL,       -- 1 = All mentioned, 2 = Partial omission, 3 = Total omission
    adc_category                VARCHAR(50) NOT NULL,   -- English label for category

    -- =========================================================================
    -- Audit fields
    -- =========================================================================
    aud_inserted_at             TIMESTAMP,              -- Timestamp of first insert
    aud_inserted_date           DATE,                   -- Date of first insert
    aud_updated_at              TIMESTAMP,              -- Timestamp of last update
    aud_updated_date            DATE,                   -- Date of last update
    aud_job                     VARCHAR(200)            -- Dagster job name that produced the row
);

-- =============================================================================
-- Indexes
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_rpt_adc_spot_id
    ON rpt_amenity_description_consistency(spot_id);

CREATE INDEX IF NOT EXISTS idx_rpt_adc_category_id
    ON rpt_amenity_description_consistency(adc_category_id);

CREATE INDEX IF NOT EXISTS idx_rpt_adc_mention_rate
    ON rpt_amenity_description_consistency(adc_mention_rate);
