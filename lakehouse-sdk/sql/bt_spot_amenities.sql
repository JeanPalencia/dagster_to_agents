-- =============================================================================
-- bt_spot_amenities.sql
-- Bridge table: spots and their amenities with category classification.
--
-- Resolves the many-to-many relationship between spots and amenities,
-- enriched with amenity metadata (name, description, category).
--
-- Source tables (MySQL S2P): spot_amenities, amenities
-- Source asset: core_bt_spot_amenities → gold_bt_spot_amenities → publish
-- =============================================================================

CREATE TABLE IF NOT EXISTS bt_spot_amenities (
    -- =========================================================================
    -- Core identifiers
    -- =========================================================================
    spa_id                      BIGINT PRIMARY KEY,
    spot_id                     BIGINT NOT NULL,
    spa_amenity_id              BIGINT,

    -- =========================================================================
    -- Amenity attributes
    -- =========================================================================
    spa_amenity_name            VARCHAR(255),
    spa_amenity_description     TEXT,
    spa_amenity_category_id     INTEGER,
    spa_amenity_category        VARCHAR(50),
    spa_amenity_created_at      TIMESTAMP,
    spa_amenity_updated_at      TIMESTAMP,

    -- =========================================================================
    -- Spot-amenity association timestamps
    -- =========================================================================
    spa_created_at              TIMESTAMP,
    spa_updated_at              TIMESTAMP,

    -- =========================================================================
    -- Audit fields
    -- =========================================================================
    aud_inserted_at             TIMESTAMP,
    aud_inserted_date           DATE,
    aud_updated_at              TIMESTAMP,
    aud_updated_date            DATE,
    aud_job                     VARCHAR(200)
);

CREATE INDEX IF NOT EXISTS idx_bt_spot_amenities_spot_id
    ON bt_spot_amenities(spot_id);
CREATE INDEX IF NOT EXISTS idx_bt_spot_amenities_amenity_id
    ON bt_spot_amenities(spa_amenity_id);
CREATE INDEX IF NOT EXISTS idx_bt_spot_amenities_category_id
    ON bt_spot_amenities(spa_amenity_category_id);
