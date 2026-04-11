-- ==================================================================[ today_exchange (te) ]======================================================================
WITH te AS (
    SELECT
        exchange_rate AS er
    FROM 
        exchanges
    ORDER BY 
        created_at DESC 
    LIMIT 1
),


-- ==================================================================[ clean_prices ]======================================================================
clean_prices AS (
    SELECT
        p.spot_id,
        p.type,
        p.currency_type,
        p.price_area,
        
        CASE 
            WHEN p.currency_type = 1 OR p.currency_type IS NULL THEN p.rate 
            ELSE p.rate * (SELECT er FROM te) 
        END AS mxn_rate,

        CASE WHEN p.currency_type = 2 THEN p.rate 
            ELSE p.rate / (SELECT er FROM te) 
        END AS usd_rate,

        CASE WHEN p.currency_type = 1 OR p.currency_type IS NULL THEN p.maintenance 
             ELSE p.maintenance * (SELECT er FROM te) 
        END AS mxn_maintenance,

        CASE WHEN p.currency_type = 2 THEN p.maintenance 
            ELSE p.maintenance / (SELECT er FROM te) 
        END AS usd_maintenance,

        CASE WHEN p.currency_type = 1 OR p.currency_type IS NULL THEN p.max_rate 
             ELSE p.max_rate * (SELECT er FROM te) 
        END AS max_mxn_rate,

        CASE WHEN p.currency_type = 2 THEN p.max_rate 
            ELSE p.max_rate / (SELECT er FROM te) 
        END AS max_usd_rate,

        ROW_NUMBER() OVER (PARTITION BY p.spot_id, p.type ORDER BY p.updated_at DESC) AS rn
    FROM prices p
    WHERE p.deleted_at IS NULL
),


-- ==================================================================[ single ]======================================================================
spots_singles AS (
    SELECT
        *,
        parent_id AS parent_id_real,
        NULL AS parent_spot_type_id,
        NULL AS parent_street,
        NULL AS parent_ext_number,
        NULL AS parent_int_number,
        NULL AS parent_zip_code_id,
        NULL AS parent_latitude,
        NULL AS parent_longitude,
        NULL AS parent_quality_state,
        NULL AS parent_state_id,
        NULL AS parent_data_municipality_id,
        NULL AS parent_city_id,
        NULL AS parent_settlement_id,
        NULL AS parent_contact_id,
        NULL AS parent_spot_state,
        NULL AS parent_state_reason,
        NULL AS parent_zone_id,
        NULL AS parent_nearest_zone_id,
        NULL AS parent_construction_date,
        NULL AS parent_parking_space_by_area
    FROM 
        spots
    WHERE 
        spots.parent_id IS NULL
        AND spots.is_complex = 0
        AND EXISTS (
            SELECT *
            FROM prices
            WHERE spots.id = prices.spot_id
            AND TYPE IN (1, 2)
            AND prices.deleted_at IS NULL
        )
        AND spot_type_id IN (13, 11, 9, 15, 17, 18)
    ),


-- ==================================================================[ complexes ]======================================================================
spots_complexes AS (
	SELECT
        *,
        parent_id AS parent_id_real,
        NULL AS parent_spot_type_id,
        NULL AS parent_street,
        NULL AS parent_ext_number,
        NULL AS parent_int_number,
        NULL AS parent_zip_code_id,
        NULL AS parent_latitude,
        NULL AS parent_longitude,
        NULL AS parent_quality_state,
        NULL AS parent_state_id,
        NULL AS parent_data_municipality_id,
        NULL AS parent_city_id,
        NULL AS parent_settlement_id,
        NULL AS parent_contact_id,
        NULL AS parent_spot_state,
        NULL AS parent_state_reason,
        NULL AS parent_zone_id,
        NULL AS parent_nearest_zone_id,
        NULL AS parent_construction_date,
        NULL AS parent_parking_space_by_area
	FROM 
        spots
	WHERE 
        spots.parent_id IS NULL
        AND spots.is_complex = 1
        AND EXISTS (
    	    SELECT *
            FROM prices
            WHERE spots.id = prices.spot_id
            AND TYPE IN (1, 2)
            AND prices.deleted_at IS NULL
        )
        AND spot_type_id IN (13, 11, 9, 15, 17, 18)
),


-- ==================================================================[ subspaces ]======================================================================
spots_subspaces AS (
    SELECT
        *
    FROM 
        spots
    INNER JOIN (
        SELECT
            id AS parent_id_real,
            spot_type_id AS parent_spot_type_id,
            contact_id AS parent_contact_id,
            street AS parent_street,
            ext_number AS parent_ext_number,
            int_number AS parent_int_number,
            zip_code_id AS parent_zip_code_id,
            latitude AS parent_latitude,
            longitude AS parent_longitude,
            quality_state AS parent_quality_state,
            state_id AS parent_state_id,
            data_municipality_id AS parent_data_municipality_id,
            city_id AS parent_city_id,
            settlement_id AS parent_settlement_id,
            spot_state AS parent_spot_state,
            state_reason AS parent_state_reason,
            zone_id AS parent_zone_id,
            nearest_zone_id AS parent_nearest_zone_id,
            construction_date AS parent_construction_date,
            parking_space_by_area AS parent_parking_space_by_area
        FROM 
            spots
        WHERE 
            spots.parent_id IS NULL
            AND spots.is_complex = 1
            AND EXISTS (
                SELECT *
                FROM prices
                WHERE spots.id = prices.spot_id
                AND type IN (1, 2)
                AND prices.deleted_at IS NULL
            )
    ) AS parent ON parent.parent_id_real = spots.parent_id
),


-- ==================================================================[ total_spots ]======================================================================
pre_total_spots AS (
    SELECT DISTINCT
        *,
        spot_type_id AS spot_type_id_real,
        street AS street_real,
        ext_number AS ext_number_real,
        int_number AS int_number_real,
        zip_code_id AS zip_code_id_real,
        latitude AS latitude_real,
        longitude AS longitude_real,
        quality_state AS quality_state_real,
        state_id AS state_id_real,
        data_municipality_id AS data_municipality_id_real,
        city_id AS city_id_real,
        settlement_id AS settlement_id_real,
        zone_id AS zone_id_real,
        nearest_zone_id AS nearest_zone_id_real,
        contact_id AS contact_id_real,
        spot_state AS spot_state_real,
        state_reason AS state_reason_real,
        construction_date AS construction_date_real,
        parking_space_by_area AS parking_space_by_area_real,
        1 AS source_table_id -- single
    FROM 
        spots_singles

    UNION
    
    SELECT DISTINCT
        *,
        spot_type_id AS spot_type_id_real,
        street AS street_real,
        ext_number AS ext_number_real,
        int_number AS int_number_real,
        zip_code_id AS zip_code_id_real,
        latitude AS latitude_real,
        longitude AS longitude_real,
        quality_state AS quality_state_real,
        state_id AS state_id_real,
        data_municipality_id AS data_municipality_id_real,
        city_id AS city_id_real,
        settlement_id AS settlement_id_real,
        zone_id AS zone_id_real,
        nearest_zone_id AS nearest_zone_id_real,
        contact_id AS contact_id_real,
        spot_state AS spot_state_real,
        state_reason AS state_reason_real,
        construction_date AS construction_date_real,
        parking_space_by_area AS parking_space_by_area_real,
        2 AS source_table_id -- complex
    FROM 
        spots_complexes
    
    UNION
    
    SELECT DISTINCT
        *,
        COALESCE(parent_spot_type_id, spot_type_id) AS spot_type_id_real,
        COALESCE(parent_street, street) AS street_real,
        COALESCE(parent_ext_number, ext_number) AS ext_number_real,
        COALESCE(parent_int_number, int_number) AS int_number_real,
        COALESCE(parent_zip_code_id, zip_code_id) AS zip_code_id_real,
        COALESCE(parent_latitude, latitude) AS latitude_real,
        COALESCE(parent_longitude, longitude) AS longitude_real,
        COALESCE(parent_quality_state, quality_state) AS quality_state_real,
        COALESCE(parent_state_id, state_id) AS state_id_real,
        COALESCE(parent_data_municipality_id, data_municipality_id) AS data_municipality_id_id_real,
        COALESCE(parent_city_id, city_id) AS city_id_real,
        COALESCE(parent_settlement_id, settlement_id) AS settlement_id_real,
        COALESCE(parent_zone_id, zone_id) AS zone_id_real,
        COALESCE(parent_nearest_zone_id, nearest_zone_id) AS nearest_zone_id_real,
        COALESCE(parent_contact_id, contact_id) AS contact_id_real,
        parent_spot_state AS spot_state_real,
        parent_state_reason AS state_reason_real,
        COALESCE(parent_construction_date, construction_date) AS construction_date_real,
        COALESCE(parent_parking_space_by_area, parking_space_by_area) AS parking_space_by_area_real,
        3 AS source_table_id -- subspace
    FROM 
        spots_subspaces
),

-- ==================================================================[ total_spots_with_statuses ]======================================================================
total_spots_with_statuses AS (
    SELECT
        pts.*,

        -- =======================
        -- PARENT: ONLY STATUS
        -- =======================
        pts.spot_state_real AS spot_parent_status_id,
        CASE pts.spot_state_real
            WHEN 1 THEN 'Public'
            WHEN 2 THEN 'Draft'
            WHEN 3 THEN 'Disabled'
            WHEN 4 THEN 'Archived'
            ELSE 'Unknown'
        END AS spot_parent_status,

        -- =======================
        -- PARENT: ONLY REASON
        -- =======================
        pts.state_reason_real AS spot_parent_status_reason_id,
        CASE pts.state_reason_real
            WHEN  1 THEN 'User'                -- USER_DRAFT
            WHEN  2 THEN 'Onboarding API'      -- ONBOARDING_API
            WHEN  3 THEN 'Onboarding Scraping' -- ONBOARDING_SCRAPING
            WHEN  4 THEN 'QA'                  -- QUALITY_ASSURANCE
            WHEN  5 THEN 'Publisher'           -- OWNER_REQUEST
            WHEN  6 THEN 'Occupied'            -- OCCUPIED
            WHEN  7 THEN 'Won'                 -- WON_IN_PLATFORM
            WHEN  8 THEN 'QA'                  -- QUALITY_ASSURANCE
            WHEN  9 THEN 'Internal Use'        -- INTERNAL_USE
            WHEN 10 THEN 'Internal'            -- ADMIN_DECISION
            WHEN 11 THEN 'Owner'               -- DELETED_BY_OWNER
            WHEN 12 THEN 'Unpublished'         -- DRAFT_DELETED
            WHEN 13 THEN 'Outdated'            -- VALIDITY_EXPIRED
            WHEN 14 THEN 'Quality'             -- QA_DECISION
            ELSE NULL
        END AS spot_parent_status_reason,

        -- =======================
        -- PARENT: COMBINED STATUS + REASON
        -- =======================
        100 * pts.spot_state_real + COALESCE(pts.state_reason_real, 0) AS spot_parent_status_full_id,
        CASE pts.spot_state_real
            -- 1: Public (no reason)
            WHEN 1 THEN 'Public'

            -- 2: Draft
            WHEN 2 THEN CASE pts.state_reason_real
                WHEN 1 THEN 'Draft User'
                WHEN 2 THEN 'Draft Onboarding API'
                WHEN 3 THEN 'Draft Onboarding Scraping'
                WHEN 4 THEN 'Draft QA'
                ELSE 'Draft'
            END

            -- 3: Disabled
            WHEN 3 THEN CASE pts.state_reason_real
                WHEN 5 THEN 'Disabled Publisher'
                WHEN 6 THEN 'Disabled Occupied'
                WHEN 7 THEN 'Disabled Won'
                WHEN 8 THEN 'Disabled QA'
                WHEN 9 THEN 'Disabled Internal Use'
                ELSE 'Disabled'
            END

            -- 4: Archived
            WHEN 4 THEN CASE pts.state_reason_real
                WHEN 10 THEN 'Archived Internal'
                WHEN 11 THEN 'Archived Owner'
                WHEN 12 THEN 'Archived Unpublished'
                WHEN 13 THEN 'Archived Outdated'
                WHEN 14 THEN 'Archived Quality'
                ELSE 'Archived'
            END

            ELSE 'Unknown'
        END AS spot_parent_status_full,

        -- =======================
        -- SPOT: ONLY STATUS
        -- =======================
        pts.spot_state AS spot_status_id,
        CASE pts.spot_state
            WHEN 1 THEN 'Public'
            WHEN 2 THEN 'Draft'
            WHEN 3 THEN 'Disabled'
            WHEN 4 THEN 'Archived'
            ELSE 'Unknown'
        END AS spot_status,

        -- =======================
        -- SPOT: ONLY REASON
        -- =======================
        pts.state_reason AS spot_status_reason_id,
        CASE pts.state_reason
            WHEN  1 THEN 'User'                -- USER_DRAFT
            WHEN  2 THEN 'Onboarding API'      -- ONBOARDING_API
            WHEN  3 THEN 'Onboarding Scraping' -- ONBOARDING_SCRAPING
            WHEN  4 THEN 'QA'                  -- QUALITY_ASSURANCE
            WHEN  5 THEN 'Publisher'           -- OWNER_REQUEST
            WHEN  6 THEN 'Occupied'            -- OCCUPIED
            WHEN  7 THEN 'Won'                 -- WON_IN_PLATFORM
            WHEN  8 THEN 'QA'                  -- QUALITY_ASSURANCE
            WHEN  9 THEN 'Internal Use'        -- INTERNAL_USE
            WHEN 10 THEN 'Internal'            -- ADMIN_DECISION
            WHEN 11 THEN 'Owner'               -- DELETED_BY_OWNER
            WHEN 12 THEN 'Unpublished'         -- DRAFT_DELETED
            WHEN 13 THEN 'Outdated'            -- VALIDITY_EXPIRED
            WHEN 14 THEN 'Quality'             -- QA_DECISION
            ELSE NULL
        END AS spot_status_reason,

        -- =======================
        -- SPOT: COMBINED STATUS + REASON
        -- =======================
        100 * pts.spot_state + COALESCE(pts.state_reason, 0) AS spot_status_full_id,
        CASE pts.spot_state
            -- 1: Public (no reason)
            WHEN 1 THEN 'Public'

            -- 2: Draft
            WHEN 2 THEN CASE pts.state_reason
                WHEN 1 THEN 'Draft User'
                WHEN 2 THEN 'Draft Onboarding API'
                WHEN 3 THEN 'Draft Onboarding Scraping'
                WHEN 4 THEN 'Draft QA'
                ELSE 'Draft'
            END

            -- 3: Disabled
            WHEN 3 THEN CASE pts.state_reason
                WHEN 5 THEN 'Disabled Publisher'
                WHEN 6 THEN 'Disabled Occupied'
                WHEN 7 THEN 'Disabled Won'
                WHEN 8 THEN 'Disabled QA'
                WHEN 9 THEN 'Disabled Internal Use'
                ELSE 'Disabled'
            END

            -- 4: Archived
            WHEN 4 THEN CASE pts.state_reason
                WHEN 10 THEN 'Archived Internal'
                WHEN 11 THEN 'Archived Owner'
                WHEN 12 THEN 'Archived Unpublished'
                WHEN 13 THEN 'Archived Outdated'
                WHEN 14 THEN 'Archived Quality'
                ELSE 'Archived'
            END

            ELSE 'Unknown'
        END AS spot_status_full

    FROM pre_total_spots AS pts
),

-- ==================================================================[ total_spots ]======================================================================
total_spots AS (
    SELECT
        tsws.*,
        COALESCE(tsws.state_id_real, zc.state_id) AS state_id_real2
    FROM total_spots_with_statuses AS tsws
    LEFT JOIN zip_codes AS zc ON tsws.zip_code_id_real = zc.id
),

-- ==================================================================[ basic_spots_prices ]======================================================================
basic_spots_prices AS (
    SELECT 
        ts.id AS spot_id,
        ts.parent_id AS spot_parent_id,
        ts.source_table_id AS spot_type_id,
        ts.square_space,
        
        -- Spot modality (determines whether the spot is for rent, sale, or both)
        CASE
            WHEN r_pr.mxn_rate IS NOT NULL AND s_pr.mxn_rate IS NULL THEN 1
            WHEN r_pr.mxn_rate IS NULL AND s_pr.mxn_rate IS NOT NULL THEN 2
            WHEN r_pr.mxn_rate IS NOT NULL AND s_pr.mxn_rate IS NOT NULL THEN 3
            ELSE NULL
        END AS spot_modality_id,
        CASE
            WHEN r_pr.mxn_rate IS NOT NULL AND s_pr.mxn_rate IS NULL THEN 'Rent'
            WHEN r_pr.mxn_rate IS NULL AND s_pr.mxn_rate IS NOT NULL THEN 'Sale'
            WHEN r_pr.mxn_rate IS NOT NULL AND s_pr.mxn_rate IS NOT NULL THEN 'Rent & Sale'
            ELSE NULL
        END AS spot_modality,
        
        -- Rental price type (total or per sqm)
        r_pr.price_area AS spot_price_area_rent_id,
        CASE
            WHEN r_pr.price_area = 1 THEN 'Total Price'
            WHEN r_pr.price_area = 2 THEN 'Price per SQM'
        END AS spot_price_area_rent,

        -- Rental currency
        CASE
            WHEN r_pr.currency_type = 1 OR r_pr.currency_type IS NULL THEN 1
            WHEN r_pr.currency_type = 2 THEN 2
        END AS spot_currency_rent_id,
        CASE
            WHEN r_pr.currency_type = 1 OR r_pr.currency_type IS NULL THEN "MXN"
            WHEN r_pr.currency_type = 2 THEN "USD"
        END AS spot_currency_rent,
        
        -- Sale price type (total or per sqm)
        s_pr.price_area AS spot_price_area_sale_id,
        CASE
            WHEN s_pr.price_area = 1 THEN 'Total Price'
            WHEN s_pr.price_area = 2 THEN 'Price per SQM'
        END AS spot_price_area_sale,

        -- Sale currency
        CASE
            WHEN s_pr.currency_type = 1 OR s_pr.currency_type IS NULL THEN 1
            WHEN s_pr.currency_type = 2 THEN 2
        END AS spot_currency_sale_id,
        CASE
            WHEN s_pr.currency_type = 1 OR s_pr.currency_type IS NULL THEN "MXN"
            WHEN s_pr.currency_type = 2 THEN "USD"
        END AS spot_currency_sale,
        
        -- Total rental prices
        CASE
            WHEN r_pr.price_area = 1 THEN r_pr.mxn_rate
            WHEN r_pr.price_area = 2 THEN r_pr.mxn_rate * ts.square_space
            ELSE NULL
        END AS spot_price_total_mxn_rent,
        CASE
            WHEN r_pr.price_area = 1 THEN r_pr.usd_rate
            WHEN r_pr.price_area = 2 THEN r_pr.usd_rate * ts.square_space
            ELSE NULL
        END AS spot_price_total_usd_rent,
        
        -- Rental prices per sqm
        CASE
            WHEN r_pr.price_area = 1 AND ts.square_space > 0 THEN r_pr.mxn_rate / ts.square_space
            WHEN r_pr.price_area = 2 THEN r_pr.mxn_rate 
            ELSE NULL
        END AS spot_price_sqm_mxn_rent,
        CASE
            WHEN r_pr.price_area = 1 AND ts.square_space > 0 THEN r_pr.usd_rate / ts.square_space
            WHEN r_pr.price_area = 2 THEN r_pr.usd_rate 
            ELSE NULL
        END AS spot_price_sqm_usd_rent,
        
        -- Total sale prices
        CASE
            WHEN s_pr.price_area = 1 THEN s_pr.mxn_rate
            WHEN s_pr.price_area = 2 THEN s_pr.mxn_rate * ts.square_space
            ELSE NULL
        END AS spot_price_total_mxn_sale,
        CASE
            WHEN s_pr.price_area = 1 THEN s_pr.usd_rate
            WHEN s_pr.price_area = 2 THEN s_pr.usd_rate * ts.square_space
            ELSE NULL
        END AS spot_price_total_usd_sale,

        -- Sale prices per sqm
        CASE
            WHEN s_pr.price_area = 1 AND ts.square_space > 0 THEN s_pr.mxn_rate / ts.square_space
            WHEN s_pr.price_area = 2 THEN s_pr.mxn_rate 
            ELSE NULL
        END AS spot_price_sqm_mxn_sale,
        CASE
            WHEN s_pr.price_area = 1 AND ts.square_space > 0 THEN s_pr.usd_rate / ts.square_space
            WHEN s_pr.price_area = 2 THEN s_pr.usd_rate 
            ELSE NULL
        END AS spot_price_sqm_usd_sale,

        -- Maintenance costs
        r_pr.mxn_maintenance AS spot_maintenance_cost_mxn,
        r_pr.mxn_maintenance AS spot_maintenance_cost_usd

    FROM total_spots AS ts
    LEFT JOIN clean_prices r_pr ON ts.id = r_pr.spot_id AND r_pr.type = 1 AND r_pr.rn = 1
    LEFT JOIN clean_prices s_pr ON ts.id = s_pr.spot_id AND s_pr.type = 2 AND s_pr.rn = 1
),

-- ==================================================================[ complex_price_stats ]======================================================================
complex_price_stats AS (
    SELECT 
        spot_parent_id, 
        
        -- Subspace statistics
        SUM(square_space) AS subspace_total_area,  -- Total area of subspaces
        COUNT(spot_id) AS subspace_count,  -- Number of subspaces

        -- Subspace rental prices (total in MXN)
        MIN(spot_price_total_mxn_rent) AS min_price_total_mxn_rent,
        MAX(spot_price_total_mxn_rent) AS max_price_total_mxn_rent,
        AVG(spot_price_total_mxn_rent) AS mean_price_total_mxn_rent,

        -- Subspace rental prices per sqm in MXN
        MIN(spot_price_sqm_mxn_rent) AS min_price_sqm_mxn_rent,
        MAX(spot_price_sqm_mxn_rent) AS max_price_sqm_mxn_rent,
        SUM(spot_price_total_mxn_rent) / SUM(square_space) AS mean_price_sqm_mxn_rent,

        -- Subspace rental prices (total in USD)
        MIN(spot_price_total_usd_rent) AS min_price_total_usd_rent,
        MAX(spot_price_total_usd_rent) AS max_price_total_usd_rent,
        AVG(spot_price_total_usd_rent) AS mean_price_total_usd_rent,

        -- Subspace rental prices per sqm in USD
        MIN(spot_price_sqm_usd_rent) AS min_price_sqm_usd_rent,
        MAX(spot_price_sqm_usd_rent) AS max_price_sqm_usd_rent,
        SUM(spot_price_total_usd_rent) / SUM(square_space) AS mean_price_sqm_usd_rent,
        
        -- Subspace sale prices (total in MXN)
        MIN(spot_price_total_mxn_sale) AS min_price_total_mxn_sale,
        MAX(spot_price_total_mxn_sale) AS max_price_total_mxn_sale,
        AVG(spot_price_total_mxn_sale) AS mean_price_total_mxn_sale,

        -- Subspace sale prices per sqm in MXN
        MIN(spot_price_sqm_mxn_sale) AS min_price_sqm_mxn_sale,
        MAX(spot_price_sqm_mxn_sale) AS max_price_sqm_mxn_sale,
        SUM(spot_price_total_mxn_sale) / SUM(square_space) AS mean_price_sqm_mxn_sale,

        -- Subspace sale prices (total in USD)
        MIN(spot_price_total_usd_sale) AS min_price_total_usd_sale,
        MAX(spot_price_total_usd_sale) AS max_price_total_usd_sale,
        AVG(spot_price_total_usd_sale) AS mean_price_total_usd_sale,

        -- Subspace sale prices per sqm in USD
        MIN(spot_price_sqm_usd_sale) AS min_price_sqm_usd_sale,
        MAX(spot_price_sqm_usd_sale) AS max_price_sqm_usd_sale,
        SUM(spot_price_total_usd_sale) / SUM(square_space) AS mean_price_sqm_usd_sale
    FROM basic_spots_prices
    WHERE spot_type_id = 3
    GROUP BY spot_parent_id
),


-- ==================================================================[ spots_prices ]======================================================================
spots_prices AS (
    SELECT 
        bsp.spot_id,
        
        -- Spot modality
        bsp.spot_modality_id,
        bsp.spot_modality,
        
        -- Rental price per area
        bsp.spot_price_area_rent_id,
        bsp.spot_price_area_rent,
        
        -- Sale price per area
        bsp.spot_price_area_sale_id,
        bsp.spot_price_area_sale,
        
        -- Rental currency
        bsp.spot_currency_rent_id,
        bsp.spot_currency_rent,
        
        -- Sale currency
        bsp.spot_currency_sale_id,
        bsp.spot_currency_sale,
        
        -- Total rental prices
        bsp.spot_price_total_mxn_rent,
        bsp.spot_price_total_usd_rent,
        
        -- Total sale prices
        bsp.spot_price_total_mxn_sale,
        bsp.spot_price_total_usd_sale,

        -- Rental prices per sqm
        bsp.spot_price_sqm_mxn_rent,
        bsp.spot_price_sqm_usd_rent,
        
        -- Sale prices per sqm
        bsp.spot_price_sqm_mxn_sale,
        bsp.spot_price_sqm_usd_sale,
        
        -- Maintenance costs
        bsp.spot_maintenance_cost_mxn,
        bsp.spot_maintenance_cost_usd,
        
        -- Subspace rental prices (total in MXN)
        cps.min_price_total_mxn_rent AS spot_sub_min_price_total_mxn_rent,
        cps.max_price_total_mxn_rent AS spot_sub_max_price_total_mxn_rent,
        cps.mean_price_total_mxn_rent AS spot_sub_mean_price_total_mxn_rent,

        -- Subspace rental prices per sqm in MXN
        cps.min_price_sqm_mxn_rent AS spot_sub_min_price_sqm_mxn_rent,
        cps.max_price_sqm_mxn_rent AS spot_sub_max_price_sqm_mxn_rent,
        cps.mean_price_sqm_mxn_rent AS spot_sub_mean_price_sqm_mxn_rent,

        -- Subspace rental prices (total in USD)
        cps.min_price_total_usd_rent AS spot_sub_min_price_total_usd_rent,
        cps.max_price_total_usd_rent AS spot_sub_max_price_total_usd_rent,
        cps.mean_price_total_usd_rent AS spot_sub_mean_price_total_usd_rent,

        -- Subspace rental prices per sqm in USD
        cps.min_price_sqm_usd_rent AS spot_sub_min_price_sqm_usd_rent,
        cps.max_price_sqm_usd_rent AS spot_sub_max_price_sqm_usd_rent,
        cps.mean_price_sqm_usd_rent AS spot_sub_mean_price_sqm_usd_rent,

        -- Subspace sale prices (total in MXN)
        cps.min_price_total_mxn_sale AS spot_sub_min_price_total_mxn_sale,
        cps.max_price_total_mxn_sale AS spot_sub_max_price_total_mxn_sale,
        cps.mean_price_total_mxn_sale AS spot_sub_mean_price_total_mxn_sale,

        -- Subspace sale prices per sqm in MXN
        cps.min_price_sqm_mxn_sale AS spot_sub_min_price_sqm_mxn_sale,
        cps.max_price_sqm_mxn_sale AS spot_sub_max_price_sqm_mxn_sale,
        cps.mean_price_sqm_mxn_sale AS spot_sub_mean_price_sqm_mxn_sale,

        -- Subspace sale prices (total in USD)
        cps.min_price_total_usd_sale AS spot_sub_min_price_total_usd_sale,
        cps.max_price_total_usd_sale AS spot_sub_max_price_total_usd_sale,
        cps.mean_price_total_usd_sale AS spot_sub_mean_price_total_usd_sale,

        -- Subspace sale prices per sqm in USD
        cps.min_price_sqm_usd_sale AS spot_sub_min_price_sqm_usd_sale,
        cps.max_price_sqm_usd_sale AS spot_sub_max_price_sqm_usd_sale,
        cps.mean_price_sqm_usd_sale AS spot_sub_mean_price_sqm_usd_sale
    FROM basic_spots_prices AS bsp
    LEFT JOIN complex_price_stats cps ON bsp.spot_id = cps.spot_parent_id
),

-- ==================================================================[ latest_active_reports_ranked ]======================================================================
latest_active_reports_ranked AS (
    SELECT 
        sr.spot_id,
        sr.reason_id AS spot_last_active_report_reason_id,
        CASE sr.reason_id
            WHEN 1 THEN 'Fotos de mala calidad'
            WHEN 2 THEN 'Fotos con marca de agua'
            WHEN 3 THEN 'Fotos en collage'
            WHEN 4 THEN 'Fotos insuficientes'
            WHEN 5 THEN 'Error en precio de renta/venta'
            WHEN 6 THEN 'Error en precio de mantenimiento'
            WHEN 7 THEN 'Fotos con publicidad de lonas'
            ELSE NULL
        END AS spot_last_active_report_reason,
        sr.reported_by AS spot_last_active_report_user_id,
        sr.created_at AS spot_last_active_report_created_at,
        ROW_NUMBER() OVER (PARTITION BY sr.spot_id ORDER BY sr.created_at DESC) AS rn
    FROM spot_reports sr
    WHERE 
		sr.deleted_at IS NULL
		AND sr.reason_id IS NOT NULL
),

-- ==================================================================[ latest_active_reports ]======================================================================
latest_active_reports AS (
    SELECT *
    FROM latest_active_reports_ranked
    WHERE rn = 1
),

-- ==================================================================[ ordered_reports ]======================================================================
ordered_reports AS (
    SELECT 
        sr.*
    FROM spot_reports sr
	WHERE
		sr.reason_id IS NOT NULL
    ORDER BY sr.spot_id, sr.created_at DESC
),

-- ==================================================================[ report_history ]======================================================================
report_history AS (
    SELECT 
        sr.spot_id,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'spot_report_id', sr.id,
                'spot_reason_id', sr.reason_id,
                'spot_reason',
                    CASE sr.reason_id
                        WHEN 1 THEN 'Fotos de mala calidad'
                        WHEN 2 THEN 'Fotos con marca de agua'
                        WHEN 3 THEN 'Fotos en collage'
                        WHEN 4 THEN 'Fotos insuficientes'
                        WHEN 5 THEN 'Error en precio de renta/venta'
                        WHEN 6 THEN 'Error en precio de mantenimiento'
                        WHEN 7 THEN 'Fotos con publicidad de lonas'
                        ELSE NULL
                    END,
                'user_reporter_id', sr.reported_by,
                'spot_reporter_created_at', sr.created_at,
                'spot_reporter_updated_at', sr.updated_at,
                'spot_reporter_deleted_at', sr.deleted_at
            )
        ) AS spot_reports_full_history
    FROM ordered_reports sr
    GROUP BY sr.spot_id
),

-- ==================================================================[ spot_reports_summary ]======================================================================
spot_reports_summary AS (
    SELECT 
        h.spot_id,
        r.spot_last_active_report_reason_id,
        r.spot_last_active_report_reason,
        r.spot_last_active_report_user_id,
        r.spot_last_active_report_created_at,
        h.spot_reports_full_history,
        CASE WHEN r.spot_id IS NOT NULL THEN 1 ELSE 0 END AS spot_has_active_report
    FROM report_history h
    LEFT JOIN latest_active_reports r ON h.spot_id = r.spot_id
),

-- ==================================================================[ spot_listings ]======================================================================
spot_listings AS (
    SELECT
        ls.spot_id,
        ls.listing_id AS spot_listing_id,
        CASE
            WHEN l.status_id = 1 THEN 1
            WHEN l.status_id = 2 THEN 2
            WHEN l.status_id = 3 THEN 3
            ELSE 0
        END AS spot_listing_representative_status_id,
        CASE
            WHEN l.status_id = 1 THEN 'Available'
            WHEN l.status_id = 2 THEN 'Unavailable'
            WHEN l.status_id = 3 THEN 'Deleted'
            ELSE 'Not applicable'
        END AS spot_listing_representative_status,
        CASE
            WHEN ls.status_id = 1 THEN 1
            WHEN ls.status_id = 2 THEN 2
            WHEN ls.status_id = 3 THEN 3
            ELSE 0
        END AS spot_listing_status_id,
        CASE
            WHEN ls.status_id = 1 THEN 'Created'
            WHEN ls.status_id = 2 THEN 'Verified'
            WHEN ls.status_id = 3 THEN 'Published'
            ELSE "Unknown"
        END AS spot_listing_status,
        ls.hierarchy AS spot_listing_hierarchy,
        CASE
            WHEN ls.hierarchy = 1 THEN 1
            ELSE 0
        END AS spot_is_listing_id,
        CASE
            WHEN ls.hierarchy = 1 THEN 'Yes'
            ELSE 'No'
        END AS spot_is_listing,
        ls.created_at AS listing_created_at
    FROM listings_spots ls
    LEFT JOIN listings AS l ON ls.listing_id = l.id
	WHERE
		l.deleted_at IS NULL
),

spot_listings_ranked AS (
    SELECT
        sl.*,
        ROW_NUMBER() OVER (
            PARTITION BY sl.spot_id
            ORDER BY
                (sl.spot_listing_status_id = 1) DESC,
                sl.listing_created_at DESC
        ) AS rn
    FROM spot_listings sl
),

spot_listings_filtered AS (
    SELECT
        *
    FROM spot_listings_ranked
    WHERE rn = 1
),

-- ==================================================================[ spot_photos ]======================================================================
spot_photos AS (
    SELECT
            photoable_id AS photo_spot_id, 
            COUNT(id) AS spot_photo_count,
            SUM(CASE
                WHEN photoable_type LIKE 'App\\\\Models\\\\Spot'
                    AND social_type IS NULL
                    AND type != 2
                    AND is_visible = true
                    AND deleted_at is NULL THEN 1
                    ELSE 0
                END) AS  spot_photo_platform_count
        FROM photos
        GROUP BY photoable_id
),

-- ==================================================================[ final_results ]======================================================================
final_results AS (
    SELECT
        -- Identificadores
        ts.id AS spot_id,
        CONCAT("https://platform.spot2.mx/admin/spots/", ts.id) AS spot_link,
        CONCAT('https://spot2.mx/spots/', ts.id) AS spot_public_link,
        
        -- Status
        ts.spot_parent_status_id,
        ts.spot_parent_status,
        ts.spot_parent_status_reason_id,
        ts.spot_parent_status_reason,
        ts.spot_parent_status_full_id,
        ts.spot_parent_status_full,
        ts.spot_status_id,
        ts.spot_status,
        ts.spot_status_reason_id,
        ts.spot_status_reason,
        ts.spot_status_full_id,
        ts.spot_status_full,
    
        -- Sector and Type
        ts.spot_type_id_real AS spot_sector_id,
        CASE
            WHEN ts.spot_type_id_real = 9 THEN 'Industrial'
            WHEN ts.spot_type_id_real = 11 THEN 'Office'
            WHEN ts.spot_type_id_real = 13 THEN 'Retail'
            WHEN ts.spot_type_id_real = 15 THEN 'Land'
            ELSE NULL
        END AS spot_sector,
        source_table_id AS spot_type_id,
        CASE
            WHEN source_table_id = 1 THEN 'Single'
            WHEN source_table_id = 2 THEN 'Complex'
            WHEN source_table_id = 3 THEN 'Subspace'
        END AS spot_type,

        -- Complex and Parent
        ts.is_complex AS spot_is_complex,
        ts.parent_id AS spot_parent_id,

        -- Title and Description
        ts.name AS spot_title,
        ts.description AS spot_description,

        -- ------------------------------- Address ----------------------------------
        CONCAT_WS(', ',
            CASE
                WHEN ts.street_real IS NOT NULL AND ts.ext_number_real IS NOT NULL THEN CONCAT(ts.street_real, ' ', ts.ext_number_real)
                WHEN ts.street_real IS NOT NULL THEN ts.street_real
                WHEN ts.ext_number_real IS NOT NULL THEN ts.ext_number_real
                ELSE NULL
            END, -- Concatena calle y número exterior, agregando un espacio solo si ambos existen
            COALESCE(cities.name, zip_c.municipality),
            states.name,
            IF(zip_c.code IS NOT NULL, CONCAT('CP. ', zip_c.code), NULL)  -- Código postal precedido por 'CP.' si no es NULL
        ) AS spot_address,
        ts.street_real AS spot_street,
        ts.ext_number_real AS spot_ext_number,
        ts.int_number AS spot_int_number,
        zip_c.spot_settlement AS spot_settlement,
        settlement_id_real AS spot_data_settlement_id,
        data_settlements.name AS spot_data_settlement,
        ts.city_id_real AS spot_municipality_id,
        COALESCE(cities.name, zip_c.municipality) AS spot_municipality,
        ts.data_municipality_id_real AS spot_data_municipality_id,
        data_municipalities.name AS spot_data_municipality,
        ts.state_id_real2 AS spot_state_id,
        states.name AS spot_state,
        data_states.name AS spot_data_state,
        CASE
            WHEN states_c.id IN (7, 15, 12, 13, 17, 21, 29) THEN 1
            WHEN states_c.id IN (2, 3, 6, 8, 19, 26, 28) THEN 2
            WHEN states_c.id IN (1, 11, 22, 24, 32) THEN 3
            WHEN states_c.id IN (4, 5, 20, 23, 27, 30, 31) THEN 4
            WHEN states_c.id IN (9, 10, 14, 16, 18, 25) THEN 5
        END AS spot_region_id,
        CASE
            WHEN states_c.id IN (7, 15, 12, 13, 17, 21, 29) THEN 'Center'
            WHEN states_c.id IN (2, 3, 6, 8, 19, 26, 28) THEN 'North'
            WHEN states_c.id IN (1, 11, 22, 24, 32) THEN 'Shallows'
            WHEN states_c.id IN (4, 5, 20, 23, 27, 30, 31) THEN 'Southeast'
            WHEN states_c.id IN (9, 10, 14, 16, 18, 25) THEN 'West'
        END AS spot_region,
        zone_id_real AS spot_corridor_id,
        zones.name AS spot_corridor,
        nearest_zone_id_real AS spot_nearest_corridor_id,
        mearest_zones.name AS spot_nearest_corridor,
        ts.latitude_real AS spot_latitude,
        ts.longitude_real AS spot_longitude,
        ts.zip_code_id_real AS spot_zip_code_id,
        zip_c.code AS spot_zip_code,
        zip_c.municipality AS spot_municipality_zip_code,
        states_c.name AS spot_state_zip_code,
        CASE TRIM(zip_c.spot_settlement_type)
            WHEN 'Ranchería'             THEN 1
            WHEN 'Colonia'               THEN 2
            WHEN 'Fraccionamiento'       THEN 3
            WHEN 'Pueblo'                THEN 4
            WHEN 'Ejido'                 THEN 5
            WHEN 'Rancho'                THEN 6
            WHEN 'Barrio'                THEN 7
            WHEN 'Unidad habitacional'   THEN 8
            WHEN 'Congregación'          THEN 9
            WHEN 'Condominio'            THEN 10
            WHEN 'Zona industrial'       THEN 11
            WHEN 'Poblado comunal'       THEN 12
            WHEN 'Granja'                THEN 13
            WHEN 'Conjunto habitacional' THEN 14
            WHEN 'Hacienda'              THEN 15
            WHEN 'Equipamiento'          THEN 16
            WHEN 'Finca'                 THEN 17
            WHEN 'Zona comercial'        THEN 18
            WHEN 'Paraje'                THEN 19
            WHEN 'Residencial'           THEN 20
            WHEN 'Ampliación'            THEN 21
            WHEN 'Aeropuerto'            THEN 22
            WHEN 'Zona federal'          THEN 23
            WHEN 'Exhacienda'            THEN 24
            WHEN 'Gran usuario'          THEN 25
            WHEN 'Parque industrial'     THEN 26
            WHEN 'Estación'              THEN 27
            WHEN 'Villa'                 THEN 28
            WHEN 'Campamento'            THEN 29
            WHEN 'Puerto'                THEN 30
            WHEN 'Zona militar'          THEN 31
            WHEN 'Zona naval'            THEN 32
            ELSE 0
        END AS spot_settlement_type_id,
        CASE
            WHEN zip_c.spot_settlement_type IS NOT NULL THEN TRIM(zip_c.spot_settlement_type)
            ELSE 'Unknown'
        END AS spot_settlement_type,
        CASE TRIM(zip_c.spot_settlement_type)
            WHEN 'Ranchería'             THEN 'Hamlet'
            WHEN 'Colonia'               THEN 'Neighborhood'
            WHEN 'Fraccionamiento'       THEN 'Residential subdivision'
            WHEN 'Pueblo'                THEN 'Village'
            WHEN 'Ejido'                 THEN 'Ejido'                     -- término propio
            WHEN 'Rancho'                THEN 'Ranch'
            WHEN 'Barrio'                THEN 'Quarter'
            WHEN 'Unidad habitacional'   THEN 'Housing complex'
            WHEN 'Congregación'          THEN 'Settlement (congregación)'
            WHEN 'Condominio'            THEN 'Condominium'
            WHEN 'Zona industrial'       THEN 'Industrial zone'
            WHEN 'Poblado comunal'       THEN 'Communal settlement'
            WHEN 'Granja'                THEN 'Farm'
            WHEN 'Conjunto habitacional' THEN 'Residential complex'
            WHEN 'Hacienda'              THEN 'Hacienda'                  -- término propio
            WHEN 'Equipamiento'          THEN 'Public facilities area'
            WHEN 'Finca'                 THEN 'Country estate'
            WHEN 'Zona comercial'        THEN 'Commercial zone'
            WHEN 'Paraje'                THEN 'Locality (paraje)'
            WHEN 'Residencial'           THEN 'Residential area'
            WHEN 'Ampliación'            THEN 'Extension (ampliación)'
            WHEN 'Aeropuerto'            THEN 'Airport'
            WHEN 'Zona federal'          THEN 'Federal zone'
            WHEN 'Exhacienda'            THEN 'Former hacienda'
            WHEN 'Gran usuario'          THEN 'Large user facility'
            WHEN 'Parque industrial'     THEN 'Industrial park'
            WHEN 'Estación'              THEN 'Station'
            WHEN 'Villa'                 THEN 'Town (villa)'
            WHEN 'Campamento'            THEN 'Camp'
            WHEN 'Puerto'                THEN 'Port'
            WHEN 'Zona militar'          THEN 'Military zone'
            WHEN 'Zona naval'            THEN 'Naval zone'
            ELSE 'Unknown'
        END AS spot_settlement_type_en,

        CASE TRIM(zip_c.spot_zone_type)
            WHEN 'Rural'       THEN 1
            WHEN 'Urbano'      THEN 2
            WHEN 'Semiurbano'  THEN 3
            ELSE 0
        END AS spot_zone_type_id,
        CASE
            WHEN zip_c.spot_zone_type IS NOT NULL THEN TRIM(zip_c.spot_zone_type)
            ELSE 'Unknown'
        END AS spot_zone_type,
        CASE TRIM(zip_c.spot_zone_type)
            WHEN 'Rural'       THEN 'Rural'
            WHEN 'Urbano'      THEN 'Urban'
            WHEN 'Semiurbano'  THEN 'Semi-urban'
            ELSE 'Unknown'
        END AS spot_zone_type_en,
        -- ------------------------------- Listings ----------------------------------
        sl.spot_listing_id,
        sl.spot_listing_representative_status_id,
        sl.spot_listing_representative_status,
        sl.spot_listing_status_id,
        sl.spot_listing_status,
        sl.spot_listing_hierarchy,
        sl.spot_is_listing_id,
        sl.spot_is_listing,

        -- ------------------------------- Prices and areas ----------------------------------
        ts.square_space AS spot_area_in_sqm,

        -- Spot modality
        sp.spot_modality_id,
        sp.spot_modality,
        
        -- Rental price per area
        sp.spot_price_area_rent_id,
        sp.spot_price_area_rent,
        
        -- Sale price per area
        sp.spot_price_area_sale_id,
        sp.spot_price_area_sale,
        
        -- Rental currency
        sp.spot_currency_rent_id,
        sp.spot_currency_rent,
        
        -- Sale currency
        sp.spot_currency_sale_id,
        sp.spot_currency_sale,
        
        -- Total rental prices
        sp.spot_price_total_mxn_rent,
        sp.spot_price_total_usd_rent,
        
        -- Rental prices per sqm
        sp.spot_price_sqm_mxn_rent,
        sp.spot_price_sqm_usd_rent,
        
        -- Total sale prices
        sp.spot_price_total_mxn_sale,
        sp.spot_price_total_usd_sale,
        
        -- Sale prices per sqm
        sp.spot_price_sqm_mxn_sale,
        sp.spot_price_sqm_usd_sale,
        
        -- Maintenance costs
        sp.spot_maintenance_cost_mxn,
        sp.spot_maintenance_cost_usd,
        
        -- Subspace rental prices (total in MXN)
        sp.spot_sub_min_price_total_mxn_rent,
        sp.spot_sub_max_price_total_mxn_rent,
        sp.spot_sub_mean_price_total_mxn_rent,
        
        -- Subspace rental prices per sqm in MXN
        sp.spot_sub_min_price_sqm_mxn_rent,
        sp.spot_sub_max_price_sqm_mxn_rent,
        sp.spot_sub_mean_price_sqm_mxn_rent,
        
        -- Subspace rental prices (total in USD)
        sp.spot_sub_min_price_total_usd_rent,
        sp.spot_sub_max_price_total_usd_rent,
        sp.spot_sub_mean_price_total_usd_rent,
        
        -- Subspace rental prices per sqm in USD
        sp.spot_sub_min_price_sqm_usd_rent,
        sp.spot_sub_max_price_sqm_usd_rent,
        sp.spot_sub_mean_price_sqm_usd_rent,
        
        -- Subspace sale prices (total in MXN)
        sp.spot_sub_min_price_total_mxn_sale,
        sp.spot_sub_max_price_total_mxn_sale,
        sp.spot_sub_mean_price_total_mxn_sale,
        
        -- Subspace sale prices per sqm in MXN
        sp.spot_sub_min_price_sqm_mxn_sale,
        sp.spot_sub_max_price_sqm_mxn_sale,
        sp.spot_sub_mean_price_sqm_mxn_sale,
        
        -- Subspace sale prices (total in USD)
        sp.spot_sub_min_price_total_usd_sale,
        sp.spot_sub_max_price_total_usd_sale,
        sp.spot_sub_mean_price_total_usd_sale,
        
        -- Subspace sale prices per sqm in USD
        sp.spot_sub_min_price_sqm_usd_sale,
        sp.spot_sub_max_price_sqm_usd_sale,
        sp.spot_sub_mean_price_sqm_usd_sale,

        -- ------------------------------- Contacts ----------------------------------
        contacts.id AS contact_id,
        contacts.email AS contact_email,
        LOWER(TRIM(SUBSTRING_INDEX(contacts.email, '@', -1))) AS contact_domain,
        contacts.subgroup as contact_subgroup_id,
        CASE 
            WHEN contacts.subgroup = '1' THEN 'Individual Broker'
            WHEN contacts.subgroup = '2' THEN 'Independent Property Owners'
            WHEN contacts.subgroup = '3' THEN 'Institutional Developers & REITS'
            WHEN contacts.subgroup = '4' THEN 'Legacy Broker'
            WHEN contacts.subgroup = '5' THEN 'Franchise Broker'
            WHEN contacts.subgroup = '6' THEN 'SMB Developers'
        END AS contact_subgroup, 
        
        contacts.category AS contact_category_id,
        CASE
            WHEN contacts.category = 1 THEN 'DI'
            WHEN contacts.category = 2 THEN 'Broker'
            WHEN contacts.category = 3 THEN 'Owner'
        END AS contact_category,

        contacts.company AS contact_company,

        --  ------------------------------- Users ----------------------------------
        users.id AS user_id,
        profiles.id AS user_profile_id,

        COALESCE(mhr.max_role, 0) AS user_max_role_id,
        CASE
            WHEN mhr.max_role = 1 THEN "Admin"
            WHEN mhr.max_role = 2 THEN "Tenant"
            WHEN mhr.max_role = 3 THEN "External Broker"
            WHEN mhr.max_role = 4 THEN "Landlord"
            WHEN mhr.max_role = 5 THEN "Internal Broker"
            ELSE "Unknown"
        END AS user_max_role,

        COALESCE(profiles.tenant_type, 0) AS user_industria_role_id,
        CASE
            WHEN profiles.tenant_type = 1 THEN "Tenant"
            WHEN profiles.tenant_type = 2 THEN "Broker"
            WHEN profiles.tenant_type = 4 THEN "Landlord"
            WHEN profiles.tenant_type = 5 THEN "Developer"
            ELSE "Unknown"
        END AS user_industria_role,

        LOWER(TRIM(users.email)) AS user_email,
        LOWER(TRIM(SUBSTRING_INDEX(users.email, '@', -1))) AS user_domain,
        
        CASE
            WHEN LOWER(TRIM(users.email)) LIKE '%@spot2.mx' OR LOWER(TRIM(users.email)) LIKE '%@spot2-services.com' THEN 1
            ELSE 0
        END AS user_affiliation_id,
        CASE
            WHEN LOWER(TRIM(users.email)) LIKE '%@spot2.mx' OR LOWER(TRIM(users.email)) LIKE '%@spot2-services.com' THEN 'Internal User'
            ELSE 'External User'
        END AS user_affiliation,

        CASE 
			WHEN users.level_id IN (1, 2, 3) THEN users.level_id
			ELSE 0
		END AS user_level_id,
		CASE users.level_id
			WHEN 1 THEN "Gold"
			WHEN 2 THEN "Platinum"
			WHEN 3 THEN "Titanium"
			ELSE "Others"
		END AS user_level,
        
        CASE
            WHEN LOWER(TRIM(users.email)) LIKE '%@nextagents.mx' THEN 1
            ELSE 0
        END AS user_broker_next_id,
        CASE
            WHEN LOWER(TRIM(users.email)) LIKE '%@nextagents.mx' THEN 'Yes'
            ELSE 'No'
        END AS user_broker_next,
        
        -- ------------------------------- Qualities ----------------------------------
        ts.external_id AS spot_external_id,
        ts.external_updated_at AS spot_external_updated_at,
        ts.quality_state_real AS spot_quality_control_status_id,
        CASE
            WHEN ts.quality_state_real = 1 THEN "Pending"
            WHEN ts.quality_state_real = 2 THEN "Reported"
            WHEN ts.quality_state_real = 3 THEN "Verified"
        END AS spot_quality_control_status,
        IFNULL(phs.spot_photo_count, 0) AS spot_photo_count,
        IFNULL(phs.spot_photo_platform_count, 0) AS spot_photo_platform_count,
        IFNULL(parent_phs.spot_photo_count, 0) AS spot_parent_photo_count,
        IFNULL(parent_phs.spot_photo_platform_count, 0) AS spot_parent_photo_platform_count,
        CASE 
            WHEN IFNULL(phs.spot_photo_count, 0) > 0 THEN IFNULL(phs.spot_photo_count, 0)
            ELSE IFNULL(parent_phs.spot_photo_count, 0)
        END AS spot_photo_effective_count,

        CASE 
            WHEN IFNULL(phs.spot_photo_platform_count, 0) > 0 THEN IFNULL(phs.spot_photo_platform_count, 0)
            ELSE IFNULL(parent_phs.spot_photo_platform_count, 0)
        END AS spot_photo_platform_effective_count,

        ts.class AS spot_class_id,
        CASE ts.class 
            WHEN 1 THEN "A+"
            WHEN 2 THEN "A"
            WHEN 3 THEN "B"
            WHEN 4 THEN "C"
        END AS spot_class,
        ts.parking_spaces AS spot_parking_spaces,
        ts.parking_space_by_area_real AS spot_parking_space_by_area,
        ts.spot_condition AS spot_condition_id,
        CASE ts.spot_condition
            WHEN 1 THEN 'Shell condition'
            WHEN 2 THEN 'Conditioned'
            WHEN 3 THEN 'Furnished'
            ELSE 'Unknown'
        END AS spot_condition,
        ts.construction_date AS spot_construction_date,
        sp_scores.score AS spot_score,
        COALESCE(ts.is_exclusive, 0) AS spot_exclusive_id,
        CASE 
            WHEN ts.is_exclusive = 1 THEN 'Yes'
            ELSE 'No'
        END AS spot_exclusive,
        COALESCE(ts.landlord_exclusive, 0) AS spot_landlord_exclusive_id,
        CASE 
            WHEN ts.landlord_exclusive = 1 THEN 'Yes'
            ELSE 'No'
        END AS spot_landlord_exclusive,

        -- Area range validation
        CASE 
            WHEN ts.square_space IS NULL THEN NULL  -- No tiene área definida
            -- Singles y Subspaces (mismo límite)
            WHEN source_table_id IN (1, 3) AND ts.spot_type_id_real = 11 AND ts.square_space BETWEEN 30 AND 2000 THEN 1        -- Office
            WHEN source_table_id IN (1, 3) AND ts.spot_type_id_real = 13 AND ts.square_space BETWEEN 20 AND 15000 THEN 1       -- Retail  
            WHEN source_table_id IN (1, 3) AND ts.spot_type_id_real = 9 AND ts.square_space BETWEEN 100 AND 50000 THEN 1       -- Industrial
            WHEN source_table_id IN (1, 3) AND ts.spot_type_id_real = 15 AND ts.square_space BETWEEN 200 AND 1000000 THEN 1    -- Land
            
            -- Complex
            WHEN source_table_id = 2 AND ts.spot_type_id_real = 11 AND ts.square_space BETWEEN 250 AND 25000 THEN 1             -- Office
            WHEN source_table_id = 2 AND ts.spot_type_id_real = 13 AND ts.square_space BETWEEN 20 AND 20000 THEN 1              -- Retail
            WHEN source_table_id = 2 AND ts.spot_type_id_real = 9 AND ts.square_space BETWEEN 1000 AND 150000 THEN 1            -- Industrial  
            WHEN source_table_id = 2 AND ts.spot_type_id_real = 15 AND ts.square_space BETWEEN 430 AND 1394.26406871985 THEN 1  -- Land
            
            ELSE 0
        END AS spot_is_area_in_range_id,

        CASE 
            WHEN ts.square_space IS NULL THEN NULL  -- No tiene área definida
            -- Singles y Subspaces (mismo límite)
            WHEN source_table_id IN (1, 3) AND ts.spot_type_id_real = 11 AND ts.square_space BETWEEN 30 AND 2000 THEN 'Yes'        
            WHEN source_table_id IN (1, 3) AND ts.spot_type_id_real = 13 AND ts.square_space BETWEEN 20 AND 15000 THEN 'Yes'       
            WHEN source_table_id IN (1, 3) AND ts.spot_type_id_real = 9 AND ts.square_space BETWEEN 100 AND 50000 THEN 'Yes'       
            WHEN source_table_id IN (1, 3) AND ts.spot_type_id_real = 15 AND ts.square_space BETWEEN 200 AND 1000000 THEN 'Yes'    
            
            -- Complex
            WHEN source_table_id = 2 AND ts.spot_type_id_real = 11 AND ts.square_space BETWEEN 250 AND 25000 THEN 'Yes'             
            WHEN source_table_id = 2 AND ts.spot_type_id_real = 13 AND ts.square_space BETWEEN 20 AND 20000 THEN 'Yes'              
            WHEN source_table_id = 2 AND ts.spot_type_id_real = 9 AND ts.square_space BETWEEN 1000 AND 150000 THEN 'Yes'            
            WHEN source_table_id = 2 AND ts.spot_type_id_real = 15 AND ts.square_space BETWEEN 430 AND 1394.26406871985 THEN 'Yes'  
            
            ELSE 'No'
        END AS spot_is_area_in_range,

        -- Price range validation
        CASE 
            WHEN sp.spot_price_sqm_mxn_rent IS NULL THEN NULL  -- No tiene precio de renta
            WHEN ts.spot_type_id_real = 11 AND sp.spot_price_sqm_mxn_rent BETWEEN 80 AND 900 THEN 1      -- Office
            WHEN ts.spot_type_id_real = 13 AND sp.spot_price_sqm_mxn_rent BETWEEN 45 AND 1200 THEN 1     -- Retail
            WHEN ts.spot_type_id_real = 9 AND sp.spot_price_sqm_mxn_rent BETWEEN 50 AND 300 THEN 1       -- Industrial
            WHEN ts.spot_type_id_real = 15 AND sp.spot_price_sqm_mxn_rent BETWEEN 50 AND 250 THEN 1      -- Land
            ELSE 0
        END AS spot_is_rent_price_in_range_id,

        CASE 
            WHEN sp.spot_price_sqm_mxn_rent IS NULL THEN NULL  -- No tiene precio de renta
            WHEN ts.spot_type_id_real = 11 AND sp.spot_price_sqm_mxn_rent BETWEEN 80 AND 900 THEN 'Yes'
            WHEN ts.spot_type_id_real = 13 AND sp.spot_price_sqm_mxn_rent BETWEEN 45 AND 1200 THEN 'Yes'
            WHEN ts.spot_type_id_real = 9 AND sp.spot_price_sqm_mxn_rent BETWEEN 50 AND 300 THEN 'Yes'
            WHEN ts.spot_type_id_real = 15 AND sp.spot_price_sqm_mxn_rent BETWEEN 50 AND 250 THEN 'Yes'
            ELSE 'No'
        END AS spot_is_rent_price_in_range,

        CASE 
            WHEN sp.spot_price_sqm_mxn_sale IS NULL THEN NULL  -- No tiene precio de venta
            WHEN ts.spot_type_id_real = 11 AND sp.spot_price_sqm_mxn_sale BETWEEN 12000 AND 90000 THEN 1   -- Office
            WHEN ts.spot_type_id_real = 13 AND sp.spot_price_sqm_mxn_sale BETWEEN 6000 AND 150000 THEN 1   -- Retail
            WHEN ts.spot_type_id_real = 9 AND sp.spot_price_sqm_mxn_sale BETWEEN 5000 AND 40000 THEN 1     -- Industrial
            WHEN ts.spot_type_id_real = 15 AND sp.spot_price_sqm_mxn_sale BETWEEN 100 AND 250000 THEN 1    -- Land
            ELSE 0
        END AS spot_is_sale_price_in_range_id,

        CASE 
            WHEN sp.spot_price_sqm_mxn_sale IS NULL THEN NULL  -- No tiene precio de venta
            WHEN ts.spot_type_id_real = 11 AND sp.spot_price_sqm_mxn_sale BETWEEN 12000 AND 90000 THEN 'Yes'
            WHEN ts.spot_type_id_real = 13 AND sp.spot_price_sqm_mxn_sale BETWEEN 6000 AND 150000 THEN 'Yes'
            WHEN ts.spot_type_id_real = 9 AND sp.spot_price_sqm_mxn_sale BETWEEN 5000 AND 40000 THEN 'Yes'
            WHEN ts.spot_type_id_real = 15 AND sp.spot_price_sqm_mxn_sale BETWEEN 100 AND 250000 THEN 'Yes'
            ELSE 'No'
        END AS spot_is_sale_price_in_range,

        -- Maintenance price range validation
        CASE 
            WHEN sp.spot_maintenance_cost_mxn IS NULL THEN NULL                                        -- No tiene costo de mantenimiento
            WHEN ts.spot_type_id_real = 15 THEN NULL                                                  -- Land no aplica
            WHEN ts.spot_type_id_real = 11 AND sp.spot_maintenance_cost_mxn BETWEEN 15 AND 135 THEN 1 -- Office
            WHEN ts.spot_type_id_real = 13 AND sp.spot_maintenance_cost_mxn BETWEEN 6 AND 180 THEN 1  -- Retail
            WHEN ts.spot_type_id_real = 9 AND sp.spot_maintenance_cost_mxn BETWEEN 7 AND 45 THEN 1    -- Industrial
            ELSE 0
        END AS spot_is_maintenance_price_in_range_id,

        CASE 
            WHEN sp.spot_maintenance_cost_mxn IS NULL THEN NULL                                        -- No tiene costo de mantenimiento
            WHEN ts.spot_type_id_real = 15 THEN NULL                                                  -- Land no aplica
            WHEN ts.spot_type_id_real = 11 AND sp.spot_maintenance_cost_mxn BETWEEN 15 AND 135 THEN 'Yes' -- Office
            WHEN ts.spot_type_id_real = 13 AND sp.spot_maintenance_cost_mxn BETWEEN 6 AND 180 THEN 'Yes'  -- Retail
            WHEN ts.spot_type_id_real = 9 AND sp.spot_maintenance_cost_mxn BETWEEN 7 AND 45 THEN 'Yes'    -- Industrial
            ELSE 'No'
        END AS spot_is_maintenance_price_in_range,

        -- Origin tracking fields
        COALESCE(ts.origin, 0) AS spot_origin_id,
        CASE 
            WHEN ts.origin = 1 THEN 'Spot2'
            WHEN ts.origin = 2 THEN 'NocNok'
            WHEN ts.origin = 3 THEN 'Tokko'
            WHEN ts.origin = 4 THEN 'EasyBroker'
            ELSE 'Unknown'
        END AS spot_origin,

        -- ------------------------------- Reports ----------------------------------
        COALESCE(sr.spot_last_active_report_reason_id, 0) AS spot_last_active_report_reason_id,
        COALESCE(sr.spot_last_active_report_reason, 'No active reports') AS spot_last_active_report_reason,
        sr.spot_last_active_report_user_id,
        sr.spot_last_active_report_created_at,
        COALESCE(sr.spot_has_active_report, 0) AS spot_has_active_report,
        sr.spot_reports_full_history,

        --  ------------------------------- Dates ----------------------------------
        ts.created_at AS spot_created_at,
        DATE(ts.created_at) AS spot_created_date,
        ts.updated_at AS spot_updated_at,
        DATE(ts.updated_at) AS spot_updated_date,
        ts.valid_through AS spot_valid_through,
        DATE(ts.valid_through) AS spot_valid_through_date,
        ts.deleted_at AS spot_deleted_at,
        DATE(ts.deleted_at) AS spot_deleted_date,

        -- ------------------------------- Auditing ----------------------------------
        NOW() AS aud_inserted_at,
        CURDATE() AS aud_inserted_date,
        NOW() AS aud_updated_at,
        CURDATE() AS aud_updated_date,
        'stg_lk_spots' AS aud_job

-- ------------------------------------------------------------------[ FROM ]----------------------------------------------------------------------
        FROM
            total_spots AS ts

-- ------------------------------------------------------------------[ LEFT JOIN ]----------------------------------------------------------------------
    LEFT JOIN contacts ON ts.contact_id_real = contacts.id
    LEFT JOIN users ON ts.user_id = users.id
    LEFT JOIN profiles ON ts.user_id = profiles.user_id
    LEFT JOIN spots_prices AS sp ON ts.id = sp.spot_id
    LEFT JOIN (
        SELECT
            id,
            code,
            settlement AS spot_settlement,
            settlement_type AS spot_settlement_type,
            zone AS spot_zone_type,
            municipality,
            state_id
        FROM
            zip_codes
    ) zip_c ON ts.zip_code_id_real = zip_c.id
    LEFT JOIN (
        SELECT
            id,
            name
        FROM
            states
    ) states_c ON zip_c.state_id = states_c.id
    LEFT JOIN data_states ON ts.state_id_real2 = data_states.id
    LEFT JOIN states ON ts.state_id_real2    = states.id
    LEFT JOIN data_municipalities ON ts.data_municipality_id_real = data_municipalities.id
    LEFT JOIN cities ON ts.city_id_real = cities.id
    LEFT JOIN data_settlements ON ts.settlement_id_real = data_settlements.id
    LEFT JOIN zones ON ts.zone_id_real = zones.id
    LEFT JOIN zones AS mearest_zones ON ts.nearest_zone_id_real = mearest_zones.id
    LEFT JOIN spot_photos AS phs ON ts.id = phs.photo_spot_id
    LEFT JOIN spot_photos AS parent_phs ON ts.parent_id = parent_phs.photo_spot_id
    LEFT JOIN (
        SELECT
            model_id,
            max(role_id) AS max_role
        FROM
            model_has_roles
        GROUP BY
            model_id
    ) mhr ON users.id = mhr.model_id
    /*LEFT JOIN (
        SELECT 
            spot_id,
            score 
        FROM spot_rankings
    )  sp_scores ON ts.id = sp_scores.spot_id*/
    LEFT JOIN (
        SELECT 
            spot_id, 
            score
        FROM (
            SELECT 
                spot_id, 
                score,
                ROW_NUMBER() OVER (PARTITION BY spot_id ORDER BY created_at DESC) AS rn
            FROM spot_rankings
        ) ranked
        WHERE rn = 1
    ) sp_scores ON ts.id = sp_scores.spot_id
    LEFT JOIN spot_reports_summary sr ON ts.id = sr.spot_id
    LEFT JOIN spot_listings_filtered sl ON ts.id = sl.spot_id
)

SELECT * FROM final_results;
