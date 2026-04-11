WITH user_spot_metrics AS (
    SELECT
        user_id,
        SUM(CASE WHEN spot_sector_id = 9 THEN 1 ELSE 0 END) AS user_industrial_count,
        SUM(CASE WHEN spot_sector_id = 11 THEN 1 ELSE 0 END) AS user_office_count,
        SUM(CASE WHEN spot_sector_id = 13 THEN 1 ELSE 0 END) AS user_retail_count,
        SUM(CASE WHEN spot_sector_id = 15 THEN 1 ELSE 0 END) AS user_land_count,
        COUNT(spot_sector_id) AS user_spot_count,
        SUM(CASE WHEN spot_status_id = 1 THEN 1 ELSE 0 END) AS user_public_spot_count,
        SUM(CASE WHEN spot_sector_id = 9 THEN spot_area_in_sqm ELSE 0.0 END) AS user_total_industrial_sqm,
        SUM(CASE WHEN spot_sector_id = 11 THEN spot_area_in_sqm ELSE 0.0 END) AS user_total_office_sqm,
        SUM(CASE WHEN spot_sector_id = 13 THEN spot_area_in_sqm ELSE 0.0 END) AS user_total_retail_sqm,
        SUM(CASE WHEN spot_sector_id = 15 THEN spot_area_in_sqm ELSE 0.0 END) AS user_total_land_sqm,
        SUM(spot_area_in_sqm) AS user_total_spot_sqm,
        MIN(spot_created_at) AS user_first_spot_created_at,
        MAX(spot_created_at) AS user_last_spot_created_at,
        MAX(spot_updated_at) AS user_last_spot_updated_at,
        CASE 
            WHEN MAX(spot_created_at) >= MAX(spot_updated_at) THEN MAX(spot_created_at)
            ELSE MAX(spot_updated_at)
        END AS user_last_spot_activity_at,
        CAST((julianday(date('now')) - julianday(MAX(spot_created_at))) AS INTEGER) AS user_days_since_last_spot_created,
        CAST((julianday(date('now')) - julianday(MAX(spot_updated_at))) AS INTEGER) AS user_days_since_last_spot_updated,
        CAST((julianday(date('now')) - julianday(
            CASE 
                WHEN MAX(spot_created_at) >= MAX(spot_updated_at) THEN MAX(spot_created_at)
                ELSE MAX(spot_updated_at)
            END
            )
        ) AS INTEGER) AS user_days_since_last_spot_activity
    FROM lk_spots
    WHERE spot_deleted_at IS NULL
    GROUP BY user_id
),

user_kam_ranked AS (
    SELECT
        user_id,
        agent_kam_id,
        lead_updated_at,
        lead_id,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY lead_updated_at DESC, lead_id DESC
        ) AS rn
    FROM lk_leads
    WHERE user_id IS NOT NULL
      AND agent_kam_id IS NOT NULL
),

user_kam AS (
    SELECT user_id, agent_kam_id
    FROM user_kam_ranked
    WHERE rn = 1
),

final_lk_users AS (
    SELECT 
        users.user_id AS user_id,
        users.user_uuid AS user_uuid,
        users.user_name AS user_name,
        users.user_last_name AS user_last_name,
        users.user_mothers_last_name AS user_mothers_last_name,
        users.user_full_name AS user_full_name,
        users.user_email AS user_email,
        users.user_domain AS user_domain,
        users.user_phone_indicator AS user_phone_indicator,
        users.user_phone_number AS user_phone_number,
        users.user_phone_full_number AS user_phone_full_number,
        users.user_has_whatsapp AS user_has_whatsapp,
        users.user_profile_id AS user_profile_id,
        users.user_owner_id AS user_owner_id,
        users.contact_id AS contact_id,
        users.user_hubspot_owner_id AS user_hubspot_owner_id,
        users.user_hubspot_user_id AS user_hubspot_user_id,
        users.user_person_type_id AS user_person_type_id,
        users.user_person_type AS user_person_type,
        users.user_max_role_id AS user_max_role_id,
        users.user_max_role AS user_max_role,
        users.user_industria_role_id AS user_industria_role_id,
        users.user_industria_role AS user_industria_role,
        users.user_type_id AS user_type_id,
        users.user_type AS user_type,
        duc.user_category_id AS user_category_id,
        duc.user_category AS user_category,
        users.user_affiliation_id AS user_affiliation_id,
        users.user_affiliation AS user_affiliation,
        lkl.agent_kam_id AS agent_kam_id,
        lkua.user_full_name AS agent_kam_full_name,
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM lk_leads 
                WHERE lk_leads.agent_kam_id = users.user_id
            ) THEN 1
            ELSE 0
        END AS user_is_kam,
        IFNULL(lks.user_industrial_count, 0) AS user_industrial_count,
        IFNULL(lks.user_office_count, 0) AS user_office_count,
        IFNULL(lks.user_retail_count, 0) AS user_retail_count,
        IFNULL(lks.user_land_count, 0) AS user_land_count,
        IFNULL(lks.user_spot_count, 0) AS user_spot_count,
        IFNULL(lks.user_public_spot_count, 0) AS user_public_spot_count,
        IFNULL(lks.user_total_industrial_sqm, 0) AS user_total_industrial_sqm,
        IFNULL(lks.user_total_office_sqm, 0) AS user_total_office_sqm,
        IFNULL(lks.user_total_retail_sqm, 0) AS user_total_retail_sqm,
        IFNULL(lks.user_total_land_sqm, 0) AS user_total_land_sqm,
        IFNULL(lks.user_total_spot_sqm, 0) AS user_total_spot_sqm,
        lks.user_first_spot_created_at AS user_first_spot_created_at,
        lks.user_last_spot_created_at AS user_last_spot_created_at,
        lks.user_last_spot_updated_at AS user_last_spot_updated_at,
        lks.user_last_spot_activity_at AS user_last_spot_activity_at,
        lks.user_days_since_last_spot_created AS user_days_since_last_spot_created,
        lks.user_days_since_last_spot_updated AS user_days_since_last_spot_updated,
        lks.user_days_since_last_spot_activity AS user_days_since_last_spot_activity,
        -- users.user_last_log_at AS user_last_log_at,
        -- users.user_days_since_last_log AS user_days_since_last_log,
        -- users.user_total_log_count AS user_total_log_count,
        NULL AS user_last_log_at,
        NULL AS user_days_since_last_log,
        NULL AS user_total_log_count,
        users.user_kyc_status_id AS user_kyc_status_id,
        users.user_kyc_status AS user_kyc_status,
        -- ------------------------------- Dates ----------------------------------
        CASE 
            WHEN users.user_affiliation_id = 1 AND users.user_kyc_created_at IS NULL THEN users.user_created_at
            ELSE users.user_kyc_created_at
        END AS user_kyc_created_at,
        users.user_created_at AS user_created_at,
        users.user_created_date AS user_created_date,
        users.user_updated_at AS user_updated_at,
        users.user_updated_date AS user_updated_date,
        users.user_deleted_at AS user_deleted_at,
        users.user_deleted_date AS user_deleted_date,
        -- ------------------------------- Auditing ----------------------------------
        datetime('now') AS aud_inserted_at,
        date('now') AS aud_inserted_date,
        datetime('now') AS aud_updated_at,
        date('now') AS aud_updated_date,
        'lk_users' AS aud_job,
        users.filter AS filter
    FROM stg_lk_users AS users
    LEFT JOIN user_spot_metrics AS lks ON lks.user_id = users.user_id
    LEFT JOIN lk_duc_domain_user_categories AS duc
        ON duc.user_industria_role_id = users.user_industria_role_id
        AND LOWER(TRIM(substr(users.user_email, instr(users.user_email, '@')+1))) = LOWER(TRIM(duc.duc_domain))
    LEFT JOIN user_kam AS lkl
        ON lkl.user_id = users.user_id
    LEFT JOIN stg_lk_users AS lkua   
        ON lkl.agent_kam_id = lkua.user_id
)

SELECT 
    lks.user_id AS user_id,
    lks.user_uuid AS user_uuid,
    lks.user_name AS user_name,
    lks.user_last_name AS user_last_name,
    lks.user_mothers_last_name AS user_mothers_last_name,
    lks.user_full_name AS user_full_name,
    lks.user_email AS user_email,
    lks.user_domain AS user_domain,
    lks.user_phone_indicator AS user_phone_indicator,
    lks.user_phone_number AS user_phone_number,
    lks.user_phone_full_number AS user_phone_full_number,
    lks.user_has_whatsapp AS user_has_whatsapp,
    lks.user_profile_id AS user_profile_id,
    lks.user_owner_id AS user_owner_id,
    lks.contact_id AS contact_id,
    lks.user_hubspot_owner_id AS user_hubspot_owner_id,
    lks.user_hubspot_user_id AS user_hubspot_user_id,
    lks.user_person_type_id AS user_person_type_id,
    lks.user_person_type AS user_person_type,
    lks.user_max_role_id AS user_max_role_id,
    lks.user_max_role AS user_max_role,
    lks.user_industria_role_id AS user_industria_role_id,
    lks.user_industria_role AS user_industria_role,
    lks.user_type_id AS user_type_id,
    lks.user_type AS user_type,
    lks.user_category_id AS user_category_id,
    lks.user_category AS user_category,
    lks.user_affiliation_id AS user_affiliation_id,
    lks.user_affiliation AS user_affiliation,
    lks.agent_kam_id AS agent_kam_id,
    lks.agent_kam_full_name AS agent_kam_full_name,
    lks.user_is_kam AS user_is_kam,
    lks.user_industrial_count AS user_industrial_count,
    lks.user_office_count AS user_office_count,
    lks.user_retail_count AS user_retail_count,
    lks.user_land_count AS user_land_count,
    lks.user_spot_count AS user_spot_count,
    lks.user_public_spot_count AS user_public_spot_count,
    lks.user_total_industrial_sqm AS user_total_industrial_sqm,
    lks.user_total_office_sqm AS user_total_office_sqm,
    lks.user_total_retail_sqm AS user_total_retail_sqm,
    lks.user_total_land_sqm AS user_total_land_sqm,
    lks.user_total_spot_sqm AS user_total_spot_sqm,
    lks.user_first_spot_created_at AS user_first_spot_created_at,
    lks.user_last_spot_created_at AS user_last_spot_created_at,
    lks.user_last_spot_updated_at AS user_last_spot_updated_at,
    lks.user_last_spot_activity_at AS user_last_spot_activity_at,
    lks.user_days_since_last_spot_created AS user_days_since_last_spot_created,
    lks.user_days_since_last_spot_updated AS user_days_since_last_spot_updated,
    lks.user_days_since_last_spot_activity AS user_days_since_last_spot_activity,
    -- lks.user_last_log_at AS user_last_log_at,
    -- lks.user_days_since_last_log AS user_days_since_last_log,
    -- lks.user_total_log_count AS user_total_log_count,
    NULL AS user_last_log_at,
    NULL AS user_days_since_last_log,
    NULL AS user_total_log_count,
    lks.user_kyc_status_id AS user_kyc_status_id,
    lks.user_kyc_status AS user_kyc_status,
    -- ------------------------------- Dates ----------------------------------
    lks.user_kyc_created_at AS user_kyc_created_at,
    lks.user_created_at AS user_created_at,
    lks.user_created_date AS user_created_date,
    lks.user_updated_at AS user_updated_at,
    lks.user_updated_date AS user_updated_date,
    lks.user_deleted_at AS user_deleted_at,
    lks.user_deleted_date AS user_deleted_date,
    -- ------------------------------- Auditing ----------------------------------
    lks.aud_inserted_at AS aud_inserted_at,
    lks.aud_inserted_date AS aud_inserted_date,
    lks.aud_updated_at AS aud_updated_at,
    lks.aud_updated_date AS aud_updated_date,
    lks.aud_job AS aud_job,
    lks.filter AS filter
FROM final_lk_users AS lks;
