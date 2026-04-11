-- ==================================================================[ Inquiry ]======================================================================
WITH inquiry AS (
    SELECT user_id, event_name, created_at, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS ranking
    FROM persona_inquiries
),

-- ==================================================================[ KYC ]======================================================================
kyc AS (
    SELECT
        users.id AS kyc_user_id,
        inquiry.user_id AS inquiry_user_id,
        inquiry.event_name AS event_name,
        inquiry.created_at AS inquiry_created_at,
        CASE 
            WHEN event_name IN ('inquiry.completed', 'inquiry.approved') THEN 1 -- 'Completed
            WHEN email REGEXP '(@spot2\\.mx$|@spot2-services\\.com$)' THEN 1 -- 'Completed
            WHEN event_name IN ('inquiry.failed', 'inquiry.declined') THEN 2 -- 'Failed'
            WHEN inquiry.user_id IS NULL THEN 3 -- 'Pending'
            ELSE 3 -- 'Pending'
        END AS user_kyc_status_id
    FROM users LEFT JOIN inquiry ON users.id = inquiry.user_id 
    WHERE 
        (
            (
                inquiry.ranking = 1 -- Last inquiry
                OR inquiry.user_id IS NULL -- Pending
            )
            AND EXISTS ( -- Only brokers
                SELECT *
                FROM profiles
                WHERE users.id = profiles.user_id AND profiles.tenant_type = 2
            )
        ) OR users.email REGEXP '(@spot2\\.mx$|@spot2-services\\.com$)' -- Make sure to always include the spotters
    ORDER BY event_name DESC
),

-- ==================================================================[ User Activity Log ]======================================================================
-- user_alg AS (
--     SELECT
--         causer_id AS user_id,
--         MAX(created_at) AS user_last_log_at,
--         DATEDIFF(CURRENT_DATE, MAX(created_at)) AS user_days_since_last_log,
--         COUNT(*) AS user_total_log_count
--     FROM activity_log
--     GROUP BY
--         user_id
-- ),

-- ==================================================================[ Earliest Profiles ]======================================================================
earliest_profiles AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at ASC) AS rn
        FROM profiles
    ) p
    WHERE p.rn = 1
),

-- ==================================================================[ Total Users ]======================================================================
total_users AS (
    SELECT 
        users.id AS user_id,
        uuid AS user_uuid,
        ep.name AS user_name,
        ep.last_name AS user_last_name,
        ep.mothers_last_name AS user_mothers_last_name,
        NULLIF(TRIM(CONCAT_WS(' ', ep.name, ep.last_name, ep.mothers_last_name)), '') AS user_full_name,
        users.email AS user_email,
        LOWER(TRIM(SUBSTRING_INDEX(users.email, '@', -1))) AS user_domain,
        ep.phone_indicator AS user_phone_indicator,
        ep.phone_number AS user_phone_number,
        COALESCE(ep.phone_full_number, CONCAT(ep.phone_indicator, ep.phone_number)) AS user_phone_full_number,
        ep.has_whatsapp AS user_has_whatsapp,
        ep.id AS user_profile_id,
        users.owner_id AS user_owner_id,
        users.contact_id AS contact_id,
        users.hubspot_owner_id AS user_hubspot_owner_id,
        users.hubspot_user_id AS user_hubspot_user_id,
        -- profiles.lead_id AS user_lead_id, por el momento
        COALESCE(ep.person_type, 0) AS user_person_type_id,
        CASE
            WHEN ep.person_type = 1 THEN 'Legal Entity'
            WHEN ep.person_type = 2 THEN 'Individual'
            ELSE "Unknown"
        END AS user_person_type,
        COALESCE(mhr.max_role, 0) AS user_max_role_id,
        CASE
            WHEN mhr.max_role = 1 THEN "Admin"
            WHEN mhr.max_role = 2 THEN "Tenant"
            WHEN mhr.max_role = 3 THEN "External Broker"
            WHEN mhr.max_role = 4 THEN "Landlord"
            WHEN mhr.max_role = 5 THEN "Internal Broker"
            ELSE "Unknown"
        END AS user_max_role,
        COALESCE(ep.tenant_type, 0) AS user_industria_role_id,
        CASE
            WHEN ep.tenant_type = 1 THEN "Tenant"
            WHEN ep.tenant_type = 2 THEN "Broker"
            WHEN ep.tenant_type = 4 THEN "Landlord"
            WHEN ep.tenant_type = 5 THEN "Developer"
            ELSE "Unknown"
        END AS user_industria_role,
        CASE
            WHEN ep.tenant_type = 1 THEN 1
            WHEN ep.tenant_type IN (2, 4, 5) THEN 2
            ELSE 0
        END AS user_type_id,
        CASE
            WHEN ep.tenant_type = 1 THEN "Buyer"
            WHEN ep.tenant_type IN (2, 4, 5) THEN "Seller"
            ELSE "Unknown"
        END AS user_type,
        CASE
            WHEN users.email LIKE '%@spot2.mx' OR users.email LIKE '%@spot2-services.com' THEN 1
            ELSE 0
        END AS user_affiliation_id,
        CASE
            WHEN users.email LIKE '%@spot2.mx' OR users.email LIKE '%@spot2-services.com' THEN 'Internal User'
            ELSE 'External User'
        END AS user_affiliation,
        
        -- user_alg.user_last_log_at AS user_last_log_at,
        -- user_alg.user_days_since_last_log AS user_days_since_last_log,
        -- user_alg.user_total_log_count AS user_total_log_count,
        NULL AS user_last_log_at,
        NULL AS user_days_since_last_log,
        NULL AS user_total_log_count,
        IFNULL(kyc.user_kyc_status_id, 0) AS user_kyc_status_id,
        CASE kyc.user_kyc_status_id
            WHEN 1 THEN 'Completed'
            WHEN 2 THEN 'Failed'
            WHEN 3 THEN 'Pending'
            ELSE 'Not Applicable'
        END AS user_kyc_status,
        kyc.inquiry_created_at AS user_kyc_created_at,
        users.created_at AS user_created_at,
        DATE(users.created_at) AS user_created_date,
        users.updated_at AS user_updated_at,
        DATE(users.updated_at) AS user_updated_date,
        users.deleted_at AS user_deleted_at,
        DATE(users.deleted_at) AS user_deleted_date,
        NOW() AS aud_inserted_at,
        CURDATE() AS aud_inserted_date,
        NOW() AS aud_updated_at,
        CURDATE() AS aud_updated_date,
        'stg_lk_users' AS aud_job,
        CASE -- user not deleted
            WHEN users.deleted_at IS NULL THEN 1
            ELSE 0
        END AS filter
    FROM users

-- ------------------------------------------------------------------[ LEFT JOIN ]----------------------------------------------------------------------
    LEFT JOIN earliest_profiles AS ep ON users.id = ep.user_id
    LEFT JOIN (
        SELECT
            model_id,
            max(role_id) AS max_role
        FROM
            model_has_roles
        GROUP BY
            model_id
    ) mhr ON users.id = mhr.model_id
    LEFT JOIN kyc ON users.id = kyc.kyc_user_id
    -- LEFT JOIN user_alg ON user_alg.user_id = users.id
)

SELECT * FROM total_users;
