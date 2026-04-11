WITH te AS (
    SELECT
        exchange_rate AS er
    FROM 
        exchanges
    ORDER BY 
        created_at DESC 
    LIMIT 1
),

stg_lk_leads AS (
    SELECT
        lds.id AS lead_id,
        lds.name AS lead_name,
        lds.lastname AS lead_last_name,
        lds.mothers_lastname AS lead_mothers_last_name,
        lds.email AS lead_email,
        LOWER(TRIM(SUBSTRING_INDEX(lds.email, '@', -1))) AS lead_domain,
        lds.phone_indicator AS lead_phone_indicator, 
        lds.phone_number AS lead_phone_number,
        CONCAT(IF(lds.phone_indicator != '', CONCAT(lds.phone_indicator, ' '), ''), lds.phone_number) AS lead_full_phone_number,
        lds.company AS lead_company,
        lds.position AS lead_position,
        lds.origin AS lead_origin_id,
        CASE lds.origin
            WHEN 1 THEN 'Spot2'
            WHEN 2 THEN 'Hubspot'
        END AS lead_origin,
        IF(lds.lead0_at IS NOT NULL, TRUE, FALSE) AS lead_l0,
        IF(lds.lead1_at IS NOT NULL, TRUE, FALSE) AS lead_l1,
        IF(lds.lead2_at IS NOT NULL, TRUE, FALSE) AS lead_l2,
        IF(lds.lead3_at IS NOT NULL, TRUE, FALSE) AS lead_l3,
        IF(lds.lead4_at IS NOT NULL, TRUE, FALSE) AS lead_l4,
        IF(lds.supply_at IS NOT NULL, TRUE, FALSE) AS lead_supply,
        IF(lds.supply0_at IS NOT NULL, TRUE, FALSE) AS lead_s0,
        IF(lds.supply1_at IS NOT NULL, TRUE, FALSE) AS lead_s1,
        IF(lds.supply3_at IS NOT NULL, TRUE, FALSE) AS lead_s3,
        lds.client_type AS lead_client_type_id,
        CASE lds.client_type
            WHEN 1 THEN 'Tenant'
            WHEN 2 THEN 'Broker'
            WHEN 3 THEN 'Landlord'
        END AS lead_client_type,
        lds.user_id AS user_id,
        lds.client_agent_id AS agent_id,
        lds.client_agent_by_api AS lead_agent_by_api, -- que es
        lds.kam_agent_id AS agent_kam_id,
        lds.spot_type_id AS spot_sector_id,
        CASE
            WHEN spot_type_id = 9 THEN 'Industrial'
            WHEN spot_type_id = 11 THEN 'Office'
            WHEN spot_type_id = 13 THEN 'Retail'
            WHEN spot_type_id = 15 THEN 'Land'
            ELSE NULL
        END AS spot_sector,
        lds.client_state AS lead_status_id,
        CASE lds.client_state
            WHEN 1 THEN "New Client"
            WHEN 2 THEN "Reactivated"
            WHEN 3 THEN "Open Requirement"
            WHEN 4 THEN "Sales Nurturing"
            WHEN 5 THEN "Stand By"
            WHEN 6 THEN "Residential Search"
            WHEN 7 THEN "Low Budget"
            WHEN 8 THEN "Off-Market Footage"
            WHEN 9 THEN "Other Reason"
            WHEN 10 THEN "Budget Not Met"
            WHEN 11 THEN "Footage Requirements Not Met"
            WHEN 12 THEN "No Offer"
            WHEN 13 THEN "Never Replied"
            WHEN 14 THEN "Stopped Replying"
            WHEN 15 THEN "No Longer Searching"
            WHEN 16 THEN "Other Nurturing"
            WHEN 17 THEN "Contact Attempt"
            WHEN 18 THEN "No Profile"
            WHEN 19 THEN "Spot Not Available"
            WHEN 20 THEN "Documentation Missing"
            WHEN 21 THEN "Visit Not Allowed"
            WHEN 22 THEN "Wrong Phone Number"
            WHEN 23 THEN "Duplicated Client"
        END AS lead_status,
        client_first_contact_time AS lead_first_contact_time,
        min_square_space AS spot_min_square_space,
        max_square_space AS spot_max_square_space,
        -- hs_analytics_source_data_1, -- que es
        hs_client_source AS lead_sourse_id,
        CASE lds.hs_client_source
            WHEN 1 THEN 'Created by Spot Agent'
            WHEN 2 THEN 'Paid social media'
            WHEN 3 THEN 'Platform'
            WHEN 4 THEN 'Trebel'
            WHEN 5 THEN 'Created by Supply Agent'
            WHEN 6 THEN 'Paid search'
        END AS lead_sourse,
        hs_first_conversion AS lead_hs_first_conversion,
        hs_utm_campaign AS lead_hs_utm_campaign,
        hs_analytics_source_data_1 AS lead_hs_analytics_source_data_1,
        hs_recent_conversion AS lead_hs_recent_conversion,
        hs_recent_conversion_date AS lead_hs_recent_conversion_date,
        hs_industrial_profiling AS lead_hs_industrial_profiling,
        JSON_UNQUOTE(JSON_EXTRACT(hs_industrial_profiling, '$[0].region')) AS lead_industrial_profile_state,
        JSON_UNQUOTE(JSON_EXTRACT(hs_industrial_profiling, '$[0].state')) AS lead_industrial_profile_zone,
        CASE JSON_UNQUOTE(JSON_EXTRACT(hs_industrial_profiling, '$[0].size'))
            WHEN '1' THEN 1
            WHEN '2' THEN 2
            WHEN '3' THEN 3
            WHEN '4' THEN 4
            WHEN '5' THEN 5
            WHEN '6' THEN 6
            WHEN '7' THEN 7
            WHEN '8' THEN 8
            WHEN '9' THEN 9
            WHEN '10' THEN 10
            ELSE NULL
        END AS lead_industrial_profile_size_id,
        CASE JSON_UNQUOTE(JSON_EXTRACT(hs_industrial_profiling, '$[0].size'))
            WHEN '1' THEN 'Less than 1,000m²'
            WHEN '2' THEN 'From 1,000m² to 3,000m²'
            WHEN '3' THEN 'From 3,000m² to 10,000m²'
            WHEN '4' THEN 'From 10,000m² to 20,000m²'
            WHEN '5' THEN 'More than 20,000m²'
            WHEN '6' THEN 'Less than 700m²'
            WHEN '7' THEN 'From 700m² to 2,000m²'
            WHEN '8' THEN 'From 2,000m² to 10,000m²'
            WHEN '9' THEN 'More than 100,000m²'
            WHEN '10' THEN 'Less than 10,000m²'
        END AS lead_industrial_profile_size,
        CASE JSON_UNQUOTE(JSON_EXTRACT(hs_industrial_profiling, '$[0].budget'))
            WHEN '1' THEN 1
            WHEN '2' THEN 2
            WHEN '3' THEN 3
            WHEN '4' THEN 4
            WHEN '5' THEN 5
            WHEN '6' THEN 6
            WHEN '7' THEN 7
            WHEN '8' THEN 8
            WHEN '9' THEN 9
            WHEN '10' THEN 10
            ELSE NULL
        END AS lead_industrial_profile_budget_id,
        CASE JSON_UNQUOTE(JSON_EXTRACT(hs_industrial_profiling, '$[0].budget'))
            WHEN '1' THEN 'Less than $100,000 MXN'
            WHEN '2' THEN 'From $100,000 to $300,000 MXN'
            WHEN '3' THEN 'From $300,000 to $1,000,000 MXN'
            WHEN '4' THEN 'More than $1,000,000 MXN'
            WHEN '5' THEN 'Less than $60,000 MXN'
            WHEN '6' THEN 'From $60,000 to $150,000 MXN'
            WHEN '7' THEN 'From $150,000 to $300,000 MXN'
            WHEN '8' THEN 'More than $300,000 MXN'
            WHEN '9' THEN 'Less than $30,000 MXN'
            WHEN '10' THEN 'From $30,000 to $100,000 MXN'
        END AS lead_industrial_profile_budget,
        hs_office_profiling AS lead_hs_office_profiling,
        JSON_UNQUOTE(JSON_EXTRACT(hs_office_profiling, '$[0].region')) AS lead_office_profile_state,
        JSON_UNQUOTE(JSON_EXTRACT(hs_office_profiling, '$[0].state')) AS lead_office_profile_zone,
        CASE JSON_UNQUOTE(JSON_EXTRACT(hs_office_profiling, '$[0].size'))
            WHEN '1' THEN 1
            WHEN '2' THEN 2
            WHEN '3' THEN 3
            WHEN '4' THEN 4
            WHEN '5' THEN 5
            WHEN '6' THEN 6
            ELSE NULL
        END AS lead_office_profile_size_id,
        CASE JSON_UNQUOTE(JSON_EXTRACT(hs_office_profiling, '$[0].size'))
            WHEN '1' THEN 'Less than 100m²'
            WHEN '2' THEN 'From 100 to 200m²'
            WHEN '3' THEN 'Less than 200m²'
            WHEN '4' THEN 'From 200 to 500m²'
            WHEN '5' THEN 'From 500 to 1000m²'
            WHEN '6' THEN 'More than 1000m²'
        END AS lead_office_profile_size,
        CASE JSON_UNQUOTE(JSON_EXTRACT(hs_office_profiling, '$[0].budget'))
            WHEN '1' THEN 1
            WHEN '2' THEN 2
            WHEN '3' THEN 3
            WHEN '4' THEN 4
            WHEN '5' THEN 5
            WHEN '6' THEN 6
            WHEN '7' THEN 7
            WHEN '8' THEN 8
            ELSE NULL
        END AS lead_office_profile_budget_id,
        CASE JSON_UNQUOTE(JSON_EXTRACT(hs_office_profiling, '$[0].budget'))
            WHEN '1' THEN 'Less than $25,000 MXN'
            WHEN '2' THEN 'From $25,000 to $75,000 MXN'
            WHEN '3' THEN 'From $75,000 to $150,000 MXN'
            WHEN '4' THEN 'More than $150,000 MXN'
            WHEN '5' THEN 'Less than $50,000 MXN'
            WHEN '6' THEN 'From $50,000 to $100,000 MXN'
            WHEN '7' THEN 'From $100,000 to $200,000 MXN'
            WHEN '8' THEN 'More than $200,000 MXN'
        END AS lead_office_profile_budget,
        (SELECT er FROM te) AS lead_dollar_exchange,
        (
            SELECT COUNT(*)
            FROM project_requirements
            WHERE lds.id = project_requirements.client_id
        ) AS lead_projects_count,
        lds.lead0_at AS lead_lead0_at,
        lds.lead1_at AS lead_lead1_at,
        lds.lead2_at AS lead_lead2_at,
        lds.lead3_at AS lead_lead3_at,
        lds.lead4_at AS lead_lead4_at,
        lds.supply_at AS lead_supply_at,
        lds.supply0_at AS lead_supply0_at,
        lds.supply1_at AS lead_supply1_at,
        lds.supply3_at AS lead_supply3_at,
        lds.client_at AS lead_client_at,
        lds.created_at AS lead_created_at,
        DATE(lds.created_at) AS lead_created_date,
        lds.updated_at AS lead_updated_at,
        DATE(lds.updated_at) AS lead_updated_date,
        lds.deleted_at AS lead_deleted_at,
        DATE(lds.deleted_at) AS lead_deleted_date,
        NOW() AS aud_inserted_at,
        CURDATE() AS aud_inserted_date,
        NOW() AS aud_updated_at,
        CURDATE() AS aud_updated_date,
        'stg_lk_leads' AS aud_job
    FROM clients AS lds
    ORDER BY
        lds.created_at DESC
)


SELECT 
    stg.*,
    CASE 
        WHEN stg.lead_created_at IS NOT NULL
            AND (stg.spot_sector_id IS NULL OR stg.spot_sector_id IN (13, 11, 9, 15))
            AND stg.lead_deleted_at IS NULL 
        THEN 1
        ELSE 0
    END AS filter
FROM stg_lk_leads AS stg
