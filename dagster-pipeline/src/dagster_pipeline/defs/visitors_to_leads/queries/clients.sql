WITH clients_with_min AS (
  SELECT
    *,
    NULLIF(LEAST(
      COALESCE(lead0_at, TIMESTAMP('9999-12-31')),
      COALESCE(lead1_at, TIMESTAMP('9999-12-31')),
      COALESCE(lead2_at, TIMESTAMP('9999-12-31')),
      COALESCE(lead3_at, TIMESTAMP('9999-12-31')),
      COALESCE(lead4_at, TIMESTAMP('9999-12-31'))
    ), TIMESTAMP('9999-12-31')) as lead_min_at
  FROM clients
)
SELECT
id as client_id,
name,
lastname,
created_at as created_at,
lead_min_at,
CASE
  WHEN lead_min_at = lead0_at THEN 'L0'
  WHEN lead_min_at = lead1_at THEN 'L1'
  WHEN lead_min_at = lead2_at THEN 'L2'
  WHEN lead_min_at = lead3_at THEN 'L3'
  WHEN lead_min_at = lead4_at THEN 'L4'
  ELSE NULL
END AS lead_min_type,
CASE
      WHEN lead4_at IS NOT NULL THEN 'L4'
      WHEN lead3_at IS NOT NULL THEN 'L3'
      WHEN lead2_at IS NOT NULL THEN 'L2'
      WHEN lead1_at IS NOT NULL THEN 'L1'
      WHEN lead0_at IS NOT NULL THEN 'L0'
      ELSE 'others'
    END AS lead_type, 
email as email,
phone_number,
phone_indicator,
phone_number_alt1,
phone_number_alt2,
hs_utm_campaign,
hs_utm_source,
hs_utm_medium,
hs_whatsapp_interaction,
traffic_type
FROM clients_with_min
