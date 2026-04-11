-- Query for fetching conversation events from Staging database
-- Parameters: {previous_day_start}, {end_date} (misma lógica de fechas que messages.sql)
SELECT
    conversation_id,
    contact_id,
    client_id AS lead_id,
    event_type,
    event_section,
    event_data,
    to_char(
        created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City',
        'YYYY-MM-DD HH24:MI:SS'
    ) AS created_at_local
FROM conversation_events
WHERE created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' >= '{previous_day_start}'
  AND created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' <= '{end_date}'
ORDER BY conversation_id, created_at_local ASC
