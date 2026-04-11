-- Query to fetch conversation messages from Chatbot
-- Placeholders:
--   {cte_where} - Conditions for filtering phones in date range
--   {main_where} - Conditions for filtering all messages

WITH conversations_with_messages_today AS (
    SELECT DISTINCT m.phone_number
    FROM messages m
    WHERE {cte_where}
)
SELECT 
    m.id, 
    m.phone_number, 
    m.content->'text'->>'body' AS message_body, 
    m.type, 
    to_char(m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City', 'YYYY-MM-DD HH24:MI:SS') AS created_at_utc,
    DATE(m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City') AS created_dt
FROM messages m
INNER JOIN conversations_with_messages_today c ON m.phone_number = c.phone_number
WHERE {main_where}
ORDER BY m.phone_number, m.created_at_utc ASC, m.id ASC;
