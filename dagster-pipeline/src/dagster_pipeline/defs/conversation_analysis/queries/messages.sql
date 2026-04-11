-- Query for fetching conversation messages from PostgreSQL (Chatbot DB)
-- Parameters: {start_date}, {end_date}, {previous_day_start}

WITH conversations_with_messages_today AS (
            SELECT DISTINCT m.phone_number
            FROM messages m
            WHERE m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' >= '{start_date}'
            AND m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' <= '{end_date}'
        )
        SELECT 
            m.conversation_id AS id, 
            m.phone_number, 
            CASE 
                -- Texto normal (WhatsApp / estructura estándar)
                WHEN m.content->'text'->>'body' IS NOT NULL 
                THEN m.content->'text'->>'body'

                -- content guardado como JSON string escalar ("texto plano" serializado en jsonb)
                WHEN jsonb_typeof(m.content) = 'string'
                THEN trim(both '"' from m.content::text)

                -- Objeto plano con body en la raíz (sin text anidado; clave body en root)
                WHEN m.content ? 'body'
                     AND NULLIF(trim(m.content->>'body'), '') IS NOT NULL
                THEN m.content->>'body'
                
                -- Mensaje interactivo del asistente con botones
                WHEN m.content->'interactive'->'action'->'buttons' IS NOT NULL 
                THEN CONCAT(
                    m.content->'interactive'->'body'->>'text',
                    E'\n\n[',
                    COALESCE(m.content->'interactive'->'action'->'buttons'->0->'reply'->>'title', ''),
                    CASE 
                        WHEN m.content->'interactive'->'action'->'buttons'->1->'reply'->>'title' IS NOT NULL 
                        THEN CONCAT(' o ', m.content->'interactive'->'action'->'buttons'->1->'reply'->>'title')
                        ELSE ''
                    END,
                    ']'
                )
                
                -- Mensaje interactivo del asistente sin botones
                WHEN m.content->'interactive'->'body'->>'text' IS NOT NULL 
                THEN m.content->'interactive'->'body'->>'text'
                
                -- Respuesta de botón del usuario
                WHEN m.content->'interactive'->'button_reply'->>'title' IS NOT NULL 
                THEN m.content->'interactive'->'button_reply'->>'title'
                
                ELSE NULL
            END AS message_body, 
            m.type, 
            to_char(m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City', 'YYYY-MM-DD HH24:MI:SS') AS created_at_utc,
            DATE(m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City') AS created_dt
        FROM messages m
        INNER JOIN conversations_with_messages_today c ON m.phone_number = c.phone_number
        WHERE m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' >= '{previous_day_start}'
        AND m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City' <= '{end_date}'
        ORDER BY m.phone_number, m.created_at_utc ASC, m.id ASC
