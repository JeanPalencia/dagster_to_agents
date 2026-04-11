-- Pasos secuenciales del funnel por fecha y lead_type, usando funnel_events (sin funnel_raw).
-- {{fecha_filtro}}: placeholder para filtro de fecha (ej. cv.conv_start_date BETWEEN '...' AND '...').
WITH base_pivotada AS (
    SELECT
        cv.conv_id,
        date(cv.conv_start_date) AS fecha,
        CASE
            WHEN ld.lead_lead3_at <= cv.conv_end_date THEN 'L3'
            WHEN ld.lead_lead2_at <= cv.conv_end_date THEN 'L2'
            WHEN ld.lead_lead1_at <= cv.conv_end_date THEN 'L1'
            WHEN ld.lead_lead0_at <= cv.conv_end_date THEN 'L0'
            ELSE 'No Registered'
        END AS lead_type,
        MAX(CASE WHEN vars->>'conv_variable_name' = 'unification_mgs' THEN vars->>'conv_variable_value' END) AS unification_msg_test,
        MAX(CASE WHEN vars->>'conv_variable_name' = 'visit_intention' THEN vars->>'conv_variable_value' END) AS visit_intention,
        MAX(CASE WHEN vars->>'conv_variable_name' = 'scheduled_visit' AND (vars->>'conv_variable_value')::int > 0 THEN 1 ELSE 0 END) AS scheduled_visit,
        -- funnel_events
        MAX(CASE WHEN vars->>'conv_variable_category' = 'funnel_events' AND vars->>'conv_variable_name' = 'spot_link_to_spot_conf' THEN (vars->>'conv_variable_value')::int END) AS spot_link_to_spot_conf,
        MAX(CASE WHEN vars->>'conv_variable_category' = 'funnel_events' AND vars->>'conv_variable_name' = 'spot_conf_to_request_visit' THEN (vars->>'conv_variable_value')::int END) AS spot_conf_to_request_visit,
        MAX(CASE WHEN vars->>'conv_variable_category' = 'funnel_events' AND vars->>'conv_variable_name' = 'visit_request_to_schedule' THEN (vars->>'conv_variable_value')::int END) AS visit_request_to_schedule,
        MAX(CASE WHEN vars->>'conv_variable_category' = 'funnel_events' AND vars->>'conv_variable_name' = 'schedule_form_index' THEN (vars->>'conv_variable_value')::int END) AS schedule_form_index,
        MAX(CASE WHEN vars->>'conv_variable_category' = 'funnel_events' AND vars->>'conv_variable_name' = 'reco_index' THEN (vars->>'conv_variable_value')::int END) AS reco_index
    FROM bt_conv_conversations cv
    LEFT JOIN lk_leads ld ON cv.lead_id::text = ld.lead_id::text
    CROSS JOIN LATERAL jsonb_array_elements(cv.conv_variables::jsonb) AS vars
    WHERE {{fecha_filtro}}
      AND cv.conv_start_date >= '2026-01-01'
    GROUP BY 1, 2, cv.conv_end_date, ld.lead_lead3_at, ld.lead_lead2_at, ld.lead_lead1_at, ld.lead_lead0_at
),
pasos_secuenciales AS (
    SELECT
        conv_id,
        fecha,
        lead_type,
        -- 1. Base (toda conversación)
        1 AS p1,
        -- 1.2. Spot link: en funnel (spot_link_received o combined); variable solo existe en ese caso
        CASE WHEN spot_link_to_spot_conf IS NOT NULL THEN 1 ELSE 0 END AS p1_2,
        -- 2. Spot Conf: spot_link -> spot_conf o combined
        CASE WHEN spot_link_to_spot_conf = 1 THEN 1 ELSE 0 END AS p2,
        -- 3. Visit Request: (spot_conf o combined) y luego explicit_visit_request
        CASE WHEN spot_conf_to_request_visit = 1 THEN 1 ELSE 0 END AS p3,
        -- 4. Scheduling Form Sent: P3 + scheduling_form_sent (schedule_form_index presente)
        CASE WHEN spot_conf_to_request_visit = 1 AND schedule_form_index IS NOT NULL THEN 1 ELSE 0 END AS p4,
        -- 5. Scheduling Completed: explicit_visit_request y (visit_scheduled o scheduled_visit > 0)
        CASE WHEN visit_request_to_schedule = 1 THEN 1 ELSE 0 END AS p5,
        -- 6. Reco Sent: P5 + recommendations_sent (reco_index presente)
        CASE WHEN visit_request_to_schedule = 1 AND schedule_form_index IS NOT NULL AND reco_index IS NOT NULL THEN 1 ELSE 0 END AS p6,
        -- 7. Reco Scheduling: P6 + recommendation_scheduling (reco_index = 1)
        CASE WHEN visit_request_to_schedule = 1 AND schedule_form_index IS NOT NULL AND reco_index = 1 THEN 1 ELSE 0 END AS p7
    FROM base_pivotada
),
unpivot AS (
    SELECT fecha, lead_type, 1 AS orden, '1. Conv. Start' AS etapa, p1 AS cumplio FROM pasos_secuenciales
    UNION ALL
    SELECT fecha, lead_type, 2, '1.2. Spot link.', p1_2 FROM pasos_secuenciales
    UNION ALL
    SELECT fecha, lead_type, 3, '2. Spot Conf.', p2 FROM pasos_secuenciales
    UNION ALL
    SELECT fecha, lead_type, 4, '3. Visit Request', p3 FROM pasos_secuenciales
    UNION ALL
    SELECT fecha, lead_type, 5, '4. Scheduling Form Sent', p4 FROM pasos_secuenciales
    UNION ALL
    SELECT fecha, lead_type, 6, '5. Scheduling Completed', p5 FROM pasos_secuenciales
    UNION ALL
    SELECT fecha, lead_type, 7, '6. Reco. Sent', p6 FROM pasos_secuenciales
    UNION ALL
    SELECT fecha, lead_type, 8, '7. Reco. Scheduling.', p7 FROM pasos_secuenciales
)
SELECT
    fecha,
    lead_type,
    orden,
    etapa,
    SUM(cumplio) AS usuarios
FROM unpivot
GROUP BY 1, 2, 3, 4
ORDER BY fecha DESC, orden ASC;
