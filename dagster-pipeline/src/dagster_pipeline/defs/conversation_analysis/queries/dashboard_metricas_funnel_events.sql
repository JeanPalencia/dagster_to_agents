-- Dashboard métricas: periodos MTD / WTD usando conv_variables y funnel_events (spot_link_to_spot_conf, spot_conf_to_request_visit, visit_request_to_schedule, reco_index).
WITH period_bounds AS (
    -- Fila para Month To Date
    SELECT
        'Month To Date' AS periodo_tipo,
        date_trunc('month', current_date)::date AS cur_start,
        current_date AS cur_end,
        date_trunc('month', current_date - interval '1 month')::date AS prev_start,
        (current_date - interval '1 month')::date AS prev_end
    UNION ALL
    -- Fila para Week To Date
    SELECT
        'Week To Date' AS periodo_tipo,
        (date_trunc('week', current_date + interval '1 day') - interval '1 day')::date AS cur_start,
        current_date AS cur_end,
        (date_trunc('week', (current_date - interval '1 week') + interval '1 day') - interval '1 day')::date AS prev_start,
        (current_date - interval '1 week')::date AS prev_end
),
base_pivotada AS (
    SELECT
        cv.conv_id,
        cv.conv_start_date::date AS fecha,
        MAX(CASE WHEN vars->>'conv_variable_name' = 'is_broker' AND vars->>'conv_variable_value' IN ('true', '1') THEN 1 ELSE 0 END) AS is_broker,
        MAX(CASE WHEN vars->>'conv_variable_name' = 'scaling_agent' AND vars->>'conv_variable_value' IN ('true', '1') THEN 1 ELSE 0 END) AS scaling_agent,
        MAX(CASE WHEN vars->>'conv_variable_name' = 'scheduled_visit' AND (vars->>'conv_variable_value')::int > 0 THEN 1 ELSE 0 END) AS scheduled_visit,
        MAX(CASE WHEN vars->>'conv_variable_category' = 'score' AND vars->>'conv_variable_name' LIKE '%failure_cause%' AND vars->>'conv_variable_value' > '' AND vars->>'conv_variable_value' <> 'user_abandonment' THEN 1 ELSE 0 END) AS tiene_friccion,
        MAX(CASE WHEN vars->>'conv_variable_name' = 'unification_mgs' THEN vars->>'conv_variable_value' END) AS unification_msg_test,
        -- funnel_events (nuevas variables)
        MAX(CASE WHEN vars->>'conv_variable_category' = 'funnel_events' AND vars->>'conv_variable_name' = 'spot_link_to_spot_conf' THEN (vars->>'conv_variable_value')::int END) AS spot_link_to_spot_conf,
        MAX(CASE WHEN vars->>'conv_variable_category' = 'funnel_events' AND vars->>'conv_variable_name' = 'spot_conf_to_request_visit' THEN (vars->>'conv_variable_value')::int END) AS spot_conf_to_request_visit,
        MAX(CASE WHEN vars->>'conv_variable_category' = 'funnel_events' AND vars->>'conv_variable_name' = 'visit_request_to_schedule' THEN (vars->>'conv_variable_value')::int END) AS visit_request_to_schedule,
        MAX(CASE WHEN vars->>'conv_variable_category' = 'funnel_events' AND vars->>'conv_variable_name' = 'reco_index' THEN (vars->>'conv_variable_value')::int END) AS reco_index
    FROM bt_conv_conversations cv
    CROSS JOIN LATERAL jsonb_array_elements(cv.conv_variables::jsonb) AS vars
    WHERE cv.conv_start_date >= date_trunc('month', current_date - interval '1 month')
    GROUP BY 1, 2
),
metricas_finales AS (
    SELECT
        fecha,
        conv_id,
        is_broker,
        scaling_agent,
        tiene_friccion,
        scheduled_visit,
        -- es_spot: 1 si llegó a spot_conf (o combined); usa variable funnel_events.spot_link_to_spot_conf
        CASE WHEN spot_link_to_spot_conf = 1 THEN 1 ELSE 0 END AS es_spot,
        -- es_visit_request: 1 si (spot_conf o combined) y luego explicit_visit_request
        CASE WHEN spot_conf_to_request_visit = 1 THEN 1 ELSE 0 END AS es_visit_request,
        -- es_visit: 1 si explicit_visit_request y luego visit_scheduled o scheduled_visit > 0
        CASE WHEN visit_request_to_schedule = 1 THEN 1 ELSE 0 END AS es_visit,
        -- es_reco_sent: 1 si tuvieron recommendations_sent (reco_index presente = 0 o 1)
        CASE WHEN reco_index IS NOT NULL THEN 1 ELSE 0 END AS es_reco_sent,
        -- es_reco_scheduling: 1 si recommendations_sent y luego recommendation_scheduling
        CASE WHEN reco_index = 1 THEN 1 ELSE 0 END AS es_reco_scheduling
    FROM base_pivotada
)
SELECT
    pb.periodo_tipo,
    current_date AS fecha_referencia,

    /* TOTAL CONVERSATIONS */
    COUNT(DISTINCT CASE WHEN m.fecha BETWEEN pb.cur_start AND pb.cur_end THEN m.conv_id END)::integer AS total_current,
    COUNT(DISTINCT CASE WHEN m.fecha BETWEEN pb.prev_start AND pb.prev_end THEN m.conv_id END)::integer AS total_previous,

    /* BROKERS */
    SUM(CASE WHEN m.fecha BETWEEN pb.cur_start AND pb.cur_end AND m.is_broker = 1 THEN 1 ELSE 0 END)::integer AS broker_current,
    SUM(CASE WHEN m.fecha BETWEEN pb.prev_start AND pb.prev_end AND m.is_broker = 1 THEN 1 ELSE 0 END)::integer AS broker_previous,

    /* FRICCION */
    SUM(CASE WHEN m.fecha BETWEEN pb.cur_start AND pb.cur_end AND m.tiene_friccion = 1 THEN 1 ELSE 0 END)::integer AS friccion_current,
    SUM(CASE WHEN m.fecha BETWEEN pb.prev_start AND pb.prev_end AND m.tiene_friccion = 1 THEN 1 ELSE 0 END)::integer AS friccion_previous,

    /* Visit Request */
    SUM(CASE WHEN m.fecha BETWEEN pb.cur_start AND pb.cur_end AND m.es_visit_request = 1 THEN 1 ELSE 0 END)::integer AS es_visit_request_current,
    SUM(CASE WHEN m.fecha BETWEEN pb.prev_start AND pb.prev_end AND m.es_visit_request = 1 THEN 1 ELSE 0 END)::integer AS es_visit_request_previous,

    /* VISITS */
    SUM(CASE WHEN m.fecha BETWEEN pb.cur_start AND pb.cur_end AND m.es_visit = 1 THEN 1 ELSE 0 END)::integer AS visit_current,
    SUM(CASE WHEN m.fecha BETWEEN pb.prev_start AND pb.prev_end AND m.es_visit = 1 THEN 1 ELSE 0 END)::integer AS visit_previous,

    /* SPOT */
    SUM(CASE WHEN m.fecha BETWEEN pb.cur_start AND pb.cur_end AND m.es_spot = 1 THEN 1 ELSE 0 END)::integer AS spot_current,
    SUM(CASE WHEN m.fecha BETWEEN pb.prev_start AND pb.prev_end AND m.es_spot = 1 THEN 1 ELSE 0 END)::integer AS spot_previous,

    /* Reco Sent */
    SUM(CASE WHEN m.fecha BETWEEN pb.cur_start AND pb.cur_end AND m.es_reco_sent = 1 THEN 1 ELSE 0 END)::integer AS reco_sent_current,
    SUM(CASE WHEN m.fecha BETWEEN pb.prev_start AND pb.prev_end AND m.es_reco_sent = 1 THEN 1 ELSE 0 END)::integer AS reco_sent_previous,

    /* Reco Scheduling */
    SUM(CASE WHEN m.fecha BETWEEN pb.cur_start AND pb.cur_end AND m.es_reco_scheduling = 1 THEN 1 ELSE 0 END)::integer AS reco_scheduling_current,
    SUM(CASE WHEN m.fecha BETWEEN pb.prev_start AND pb.prev_end AND m.es_reco_scheduling = 1 THEN 1 ELSE 0 END)::integer AS reco_scheduling_previous,

    /* Total Visit (scheduled_visit tag) */
    SUM(CASE WHEN m.fecha BETWEEN pb.cur_start AND pb.cur_end AND m.scheduled_visit = 1 THEN 1 ELSE 0 END)::integer AS total_visit_current,
    SUM(CASE WHEN m.fecha BETWEEN pb.prev_start AND pb.prev_end AND m.scheduled_visit = 1 THEN 1 ELSE 0 END)::integer AS total_visit_previous

FROM metricas_finales m
CROSS JOIN period_bounds pb
GROUP BY 1, 2;
