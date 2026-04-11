WITH current_counts AS (
	WITH leads_base AS (
	    SELECT
	        l.lead_id,
	        LEAST(
	            COALESCE(l.lead_lead0_at::timestamp, TIMESTAMP '9999-12-31'),
	            COALESCE(l.lead_lead1_at::timestamp, TIMESTAMP '9999-12-31'),
	            COALESCE(l.lead_lead2_at::timestamp, TIMESTAMP '9999-12-31'),
	            COALESCE(l.lead_lead3_at::timestamp, TIMESTAMP '9999-12-31'),
	            COALESCE(l.lead_lead4_at::timestamp, TIMESTAMP '9999-12-31')
	        ) AS primera_fecha_lead
	    FROM lk_leads_v2 l
	    WHERE (l.lead_l0 OR l.lead_l1 OR l.lead_l2 OR l.lead_l3 OR l.lead_l4)
	      AND (l.lead_domain NOT IN ('spot2.mx') OR l.lead_domain IS NULL)
	      AND l.lead_deleted_at IS NULL
	),
	leads_base_filt AS (
	    SELECT
	        lead_id,
	        primera_fecha_lead,
	        (primera_fecha_lead::date) AS lead_first_date
	    FROM leads_base
	    WHERE primera_fecha_lead >= TIMESTAMP '2021-01-01'
	      AND primera_fecha_lead < CURRENT_DATE
	      AND primera_fecha_lead < TIMESTAMP '9999-12-31'
	),

	projects_base AS (
	    SELECT
	        lb.lead_id,
	        p.project_id,
	        p.project_created_at,
	        p.project_funnel_visit_created_date AS visita_solicitada_at,   -- date
	        p.project_funnel_visit_confirmed_at AS visita_confirmada_at,   -- timestamp (appointment date)
	        p.project_funnel_visit_realized_at  AS visita_realizada_at,    -- timestamp
	        p.project_funnel_loi_date,
	        p.project_won_date,

	        /* Cohort-date (solo para el modo Cohort) */
	        (
	            CASE
	                WHEN p.project_created_at IS NOT NULL
	                 AND p.project_created_at >= lb.primera_fecha_lead + INTERVAL '30 days'
	                    THEN p.project_created_at
	                ELSE lb.primera_fecha_lead
	            END
	        )::date AS cohort_date_real
	    FROM leads_base_filt lb
	    JOIN lk_projects_v2 p USING (lead_id)
	    WHERE p.project_id IS NOT NULL
	),

	ptd_windows AS (
	    SELECT
	        (CURRENT_DATE - INTERVAL '1 day')::date AS as_of_date,
	        'Month to Date'::text  AS ptd_type,
	        gs.month_start::date AS period_start,
	        CASE
	            WHEN gs.month_start = DATE_TRUNC('month', (CURRENT_DATE - INTERVAL '1 day'))::date
	            THEN (CURRENT_DATE - INTERVAL '1 day')::date
	            ELSE (gs.month_start + INTERVAL '1 month - 1 day')::date
	        END AS period_end
	    FROM generate_series(
	        DATE_TRUNC('year', (CURRENT_DATE - INTERVAL '1 day'))::date,
	        DATE_TRUNC('month', (CURRENT_DATE - INTERVAL '1 day'))::date,
	        INTERVAL '1 month'
	    ) AS gs(month_start)
	    UNION ALL
	    SELECT
	        (CURRENT_DATE - INTERVAL '1 day')::date AS as_of_date,
	        'Quarter to Date'::text AS ptd_type,
	        DATE_TRUNC('quarter', (CURRENT_DATE - INTERVAL '1 day'))::date AS period_start,
	        (CURRENT_DATE - INTERVAL '1 day')::date                         AS period_end
	),

	cohort_types AS (
	    SELECT 'New'::text AS cohort_type
	    UNION ALL
	    SELECT 'Reactivated'::text
	),

	/* ============================================================
	   TARGETS PTD (a la fecha): suma por mes * fracción
	   ============================================================ */
	ptd_months AS (
	    SELECT
	        w.as_of_date,
	        w.ptd_type,
	        w.period_start,
	        w.period_end,
	        m.month_start::date AS month_start,
	        (m.month_start + INTERVAL '1 month - 1 day')::date AS month_end,
	        EXTRACT(DAY FROM (m.month_start + INTERVAL '1 month - 1 day'))::int AS days_in_month,
	        (LEAST((m.month_start + INTERVAL '1 month - 1 day')::date, w.period_end) - m.month_start::date + 1)::int AS days_elapsed,
	        (
	          (LEAST((m.month_start + INTERVAL '1 month - 1 day')::date, w.period_end) - m.month_start::date + 1)::numeric
	          / NULLIF(EXTRACT(DAY FROM (m.month_start + INTERVAL '1 month - 1 day'))::numeric, 0)
	        ) AS month_fraction
	    FROM ptd_windows w
	    CROSS JOIN LATERAL generate_series(
	        DATE_TRUNC('month', w.period_start)::date,
	        DATE_TRUNC('month', w.period_end)::date,
	        INTERVAL '1 month'
	    ) AS m(month_start)
	),
	ptd_targets AS (
	    SELECT
	        pm.as_of_date,
	        pm.ptd_type,
	        pm.period_start,
	        pm.period_end,
	        SUM(COALESCE(o.okr_leads, 0)            * pm.month_fraction) AS leads_ptd,
	        SUM(COALESCE(o.okr_projects, 0)         * pm.month_fraction) AS projects_ptd,
	        SUM(COALESCE(o.okr_scheduled_visits, 0) * pm.month_fraction) AS scheduled_visits_ptd,
	        SUM(COALESCE(o.okr_confirmed_visits, 0) * pm.month_fraction) AS confirmed_visits_ptd,
	        SUM(COALESCE(o.okr_completed_visits, 0) * pm.month_fraction) AS completed_visits_ptd,
	        SUM(COALESCE(o.okr_lois, 0)              * pm.month_fraction) AS lois_ptd
	    FROM ptd_months pm
	    LEFT JOIN lk_okrs o
	      ON o.okr_month_start_ts::date = pm.month_start
	    GROUP BY 1,2,3,4
	),

	/* ============================================================
	   OKR TOTAL del periodo:
	   - MTD: okr del mes completo
	   - QTD: suma de los 3 meses del quarter completo
	   ============================================================ */
	okr_total_months AS (
	    SELECT
	        w.as_of_date,
	        w.ptd_type,
	        w.period_start,
	        w.period_end,
	        gs.month_start::date AS month_start
	    FROM ptd_windows w
	    CROSS JOIN LATERAL generate_series(
	        DATE_TRUNC('month', w.period_start)::date,
	        CASE
	          WHEN w.ptd_type = 'Quarter to Date'
	            THEN (DATE_TRUNC('month', w.period_start)::date + INTERVAL '2 months')::date
	          ELSE DATE_TRUNC('month', w.period_start)::date
	        END,
	        INTERVAL '1 month'
	    ) AS gs(month_start)
	),
	okr_totals AS (
	    SELECT
	        tm.as_of_date,
	        tm.ptd_type,
	        tm.period_start,
	        tm.period_end,
	        SUM(COALESCE(o.okr_leads, 0))            AS okr_leads_total,
	        SUM(COALESCE(o.okr_projects, 0))         AS okr_projects_total,
	        SUM(COALESCE(o.okr_scheduled_visits, 0)) AS okr_scheduled_visits_total,
	        SUM(COALESCE(o.okr_confirmed_visits, 0)) AS okr_confirmed_visits_total,
	        SUM(COALESCE(o.okr_completed_visits, 0)) AS okr_completed_visits_total,
	        SUM(COALESCE(o.okr_lois, 0))             AS okr_lois_total
	    FROM okr_total_months tm
	    LEFT JOIN lk_okrs o
	      ON o.okr_month_start_ts::date = tm.month_start
	    GROUP BY 1,2,3,4
	),

	/* ============================================================
	   METRICS: COHORT (tal cual pulida)
	   ============================================================ */

	/* cohort “candidatos” por lead: su fecha inicial + cohort_date de proyectos */
	lead_cohort_rows AS (
	    SELECT
	        lb.lead_id,
	        lb.lead_first_date,
	        lb.lead_first_date AS cohort_date_real
	    FROM leads_base_filt lb
	    UNION ALL
	    SELECT
	        p.lead_id,
	        lb.lead_first_date,
	        p.cohort_date_real
	    FROM projects_base p
	    JOIN leads_base_filt lb USING (lead_id)
	),

	/* Normalización aditiva por ventana: cada lead cuenta 1 vez (MIN cohort_date_real dentro del periodo) */
	lead_assign AS (
	    SELECT
	        w.as_of_date,
	        w.ptd_type,
	        w.period_start,
	        w.period_end,
	        r.lead_id,
	        r.lead_first_date,
	        MIN(r.cohort_date_real) AS assigned_cohort_date
	    FROM ptd_windows w
	    JOIN lead_cohort_rows r
	      ON r.cohort_date_real BETWEEN w.period_start AND w.period_end
	    GROUP BY 1,2,3,4,5,6
	),
	cohort_leads_current AS (
	    SELECT
	        la.as_of_date,
	        la.ptd_type,
	        la.period_start,
	        la.period_end,
	        CASE
	            WHEN la.assigned_cohort_date = la.lead_first_date THEN 'New'
	            ELSE 'Reactivated'
	        END AS cohort_type,
	        COUNT(*) AS leads_current
	    FROM lead_assign la
	    GROUP BY 1,2,3,4,5
	),

	cohort_projects_current AS (
	  SELECT
	    w.as_of_date,
	    w.ptd_type,
	    w.period_start,
	    w.period_end,
	    CASE
	      WHEN p.cohort_date_real = lb.lead_first_date THEN 'New'
	      ELSE 'Reactivated'
	    END AS cohort_type,

	    /* ✅ proyectos creados hasta AYER (evita meter hoy) */
	    COUNT(DISTINCT CASE
	      WHEN p.project_created_at IS NOT NULL
	       AND p.project_created_at::date <= w.as_of_date
	      THEN p.project_id
	    END) AS projects_current,

	    /* ✅ acumulados hasta AYER */
	    COUNT(DISTINCT CASE
	      WHEN p.visita_solicitada_at IS NOT NULL
	       AND p.visita_solicitada_at <= w.as_of_date
	      THEN p.project_id
	    END) AS scheduled_visits_current,

	    /* ✅ confirmadas (Cohort ref):
	       - pertenencia al periodo: por cohort_date_real (ya lo hace el JOIN)
	       - foto al corte: por ancla de "ya estaba agendada al corte"
	       - NO acotar por visita_confirmada_at (es fecha de cita)
	    */
	    COUNT(DISTINCT CASE
	      WHEN p.visita_solicitada_at IS NOT NULL
	       AND p.visita_solicitada_at <= w.as_of_date
	       AND p.visita_confirmada_at IS NOT NULL
	      THEN p.project_id
	    END) AS confirmed_visits_current,

	    COUNT(DISTINCT CASE
	      WHEN p.visita_realizada_at IS NOT NULL
	       AND p.visita_realizada_at::date <= w.as_of_date
	      THEN p.project_id
	    END) AS completed_visits_current,

	    COUNT(DISTINCT CASE
	      WHEN p.project_funnel_loi_date IS NOT NULL
	       AND p.project_funnel_loi_date::date <= w.as_of_date
	      THEN p.project_id
	    END) AS lois_current,

	    COUNT(DISTINCT CASE
	      WHEN p.project_won_date IS NOT NULL
	       AND p.project_won_date::date <= w.as_of_date
	      THEN p.project_id
	    END) AS won_current

	  FROM ptd_windows w
	  JOIN projects_base p
	    ON p.cohort_date_real BETWEEN w.period_start AND w.period_end
	  JOIN leads_base_filt lb
	    ON lb.lead_id = p.lead_id
	  GROUP BY 1,2,3,4,5
	),

	cohort_output AS (
	    SELECT
	        'Cohort'::text AS counting_method,
	        w.as_of_date,
	        w.ptd_type,
	        w.period_start,
	        w.period_end,
	        ct.cohort_type,

	        COALESCE(lc.leads_current, 0) AS leads_current,

	        COALESCE(pc.projects_current, 0)         AS projects_current,
	        COALESCE(pc.scheduled_visits_current, 0) AS scheduled_visits_current,
	        COALESCE(pc.confirmed_visits_current, 0) AS confirmed_visits_current,
	        COALESCE(pc.completed_visits_current, 0) AS completed_visits_current,
	        COALESCE(pc.lois_current, 0)             AS lois_current,
	        COALESCE(pc.won_current, 0)              AS won_current,

	        /* PTD */
	        COALESCE(pt.leads_ptd, 0)            AS leads_ptd,
	        COALESCE(pt.projects_ptd, 0)         AS projects_ptd,
	        COALESCE(pt.scheduled_visits_ptd, 0) AS scheduled_visits_ptd,
	        COALESCE(pt.confirmed_visits_ptd, 0) AS confirmed_visits_ptd,
	        COALESCE(pt.completed_visits_ptd, 0) AS completed_visits_ptd,
	        COALESCE(pt.lois_ptd, 0)             AS lois_ptd,

	        /* Total OKR */
	        COALESCE(ot.okr_leads_total, 0)            AS okr_leads_total,
	        COALESCE(ot.okr_projects_total, 0)         AS okr_projects_total,
	        COALESCE(ot.okr_scheduled_visits_total, 0) AS okr_scheduled_visits_total,
	        COALESCE(ot.okr_confirmed_visits_total, 0) AS okr_confirmed_visits_total,
	        COALESCE(ot.okr_completed_visits_total, 0) AS okr_completed_visits_total,
	        COALESCE(ot.okr_lois_total, 0)             AS okr_lois_total

	    FROM ptd_windows w
	    CROSS JOIN cohort_types ct
	    LEFT JOIN cohort_leads_current lc
	      ON lc.as_of_date   = w.as_of_date
	     AND lc.ptd_type     = w.ptd_type
	     AND lc.period_start = w.period_start
	     AND lc.period_end   = w.period_end
	     AND lc.cohort_type  = ct.cohort_type
	    LEFT JOIN cohort_projects_current pc
	      ON pc.as_of_date   = w.as_of_date
	     AND pc.ptd_type     = w.ptd_type
	     AND pc.period_start = w.period_start
	     AND pc.period_end   = w.period_end
	     AND pc.cohort_type  = ct.cohort_type
	    LEFT JOIN ptd_targets pt
	      ON pt.as_of_date   = w.as_of_date
	     AND pt.ptd_type     = w.ptd_type
	     AND pt.period_start = w.period_start
	     AND pt.period_end   = w.period_end
	    LEFT JOIN okr_totals ot
	      ON ot.as_of_date   = w.as_of_date
	     AND ot.ptd_type     = w.ptd_type
	     AND ot.period_start = w.period_start
	     AND ot.period_end   = w.period_end
	),

	/* ============================================================
	   METRICS: ROLLING (conteo por fecha propia de cada evento)
	   -> No New/Reactivated distinction: everything falls under 'New'
	   ============================================================ */

	rolling_leads_current AS (
	    /* Rolling: leads por lead_first_date dentro del periodo -> todo 'New' */
	    SELECT
	        w.as_of_date,
	        w.ptd_type,
	        w.period_start,
	        w.period_end,
	        'New'::text AS cohort_type,
	        COUNT(DISTINCT lb.lead_id) AS leads_current
	    FROM ptd_windows w
	    JOIN leads_base_filt lb
	      ON lb.lead_first_date BETWEEN w.period_start AND w.period_end
	    GROUP BY 1,2,3,4,5
	),

	rolling_projects_current AS (
	    /* Rolling: cada métrica cuenta por su propia fecha dentro del periodo -> todo 'New' */
	    SELECT
	        w.as_of_date,
	        w.ptd_type,
	        w.period_start,
	        w.period_end,
	        'New'::text AS cohort_type,

	        COUNT(DISTINCT CASE
	          WHEN p.project_created_at IS NOT NULL
	           AND p.project_created_at::date BETWEEN w.period_start AND w.period_end
	            THEN p.project_id
	        END) AS projects_current,

	        COUNT(DISTINCT CASE
	          WHEN p.visita_solicitada_at IS NOT NULL
	           AND p.visita_solicitada_at BETWEEN w.period_start AND w.period_end
	            THEN p.project_id
	        END) AS scheduled_visits_current,

	        COUNT(DISTINCT CASE
	          /* ✅ confirmadas (Rolling ref):
	             - ancla = visita_confirmada_at::date (como referencia rolling)
	             - cota superior = fin de periodo (mes/quarter), no ayer
	          */
	          WHEN p.visita_confirmada_at IS NOT NULL
	           AND p.visita_confirmada_at::date BETWEEN w.period_start AND
	               CASE
	                 WHEN w.ptd_type = 'Month to Date'
	                   THEN (DATE_TRUNC('month', w.period_start)::date + INTERVAL '1 month - 1 day')::date
	                 WHEN w.ptd_type = 'Quarter to Date'
	                   THEN (DATE_TRUNC('quarter', w.period_start)::date + INTERVAL '3 month - 1 day')::date
	                 ELSE w.period_end
	               END
	          THEN p.project_id
	        END) AS confirmed_visits_current,

	        COUNT(DISTINCT CASE
	          WHEN p.visita_realizada_at IS NOT NULL
	           AND p.visita_realizada_at::date BETWEEN w.period_start AND w.period_end
	            THEN p.project_id
	        END) AS completed_visits_current,

	        COUNT(DISTINCT CASE
	          WHEN p.project_funnel_loi_date IS NOT NULL
	           AND p.project_funnel_loi_date BETWEEN w.period_start AND w.period_end
	            THEN p.project_id
	        END) AS lois_current,

	        COUNT(DISTINCT CASE
	          WHEN p.project_won_date IS NOT NULL
	           AND p.project_won_date BETWEEN w.period_start AND w.period_end
	            THEN p.project_id
	        END) AS won_current

	    FROM ptd_windows w
	    JOIN projects_base p
	      ON (
	            (p.project_created_at IS NOT NULL AND p.project_created_at::date BETWEEN w.period_start AND w.period_end)
	         OR (p.visita_solicitada_at IS NOT NULL AND p.visita_solicitada_at BETWEEN w.period_start AND w.period_end)
	         /* ✅ expandimos SOLO el gating de confirmadas al fin de periodo */
	         OR (p.visita_confirmada_at IS NOT NULL AND p.visita_confirmada_at::date BETWEEN w.period_start AND
	               CASE
	                 WHEN w.ptd_type = 'Month to Date'
	                   THEN (DATE_TRUNC('month', w.period_start)::date + INTERVAL '1 month - 1 day')::date
	                 WHEN w.ptd_type = 'Quarter to Date'
	                   THEN (DATE_TRUNC('quarter', w.period_start)::date + INTERVAL '3 month - 1 day')::date
	                 ELSE w.period_end
	               END
	            )
	         OR (p.visita_realizada_at IS NOT NULL AND p.visita_realizada_at::date BETWEEN w.period_start AND w.period_end)
	         OR (p.project_funnel_loi_date IS NOT NULL AND p.project_funnel_loi_date BETWEEN w.period_start AND w.period_end)
	         OR (p.project_won_date IS NOT NULL AND p.project_won_date BETWEEN w.period_start AND w.period_end)
	         )
	    GROUP BY 1,2,3,4,5
	),

	rolling_output AS (
	    SELECT
	        'Rolling'::text AS counting_method,
	        w.as_of_date,
	        w.ptd_type,
	        w.period_start,
	        w.period_end,
	        ct.cohort_type,

	        COALESCE(rl.leads_current, 0) AS leads_current,

	        COALESCE(rp.projects_current, 0)         AS projects_current,
	        COALESCE(rp.scheduled_visits_current, 0) AS scheduled_visits_current,
	        COALESCE(rp.confirmed_visits_current, 0) AS confirmed_visits_current,
	        COALESCE(rp.completed_visits_current, 0) AS completed_visits_current,
	        COALESCE(rp.lois_current, 0)             AS lois_current,
	        COALESCE(rp.won_current, 0)              AS won_current,

	        /* PTD */
	        COALESCE(pt.leads_ptd, 0)            AS leads_ptd,
	        COALESCE(pt.projects_ptd, 0)         AS projects_ptd,
	        COALESCE(pt.scheduled_visits_ptd, 0) AS scheduled_visits_ptd,
	        COALESCE(pt.confirmed_visits_ptd, 0) AS confirmed_visits_ptd,
	        COALESCE(pt.completed_visits_ptd, 0) AS completed_visits_ptd,
	        COALESCE(pt.lois_ptd, 0)             AS lois_ptd,

	        /* Total OKR */
	        COALESCE(ot.okr_leads_total, 0)            AS okr_leads_total,
	        COALESCE(ot.okr_projects_total, 0)         AS okr_projects_total,
	        COALESCE(ot.okr_scheduled_visits_total, 0) AS okr_scheduled_visits_total,
	        COALESCE(ot.okr_confirmed_visits_total, 0) AS okr_confirmed_visits_total,
	        COALESCE(ot.okr_completed_visits_total, 0) AS okr_completed_visits_total,
	        COALESCE(ot.okr_lois_total, 0)             AS okr_lois_total

	    FROM ptd_windows w
	    CROSS JOIN cohort_types ct

	    /* Solo matchea ct='New'; ct='Reactivated' queda en 0 */
	    LEFT JOIN rolling_leads_current rl
	      ON rl.as_of_date   = w.as_of_date
	     AND rl.ptd_type     = w.ptd_type
	     AND rl.period_start = w.period_start
	     AND rl.period_end   = w.period_end
	     AND rl.cohort_type  = ct.cohort_type

	    LEFT JOIN rolling_projects_current rp
	      ON rp.as_of_date   = w.as_of_date
	     AND rp.ptd_type     = w.ptd_type
	     AND rp.period_start = w.period_start
	     AND rp.period_end   = w.period_end
	     AND rp.cohort_type  = ct.cohort_type

	    LEFT JOIN ptd_targets pt
	      ON pt.as_of_date   = w.as_of_date
	     AND pt.ptd_type     = w.ptd_type
	     AND pt.period_start = w.period_start
	     AND pt.period_end   = w.period_end

	    LEFT JOIN okr_totals ot
	      ON ot.as_of_date   = w.as_of_date
	     AND ot.ptd_type     = w.ptd_type
	     AND ot.period_start = w.period_start
	     AND ot.period_end   = w.period_end
	)

	SELECT * FROM cohort_output
	UNION ALL
	SELECT * FROM rolling_output
	ORDER BY
	    counting_method,
	    ptd_type,
	    cohort_type
),

-- ==========================================================================================================================================================
-- =====================================================[ Projection ]=======================================================================================
-- ==========================================================================================================================================================

projections AS (
	WITH params AS (
	  SELECT
	    (CURRENT_DATE - INTERVAL '1 day')::date                      AS ref_date,
	    ((CURRENT_DATE - INTERVAL '1 day')::date + INTERVAL '1 day') AS ref_ts_end, -- exclusivo
	    DATE_TRUNC('quarter', (CURRENT_DATE - INTERVAL '1 day'))::date AS q_start_current,
	    (DATE_TRUNC('quarter', (CURRENT_DATE - INTERVAL '1 day')) - INTERVAL '6 months')::date AS ventana_inicio_2q
	),

	/* =========================
	   BASE / UNIVERSO (igual que PTD)
	   ========================= */
	leads_base AS (
	  SELECT
	    l.lead_id,
	    LEAST(
	      COALESCE(l.lead_lead0_at::timestamp, TIMESTAMP '9999-12-31'),
	      COALESCE(l.lead_lead1_at::timestamp, TIMESTAMP '9999-12-31'),
	      COALESCE(l.lead_lead2_at::timestamp, TIMESTAMP '9999-12-31'),
	      COALESCE(l.lead_lead3_at::timestamp, TIMESTAMP '9999-12-31'),
	      COALESCE(l.lead_lead4_at::timestamp, TIMESTAMP '9999-12-31')
	    ) AS primera_fecha_lead
	  FROM lk_leads_v2 l
	  WHERE (l.lead_l0 OR l.lead_l1 OR l.lead_l2 OR l.lead_l3 OR l.lead_l4)
	    AND (l.lead_domain NOT IN ('spot2.mx') OR l.lead_domain IS NULL)
	    AND l.lead_deleted_at IS NULL
	),
	leads_base_filt AS (
	  SELECT
	    lead_id,
	    primera_fecha_lead,
	    primera_fecha_lead::date AS lead_first_date
	  FROM leads_base
	  WHERE primera_fecha_lead >= TIMESTAMP '2021-01-01'
	    AND primera_fecha_lead < CURRENT_DATE
	    AND primera_fecha_lead < TIMESTAMP '9999-12-31'
	),

	projects_base AS (
	  SELECT
	    lb.lead_id,
	    lb.lead_first_date,
	    lb.primera_fecha_lead,
	    p.project_id,
	    p.project_enable_id, -- 1 activo, 0 inactivo
	    p.project_created_at,
	    p.project_funnel_visit_created_date AS visit_scheduled_at, -- date
	    p.project_funnel_visit_realized_at  AS visit_completed_at, -- timestamp
	    p.project_funnel_loi_date           AS loi_signed_at,      -- date/timestamp según esquema

	    /* cohort_date solo para Cohort */
	    (
	      CASE
	        WHEN p.project_created_at IS NOT NULL
	         AND p.project_created_at >= lb.primera_fecha_lead + INTERVAL '30 days'
	          THEN p.project_created_at
	        ELSE lb.primera_fecha_lead
	      END
	    )::date AS cohort_date_real
	  FROM leads_base_filt lb
	  JOIN lk_projects_v2 p USING (lead_id)
	  WHERE p.project_id IS NOT NULL
	),

	/* Ventanas para Month-to-Date y Quarter-to-Date (ref = ayer) */
	ptd_windows AS (
	  SELECT
	        prm.ref_date AS as_of_date,
	        'Month to Date'::text AS ptd_type,
	        m.month_start::date AS period_start,
	        CASE
	            WHEN m.month_start = DATE_TRUNC('month', prm.ref_date)::date
	            THEN prm.ref_date
	            ELSE (m.month_start + INTERVAL '1 month - 1 day')::date
	        END AS period_end,
	        prm.ref_ts_end
	    FROM params prm
	    CROSS JOIN generate_series(
	        DATE_TRUNC('year', prm.ref_date)::date,
	        DATE_TRUNC('month', prm.ref_date)::date,
	        INTERVAL '1 month'
	    ) AS m(month_start)
	  UNION ALL
	  SELECT
	    prm.ref_date AS as_of_date,
	    'Quarter to Date'::text AS ptd_type,
	    DATE_TRUNC('quarter', prm.ref_date)::date AS period_start,
	    prm.ref_date AS period_end,
	    prm.ref_ts_end
	  FROM params prm
	),

	/* =========================
	   Percentiles (igual a tu versión validada)
	   ========================= */
	pct_base AS (
	  SELECT
	    p.project_id,
	    p.project_created_at::date         AS created_dt,
	    p.visit_scheduled_at::date         AS agenda_dt,
	    p.visit_completed_at::date         AS realizada_dt,
	    p.loi_signed_at::date              AS loi_dt,
	    p.cohort_date_real                 AS cohort_dt
	  FROM projects_base p
	  CROSS JOIN params prm
	  WHERE p.project_created_at IS NOT NULL
	    AND p.project_created_at < prm.q_start_current + INTERVAL '0 day'
	),

	pct_pre_by_method AS (
	  SELECT
	    'Cohort'::text AS counting_method,

	    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY (agenda_dt - created_dt))
	      FILTER (
	        WHERE agenda_dt IS NOT NULL
	          AND created_dt IS NOT NULL
	          AND (agenda_dt - created_dt) >= 0
	          AND cohort_dt >= prm.ventana_inicio_2q
	          AND cohort_dt <  prm.q_start_current
	      ) AS p75_created_to_agenda_pre,

	    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY (realizada_dt - agenda_dt))
	      FILTER (
	        WHERE realizada_dt IS NOT NULL
	          AND agenda_dt IS NOT NULL
	          AND (realizada_dt - agenda_dt) >= 0
	          AND cohort_dt >= prm.ventana_inicio_2q
	          AND cohort_dt <  prm.q_start_current
	      ) AS p75_agenda_to_realizada_pre,

	    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY (loi_dt - realizada_dt))
	      FILTER (
	        WHERE loi_dt IS NOT NULL
	          AND realizada_dt IS NOT NULL
	          AND (loi_dt - realizada_dt) >= 0
	          AND cohort_dt >= prm.ventana_inicio_2q
	          AND cohort_dt <  prm.q_start_current
	      ) AS p75_realizada_to_loi_pre
	  FROM pct_base
	  CROSS JOIN params prm

	  UNION ALL

	  SELECT
	    'Rolling'::text AS counting_method,

	    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY (agenda_dt - created_dt))
	      FILTER (
	        WHERE agenda_dt IS NOT NULL
	          AND created_dt IS NOT NULL
	          AND (agenda_dt - created_dt) >= 0
	          AND created_dt >= prm.ventana_inicio_2q
	          AND created_dt <  prm.q_start_current
	      ) AS p75_created_to_agenda_pre,

	    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY (realizada_dt - agenda_dt))
	      FILTER (
	        WHERE realizada_dt IS NOT NULL
	          AND agenda_dt IS NOT NULL
	          AND (realizada_dt - agenda_dt) >= 0
	          AND created_dt >= prm.ventana_inicio_2q
	          AND created_dt <  prm.q_start_current
	      ) AS p75_agenda_to_realizada_pre,

	    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY (loi_dt - realizada_dt))
	      FILTER (
	        WHERE loi_dt IS NOT NULL
	          AND realizada_dt IS NOT NULL
	          AND (loi_dt - realizada_dt) >= 0
	          AND created_dt >= prm.ventana_inicio_2q
	          AND created_dt <  prm.q_start_current
	      ) AS p75_realizada_to_loi_pre
	  FROM pct_base
	  CROSS JOIN params prm
	),

	pct_mature_by_method AS (
	  SELECT
	    pbm.counting_method,

	    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY (agenda_dt - created_dt))
	      FILTER (
	        WHERE agenda_dt IS NOT NULL
	          AND created_dt IS NOT NULL
	          AND (agenda_dt - created_dt) >= 0
	          AND (
	            (pbm.counting_method = 'Cohort'  AND cohort_dt >= prm.ventana_inicio_2q AND cohort_dt < prm.q_start_current)
	            OR
	            (pbm.counting_method = 'Rolling' AND created_dt >= prm.ventana_inicio_2q AND created_dt < prm.q_start_current)
	          )
	          AND (pbm.p75_created_to_agenda_pre IS NULL OR created_dt <= (prm.ref_date - pbm.p75_created_to_agenda_pre))
	      ) AS p75_created_to_agenda_mature,

	    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY (realizada_dt - agenda_dt))
	      FILTER (
	        WHERE realizada_dt IS NOT NULL
	          AND agenda_dt IS NOT NULL
	          AND (realizada_dt - agenda_dt) >= 0
	          AND (
	            (pbm.counting_method = 'Cohort'  AND cohort_dt >= prm.ventana_inicio_2q AND cohort_dt < prm.q_start_current)
	            OR
	            (pbm.counting_method = 'Rolling' AND created_dt >= prm.ventana_inicio_2q AND created_dt < prm.q_start_current)
	          )
	          AND (pbm.p75_agenda_to_realizada_pre IS NULL OR agenda_dt <= (prm.ref_date - pbm.p75_agenda_to_realizada_pre))
	      ) AS p75_agenda_to_realizada_mature,

	    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY (loi_dt - realizada_dt))
	      FILTER (
	        WHERE loi_dt IS NOT NULL
	          AND realizada_dt IS NOT NULL
	          AND (loi_dt - realizada_dt) >= 0
	          AND (
	            (pbm.counting_method = 'Cohort'  AND cohort_dt >= prm.ventana_inicio_2q AND cohort_dt < prm.q_start_current)
	            OR
	            (pbm.counting_method = 'Rolling' AND created_dt >= prm.ventana_inicio_2q AND created_dt < prm.q_start_current)
	          )
	          AND (pbm.p75_realizada_to_loi_pre IS NULL OR realizada_dt <= (prm.ref_date - pbm.p75_realizada_to_loi_pre))
	      ) AS p75_realizada_to_loi_mature

	  FROM pct_base
	  CROSS JOIN params prm
	  JOIN pct_pre_by_method pbm ON TRUE
	  GROUP BY 1
	),

	pct_by_method AS (
	  SELECT
	    ppre.counting_method,
	    COALESCE(pm.p75_created_to_agenda_mature,   ppre.p75_created_to_agenda_pre)   AS p75_created_to_agenda,
	    COALESCE(pm.p75_agenda_to_realizada_mature, ppre.p75_agenda_to_realizada_pre) AS p75_agenda_to_realizada,
	    COALESCE(pm.p75_realizada_to_loi_mature,    ppre.p75_realizada_to_loi_pre)    AS p75_realizada_to_loi
	  FROM pct_pre_by_method ppre
	  LEFT JOIN pct_mature_by_method pm USING (counting_method)
	),

	/* =========================
	   Cutoff a ref_ts_end (no usamos eventos futuros)
	   ========================= */
	projects_cutoff AS (
	  SELECT
	    p.lead_id,
	    p.lead_first_date,
	    p.project_id,
	    p.project_enable_id,
	    p.cohort_date_real,

	    CASE WHEN p.project_created_at < w.ref_ts_end THEN p.project_created_at END AS project_created_at,
	    CASE WHEN p.visit_scheduled_at < w.ref_ts_end THEN p.visit_scheduled_at END AS visit_scheduled_at,
	    CASE WHEN p.visit_completed_at < w.ref_ts_end THEN p.visit_completed_at END AS visit_completed_at,
	    CASE WHEN p.loi_signed_at < w.ref_ts_end THEN p.loi_signed_at END AS loi_signed_at,

	    w.as_of_date,
	    w.ptd_type,
	    w.period_start,
	    w.period_end
	  FROM ptd_windows w
	  JOIN projects_base p
	    ON p.project_created_at IS NOT NULL
	   AND p.project_created_at < w.ref_ts_end
	),

	/* =========================
	   Scopes separados
	   ========================= */
	scope_cohort AS (
	  SELECT
	    'Cohort'::text AS counting_method,
	    pc.*,
	    CASE
	      WHEN pc.cohort_date_real = pc.lead_first_date THEN 'New'
	      ELSE 'Reactivated'
	    END AS cohort_type
	  FROM projects_cutoff pc
	  WHERE pc.cohort_date_real BETWEEN pc.period_start AND pc.period_end
	),

	scope_rolling AS (
	  SELECT
	    'Rolling'::text AS counting_method,
	    pc.*,
	    'New'::text AS cohort_type
	  FROM projects_cutoff pc
	  WHERE pc.period_end = pc.as_of_date
	    AND pc.project_created_at::date <= pc.as_of_date
	),

	monitor_scope AS (
	  SELECT * FROM scope_cohort
	  UNION ALL
	  SELECT * FROM scope_rolling
	),

	/* =========================
   Candidatura por etapa
   ========================= */
eligible AS (
  SELECT
    ms.*,

    /* flags por fecha dentro de la ventana (diagnóstico / apoyo) */
    (ms.project_created_at::date BETWEEN ms.period_start AND ms.period_end) AS created_in_window,
    (ms.visit_scheduled_at IS NOT NULL AND ms.visit_scheduled_at::date BETWEEN ms.period_start AND ms.period_end) AS scheduled_in_window,
    (ms.visit_completed_at IS NOT NULL AND ms.visit_completed_at::date BETWEEN ms.period_start AND ms.period_end) AS completed_in_window,
    (ms.loi_signed_at IS NOT NULL AND ms.loi_signed_at::date BETWEEN ms.period_start AND ms.period_end) AS loi_in_window,

    CASE
      WHEN ms.counting_method = 'Cohort' THEN
        (ms.visit_scheduled_at IS NOT NULL
         AND ms.visit_scheduled_at::date <= ms.as_of_date
         AND ms.visit_completed_at IS NULL)
      ELSE
        (ms.visit_scheduled_at IS NOT NULL
         AND ms.visit_scheduled_at::date <= ms.as_of_date
         AND ms.visit_completed_at IS NULL)
    END AS stage2_backlog_in_window,


    CASE
      WHEN ms.counting_method = 'Cohort' THEN
        (
          ms.visit_completed_at IS NOT NULL
          AND ms.visit_completed_at::date BETWEEN ms.period_start AND ms.period_end
          AND ms.loi_signed_at IS NULL
        )
      ELSE
        (
          ms.visit_completed_at IS NOT NULL
          AND ms.visit_completed_at::date <= ms.as_of_date
          AND ms.loi_signed_at IS NULL
        )
    END AS stage3_backlog_in_window,

    /* -------------------------
       Candidate: Schedule Visit
       ------------------------- */
    CASE
      WHEN ms.counting_method = 'Cohort' THEN TRUE
      ELSE TRUE
    END AS is_schedule_visit_candidate,

    /* -------------------------
       Candidate: Complete Visit
       Cohort: corte a as_of_date (NO period_end) para permitir meses cerrados
       Rolling: backlog válido aunque se haya agendado antes
       ------------------------- */
    CASE
      WHEN ms.counting_method = 'Cohort'
        THEN (ms.visit_scheduled_at IS NOT NULL AND ms.visit_scheduled_at::date <= ms.as_of_date)
      ELSE
        (ms.visit_scheduled_at IS NOT NULL AND ms.visit_scheduled_at::date <= ms.as_of_date)
    END AS is_complete_visit_candidate,

    /* -------------------------
       Candidate: Sign LOI
       Cohort: corte a as_of_date (NO period_end) para permitir meses cerrados
       Rolling: backlog válido aunque la visita se haya completado antes
       ------------------------- */
    CASE
      WHEN ms.counting_method = 'Cohort'
        THEN (ms.visit_completed_at IS NOT NULL AND ms.visit_completed_at::date <= ms.as_of_date)
      ELSE
        (ms.visit_completed_at IS NOT NULL AND ms.visit_completed_at::date <= ms.as_of_date)
    END AS is_sign_loi_candidate

  FROM monitor_scope ms
),

	/* =========================
	   Scored + Rejected solo Stage 2
	   ========================= */
	scored AS (
	  SELECT
	    e.*,

	    pbm.p75_created_to_agenda,
	    pbm.p75_agenda_to_realizada,
	    pbm.p75_realizada_to_loi,

	    CASE
	      WHEN e.project_enable_id = 0 THEN FALSE
	      WHEN NOT e.stage2_backlog_in_window THEN FALSE
	      WHEN NOT EXISTS (
	        SELECT 1
	        FROM bt_lds_lead_spots b
	        WHERE b.project_id = e.project_id
	          AND b.lds_event_id = 4
	          AND b.appointment_last_date_status_id IN (1, 2, 3, 4, 7, 9)
	          -- evita b.lds_event_at::date para no matar índices
	          AND b.lds_event_at < (e.as_of_date + INTERVAL '1 day')
	          AND (
	                (e.counting_method = 'Cohort'
	                  AND b.lds_event_at >= e.period_start)
	             OR (e.counting_method = 'Rolling'
	                  AND b.lds_event_at >= (e.as_of_date - INTERVAL '365 days'))
	          )
	      ) THEN TRUE
	      ELSE FALSE
	    END AS is_visit_rejected

	  FROM eligible e
	  JOIN pct_by_method pbm
	    ON pbm.counting_method = e.counting_method
	),

	activity_catalog AS (
	  SELECT * FROM (VALUES
	    (1, 'Active Project'),
	    (2, 'Inactive Project'),
	    (3, 'Rejected or Canceled')
	  ) AS v(status_id, status)
	),

	status_catalog AS (
	  SELECT * FROM (VALUES
	    (1, 'Completed'),
	    (3, 'On time'),
	    (4, 'Late'),
	    (5, 'Inactive Project'),
	    (6, 'Rejected or Canceled')
	  ) AS v(status_id, status)
	),

	presentable AS (
	  SELECT
	    s.counting_method,
	    s.ptd_type,
	    s.cohort_type,

		 -- ✅ necesarias para EOP / horizonte de proyección
	    s.as_of_date,
	    s.period_start,
	    s.period_end,

	    s.lead_id,
	    s.project_id,
	    s.project_enable_id,

	    CASE
	      WHEN s.project_enable_id = 0 THEN 2
	      WHEN s.is_visit_rejected THEN 3
	      ELSE 1
	    END AS project_activity_status_id,

	    s.project_created_at,
	    s.visit_scheduled_at,
	    s.visit_completed_at,
	    s.loi_signed_at,

	    s.is_schedule_visit_candidate,
	    s.is_complete_visit_candidate,
	    s.is_sign_loi_candidate,

	    CASE
	      WHEN NOT s.is_schedule_visit_candidate THEN NULL
	      WHEN s.counting_method = 'Rolling'
	       AND s.visit_scheduled_at IS NOT NULL
	       AND s.visit_scheduled_at::date BETWEEN s.period_start AND s.period_end
	        THEN 1
	      WHEN s.counting_method = 'Cohort'
	       AND s.visit_scheduled_at IS NOT NULL
	       AND s.visit_scheduled_at::date <= s.as_of_date
	        THEN 1
	      WHEN s.project_enable_id = 0 THEN 5
	      WHEN s.counting_method = 'Rolling'
	       AND s.period_end < s.as_of_date
	       AND NOT (s.project_created_at::date BETWEEN s.period_start AND s.period_end)
	        THEN NULL
	      WHEN s.p75_created_to_agenda IS NULL THEN NULL
	      WHEN (s.as_of_date - s.project_created_at::date) > s.p75_created_to_agenda THEN 4
	      ELSE 3
	    END AS visit_scheduled_status_id,

	    CASE
	      WHEN NOT s.is_complete_visit_candidate THEN NULL
	      WHEN s.visit_completed_at IS NOT NULL THEN 1
	      WHEN s.project_enable_id = 0 THEN 5
	      WHEN s.is_visit_rejected THEN 6
	      WHEN s.p75_agenda_to_realizada IS NULL THEN NULL
	      WHEN (s.as_of_date - s.visit_scheduled_at::date) > s.p75_agenda_to_realizada THEN 4
	      ELSE 3
	    END AS visit_completed_status_id,

	    CASE
	      WHEN NOT s.is_sign_loi_candidate THEN NULL
	      WHEN s.loi_signed_at IS NOT NULL THEN 1
	      WHEN s.project_enable_id = 0 THEN 5
	      WHEN s.p75_realizada_to_loi IS NULL THEN NULL
	      WHEN (s.as_of_date - s.visit_completed_at::date) > s.p75_realizada_to_loi THEN 4
	      ELSE 3
	    END AS loi_signed_status_id,

	    CASE
	      WHEN s.visit_scheduled_at IS NULL THEN 1
	      WHEN s.visit_completed_at IS NULL THEN 2
	      WHEN s.loi_signed_at IS NULL THEN 3
	      ELSE 4
	    END AS current_stage_id,

	    CASE
	      WHEN s.visit_scheduled_at IS NULL THEN 'Schedule Visit'
	      WHEN s.visit_completed_at IS NULL THEN 'Complete Visit'
	      WHEN s.loi_signed_at IS NULL THEN 'Sign LOI'
	      ELSE 'Completed'
	    END AS current_stage,

	    CASE
	      WHEN s.visit_scheduled_at IS NULL THEN s.project_created_at::date
	      WHEN s.visit_completed_at IS NULL THEN s.visit_scheduled_at::date
	      WHEN s.loi_signed_at IS NULL THEN s.visit_completed_at::date
	      ELSE s.loi_signed_at::date
	    END AS last_completed_stage_at,

	    (s.as_of_date -
	      CASE
	        WHEN s.visit_scheduled_at IS NULL THEN s.project_created_at::date
	        WHEN s.visit_completed_at IS NULL THEN s.visit_scheduled_at::date
	        WHEN s.loi_signed_at IS NULL THEN s.visit_completed_at::date
	        ELSE s.loi_signed_at::date
	      END
	    ) AS days_since_last_completed_stage,

	    CASE
	      WHEN (CASE
	              WHEN s.visit_scheduled_at IS NULL THEN 1
	              WHEN s.visit_completed_at IS NULL THEN 2
	              WHEN s.loi_signed_at IS NULL THEN 3
	              ELSE 4
	            END) = 1 THEN s.p75_created_to_agenda
	      WHEN (CASE
	              WHEN s.visit_scheduled_at IS NULL THEN 1
	              WHEN s.visit_completed_at IS NULL THEN 2
	              WHEN s.loi_signed_at IS NULL THEN 3
	              ELSE 4
	            END) = 2 THEN s.p75_agenda_to_realizada
	      WHEN (CASE
	              WHEN s.visit_scheduled_at IS NULL THEN 1
	              WHEN s.visit_completed_at IS NULL THEN 2
	              WHEN s.loi_signed_at IS NULL THEN 3
	              ELSE 4
	            END) = 3 THEN s.p75_realizada_to_loi
	      ELSE NULL
	    END AS p75_current_stage_threshold_days,

	    CASE
	      WHEN (CASE
	              WHEN s.visit_scheduled_at IS NULL THEN 1
	              WHEN s.visit_completed_at IS NULL THEN 2
	              WHEN s.loi_signed_at IS NULL THEN 3
	              ELSE 4
	            END) = 4 THEN NULL
	      WHEN s.project_enable_id = 0 THEN NULL
	      WHEN (CASE
	              WHEN s.visit_scheduled_at IS NULL THEN 1
	              WHEN s.visit_completed_at IS NULL THEN 2
	              WHEN s.loi_signed_at IS NULL THEN 3
	              ELSE 4
	            END) = 2
	           AND s.is_visit_rejected THEN NULL
	      ELSE (
	        (s.as_of_date -
	          CASE
	            WHEN s.visit_scheduled_at IS NULL THEN s.project_created_at::date
	            WHEN s.visit_completed_at IS NULL THEN s.visit_scheduled_at::date
	            WHEN s.loi_signed_at IS NULL THEN s.visit_completed_at::date
	            ELSE s.loi_signed_at::date
	          END
	        )
	        <=
	        CASE
	          WHEN (CASE
	                  WHEN s.visit_scheduled_at IS NULL THEN 1
	                  WHEN s.visit_completed_at IS NULL THEN 2
	                  WHEN s.loi_signed_at IS NULL THEN 3
	                  ELSE 4
	                END) = 1 THEN s.p75_created_to_agenda
	          WHEN (CASE
	                  WHEN s.visit_scheduled_at IS NULL THEN 1
	                  WHEN s.visit_completed_at IS NULL THEN 2
	                  WHEN s.loi_signed_at IS NULL THEN 3
	                  ELSE 4
	                END) = 2 THEN s.p75_agenda_to_realizada
	          WHEN (CASE
	                  WHEN s.visit_scheduled_at IS NULL THEN 1
	                  WHEN s.visit_completed_at IS NULL THEN 2
	                  WHEN s.loi_signed_at IS NULL THEN 3
	                  ELSE 4
	                END) = 3 THEN s.p75_realizada_to_loi
	          ELSE NULL
	        END
	      )
	    END AS is_current_stage_on_time

	  FROM scored s
	),

	/* =========================
	   “Monitor” final (igual que salida validada, pero como CTE)
	   ========================= */
	monitor AS (
	  SELECT
	    p.*,
	    ac.status AS project_activity_status,
	    sc1.status AS visit_scheduled_status,
	    sc2.status AS visit_completed_status,
	    sc3.status AS loi_signed_status
	  FROM presentable p
	  LEFT JOIN activity_catalog ac ON ac.status_id = p.project_activity_status_id
	  LEFT JOIN status_catalog   sc1 ON sc1.status_id = p.visit_scheduled_status_id
	  LEFT JOIN status_catalog   sc2 ON sc2.status_id = p.visit_completed_status_id
	  LEFT JOIN status_catalog   sc3 ON sc3.status_id = p.loi_signed_status_id
	),

	/* =========================
	   1) Scope de proyección: SOLO activos + (On time/Late) en la etapa actual
	   ========================= */

	-- ================================================================
	-- ================================================================
	-- ================================================================
	projection_scope AS (
	  SELECT
	    m.*,
	    CASE
	      WHEN m.current_stage_id = 1 THEN m.visit_scheduled_status_id
	      WHEN m.current_stage_id = 2 THEN m.visit_completed_status_id
	      WHEN m.current_stage_id = 3 THEN m.loi_signed_status_id
	      ELSE NULL
	    END AS current_stage_status_id
	  FROM monitor m
	  WHERE m.project_enable_id = 1
	    AND m.current_stage_id IN (1,2,3)
	    AND (
	      CASE
	        WHEN m.current_stage_id = 1 THEN m.visit_scheduled_status_id
	        WHEN m.current_stage_id = 2 THEN m.visit_completed_status_id
	        WHEN m.current_stage_id = 3 THEN m.loi_signed_status_id
	        ELSE NULL
	      END
	    ) IN (3,4)  -- On time o Late
	),

	projection_horizon AS (
	  SELECT
	    ps.*,
	    CASE
	      WHEN ps.ptd_type = 'Month to Date'
	        THEN (DATE_TRUNC('month', ps.as_of_date)::date + INTERVAL '1 month' - INTERVAL '1 day')::date
	      WHEN ps.ptd_type = 'Quarter to Date'
	        THEN (DATE_TRUNC('quarter', ps.as_of_date)::date + INTERVAL '3 month' - INTERVAL '1 day')::date
	      ELSE NULL
	    END AS eop_date,
	    GREATEST(
	      0,
	      (
	        CASE
	          WHEN ps.ptd_type = 'Month to Date'
	            THEN (DATE_TRUNC('month', ps.as_of_date)::date + INTERVAL '1 month' - INTERVAL '1 day')::date
	          WHEN ps.ptd_type = 'Quarter to Date'
	            THEN (DATE_TRUNC('quarter', ps.as_of_date)::date + INTERVAL '3 month' - INTERVAL '1 day')::date
	          ELSE ps.as_of_date
	        END
	        - ps.as_of_date
	      )
	    )::int AS days_remaining_to_eop
	  FROM projection_scope ps
	),

	/* =========================
	   2) Entrenamiento Kaplan–Meier (ventana 2Q cerrados) – excluye SOLO desactivados
	   ========================= */
	km_params AS (
	  SELECT 30::int AS min_events  -- salvaguarda baja muestra
	),

	km_raw AS (
	  /* Stage 1: created -> agenda */
	  SELECT
	    'Cohort'::text AS counting_method,
	    CASE WHEN pb.cohort_date_real = pb.lead_first_date THEN 'New' ELSE 'Reactivated' END AS cohort_type,
	    1::int AS stage_id,
	    pb.project_created_at::date AS stage_start_dt,
	    pb.visit_scheduled_at::date AS stage_end_dt
	  FROM projects_base pb
	  CROSS JOIN params prm
	  WHERE pb.project_enable_id = 1
	    AND pb.project_created_at IS NOT NULL
	    AND pb.project_created_at::date <= prm.ref_date
	    AND pb.cohort_date_real >= prm.ventana_inicio_2q
	    AND pb.cohort_date_real <  prm.q_start_current

	  UNION ALL
	  SELECT
	    'Rolling'::text AS counting_method,
	    'New'::text   AS cohort_type,
	    1::int AS stage_id,
	    pb.project_created_at::date AS stage_start_dt,
	    pb.visit_scheduled_at::date AS stage_end_dt
	  FROM projects_base pb
	  CROSS JOIN params prm
	  WHERE pb.project_enable_id = 1
	    AND pb.project_created_at IS NOT NULL
	    AND pb.project_created_at::date <= prm.ref_date
	    AND pb.project_created_at::date >= prm.ventana_inicio_2q
	    AND pb.project_created_at::date <  prm.q_start_current

	  UNION ALL

	  /* Stage 2: agenda -> realizada */
	  SELECT
	    'Cohort'::text AS counting_method,
	    CASE WHEN pb.cohort_date_real = pb.lead_first_date THEN 'New' ELSE 'Reactivated' END AS cohort_type,
	    2::int AS stage_id,
	    pb.visit_scheduled_at::date AS stage_start_dt,
	    pb.visit_completed_at::date AS stage_end_dt
	  FROM projects_base pb
	  CROSS JOIN params prm
	  WHERE pb.project_enable_id = 1
	    AND pb.visit_scheduled_at IS NOT NULL
	    AND pb.visit_scheduled_at::date <= prm.ref_date
	    AND pb.cohort_date_real >= prm.ventana_inicio_2q
	    AND pb.cohort_date_real <  prm.q_start_current

	  UNION ALL
	  SELECT
	    'Rolling'::text AS counting_method,
	    'New'::text   AS cohort_type,
	    2::int AS stage_id,
	    pb.visit_scheduled_at::date AS stage_start_dt,
	    pb.visit_completed_at::date AS stage_end_dt
	  FROM projects_base pb
	  CROSS JOIN params prm
	  WHERE pb.project_enable_id = 1
	    AND pb.visit_scheduled_at IS NOT NULL
	    AND pb.visit_scheduled_at::date <= prm.ref_date
	    AND pb.visit_scheduled_at::date >= prm.ventana_inicio_2q
	    AND pb.visit_scheduled_at::date <  prm.q_start_current

	  UNION ALL

	  /* Stage 3: realizada -> loi */
	  SELECT
	    'Cohort'::text AS counting_method,
	    CASE WHEN pb.cohort_date_real = pb.lead_first_date THEN 'New' ELSE 'Reactivated' END AS cohort_type,
	    3::int AS stage_id,
	    pb.visit_completed_at::date AS stage_start_dt,
	    pb.loi_signed_at::date      AS stage_end_dt
	  FROM projects_base pb
	  CROSS JOIN params prm
	  WHERE pb.project_enable_id = 1
	    AND pb.visit_completed_at IS NOT NULL
	    AND pb.visit_completed_at::date <= prm.ref_date
	    AND pb.cohort_date_real >= prm.ventana_inicio_2q
	    AND pb.cohort_date_real <  prm.q_start_current

	  UNION ALL
	  SELECT
	    'Rolling'::text AS counting_method,
	    'New'::text   AS cohort_type,
	    3::int AS stage_id,
	    pb.visit_completed_at::date AS stage_start_dt,
	    pb.loi_signed_at::date      AS stage_end_dt
	  FROM projects_base pb
	  CROSS JOIN params prm
	  WHERE pb.project_enable_id = 1
	    AND pb.visit_completed_at IS NOT NULL
	    AND pb.visit_completed_at::date <= prm.ref_date
	    AND pb.visit_completed_at::date >= prm.ventana_inicio_2q
	    AND pb.visit_completed_at::date <  prm.q_start_current
	),

	km_obs AS (
	  SELECT
	    r.counting_method,
	    r.cohort_type,
	    r.stage_id,
	    /* event y tiempo observado (días) */
	    CASE WHEN r.stage_end_dt IS NOT NULL THEN 1 ELSE 0 END AS event,
	    CASE
	      WHEN r.stage_end_dt IS NOT NULL THEN (r.stage_end_dt - r.stage_start_dt)
	      ELSE (prm.ref_date - r.stage_start_dt)
	    END::int AS observed_time
	  FROM km_raw r
	  CROSS JOIN params prm
	  WHERE r.stage_start_dt IS NOT NULL
	    AND (
	      CASE WHEN r.stage_end_dt IS NOT NULL THEN (r.stage_end_dt - r.stage_start_dt)
	           ELSE (prm.ref_date - r.stage_start_dt)
	      END
	    ) >= 0
	),

	/* ====== Curvas con fallback (L3, L2, L1) usando grouping sets ====== */
	km_counts AS (
	  SELECT
	    CASE
	      WHEN GROUPING(counting_method)=0 AND GROUPING(cohort_type)=0 THEN 'L3'
	      WHEN GROUPING(counting_method)=0 AND GROUPING(cohort_type)=1 THEN 'L2'
	      WHEN GROUPING(counting_method)=1 THEN 'L1'
	    END AS curve_level,
	    COALESCE(counting_method,'All') AS counting_method,
	    COALESCE(cohort_type,'All')     AS cohort_type,
	    stage_id,
	    observed_time AS t,
	    COUNT(*) FILTER (WHERE event=1) AS d_t,
	    COUNT(*) FILTER (WHERE event=0) AS c_t
	  FROM km_obs
	  GROUP BY GROUPING SETS (
	    (counting_method, cohort_type, stage_id, observed_time),
	    (counting_method, stage_id, observed_time),
	    (stage_id, observed_time)
	  )
	),

	km_n AS (
	  SELECT
	    CASE
	      WHEN GROUPING(counting_method)=0 AND GROUPING(cohort_type)=0 THEN 'L3'
	      WHEN GROUPING(counting_method)=0 AND GROUPING(cohort_type)=1 THEN 'L2'
	      WHEN GROUPING(counting_method)=1 THEN 'L1'
	    END AS curve_level,
	    COALESCE(counting_method,'All') AS counting_method,
	    COALESCE(cohort_type,'All')     AS cohort_type,
	    stage_id,
	    COUNT(*) AS n_total,
	    COUNT(*) FILTER (WHERE event=1) AS n_events,
	    MAX(observed_time) AS max_t
	  FROM km_obs
	  GROUP BY GROUPING SETS (
	    (counting_method, cohort_type, stage_id),
	    (counting_method, stage_id),
	    (stage_id)
	  )
	),

	km_grid AS (
	  SELECT
	    n.curve_level,
	    n.counting_method,
	    n.cohort_type,
	    n.stage_id,
	    gs.t::int AS t
	  FROM km_n n
	  JOIN LATERAL generate_series(0, n.max_t) AS gs(t) ON TRUE
	),

	km_steps AS (
	  SELECT
	    g.curve_level,
	    g.counting_method,
	    g.cohort_type,
	    g.stage_id,
	    g.t,
	    COALESCE(c.d_t,0)::int AS d_t,
	    COALESCE(c.c_t,0)::int AS c_t,
	    n.n_total
	  FROM km_grid g
	  JOIN km_n n
	    ON n.curve_level      = g.curve_level
	   AND n.counting_method  = g.counting_method
	   AND n.cohort_type      = g.cohort_type
	   AND n.stage_id         = g.stage_id
	  LEFT JOIN km_counts c
	    ON c.curve_level      = g.curve_level
	   AND c.counting_method  = g.counting_method
	   AND c.cohort_type      = g.cohort_type
	   AND c.stage_id         = g.stage_id
	   AND c.t                = g.t
	),

	/* =========================
	   KM: riesgo (sin ventanas anidadas)
	   ========================= */
	km_risk_base AS (
	  SELECT
	    s.*,
	    COALESCE(
	      SUM(s.d_t + s.c_t) OVER (
	        PARTITION BY s.curve_level, s.counting_method, s.cohort_type, s.stage_id
	        ORDER BY s.t
	        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
	      ),
	      0
	    )::numeric AS exits_before_t
	  FROM km_steps s
	),

	km_risk AS (
	  SELECT
	    rb.*,
	    (rb.n_total - rb.exits_before_t) AS n_at_risk
	  FROM km_risk_base rb
	),

	/* =========================
	   KM: marca de "cero" y ln(step)
	   ========================= */
	km_zero AS (
	  SELECT
	    r.*,

	    /* si en algún punto d_t == n_at_risk => supervivencia cae a 0 desde ahí */
	    CASE
	      WHEN r.n_at_risk > 0 AND r.d_t > 0 AND r.d_t >= r.n_at_risk THEN 1
	      ELSE 0
	    END AS step_zero,

	    MAX(
	      CASE
	        WHEN r.n_at_risk > 0 AND r.d_t > 0 AND r.d_t >= r.n_at_risk THEN 1
	        ELSE 0
	      END
	    ) OVER (
	      PARTITION BY r.curve_level, r.counting_method, r.cohort_type, r.stage_id
	      ORDER BY r.t
	      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	    ) AS cum_zero,

	    /* ln del factor (solo cuando es válido; si no, lo dejamos 0 y cum_zero manda) */
	    CASE
	      WHEN r.n_at_risk <= 0 OR r.d_t = 0 THEN 0::numeric
	      WHEN r.d_t >= r.n_at_risk THEN 0::numeric
	      ELSE LN(((r.n_at_risk - r.d_t)::numeric) / NULLIF(r.n_at_risk, 0))
	    END AS ln_step
	  FROM km_risk r
	),

	/* =========================
	   KM: supervivencia final
	   ========================= */
	km_survival_final AS (
	  SELECT
	    curve_level,
	    counting_method,
	    cohort_type,
	    stage_id,
	    t,
	    n_total,
	    n_at_risk,
	    d_t,
	    c_t,
	    CASE
	      WHEN cum_zero = 1 THEN 0::numeric
	      ELSE EXP(
	        SUM(ln_step) OVER (
	          PARTITION BY curve_level, counting_method, cohort_type, stage_id
	          ORDER BY t
	          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	        )
	      )
	    END AS s_t  -- S(t) = P(T > t)
	  FROM km_zero
	),

	/* =========================
	   3) Elegir curva (salvaguarda baja muestra) y calcular prob condicional a EOP
	   ========================= */
	curve_choice AS (
	  SELECT
	    ph.*,
	    /* n_events por nivel */
	    n3.n_events AS n_events_l3,
	    n2.n_events AS n_events_l2,
	    n1.n_events AS n_events_l1,

	    CASE
	      WHEN COALESCE(n3.n_events,0) >= kp.min_events THEN 'L3'
	      WHEN COALESCE(n2.n_events,0) >= kp.min_events THEN 'L2'
	      WHEN COALESCE(n1.n_events,0) >= kp.min_events THEN 'L1'
	      ELSE NULL
	    END AS used_curve_level,

	    CASE
	      WHEN COALESCE(n3.n_events,0) >= kp.min_events THEN ph.counting_method
	      WHEN COALESCE(n2.n_events,0) >= kp.min_events THEN ph.counting_method
	      WHEN COALESCE(n1.n_events,0) >= kp.min_events THEN 'All'
	      ELSE NULL
	    END AS used_counting_method,

	    CASE
	      WHEN COALESCE(n3.n_events,0) >= kp.min_events THEN ph.cohort_type
	      WHEN COALESCE(n2.n_events,0) >= kp.min_events THEN 'All'
	      WHEN COALESCE(n1.n_events,0) >= kp.min_events THEN 'All'
	      ELSE NULL
	    END AS used_cohort_type

	  FROM projection_horizon ph
	  CROSS JOIN km_params kp
	  LEFT JOIN km_n n3
	    ON n3.curve_level     = 'L3'
	   AND n3.counting_method = ph.counting_method
	   AND n3.cohort_type     = ph.cohort_type
	   AND n3.stage_id        = ph.current_stage_id
	  LEFT JOIN km_n n2
	    ON n2.curve_level     = 'L2'
	   AND n2.counting_method = ph.counting_method
	   AND n2.cohort_type     = 'All'
	   AND n2.stage_id        = ph.current_stage_id
	  LEFT JOIN km_n n1
	    ON n1.curve_level     = 'L1'
	   AND n1.counting_method = 'All'
	   AND n1.cohort_type     = 'All'
	   AND n1.stage_id        = ph.current_stage_id
	),

	curve_times AS (
	  SELECT
	    cc.*,
	    /* t0 y t1 */
	    cc.days_since_last_completed_stage::int AS t0,
	    (cc.days_since_last_completed_stage + cc.days_remaining_to_eop)::int AS t1,
	    n.max_t
	  FROM curve_choice cc
	  LEFT JOIN km_n n
	    ON n.curve_level     = cc.used_curve_level
	   AND n.counting_method = cc.used_counting_method
	   AND n.cohort_type     = cc.used_cohort_type
	   AND n.stage_id        = cc.current_stage_id
	),

	curve_times_clamped AS (
	  SELECT
	    ct.*,
	    LEAST(ct.t0, COALESCE(ct.max_t, ct.t0)) AS t0_c,
	    LEAST(ct.t1, COALESCE(ct.max_t, ct.t1)) AS t1_c
	  FROM curve_times ct
	),

	probabilities AS (
	  SELECT
	    ctc.*,
	    s0.s_t AS s_t0,
	    s1.s_t AS s_t1,
	    CASE
	      WHEN ctc.used_curve_level IS NULL THEN NULL
	      WHEN s0.s_t IS NULL OR s0.s_t = 0 THEN 0::numeric
	      WHEN s1.s_t IS NULL THEN NULL
	      ELSE GREATEST(
	        0::numeric,
	        LEAST(
	          1::numeric,
	          1 - (s1.s_t / s0.s_t)
	        )
	      )
	    END AS prob_complete_current_stage_by_eop
	  FROM curve_times_clamped ctc
	  LEFT JOIN km_survival_final s0
	    ON s0.curve_level     = ctc.used_curve_level
	   AND s0.counting_method = ctc.used_counting_method
	   AND s0.cohort_type     = ctc.used_cohort_type
	   AND s0.stage_id        = ctc.current_stage_id
	   AND s0.t              = ctc.t0_c
	  LEFT JOIN km_survival_final s1
	    ON s1.curve_level     = ctc.used_curve_level
	   AND s1.counting_method = ctc.used_counting_method
	   AND s1.cohort_type     = ctc.used_cohort_type
	   AND s1.stage_id        = ctc.current_stage_id
	   AND s1.t              = ctc.t1_c
	)

	SELECT
	    p.counting_method,
	    p.as_of_date,
	    p.ptd_type,
	    p.period_start,
	    p.period_end,
	    p.cohort_type,

	    /* Expected projection (E[completed]) per stage */
	    SUM(CASE WHEN p.current_stage_id = 1 THEN COALESCE(p.prob_complete_current_stage_by_eop, 0) ELSE 0 END)
	      AS scheduled_visits_proj,

	    SUM(CASE WHEN p.current_stage_id = 2 THEN COALESCE(p.prob_complete_current_stage_by_eop, 0) ELSE 0 END)
	      AS completed_visits_proj,

	    SUM(CASE WHEN p.current_stage_id = 3 THEN COALESCE(p.prob_complete_current_stage_by_eop, 0) ELSE 0 END)
	      AS lois_proj,

	    /* (Opcional) diagnósticos */
	    COUNT(*) AS n_projects_in_projection_scope
	  FROM probabilities p
	  GROUP BY 1,2,3,4,5,6
),

joined AS (
  SELECT
    cc.*,

    CASE WHEN cc.counting_method = 'Rolling' AND cc.period_end < cc.as_of_date THEN 0
     	ELSE COALESCE(p.scheduled_visits_proj, 0) END AS scheduled_visits_proj,

	CASE WHEN cc.counting_method = 'Rolling' AND cc.period_end < cc.as_of_date THEN 0
	     ELSE COALESCE(p.completed_visits_proj, 0) END AS completed_visits_proj,

	CASE WHEN cc.counting_method = 'Rolling' AND cc.period_end < cc.as_of_date THEN 0
	     ELSE COALESCE(p.lois_proj, 0) END AS lois_proj,

	CASE WHEN cc.counting_method = 'Rolling' AND cc.period_end < cc.as_of_date THEN 0
	     ELSE COALESCE(p.n_projects_in_projection_scope, 0) END AS n_projects_in_projection_scope


  FROM current_counts cc
  LEFT JOIN projections p
    ON  p.counting_method = cc.counting_method
    AND p.as_of_date       = cc.as_of_date
    AND p.ptd_type          = cc.ptd_type
    AND p.period_start      = cc.period_start
    AND p.period_end        = cc.period_end
    AND p.cohort_type       = cc.cohort_type
),

final_output AS (
  /* ===== fila 1: Actual ===== */
  SELECT
    'Actuals'::text AS value_type,

    j.counting_method,
    j.as_of_date,
    j.ptd_type,
    j.period_start,
    j.period_end,
    j.cohort_type,

    j.leads_current,
    j.projects_current,
    j.scheduled_visits_current,
    j.confirmed_visits_current,
    j.completed_visits_current,
    j.lois_current,
    j.won_current,

    j.leads_ptd,
    j.projects_ptd,
    j.scheduled_visits_ptd,
    j.confirmed_visits_ptd,
    j.completed_visits_ptd,
    j.lois_ptd,

    j.okr_leads_total,
    j.okr_projects_total,
    j.okr_scheduled_visits_total,
    j.okr_confirmed_visits_total,
    j.okr_completed_visits_total,
    j.okr_lois_total,

    j.scheduled_visits_proj,
    j.completed_visits_proj,
    j.lois_proj,
    j.n_projects_in_projection_scope,

    /* PTD conversion rates: current / ptd */
    j.leads_current::float8            / NULLIF(j.leads_ptd, 0)            AS leads_ptd_rate,
    j.projects_current::float8         / NULLIF(j.projects_ptd, 0)         AS projects_ptd_rate,
    j.scheduled_visits_current::float8 / NULLIF(j.scheduled_visits_ptd, 0) AS scheduled_visits_ptd_rate,
    j.confirmed_visits_current::float8 / NULLIF(j.confirmed_visits_ptd, 0) AS confirmed_visits_ptd_rate,
    j.completed_visits_current::float8 / NULLIF(j.completed_visits_ptd, 0) AS completed_visits_ptd_rate,
    j.lois_current::float8             / NULLIF(j.lois_ptd, 0)             AS lois_ptd_rate,

    /* Linear projection: for Actuals = current count */
    j.leads_current::float8            AS leads_count,
    j.projects_current::float8         AS projects_count,
    j.scheduled_visits_current::float8 AS scheduled_visits_count,
    j.confirmed_visits_current::float8 AS confirmed_visits_count,
    j.completed_visits_current::float8 AS completed_visits_count,
    j.lois_current::float8             AS lois_count

  FROM joined j

  UNION ALL

  /* ===== fila 2: Actual + Proyección (solo etapas proyectadas) ===== */
  SELECT
    '⚠️ Actuals + Forecast'::text AS value_type,

    j.counting_method,
    j.as_of_date,
    j.ptd_type,
    j.period_start,
    j.period_end,
    j.cohort_type,

    j.leads_current,
    j.projects_current,
    (j.scheduled_visits_current + j.scheduled_visits_proj) AS scheduled_visits_current,
    j.confirmed_visits_current,
    (j.completed_visits_current + j.completed_visits_proj) AS completed_visits_current,
    (j.lois_current + j.lois_proj)                          AS lois_current,
    j.won_current,

    j.leads_ptd,
    j.projects_ptd,
    j.scheduled_visits_ptd,
    j.confirmed_visits_ptd,
    j.completed_visits_ptd,
    j.lois_ptd,

    j.okr_leads_total,
    j.okr_projects_total,
    j.okr_scheduled_visits_total,
    j.okr_confirmed_visits_total,
    j.okr_completed_visits_total,
    j.okr_lois_total,

    j.scheduled_visits_proj,
    j.completed_visits_proj,
    j.lois_proj,
    j.n_projects_in_projection_scope,

    /* PTD conversion rates: (current + forecast) / ptd */
    j.leads_current::float8                                          / NULLIF(j.leads_ptd, 0)            AS leads_ptd_rate,
    j.projects_current::float8                                       / NULLIF(j.projects_ptd, 0)         AS projects_ptd_rate,
    (j.scheduled_visits_current + j.scheduled_visits_proj)::float8   / NULLIF(j.scheduled_visits_ptd, 0) AS scheduled_visits_ptd_rate,
    j.confirmed_visits_current::float8                               / NULLIF(j.confirmed_visits_ptd, 0) AS confirmed_visits_ptd_rate,
    (j.completed_visits_current + j.completed_visits_proj)::float8   / NULLIF(j.completed_visits_ptd, 0) AS completed_visits_ptd_rate,
    (j.lois_current + j.lois_proj)::float8                           / NULLIF(j.lois_ptd, 0)             AS lois_ptd_rate,

    /* Linear projection: rate * okr_total */
    (j.leads_current::float8            / NULLIF(j.leads_ptd, 0))            * j.okr_leads_total            AS leads_count,
    (j.projects_current::float8         / NULLIF(j.projects_ptd, 0))         * j.okr_projects_total         AS projects_count,
    ((j.scheduled_visits_current + j.scheduled_visits_proj)::float8 / NULLIF(j.scheduled_visits_ptd, 0)) * j.okr_scheduled_visits_total AS scheduled_visits_count,
    (j.confirmed_visits_current::float8 / NULLIF(j.confirmed_visits_ptd, 0)) * j.okr_confirmed_visits_total AS confirmed_visits_count,
    ((j.completed_visits_current + j.completed_visits_proj)::float8 / NULLIF(j.completed_visits_ptd, 0)) * j.okr_completed_visits_total AS completed_visits_count,
    ((j.lois_current + j.lois_proj)::float8                         / NULLIF(j.lois_ptd, 0))             * j.okr_lois_total             AS lois_count

  FROM joined j
)

SELECT *
FROM final_output
ORDER BY
  counting_method,
  ptd_type,
  period_start,
  cohort_type,
  CASE WHEN value_type = 'Actuals' THEN 1 ELSE 2 END
