-- Cohorts New vs Reactivated alineados con Ventas (lk_projects_v2).
-- Reglas:
-- 1) Primera fecha del lead = LEAST(lead_lead0_at .. lead_lead4_at) desde lk_leads.
-- 2) Todo lead válido genera SIEMPRE una fila New con lead_cohort_dt = primera_fecha_lead.
-- 3) Si el lead tiene proyecto en lk_projects con project_created_at >= primera_fecha_lead + 30 días,
--    se genera una fila adicional Reactivated con lead_cohort_dt = project_created_at.

WITH lead_base AS (
    SELECT
        l.lead_id,
        l.lead_phone_number,
        l.lead_email,
        l.spot_sector AS lead_sector,
        l.lead_max_type,
        l.lead_created_date,
        LEAST(
            COALESCE(l.lead_lead0_at, TIMESTAMP '9999-12-31 23:59:59'),
            COALESCE(l.lead_lead1_at, TIMESTAMP '9999-12-31 23:59:59'),
            COALESCE(l.lead_lead2_at, TIMESTAMP '9999-12-31 23:59:59'),
            COALESCE(l.lead_lead3_at, TIMESTAMP '9999-12-31 23:59:59'),
            COALESCE(l.lead_lead4_at, TIMESTAMP '9999-12-31 23:59:59')
        ) AS primera_fecha_lead
    FROM lk_leads l
    WHERE l.lead_max_type IN ('L0', 'L1', 'L2', 'L3', 'L4')
),

-- Solo leads con al menos una fecha L0–L4 (primera_fecha_lead válida)
lead_valid AS (
    SELECT *
    FROM lead_base
    WHERE primera_fecha_lead < TIMESTAMP '9999-12-31 00:00:00'
),

-- Parte 1: una fila New por lead (lead_cohort_dt = primera_fecha_lead)
new_cohorts AS (
    SELECT
        lead_id,
        lead_phone_number,
        lead_email,
        lead_sector,
        lead_max_type,
        'New'::text AS lead_cohort_type,
        primera_fecha_lead AS lead_cohort_dt,
        lead_created_date
    FROM lead_valid
),

-- Parte 2: filas Reactivated por cada proyecto con project_created_at >= primera_fecha_lead + 30 días (lk_projects = dashboard oficial)
reactivated_cohorts AS (
    SELECT
        v.lead_id,
        v.lead_phone_number,
        v.lead_email,
        v.lead_sector,
        v.lead_max_type,
        'Reactivated'::text AS lead_cohort_type,
        p.project_created_at AS lead_cohort_dt,
        v.lead_created_date
    FROM lead_valid v
    INNER JOIN lk_projects p
        ON p.lead_id = v.lead_id
        AND p.project_created_at IS NOT NULL
        AND p.project_created_at >= v.primera_fecha_lead + INTERVAL '30 days'
)

SELECT lead_id, lead_phone_number, lead_email, lead_sector, lead_max_type, lead_cohort_type, lead_cohort_dt, lead_created_date
FROM new_cohorts
UNION ALL
SELECT lead_id, lead_phone_number, lead_email, lead_sector, lead_max_type, lead_cohort_type, lead_cohort_dt, lead_created_date
FROM reactivated_cohorts
ORDER BY lead_id, lead_cohort_dt
