"""
Diagnóstico: PoP Cohorts FIXED vs Projects Funnel FIXED
Investiga la diferencia de 2 reactivados en marzo con canales orgánicos.

Extrae el universo intermedio de cada query (antes de la agregación en PoP)
para comparar los lead_id reactivados.
"""

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "dagster-pipeline" / "src"))
from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

ORGANIC_CHANNELS = {"Direct", "Organic Search", "Organic LLMs", "Organic Social"}
TARGET_MONTH = "2026-03"


def run_q(sql: str, label: str) -> pd.DataFrame:
    print(f"  Ejecutando {label}...")
    df = query_bronze_source(query=sql, source_type="geospot_postgres").to_pandas()
    print(f"  -> {len(df)} filas")
    return df


def get_pop_reactivated() -> pd.DataFrame:
    """Extrae el universo_enriquecido de PoP (antes de agregar)."""
    sql = """
    WITH mat_lead_attrs AS (
        SELECT
            lead_id,
            lead_fecha_cohort_dt::date AS cohort_date,
            COALESCE(mat_channel, 'Unassigned') AS channel,
            COALESCE(mat_traffic_type, 'Unassigned') AS traffic_type,
            COALESCE(mat_campaign_name, 'Unassigned') AS campaign_name
        FROM (
            SELECT *, ROW_NUMBER() OVER(
                PARTITION BY lead_id, lead_fecha_cohort_dt::date
                ORDER BY mat_event_datetime DESC
            ) AS rn
            FROM lk_mat_matches_visitors_to_leads
        ) sub WHERE rn = 1
    ),
    leads_base AS (
      SELECT lk.lead_id,
        LEAST(
          COALESCE(lk.lead_lead0_at::timestamp, TIMESTAMP '9999-12-31'),
          COALESCE(lk.lead_lead1_at::timestamp, TIMESTAMP '9999-12-31'),
          COALESCE(lk.lead_lead2_at::timestamp, TIMESTAMP '9999-12-31'),
          COALESCE(lk.lead_lead3_at::timestamp, TIMESTAMP '9999-12-31'),
          COALESCE(lk.lead_lead4_at::timestamp, TIMESTAMP '9999-12-31')
        ) AS primera_fecha_lead,
        COALESCE(lk.spot_sector, 'Retail') AS spot_sector,
        CASE
          WHEN lk.lead_lead4_at IS NOT NULL THEN 'L4'
          WHEN lk.lead_lead3_at IS NOT NULL THEN 'L3'
          WHEN lk.lead_lead2_at IS NOT NULL THEN 'L2'
          WHEN lk.lead_lead1_at IS NOT NULL THEN 'L1'
          WHEN lk.lead_lead0_at IS NOT NULL THEN 'L0'
          ELSE NULL
        END AS lead_max_type
      FROM lk_leads lk
      WHERE (lk.lead_l0 OR lk.lead_l1 OR lk.lead_l2 OR lk.lead_l3 OR lk.lead_l4)
        AND (lk.lead_domain NOT IN ('spot2.mx') OR lk.lead_domain IS NULL)
        AND lk.lead_deleted_at IS NULL
    ),
    base_nuevos AS (
        SELECT lb.lead_id, lb.primera_fecha_lead, lb.spot_sector, lb.lead_max_type,
               NULL::integer AS project_id, NULL::timestamp AS project_created_at,
               lb.primera_fecha_lead AS fecha_cohort
        FROM leads_base lb
        WHERE lb.primera_fecha_lead >= TIMESTAMP '2021-01-01'
          AND lb.primera_fecha_lead < CURRENT_DATE
    ),
    rows_proyectos AS (
        SELECT lb.lead_id, lb.primera_fecha_lead, lb.spot_sector, lb.lead_max_type,
               p.project_id, p.project_created_at,
               CASE
                   WHEN p.project_created_at IS NOT NULL
                    AND p.project_created_at >= lb.primera_fecha_lead + INTERVAL '30 days'
                       THEN p.project_created_at
                   ELSE lb.primera_fecha_lead
               END AS fecha_cohort
        FROM leads_base lb
        LEFT JOIN lk_projects p USING (lead_id)
        WHERE lb.primera_fecha_lead >= TIMESTAMP '2021-01-01'
          AND lb.primera_fecha_lead < CURRENT_DATE
          AND p.project_id IS NOT NULL
    ),
    universo_base AS (
        SELECT * FROM base_nuevos
        UNION ALL
        SELECT * FROM rows_proyectos
    )
    SELECT
        ub.lead_id, ub.primera_fecha_lead, ub.fecha_cohort, ub.project_id,
        ub.project_created_at,
        DATE(ub.fecha_cohort) AS cohort_date_real,
        CASE
            WHEN DATE(ub.fecha_cohort) = DATE(ub.primera_fecha_lead) THEN 'Nuevo'
            ELSE 'Reactivado'
        END AS cohort_type,
        COALESCE(m.channel, 'Unassigned') AS channel,
        'PoP' AS source
    FROM universo_base ub
    LEFT JOIN mat_lead_attrs m
        ON m.lead_id = ub.lead_id AND m.cohort_date = DATE(ub.fecha_cohort)
    WHERE DATE(ub.fecha_cohort) >= '2026-03-01'
      AND DATE(ub.fecha_cohort) <= (CURRENT_DATE - INTERVAL '1 day')::date
    """
    return run_q(sql, "PoP universo (marzo, reactivados)")


def get_funnel_reactivated() -> pd.DataFrame:
    """Extrae el universo de Projects Funnel."""
    sql = """
    WITH params AS (
        SELECT (CURRENT_DATE - INTERVAL '1 day')::date AS cutoff_date
    ),
    mat_lead_attrs AS (
        SELECT
            lead_id,
            lead_fecha_cohort_dt::date AS cohort_date,
            COALESCE(mat_channel, 'Unassigned') AS channel,
            COALESCE(mat_traffic_type, 'Unassigned') AS traffic_type,
            COALESCE(mat_campaign_name, 'Unassigned') AS campaign_name
        FROM (
            SELECT *, ROW_NUMBER() OVER(
                PARTITION BY lead_id, lead_fecha_cohort_dt::date
                ORDER BY mat_event_datetime DESC
            ) AS rn
            FROM lk_mat_matches_visitors_to_leads
        ) sub WHERE rn = 1
    ),
    leads_calculados AS (
        SELECT
            lk.lead_id,
            lk.lead_l0, lk.lead_l1, lk.lead_l2, lk.lead_l3, lk.lead_l4,
            lk.lead_domain,
            LEAST(
                COALESCE(lk.lead_lead0_at::timestamp, TIMESTAMP '9999-12-31'),
                COALESCE(lk.lead_lead1_at::timestamp, TIMESTAMP '9999-12-31'),
                COALESCE(lk.lead_lead2_at::timestamp, TIMESTAMP '9999-12-31'),
                COALESCE(lk.lead_lead3_at::timestamp, TIMESTAMP '9999-12-31'),
                COALESCE(lk.lead_lead4_at::timestamp, TIMESTAMP '9999-12-31')
            ) AS primera_fecha_lead
        FROM lk_leads lk
        WHERE lk.lead_deleted_at IS NULL
    ),
    universo_base AS (
        SELECT
            a11.lead_id, a11.primera_fecha_lead,
            CASE
                WHEN a12.project_created_at IS NOT NULL
                 AND a12.project_created_at >= a11.primera_fecha_lead + INTERVAL '30 days'
                    THEN a12.project_created_at
                ELSE a11.primera_fecha_lead
            END AS fecha_cohort,
            a12.project_id, a12.project_created_at
        FROM leads_calculados a11
        CROSS JOIN params p
        LEFT JOIN lk_projects a12
          ON a12.lead_id = a11.lead_id
         AND a12.project_created_at::date <= p.cutoff_date
        WHERE (a11.lead_l0 OR a11.lead_l1 OR a11.lead_l2 OR a11.lead_l3 OR a11.lead_l4)
          AND (a11.lead_domain NOT IN ('spot2.mx') OR a11.lead_domain IS NULL)
          AND a11.primera_fecha_lead::date >= DATE '2021-01-01'
          AND a11.primera_fecha_lead::date <= p.cutoff_date
    ),
    total AS (
        SELECT lead_id, primera_fecha_lead, fecha_cohort, project_id, project_created_at
        FROM universo_base WHERE project_id IS NOT NULL
        UNION ALL
        SELECT DISTINCT ON (lead_id)
            lead_id, primera_fecha_lead, primera_fecha_lead AS fecha_cohort,
            NULL::integer AS project_id, NULL::timestamp AS project_created_at
        FROM universo_base u1
        WHERE NOT EXISTS (
            SELECT 1 FROM universo_base u2
            WHERE u2.lead_id = u1.lead_id
              AND DATE_TRUNC('month', u2.fecha_cohort) = DATE_TRUNC('month', u1.primera_fecha_lead)
              AND u2.project_id IS NOT NULL
        )
    )
    SELECT
        t.lead_id, t.primera_fecha_lead, t.fecha_cohort, t.project_id,
        t.project_created_at,
        DATE(t.fecha_cohort) AS cohort_date_real,
        CASE
            WHEN DATE(t.fecha_cohort) = DATE(t.primera_fecha_lead) THEN 'Nuevo'
            ELSE 'Reactivado'
        END AS cohort_type,
        COALESCE(m.channel, 'Unassigned') AS channel,
        'Funnel' AS source
    FROM total t
    CROSS JOIN params p
    LEFT JOIN mat_lead_attrs m
        ON m.lead_id = t.lead_id AND m.cohort_date = DATE(t.fecha_cohort)
    WHERE DATE(t.fecha_cohort) >= '2026-03-01'
      AND DATE(t.fecha_cohort) <= p.cutoff_date
    """
    return run_q(sql, "Funnel universo (marzo, reactivados)")


def main():
    print("=" * 80)
    print("DIAGNÓSTICO: PoP vs Funnel — Reactivados Marzo 2026")
    print("=" * 80)

    pop = get_pop_reactivated()
    funnel = get_funnel_reactivated()

    pop_r = pop[pop["cohort_type"] == "Reactivado"].copy()
    funnel_r = funnel[funnel["cohort_type"] == "Reactivado"].copy()

    pop_org = pop_r[pop_r["channel"].isin(ORGANIC_CHANNELS)]
    funnel_org = funnel_r[funnel_r["channel"].isin(ORGANIC_CHANNELS)]

    pop_ids = set(pop_org["lead_id"].unique())
    funnel_ids = set(funnel_org["lead_id"].unique())

    print(f"\n  PoP  Reactivado + orgánicos, marzo: {len(pop_ids)} distinct leads")
    print(f"  Funnel Reactivado + orgánicos, marzo: {len(funnel_ids)} distinct leads")
    print(f"  Diferencia: {len(pop_ids) - len(funnel_ids)}")

    solo_pop = pop_ids - funnel_ids
    solo_funnel = funnel_ids - pop_ids

    if solo_pop:
        print(f"\n--- Leads solo en PoP ({len(solo_pop)}) ---")
        for lid in sorted(solo_pop):
            rows = pop_org[pop_org["lead_id"] == lid]
            for _, r in rows.iterrows():
                print(f"  lead={lid}, fecha_cohort={r['cohort_date_real']}, "
                      f"channel={r['channel']}, project_id={r['project_id']}, "
                      f"project_created_at={r['project_created_at']}")
            funnel_all = funnel[funnel["lead_id"] == lid]
            if not funnel_all.empty:
                print(f"  (en Funnel pero con otro cohort/canal):")
                for _, r in funnel_all.iterrows():
                    print(f"    cohort={r['cohort_type']}, fecha={r['cohort_date_real']}, "
                          f"channel={r['channel']}, project_id={r['project_id']}")

    if solo_funnel:
        print(f"\n--- Leads solo en Funnel ({len(solo_funnel)}) ---")
        for lid in sorted(solo_funnel):
            rows = funnel_org[funnel_org["lead_id"] == lid]
            for _, r in rows.iterrows():
                print(f"  lead={lid}, fecha_cohort={r['cohort_date_real']}, "
                      f"channel={r['channel']}, project_id={r['project_id']}")

    # También comparar totales por canal
    print("\n--- Distribución Reactivado por canal (distinct lead_id) ---")
    pop_ch = pop_r.groupby("channel")["lead_id"].nunique().rename("PoP")
    fun_ch = funnel_r.groupby("channel")["lead_id"].nunique().rename("Funnel")
    comp = pd.concat([pop_ch, fun_ch], axis=1).fillna(0).astype(int)
    comp["Diff"] = comp["PoP"] - comp["Funnel"]
    comp = comp.sort_values("PoP", ascending=False)
    print(comp.to_string())

    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
