#!/usr/bin/env python3
"""
Compare lk_leads (legacy) vs lk_leads_v2 universes for funnel_ptd.

Runs the same leads_base / leads_base_filt CTEs against both tables
and compares: counts, exclusive lead_ids, date differences.

Usage (from dagster-pipeline directory):
    cd dagster-pipeline && uv run python ../../lakehouse-sdk/tests/funnel_ptd/compare_lk_leads_universes.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

DAGSTER_SRC = Path(__file__).resolve().parents[3] / "dagster-pipeline" / "src"
sys.path.insert(0, str(DAGSTER_SRC))

from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

LEADS_BASE_LEGACY = """
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
    FROM lk_leads l
    WHERE (l.lead_l0=1 OR l.lead_l1=1 OR l.lead_l2=1 OR l.lead_l3=1 OR l.lead_l4=1)
      AND (l.lead_domain NOT IN ('spot2.mx') OR l.lead_domain IS NULL)
      AND l.lead_deleted_at IS NULL
)
SELECT
    lead_id,
    primera_fecha_lead,
    primera_fecha_lead::date AS lead_first_date
FROM leads_base
WHERE primera_fecha_lead >= TIMESTAMP '2021-01-01'
  AND primera_fecha_lead < CURRENT_DATE
  AND primera_fecha_lead < TIMESTAMP '9999-12-31'
ORDER BY lead_id
"""

LEADS_BASE_V2 = """
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
)
SELECT
    lead_id,
    primera_fecha_lead,
    primera_fecha_lead::date AS lead_first_date
FROM leads_base
WHERE primera_fecha_lead >= TIMESTAMP '2021-01-01'
  AND primera_fecha_lead < CURRENT_DATE
  AND primera_fecha_lead < TIMESTAMP '9999-12-31'
ORDER BY lead_id
"""


def main() -> None:
    print("=" * 70)
    print("Comparación de universos: lk_leads (legacy) vs lk_leads_v2")
    print("=" * 70)

    print("\n▶ Consultando lk_leads (legacy)...")
    df_legacy = query_bronze_source(LEADS_BASE_LEGACY, source_type="geospot_postgres")
    df_legacy = df_legacy.cast({"lead_id": pl.Int64, "primera_fecha_lead": pl.Datetime("us"), "lead_first_date": pl.Date})
    print(f"  Filas: {df_legacy.height:,}  |  lead_id únicos: {df_legacy['lead_id'].n_unique():,}")

    print("\n▶ Consultando lk_leads_v2...")
    df_v2 = query_bronze_source(LEADS_BASE_V2, source_type="geospot_postgres")
    df_v2 = df_v2.cast({"lead_id": pl.Int64, "primera_fecha_lead": pl.Datetime("us"), "lead_first_date": pl.Date})
    print(f"  Filas: {df_v2.height:,}  |  lead_id únicos: {df_v2['lead_id'].n_unique():,}")

    # --- Lead IDs exclusivos ---
    ids_legacy = set(df_legacy["lead_id"].to_list())
    ids_v2 = set(df_v2["lead_id"].to_list())

    only_legacy = ids_legacy - ids_v2
    only_v2 = ids_v2 - ids_legacy
    common = ids_legacy & ids_v2

    print("\n" + "=" * 70)
    print("RESUMEN DE CONJUNTOS")
    print("=" * 70)
    print(f"  Leads en ambas tablas (intersección): {len(common):,}")
    print(f"  Solo en lk_leads (legacy):            {len(only_legacy):,}")
    print(f"  Solo en lk_leads_v2:                  {len(only_v2):,}")

    if only_legacy:
        sample = sorted(only_legacy)[:20]
        print(f"\n  Muestra de lead_ids solo en legacy ({min(len(only_legacy), 20)} de {len(only_legacy)}):")
        print(f"    {sample}")

    if only_v2:
        sample = sorted(only_v2)[:20]
        print(f"\n  Muestra de lead_ids solo en v2 ({min(len(only_v2), 20)} de {len(only_v2)}):")
        print(f"    {sample}")

    # --- Comparación row-by-row de fechas para leads comunes ---
    if common:
        leg = df_legacy.filter(pl.col("lead_id").is_in(list(common))).sort("lead_id")
        v2 = df_v2.filter(pl.col("lead_id").is_in(list(common))).sort("lead_id")

        merged = leg.select([
            pl.col("lead_id"),
            pl.col("primera_fecha_lead").alias("fecha_legacy"),
            pl.col("lead_first_date").alias("date_legacy"),
        ]).join(
            v2.select([
                pl.col("lead_id"),
                pl.col("primera_fecha_lead").alias("fecha_v2"),
                pl.col("lead_first_date").alias("date_v2"),
            ]),
            on="lead_id",
            how="inner",
        )

        date_mismatches = merged.filter(pl.col("date_legacy") != pl.col("date_v2"))
        ts_mismatches = merged.filter(pl.col("fecha_legacy") != pl.col("fecha_v2"))

        print("\n" + "=" * 70)
        print("COMPARACIÓN DE FECHAS (leads comunes)")
        print("=" * 70)
        print(f"  Leads comunes comparados:            {merged.height:,}")
        print(f"  Diferencias en lead_first_date:      {date_mismatches.height:,}")
        print(f"  Diferencias en primera_fecha_lead:   {ts_mismatches.height:,}")

        if ts_mismatches.height > 0:
            print(f"\n  Muestra de diferencias de timestamp ({min(ts_mismatches.height, 20)}):")
            sample = ts_mismatches.head(20)
            for row in sample.iter_rows(named=True):
                print(f"    lead_id={row['lead_id']:>8}  legacy={row['fecha_legacy']}  v2={row['fecha_v2']}")

        if date_mismatches.height > 0 and date_mismatches.height != ts_mismatches.height:
            print(f"\n  Muestra de diferencias de fecha ({min(date_mismatches.height, 20)}):")
            sample = date_mismatches.head(20)
            for row in sample.iter_rows(named=True):
                print(f"    lead_id={row['lead_id']:>8}  legacy={row['date_legacy']}  v2={row['date_v2']}")

    # --- Veredicto ---
    print("\n" + "=" * 70)
    print("VEREDICTO")
    print("=" * 70)

    issues = []
    if only_legacy:
        issues.append(f"{len(only_legacy)} leads solo en legacy")
    if only_v2:
        issues.append(f"{len(only_v2)} leads solo en v2")
    if common and date_mismatches.height > 0:
        issues.append(f"{date_mismatches.height} diferencias de fecha")

    if not issues:
        print("  ✅ Universos idénticos. Se puede proceder con lk_leads_v2.")
    else:
        print(f"  ⚠️  Diferencias detectadas: {', '.join(issues)}")
        print("  Analizar si son por datos más actualizados o lógica distinta.")
        if only_legacy and not only_v2 and (common and date_mismatches.height == 0):
            print("  → Solo hay leads legacy que faltan en v2 (posible limpieza).")
            print("  → Las fechas de los comunes son idénticas: SEGURO para migrar.")
        elif not only_legacy and only_v2 and (common and date_mismatches.height == 0):
            print("  → Solo hay leads nuevos en v2 (datos más actualizados).")
            print("  → Las fechas de los comunes son idénticas: SEGURO para migrar.")


if __name__ == "__main__":
    main()
