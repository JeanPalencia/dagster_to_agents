#!/usr/bin/env python3
"""
Validation: compares core_bt_spot_amenities (Dagster flow) vs the original
SQL query executed directly against MySQL S2P.

Both sources must match 100% — same rows, same columns, same values.

Usage:
    python validate_bt_spot_amenities.py
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import polars as pl


ORIGINAL_QUERY = """
WITH amts AS (
    SELECT
        id AS spa_amenity_id,
        name AS spa_amenity_name,
        description AS spa_amenity_description,
        CASE category
            WHEN 0 THEN 1
            WHEN 1 THEN 2
            ELSE 0
        END AS spa_amenity_category_id,
        CASE category
            WHEN 0 THEN 'Servicio'
            WHEN 1 THEN 'Amenidad'
            ELSE 'Unknown'
        END AS spa_amenity_category,
        created_at AS spa_amenity_created_at,
        updated_at AS spa_amenity_updated_at
    FROM
        amenities
),

bt_spot_amenities AS (
    SELECT
        spa.id AS spa_id,
        spa.spot_id,
        spa.amenity_id AS spa_amenity_id,
        amts.spa_amenity_name,
        amts.spa_amenity_description,
        amts.spa_amenity_category_id,
        amts.spa_amenity_category,
        amts.spa_amenity_created_at,
        amts.spa_amenity_updated_at,
        spa.created_at AS spa_created_at,
        spa.updated_at AS spa_updated_at
    FROM
        spot_amenities AS spa
    LEFT JOIN
        amts ON amts.spa_amenity_id = spa.amenity_id
)

SELECT * FROM bt_spot_amenities
"""

PK = "spa_id"

FINAL_COLUMNS = [
    "spa_id",
    "spot_id",
    "spa_amenity_id",
    "spa_amenity_name",
    "spa_amenity_description",
    "spa_amenity_category_id",
    "spa_amenity_category",
    "spa_amenity_created_at",
    "spa_amenity_updated_at",
    "spa_created_at",
    "spa_updated_at",
]


def _ensure_import_paths() -> None:
    root = Path(__file__).resolve().parents[3]
    dagster_src = root / "dagster-pipeline" / "src"
    if str(dagster_src) not in sys.path:
        sys.path.insert(0, str(dagster_src))


def _escape_md(v) -> str:
    if v is None:
        return "null"
    return str(v).replace("|", "\\|").replace("\n", " ")


def _df_to_md(df: pl.DataFrame, limit: int = 20) -> str:
    view = df if limit <= 0 else df.head(limit)
    if view.height == 0:
        return "_sin filas_"
    lines = [
        "| " + " | ".join(view.columns) + " |",
        "| " + " | ".join(["---"] * len(view.columns)) + " |",
    ]
    for row in view.iter_rows(named=True):
        lines.append("| " + " | ".join(_escape_md(row[c]) for c in view.columns) + " |")
    if limit > 0 and df.height > view.height:
        lines.append("")
        lines.append(f"_Mostrando {view.height} de {df.height} filas._")
    return "\n".join(lines)


def main() -> None:
    _ensure_import_paths()
    from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = (
        Path(__file__).resolve().parent / "reports" / f"validation_{ts}.md"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)

    # --- 1. Get expected (SQL original against MySQL) ---
    print("Ejecutando query original contra MySQL S2P...")
    df_expected = query_bronze_source(ORIGINAL_QUERY, source_type="mysql_prod")
    df_expected = df_expected.select(FINAL_COLUMNS)
    for col in df_expected.columns:
        df_expected = df_expected.with_columns(pl.col(col).cast(pl.Utf8, strict=False))
    print(f"  -> {df_expected.height:,} filas")

    # --- 2. Get actual (core_bt_spot_amenities via Dagster) ---
    print("Materializando core_bt_spot_amenities via Dagster assets...")

    from dagster_pipeline.defs.spot_amenities.bronze.raw_s2p_amenities import raw_s2p_amenities
    from dagster_pipeline.defs.spot_amenities.bronze.raw_s2p_spot_amenities import raw_s2p_spot_amenities
    from dagster_pipeline.defs.spot_amenities.silver.stg.stg_s2p_amenities import stg_s2p_amenities
    from dagster_pipeline.defs.spot_amenities.silver.stg.stg_s2p_spot_amenities import stg_s2p_spot_amenities
    from dagster_pipeline.defs.spot_amenities.silver.core.core_bt_spot_amenities import core_bt_spot_amenities
    from dagster import build_asset_context

    ctx = build_asset_context()

    raw_am = raw_s2p_amenities(ctx)
    raw_spa = raw_s2p_spot_amenities(ctx)
    stg_am = stg_s2p_amenities(ctx, raw_am)
    stg_spa = stg_s2p_spot_amenities(ctx, raw_spa)
    df_actual = core_bt_spot_amenities(ctx, stg_spa, stg_am)

    df_actual = df_actual.select(FINAL_COLUMNS)
    for col in df_actual.columns:
        df_actual = df_actual.with_columns(pl.col(col).cast(pl.Utf8, strict=False))
    print(f"  -> {df_actual.height:,} filas")

    # --- 3. Compare ---
    print("Comparando...")

    L: list[str] = []
    L.append("# Validacion: core_bt_spot_amenities vs SQL original")
    L.append("")
    L.append(f"- Fecha: `{datetime.now().isoformat(timespec='seconds')}`")
    L.append(f"- Filas esperadas (SQL): `{df_expected.height:,}`")
    L.append(f"- Filas obtenidas (Core): `{df_actual.height:,}`")
    L.append("")

    passed = True

    # Row count
    if df_expected.height != df_actual.height:
        L.append(f"**FAIL**: Row count mismatch: expected {df_expected.height}, got {df_actual.height}")
        L.append("")
        passed = False

    # IDs coverage
    ids_expected = set(df_expected[PK].to_list())
    ids_actual = set(df_actual[PK].to_list())
    only_expected = ids_expected - ids_actual
    only_actual = ids_actual - ids_expected

    if only_expected:
        L.append(f"**FAIL**: {len(only_expected)} IDs solo en SQL: `{sorted(list(only_expected))[:20]}`")
        L.append("")
        passed = False
    if only_actual:
        L.append(f"**FAIL**: {len(only_actual)} IDs solo en Core: `{sorted(list(only_actual))[:20]}`")
        L.append("")
        passed = False

    # Value comparison
    common_ids = sorted(ids_expected & ids_actual)
    if common_ids:
        a = df_expected.filter(pl.col(PK).is_in(common_ids)).sort(PK)
        b = df_actual.filter(pl.col(PK).is_in(common_ids)).sort(PK)

        value_diff_counts = []
        value_diffs: dict[str, pl.DataFrame] = {}

        for col in FINAL_COLUMNS:
            if col == PK:
                continue
            joined = a.select([PK, pl.col(col).alias("expected")]).join(
                b.select([PK, pl.col(col).alias("actual")]),
                on=PK,
                how="inner",
            )
            neq = ~(
                (pl.col("expected") == pl.col("actual"))
                | (pl.col("expected").is_null() & pl.col("actual").is_null())
            )
            diff = joined.filter(neq)
            if diff.height > 0:
                value_diff_counts.append({"column": col, "diff_rows": diff.height})
                value_diffs[col] = diff
                passed = False

        if value_diff_counts:
            vdc = pl.DataFrame(value_diff_counts).sort("diff_rows", descending=True)
            L.append("## Diferencias de valor")
            L.append("")
            L.append(_df_to_md(vdc, 0))
            L.append("")
            for row in vdc.iter_rows(named=True):
                c = row["column"]
                n = row["diff_rows"]
                L.append(f"### `{c}` ({n} diferencias)")
                L.append("")
                L.append(_df_to_md(value_diffs[c], 20))
                L.append("")
        else:
            L.append("## Resultado: todas las columnas coinciden al 100%")
            L.append("")
    else:
        L.append("**FAIL**: No hay IDs en comun para comparar.")
        L.append("")
        passed = False

    # Summary
    status = "PASS" if passed else "FAIL"
    L.insert(2, f"- Resultado: **{status}**")

    report_text = "\n".join(L)
    report_path.write_text(report_text, encoding="utf-8")
    print(f"\nResultado: {status}")
    print(f"Reporte: {report_path}")

    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
