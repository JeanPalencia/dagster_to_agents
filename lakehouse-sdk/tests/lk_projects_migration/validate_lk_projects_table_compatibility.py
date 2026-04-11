#!/usr/bin/env python3
"""
Valida compatibilidad 100% entre lk_projects_governance y lk_projects.

Comparaciones:
1) Esquema por nombre de columna (el orden no importa).
2) Columna por columna (multiconjunto de valores, con conteo; orden no importa).
3) Fila por fila (multiconjunto de filas completas, con conteo; orden no importa).

Uso recomendado (desde dagster-pipeline):
    cd dagster-pipeline && uv run python ../lakehouse-sdk/tests/lk_projects_migration/validate_lk_projects_table_compatibility.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl


TABLE_A = "lk_projects_governance"
TABLE_B = "lk_projects"
SAMPLE_LIMIT = 20


def _cast_to_comparable(df: pl.DataFrame, col: str) -> pl.Expr:
    """
    Castea una columna a representación comparable entre tablas.
    Si el dtype difiere entre tablas, se comparará como texto.
    """
    return pl.col(col).cast(pl.Utf8)


def _compare_single_column_multiset(
    left: pl.DataFrame,
    right: pl.DataFrame,
    col: str,
) -> tuple[bool, pl.DataFrame]:
    left_counts = (
        left.select(pl.col(col))
        .group_by(col)
        .agg(pl.len().alias("__count_left"))
    )
    right_counts = (
        right.select(pl.col(col))
        .group_by(col)
        .agg(pl.len().alias("__count_right"))
    )

    diff = (
        left_counts.join(right_counts, on=col, how="full")
        .with_columns(
            pl.col("__count_left").fill_null(0),
            pl.col("__count_right").fill_null(0),
        )
        .filter(pl.col("__count_left") != pl.col("__count_right"))
        .sort("__count_left", descending=True)
    )
    return diff.height == 0, diff


def _compare_row_multiset(left: pl.DataFrame, right: pl.DataFrame, columns: list[str]) -> tuple[bool, pl.DataFrame]:
    left_counts = left.group_by(columns).agg(pl.len().alias("__count_left"))
    right_counts = right.group_by(columns).agg(pl.len().alias("__count_right"))

    diff = (
        left_counts.join(right_counts, on=columns, how="full")
        .with_columns(
            pl.col("__count_left").fill_null(0),
            pl.col("__count_right").fill_null(0),
        )
        .filter(pl.col("__count_left") != pl.col("__count_right"))
    )
    return diff.height == 0, diff


def main() -> None:
    dagster_src = Path(__file__).resolve().parents[3] / "dagster-pipeline" / "src"
    sys.path.insert(0, str(dagster_src))

    from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

    print("=" * 80)
    print(f"Validación de compatibilidad: {TABLE_A} vs {TABLE_B}")
    print("=" * 80)

    print(f"\n▶ Leyendo tabla A: {TABLE_A}")
    df_a = query_bronze_source(f"SELECT * FROM {TABLE_A}", source_type="geospot_postgres")
    print(f"  A -> filas={df_a.height:,}, columnas={df_a.width}")

    print(f"\n▶ Leyendo tabla B: {TABLE_B}")
    df_b = query_bronze_source(f"SELECT * FROM {TABLE_B}", source_type="geospot_postgres")
    print(f"  B -> filas={df_b.height:,}, columnas={df_b.width}")

    cols_a = set(df_a.columns)
    cols_b = set(df_b.columns)
    only_a = sorted(cols_a - cols_b)
    only_b = sorted(cols_b - cols_a)

    print("\n" + "-" * 80)
    print("1) Validación de columnas (ignorando orden)")
    print("-" * 80)
    if only_a or only_b:
        if only_a:
            print(f"  ❌ Solo en {TABLE_A} ({len(only_a)}): {only_a}")
        if only_b:
            print(f"  ❌ Solo en {TABLE_B} ({len(only_b)}): {only_b}")
        print("\nVeredicto: NO compatibles por esquema.")
        sys.exit(1)
    print(f"  ✅ Mismas columnas ({len(cols_a)})")

    common_cols = sorted(cols_a)

    # Si el dtype difiere entre tablas para una columna, comparamos ambos como Utf8.
    cast_cols_a: list[pl.Expr] = []
    cast_cols_b: list[pl.Expr] = []
    dtype_mismatches: list[str] = []
    for c in common_cols:
        dt_a = df_a.schema[c]
        dt_b = df_b.schema[c]
        if dt_a != dt_b:
            dtype_mismatches.append(f"{c}: {dt_a} vs {dt_b}")
            cast_cols_a.append(_cast_to_comparable(df_a, c).alias(c))
            cast_cols_b.append(_cast_to_comparable(df_b, c).alias(c))
        else:
            cast_cols_a.append(pl.col(c))
            cast_cols_b.append(pl.col(c))

    df_a_cmp = df_a.select(cast_cols_a)
    df_b_cmp = df_b.select(cast_cols_b)

    if dtype_mismatches:
        print("\n⚠️ Dtypes distintos detectados (se comparan como texto en esas columnas):")
        for d in dtype_mismatches:
            print(f"  - {d}")

    print("\n" + "-" * 80)
    print("2) Validación columna a columna (multiconjunto de valores)")
    print("-" * 80)
    bad_columns: list[tuple[str, pl.DataFrame]] = []
    for idx, c in enumerate(common_cols, start=1):
        ok_col, diff_col = _compare_single_column_multiset(df_a_cmp, df_b_cmp, c)
        if not ok_col:
            bad_columns.append((c, diff_col))
            print(f"  ❌ [{idx:>3}/{len(common_cols)}] {c} -> diferencias")
        else:
            print(f"  ✅ [{idx:>3}/{len(common_cols)}] {c}")

    print("\n" + "-" * 80)
    print("3) Validación fila a fila (multiconjunto de filas completas)")
    print("-" * 80)
    ok_rows, diff_rows = _compare_row_multiset(df_a_cmp, df_b_cmp, common_cols)
    if ok_rows:
        print("  ✅ Filas idénticas ignorando orden (incluyendo duplicados).")
    else:
        print(f"  ❌ Filas diferentes: {diff_rows.height:,} combinaciones con conteo distinto.")
        print(f"  Muestra ({min(SAMPLE_LIMIT, diff_rows.height)}):")
        print(diff_rows.head(SAMPLE_LIMIT))

    print("\n" + "=" * 80)
    if not bad_columns and ok_rows:
        print("RESULTADO FINAL: ✅ 100% compatibles.")
        print("Las tablas tienen el mismo contenido columna a columna y fila a fila.")
        sys.exit(0)

    print("RESULTADO FINAL: ❌ NO compatibles.")
    if bad_columns:
        print(f"- Columnas con diferencias: {len(bad_columns)}")
        for c, diff in bad_columns[:10]:
            print(f"  * {c}: {diff.height:,} valores con conteo distinto")
            print(diff.head(min(SAMPLE_LIMIT, diff.height)))
        if len(bad_columns) > 10:
            print(f"  ... y {len(bad_columns) - 10} columnas adicionales con diferencias.")
    if not ok_rows:
        print("- También hay diferencias a nivel de filas completas.")
    print("=" * 80)
    sys.exit(1)


if __name__ == "__main__":
    main()
