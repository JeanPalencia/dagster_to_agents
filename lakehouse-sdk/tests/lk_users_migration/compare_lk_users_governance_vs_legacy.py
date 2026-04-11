#!/usr/bin/env python3
"""
Comparacion general: lk_users_governance (new) vs lk_users (legacy) en Geospot.

Objetivo:
- Detectar diferencias de tipos y valores por user_id.
- Usar SOLO columnas existentes en governance (lista automatica desde information_schema).
- Ignorar columnas deprecated en legacy de forma implicita (no aparecen en governance).
- Verificar a que tabla escribe el flujo actual (gold_lk_users_new).
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import polars as pl


DEFAULT_SCHEMA = "public"
DEFAULT_GOV_TABLE = "lk_users_governance"
DEFAULT_LEGACY_TABLE = "lk_users"
DEFAULT_SAMPLE = 20


def _ensure_import_paths() -> None:
    root = Path(__file__).resolve().parents[3]
    dagster_src = root / "dagster-pipeline" / "src"
    if str(dagster_src) not in sys.path:
        sys.path.insert(0, str(dagster_src))


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _escape_md(v) -> str:
    if v is None:
        return "null"
    return str(v).replace("|", "\\|").replace("\n", " ")


def _df_to_md(df: pl.DataFrame, limit: int) -> str:
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


def _query_columns(query_bronze_source, schema: str, table: str) -> pl.DataFrame:
    q = f"""
    SELECT
      column_name,
      data_type,
      udt_name,
      ordinal_position
    FROM information_schema.columns
    WHERE table_schema = '{schema}'
      AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    return query_bronze_source(q, source_type="geospot_postgres")


def _extract_current_load_target() -> str | None:
    file_path = Path("/home/luis/Spot2/dagster/dagster-pipeline/src/dagster_pipeline/defs/data_lakehouse/gold/gold_lk_users_new.py")
    if not file_path.exists():
        return None
    content = file_path.read_text(encoding="utf-8")
    m = re.search(r'load_to_geospot\([^)]*table_name\s*=\s*"([^"]+)"', content, flags=re.S)
    if m:
        return m.group(1)
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--gov-table", default=DEFAULT_GOV_TABLE)
    parser.add_argument("--legacy-table", default=DEFAULT_LEGACY_TABLE)
    parser.add_argument("--sample-limit", type=int, default=DEFAULT_SAMPLE, help="0 para completo.")
    parser.add_argument("--report-md", default="", help="Ruta del reporte markdown.")
    args = parser.parse_args()

    _ensure_import_paths()
    from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

    schema = args.schema
    gov_table = args.gov_table
    legacy_table = args.legacy_table
    sample_limit = args.sample_limit

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.report_md:
        report_path = Path(args.report_md).resolve()
    else:
        report_path = (
            Path(__file__).resolve().parent
            / "reports"
            / f"lk_users_general_comparison_{ts}.md"
        )
    report_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Comparando {gov_table} vs {legacy_table} (schema={schema})...")

    # 1) Verificar destino actual del flujo new
    current_target = _extract_current_load_target()

    # 2) Esquemas
    gov_cols_df = _query_columns(query_bronze_source, schema, gov_table)
    legacy_cols_df = _query_columns(query_bronze_source, schema, legacy_table)
    if gov_cols_df.height == 0:
        raise RuntimeError(f"No se encontraron columnas para {schema}.{gov_table}")
    if legacy_cols_df.height == 0:
        raise RuntimeError(f"No se encontraron columnas para {schema}.{legacy_table}")

    gov_cols = gov_cols_df["column_name"].to_list()
    legacy_cols = set(legacy_cols_df["column_name"].to_list())
    missing_in_legacy = [c for c in gov_cols if c not in legacy_cols]
    comparable_cols = [c for c in gov_cols if c in legacy_cols]
    if "user_id" not in comparable_cols:
        raise RuntimeError("`user_id` no esta en columnas comparables.")

    # 3) Dataframes completos usando solo columnas governance
    select_cols_sql = ", ".join(_quote_ident(c) for c in comparable_cols)
    q_gov = f"SELECT {select_cols_sql} FROM {schema}.{_quote_ident(gov_table)}"
    q_legacy = f"SELECT {select_cols_sql} FROM {schema}.{_quote_ident(legacy_table)}"
    df_gov = query_bronze_source(q_gov, source_type="geospot_postgres")
    df_legacy = query_bronze_source(q_legacy, source_type="geospot_postgres")

    # 4) Validaciones por user_id (duplicados, ids faltantes, etc.)
    cnt_gov = df_gov.group_by("user_id").agg(pl.len().alias("rows_gov"))
    cnt_legacy = df_legacy.group_by("user_id").agg(pl.len().alias("rows_legacy"))
    dup_gov = cnt_gov.filter(pl.col("rows_gov") > 1).sort("rows_gov", descending=True)
    dup_legacy = cnt_legacy.filter(pl.col("rows_legacy") > 1).sort("rows_legacy", descending=True)

    ids_only_gov = cnt_gov.join(cnt_legacy, on="user_id", how="anti").sort("user_id")
    ids_only_legacy = cnt_legacy.join(cnt_gov, on="user_id", how="anti").sort("user_id")
    common_counts = cnt_gov.join(cnt_legacy, on="user_id", how="inner")
    ids_count_mismatch = common_counts.filter(pl.col("rows_gov") != pl.col("rows_legacy"))
    aligned_ids = common_counts.filter((pl.col("rows_gov") == 1) & (pl.col("rows_legacy") == 1)).select("user_id")

    # 5) Comparacion de tipos (Postgres catalog)
    legacy_type_map = {
        r["column_name"]: (r["data_type"], r["udt_name"])
        for r in legacy_cols_df.iter_rows(named=True)
    }
    type_rows = []
    for r in gov_cols_df.iter_rows(named=True):
        c = r["column_name"]
        if c not in comparable_cols:
            continue
        gov_type = (r["data_type"], r["udt_name"])
        leg_type = legacy_type_map.get(c, ("<missing>", "<missing>"))
        type_rows.append({
            "column_name": c,
            "gov_data_type": gov_type[0],
            "gov_udt_name": gov_type[1],
            "legacy_data_type": leg_type[0],
            "legacy_udt_name": leg_type[1],
            "type_match": gov_type == leg_type,
        })
    type_cmp_df = pl.DataFrame(type_rows)
    type_mismatch_df = type_cmp_df.filter(~pl.col("type_match"))

    # 6) Comparacion de valores por user_id en IDs alineables
    a = df_gov.join(aligned_ids, on="user_id", how="inner").sort("user_id")
    b = df_legacy.join(aligned_ids, on="user_id", how="inner").sort("user_id")
    value_diffs: dict[str, pl.DataFrame] = {}
    value_diff_counts = []

    for col in comparable_cols:
        if col == "user_id":
            continue
        joined = a.select(["user_id", col]).join(
            b.select(["user_id", col]),
            on="user_id",
            how="inner",
            suffix="_legacy",
        )
        col_gov = col
        col_legacy = f"{col}_legacy"
        neq = ~(
            (pl.col(col_gov).cast(pl.Utf8, strict=False) == pl.col(col_legacy).cast(pl.Utf8, strict=False))
            | (pl.col(col_gov).is_null() & pl.col(col_legacy).is_null())
        )
        diff = (
            joined
            .filter(neq)
            .select([
                "user_id",
                pl.col(col_gov).alias(f"{col}_gov"),
                pl.col(col_legacy).alias(f"{col}_legacy"),
            ])
        )
        if diff.height > 0:
            value_diffs[col] = diff
            value_diff_counts.append({"column_name": col, "diff_rows": diff.height})

    value_diff_counts_df = (
        pl.DataFrame(value_diff_counts).sort("diff_rows", descending=True)
        if value_diff_counts
        else pl.DataFrame({"column_name": [], "diff_rows": []}, schema={"column_name": pl.Utf8, "diff_rows": pl.Int64})
    )

    # 7) Hallazgo previo sobre spots eliminados (analisis de codigo)
    deleted_filter_scope = (
        "En gold_lk_users_new, el filtro deleted_at IS NULL se aplica explicitamente al recomputar user_spot_count "
        "desde stg_s2p_spots_new; las otras metricas de spots se agregan desde stg_lk_spots_new en _agg_spots_by_user."
    )

    # 8) Construir reporte markdown
    lines: list[str] = []
    lines.append("# Informe general: `lk_users_governance` vs `lk_users`")
    lines.append("")
    lines.append(f"- Fecha: `{datetime.now().isoformat(timespec='seconds')}`")
    lines.append(f"- Tabla governance: `{schema}.{gov_table}`")
    lines.append(f"- Tabla legacy: `{schema}.{legacy_table}`")
    lines.append(f"- Llave de comparacion: `user_id`")
    lines.append("")
    lines.append("## Verificacion del flujo actual")
    lines.append("")
    lines.append(f"- Destino detectado en `gold_lk_users_new.py`: `{current_target}`")
    if current_target != gov_table:
        lines.append(f"- ⚠️ El destino detectado no coincide con `{gov_table}`.")
    else:
        lines.append("- ✅ El flujo actual escribe en la tabla governance esperada.")
    lines.append("")
    lines.append("## Cobertura de columnas")
    lines.append("")
    lines.append(f"- Columnas en governance: `{len(gov_cols)}`")
    lines.append(f"- Columnas comparables en legacy: `{len(comparable_cols)}`")
    lines.append(f"- Columnas governance faltantes en legacy: `{len(missing_in_legacy)}`")
    if missing_in_legacy:
        lines.append("")
        lines.append("Columnas faltantes en legacy:")
        lines.append("```")
        lines.append("\n".join(missing_in_legacy))
        lines.append("```")
    lines.append("")
    lines.append("## Integridad por `user_id`")
    lines.append("")
    lines.append(f"- Filas governance: `{df_gov.height}`")
    lines.append(f"- Filas legacy: `{df_legacy.height}`")
    lines.append(f"- `user_id` duplicados en governance: `{dup_gov.height}`")
    lines.append(f"- `user_id` duplicados en legacy: `{dup_legacy.height}`")
    lines.append(f"- IDs solo en governance: `{ids_only_gov.height}`")
    lines.append(f"- IDs solo en legacy: `{ids_only_legacy.height}`")
    lines.append(f"- IDs con distinta multiplicidad: `{ids_count_mismatch.height}`")
    lines.append(f"- IDs alineables (1 a 1): `{aligned_ids.height}`")
    lines.append("")
    if dup_gov.height:
        lines.append("### Duplicados en governance")
        lines.append("")
        lines.append(_df_to_md(dup_gov, sample_limit))
        lines.append("")
    if dup_legacy.height:
        lines.append("### Duplicados en legacy")
        lines.append("")
        lines.append(_df_to_md(dup_legacy, sample_limit))
        lines.append("")
    if ids_only_gov.height:
        lines.append("### IDs solo en governance")
        lines.append("")
        lines.append(_df_to_md(ids_only_gov, sample_limit))
        lines.append("")
    if ids_only_legacy.height:
        lines.append("### IDs solo en legacy")
        lines.append("")
        lines.append(_df_to_md(ids_only_legacy, sample_limit))
        lines.append("")

    lines.append("## Comparacion de tipos de columnas")
    lines.append("")
    lines.append(f"- Columnas con tipo distinto: `{type_mismatch_df.height}`")
    lines.append("")
    if type_mismatch_df.height:
        lines.append(_df_to_md(type_mismatch_df, sample_limit))
    else:
        lines.append("No se detectaron diferencias de tipos en columnas comparables.")
    lines.append("")

    lines.append("## Comparacion de valores por columna (`user_id` alineables)")
    lines.append("")
    lines.append(f"- Columnas con diferencias de valor: `{value_diff_counts_df.height}`")
    if value_diff_counts_df.height:
        lines.append("")
        lines.append(_df_to_md(value_diff_counts_df, sample_limit if sample_limit > 0 else 0))
        lines.append("")
        for row in value_diff_counts_df.iter_rows(named=True):
            c = row["column_name"]
            lines.append(f"### Columna `{c}`")
            lines.append("")
            lines.append(_df_to_md(value_diffs[c], sample_limit))
            lines.append("")
    else:
        lines.append("No se detectaron diferencias de valor en las columnas comparables.")
        lines.append("")

    lines.append("## Consideracion adicional sobre metricas de spots")
    lines.append("")
    lines.append(f"- {deleted_filter_scope}")
    lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Reporte guardado en: {report_path}")


if __name__ == "__main__":
    main()
