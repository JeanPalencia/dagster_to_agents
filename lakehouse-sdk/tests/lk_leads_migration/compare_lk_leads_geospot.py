#!/usr/bin/env python3
"""
Comparacion directa en Geospot: lk_leads (governance) vs lk_leads_v2 (legacy).

Compara fila a fila y columna a columna usando lead_id como llave.
Excluye columnas aud_* de la comparacion de contenido pero las usa para diagnostico.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import polars as pl


DEFAULT_SCHEMA = "public"
DEFAULT_GOV_TABLE = "lk_leads"
DEFAULT_LEGACY_TABLE = "lk_leads_v2"
DEFAULT_SAMPLE = 20
PK = "lead_id"
AUD_DIAG_COL = "aud_updated_at"


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


def _query_columns(query_fn, schema: str, table: str) -> pl.DataFrame:
    q = f"""
    SELECT column_name, data_type, udt_name, ordinal_position
    FROM information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    return query_fn(q, source_type="geospot_postgres")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compara lk_leads vs lk_leads_v2 en Geospot")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--gov-table", default=DEFAULT_GOV_TABLE)
    parser.add_argument("--legacy-table", default=DEFAULT_LEGACY_TABLE)
    parser.add_argument("--sample-limit", type=int, default=DEFAULT_SAMPLE, help="0 = sin limite")
    parser.add_argument("--report-md", default="", help="Ruta custom del reporte")
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
            Path(__file__).resolve().parent / "reports" / f"lk_leads_comparison_{ts}.md"
        )
    report_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Comparando {gov_table} vs {legacy_table} (schema={schema})...")

    # --- 1. Schema discovery ---
    gov_cols_df = _query_columns(query_bronze_source, schema, gov_table)
    legacy_cols_df = _query_columns(query_bronze_source, schema, legacy_table)
    if gov_cols_df.height == 0:
        raise RuntimeError(f"No se encontraron columnas para {schema}.{gov_table}")
    if legacy_cols_df.height == 0:
        raise RuntimeError(f"No se encontraron columnas para {schema}.{legacy_table}")

    gov_col_names = gov_cols_df["column_name"].to_list()
    legacy_col_names_set = set(legacy_cols_df["column_name"].to_list())

    only_gov = [c for c in gov_col_names if c not in legacy_col_names_set]
    only_legacy = sorted(legacy_col_names_set - set(gov_col_names))
    common = [c for c in gov_col_names if c in legacy_col_names_set]
    content_cols = [c for c in common if not c.startswith("aud_")]
    aud_cols = [c for c in common if c.startswith("aud_")]

    if PK not in content_cols:
        raise RuntimeError(f"`{PK}` no esta en columnas comparables.")

    # --- 2. Type comparison ---
    legacy_type_map = {
        r["column_name"]: (r["data_type"], r["udt_name"])
        for r in legacy_cols_df.iter_rows(named=True)
    }
    type_rows = []
    for r in gov_cols_df.iter_rows(named=True):
        c = r["column_name"]
        if c not in common:
            continue
        g = (r["data_type"], r["udt_name"])
        l = legacy_type_map.get(c, ("<missing>", "<missing>"))
        if g != l:
            type_rows.append({
                "column": c,
                "gov_type": g[0],
                "gov_udt": g[1],
                "legacy_type": l[0],
                "legacy_udt": l[1],
            })
    type_mismatch_df = pl.DataFrame(type_rows) if type_rows else pl.DataFrame(
        {"column": [], "gov_type": [], "gov_udt": [], "legacy_type": [], "legacy_udt": []},
    )

    # --- 3. Load data ---
    gov_type_map = {
        r["column_name"]: r["udt_name"] for r in gov_cols_df.iter_rows(named=True)
    }
    jsonb_cols = {c for c in content_cols if gov_type_map.get(c) == "jsonb"}

    def _col_expr(c: str) -> str:
        if c in jsonb_cols:
            return f"{_quote_ident(c)}::text AS {_quote_ident(c)}"
        return _quote_ident(c)

    select_content = ", ".join(_col_expr(c) for c in content_cols)
    diag_col_sql = f", {_quote_ident(AUD_DIAG_COL)}" if AUD_DIAG_COL in aud_cols else ""

    q_gov = f"SELECT {select_content}{diag_col_sql} FROM {schema}.{_quote_ident(gov_table)}"
    q_leg = f"SELECT {select_content}{diag_col_sql} FROM {schema}.{_quote_ident(legacy_table)}"

    print("  Cargando governance...")
    df_gov = query_bronze_source(q_gov, source_type="geospot_postgres")
    print(f"  -> {df_gov.height} filas")
    print("  Cargando legacy...")
    df_leg = query_bronze_source(q_leg, source_type="geospot_postgres")
    print(f"  -> {df_leg.height} filas")

    # --- 4. Integrity by lead_id ---
    cnt_gov = df_gov.group_by(PK).agg(pl.len().alias("rows_gov"))
    cnt_leg = df_leg.group_by(PK).agg(pl.len().alias("rows_leg"))

    dup_gov = cnt_gov.filter(pl.col("rows_gov") > 1).sort("rows_gov", descending=True)
    dup_leg = cnt_leg.filter(pl.col("rows_leg") > 1).sort("rows_leg", descending=True)

    ids_only_gov = cnt_gov.join(cnt_leg, on=PK, how="anti").sort(PK)
    ids_only_leg = cnt_leg.join(cnt_gov, on=PK, how="anti").sort(PK)
    common_counts = cnt_gov.join(cnt_leg, on=PK, how="inner")
    ids_count_mismatch = common_counts.filter(pl.col("rows_gov") != pl.col("rows_leg"))
    aligned_ids = common_counts.filter(
        (pl.col("rows_gov") == 1) & (pl.col("rows_leg") == 1)
    ).select(PK)

    print(f"  IDs alineables 1-a-1: {aligned_ids.height}")

    # --- 5. Value comparison column by column ---
    a = df_gov.join(aligned_ids, on=PK, how="inner").sort(PK)
    b = df_leg.join(aligned_ids, on=PK, how="inner").sort(PK)

    has_diag = AUD_DIAG_COL in a.columns

    value_diffs: dict[str, pl.DataFrame] = {}
    value_diff_counts: list[dict] = []

    for col in content_cols:
        if col == PK:
            continue

        cols_to_select_a = [PK, col]
        cols_to_select_b = [PK, col]
        if has_diag:
            cols_to_select_a.append(AUD_DIAG_COL)
            cols_to_select_b.append(AUD_DIAG_COL)

        joined = a.select(cols_to_select_a).join(
            b.select(cols_to_select_b),
            on=PK,
            how="inner",
            suffix="_leg",
        )

        col_gov = col
        col_leg = f"{col}_leg"

        neq = ~(
            (
                pl.col(col_gov).cast(pl.Utf8, strict=False)
                == pl.col(col_leg).cast(pl.Utf8, strict=False)
            )
            | (pl.col(col_gov).is_null() & pl.col(col_leg).is_null())
        )

        diff = joined.filter(neq)

        if diff.height > 0:
            out_cols = [
                pl.col(PK),
                pl.col(col_gov).alias(f"{col}_gov"),
                pl.col(col_leg).alias(f"{col}_leg"),
            ]
            if has_diag:
                out_cols.append(pl.col(AUD_DIAG_COL).alias("aud_updated_at_gov"))
                out_cols.append(pl.col(f"{AUD_DIAG_COL}_leg").alias("aud_updated_at_leg"))
            diff_out = diff.select(out_cols)
            value_diffs[col] = diff_out
            value_diff_counts.append({"column": col, "diff_rows": diff_out.height})

    value_diff_counts_df = (
        pl.DataFrame(value_diff_counts).sort("diff_rows", descending=True)
        if value_diff_counts
        else pl.DataFrame({"column": [], "diff_rows": []}, schema={"column": pl.Utf8, "diff_rows": pl.Int64})
    )

    print(f"  Columnas con diferencias: {value_diff_counts_df.height}")

    # --- 6. Build markdown report ---
    L: list[str] = []

    L.append(f"# Comparacion: `{gov_table}` (governance) vs `{legacy_table}` (legacy)")
    L.append("")
    L.append(f"- Fecha: `{datetime.now().isoformat(timespec='seconds')}`")
    L.append(f"- Tabla governance: `{schema}.{gov_table}`")
    L.append(f"- Tabla legacy: `{schema}.{legacy_table}`")
    L.append(f"- Llave de comparacion: `{PK}`")
    L.append("")

    L.append("## Cobertura de columnas")
    L.append("")
    L.append(f"- Columnas governance: `{len(gov_col_names)}`")
    L.append(f"- Columnas legacy: `{len(legacy_cols_df)}`")
    L.append(f"- Columnas comunes: `{len(common)}` (contenido: `{len(content_cols)}`, auditoria: `{len(aud_cols)}`)")
    L.append(f"- Solo en governance: `{len(only_gov)}`")
    L.append(f"- Solo en legacy: `{len(only_legacy)}`")
    if only_gov:
        L.append(f"  - `{'`, `'.join(only_gov)}`")
    if only_legacy:
        L.append(f"  - `{'`, `'.join(only_legacy)}`")
    L.append("")

    L.append("## Diferencias de tipo en columnas comunes")
    L.append("")
    if type_mismatch_df.height:
        L.append(f"Se encontraron `{type_mismatch_df.height}` columnas con tipo distinto:")
        L.append("")
        L.append(_df_to_md(type_mismatch_df, 0))
        L.append("")
        L.append("_Nota: `text` vs `character varying` son equivalentes en PostgreSQL. "
                 "`smallint` vs `integer`/`bigint` y `smallint` vs `boolean` son diferencias de precision/tipo._")
    else:
        L.append("No se detectaron diferencias de tipos.")
    L.append("")

    L.append(f"## Integridad por `{PK}`")
    L.append("")
    L.append(f"- Filas governance: `{df_gov.height}`")
    L.append(f"- Filas legacy: `{df_leg.height}`")
    L.append(f"- Duplicados governance: `{dup_gov.height}`")
    L.append(f"- Duplicados legacy: `{dup_leg.height}`")
    L.append(f"- IDs solo en governance: `{ids_only_gov.height}`")
    L.append(f"- IDs solo en legacy: `{ids_only_leg.height}`")
    L.append(f"- IDs con multiplicidad distinta: `{ids_count_mismatch.height}`")
    L.append(f"- IDs alineables (1-a-1): `{aligned_ids.height}`")
    L.append("")

    if ids_only_gov.height:
        L.append(f"### `{PK}` solo en governance")
        L.append("")
        L.append(_df_to_md(ids_only_gov, sample_limit))
        L.append("")

    if ids_only_leg.height:
        L.append(f"### `{PK}` solo en legacy")
        L.append("")
        L.append(_df_to_md(ids_only_leg, sample_limit))
        L.append("")

    if dup_gov.height:
        L.append("### Duplicados en governance")
        L.append("")
        L.append(_df_to_md(dup_gov, sample_limit))
        L.append("")

    if dup_leg.height:
        L.append("### Duplicados en legacy")
        L.append("")
        L.append(_df_to_md(dup_leg, sample_limit))
        L.append("")

    L.append(f"## Comparacion de valores (`{PK}` alineables)")
    L.append("")
    L.append(f"- Columnas comparadas: `{len(content_cols) - 1}`")
    L.append(f"- Columnas con diferencias: `{value_diff_counts_df.height}`")
    L.append("")

    if value_diff_counts_df.height:
        L.append("### Resumen por columna")
        L.append("")
        L.append(_df_to_md(value_diff_counts_df, 0))
        L.append("")

        for row in value_diff_counts_df.iter_rows(named=True):
            c = row["column"]
            n = row["diff_rows"]
            L.append(f"### `{c}` ({n} diferencias)")
            L.append("")
            L.append(_df_to_md(value_diffs[c], sample_limit))
            L.append("")
    else:
        L.append("No se detectaron diferencias de valor en las columnas de contenido.")
        L.append("")

    report_text = "\n".join(L)
    report_path.write_text(report_text, encoding="utf-8")
    print(f"\nReporte guardado en: {report_path}")


if __name__ == "__main__":
    main()
