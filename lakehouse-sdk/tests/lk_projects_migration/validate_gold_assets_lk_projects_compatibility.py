#!/usr/bin/env python3
"""
Valida compatibilidad entre los assets finales:
- gold_lk_projects (flujo legacy)
- gold_lk_projects_new (flujo governance renombrado)

Importante:
- Compara outputs de assets (NO tablas en Geospot).
- Fuerza refresco de upstream para minimizar diferencias por snapshots.
- Ignora orden de columnas y filas.
- Excluye columnas de auditoría (timestamps de corrida).

Uso:
  cd dagster-pipeline && uv run python ../lakehouse-sdk/tests/lk_projects_migration/validate_gold_assets_lk_projects_compatibility.py --partition 2026-03-03
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl


AUDIT_COLS = {
    "aud_inserted_at",
    "aud_inserted_date",
    "aud_updated_at",
    "aud_updated_date",
    "aud_job",
}
SAMPLE_LIMIT = 20
NEW_RAW_ASSETS = (
    "raw_s2p_project_requirements_new",
    "raw_s2p_clients_new",
    "raw_s2p_profiles_new",
    "raw_s2p_calendar_appointments_new",
    "raw_s2p_calendar_appointment_dates_new",
    "raw_s2p_activity_log_projects_new",
    "raw_s2p_client_event_histories_new",
)
NEW_UPSTREAM_ASSETS = (
    "stg_s2p_projects_new",
    "stg_s2p_clients_new",
    "stg_s2p_profiles_new",
    "stg_s2p_appointments_new",
    "stg_s2p_apd_appointment_dates_new",
    "stg_s2p_alg_activity_log_projects_new",
    "stg_s2p_leh_lead_event_histories_new",
    "core_project_funnel_new",
)
LEGACY_UPSTREAM_ASSETS = (
    "stg_s2p_projects",
    "stg_s2p_leads",
    "stg_s2p_user_profiles",
    "stg_s2p_appointments",
    "stg_s2p_apd_appointment_dates",
    "stg_s2p_alg_activity_log_projects",
    "stg_s2p_leh_lead_event_histories",
    "core_project_funnel",
)


def _default_partition() -> str:
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def _ensure_import_paths() -> None:
    root = Path(__file__).resolve().parents[3]
    sdk_src = root / "lakehouse-sdk" / "src"
    dagster_src = root / "dagster-pipeline" / "src"
    for p in (str(sdk_src), str(dagster_src)):
        if p not in sys.path:
            sys.path.insert(0, p)


def _to_comparable(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """
    Normaliza para comparación robusta:
    - Castea todas las columnas a Utf8 para evitar falsos positivos por dtype.
    - Mantiene nulls como nulls.
    """
    exprs: list[pl.Expr] = []
    for c in cols:
        dt = df.schema[c]
        if isinstance(dt, pl.List):
            # List[...]: string representation to compare across legacy/new
            exprs.append(
                pl.col(c)
                .map_elements(lambda v: None if v is None else str(v), return_dtype=pl.Utf8)
                .alias(c)
            )
        else:
            exprs.append(pl.col(c).cast(pl.Utf8, strict=False).alias(c))
    return df.select(exprs)


def _compare_col_multiset(a: pl.DataFrame, b: pl.DataFrame, col: str) -> tuple[bool, pl.DataFrame]:
    a_counts = a.group_by(col).agg(pl.len().alias("__count_a"))
    b_counts = b.group_by(col).agg(pl.len().alias("__count_b"))
    diff = (
        a_counts.join(b_counts, on=col, how="full")
        .with_columns(
            pl.col("__count_a").fill_null(0),
            pl.col("__count_b").fill_null(0),
        )
        .filter(pl.col("__count_a") != pl.col("__count_b"))
        .sort("__count_a", descending=True)
    )
    return diff.height == 0, diff


def _compare_row_multiset(a: pl.DataFrame, b: pl.DataFrame, cols: list[str]) -> tuple[bool, pl.DataFrame]:
    a_counts = a.group_by(cols).agg(pl.len().alias("__count_a"))
    b_counts = b.group_by(cols).agg(pl.len().alias("__count_b"))
    diff = (
        a_counts.join(b_counts, on=cols, how="full")
        .with_columns(
            pl.col("__count_a").fill_null(0),
            pl.col("__count_b").fill_null(0),
        )
        .filter(pl.col("__count_a") != pl.col("__count_b"))
    )
    return diff.height == 0, diff


def _display_limit_text(limit: int, height: int) -> str:
    return "completa" if limit <= 0 else str(min(limit, height))


def _show_df(df: pl.DataFrame, limit: int) -> pl.DataFrame:
    return df if limit <= 0 else df.head(limit)


def _escape_md(v) -> str:
    if v is None:
        return "null"
    s = str(v)
    s = s.replace("|", "\\|").replace("\n", " ")
    return s


def _df_to_markdown(df: pl.DataFrame, limit: int = 0) -> str:
    d = _show_df(df, limit)
    if d.height == 0:
        return "_sin filas_"
    headers = d.columns
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in d.iter_rows(named=True):
        lines.append("| " + " | ".join(_escape_md(row[c]) for c in headers) + " |")
    if limit > 0 and df.height > d.height:
        lines.append("")
        lines.append(f"_Mostrando {d.height} de {df.height} filas._")
    return "\n".join(lines)


def _print_detailed_diagnostics(
    a: pl.DataFrame, b: pl.DataFrame, cols: list[str], sample_limit: int, max_columns_to_show: int = 20
) -> dict:
    """
    Diagnóstico detallado centrado en project_id para entender discrepancias.
    Asume que a y b ya son comparables (tipos normalizados).
    """
    if "project_id" not in cols:
        print("\n[Diag] No existe 'project_id' en columnas comunes; se omite diagnóstico por llave.")
        return {
            "only_legacy_ids": pl.DataFrame(),
            "only_new_ids": pl.DataFrame(),
            "ids_count_diff": pl.DataFrame(),
            "aligned_ids_count": 0,
            "diff_counts": [],
            "samples_by_column": {},
        }

    print("\n" + "-" * 90)
    print("4) Diagnóstico detallado por project_id")
    print("-" * 90)

    cnt_a = a.group_by("project_id").agg(pl.len().alias("__rows_a"))
    cnt_b = b.group_by("project_id").agg(pl.len().alias("__rows_b"))

    only_a = cnt_a.join(cnt_b, on="project_id", how="anti").sort("project_id")
    only_b = cnt_b.join(cnt_a, on="project_id", how="anti").sort("project_id")

    cnt_join = cnt_a.join(cnt_b, on="project_id", how="inner")
    cnt_diff = cnt_join.filter(pl.col("__rows_a") != pl.col("__rows_b")).sort("project_id")

    print(f"- IDs solo en legacy: {only_a.height:,}")
    if only_a.height:
        print(f"  Muestra ({_display_limit_text(sample_limit, only_a.height)}):")
        print(_show_df(only_a, sample_limit))

    print(f"- IDs solo en new:    {only_b.height:,}")
    if only_b.height:
        print(f"  Muestra ({_display_limit_text(sample_limit, only_b.height)}):")
        print(_show_df(only_b, sample_limit))

    print(f"- IDs con distinta cantidad de filas: {cnt_diff.height:,}")
    if cnt_diff.height:
        print(f"  Muestra ({_display_limit_text(sample_limit, cnt_diff.height)}):")
        print(_show_df(cnt_diff, sample_limit))

    # IDs alineables para diff columna a columna: exactamente 1 fila por project_id en ambos lados
    single_ids = cnt_join.filter((pl.col("__rows_a") == 1) & (pl.col("__rows_b") == 1)).select("project_id")
    print(f"- IDs alineables (1 fila por lado): {single_ids.height:,}")
    if single_ids.height == 0:
        print("  No hay IDs alineables; omito diff columna a columna por llave.")
        return {
            "only_legacy_ids": only_a,
            "only_new_ids": only_b,
            "ids_count_diff": cnt_diff,
            "aligned_ids_count": 0,
            "diff_counts": [],
            "samples_by_column": {},
        }

    a_single = a.join(single_ids, on="project_id", how="inner").sort("project_id")
    b_single = b.join(single_ids, on="project_id", how="inner").sort("project_id")

    right_cols = [c for c in cols if c != "project_id"]
    joined = a_single.join(
        b_single.select(["project_id", *right_cols]),
        on="project_id",
        how="inner",
        suffix="_new",
    )

    diff_counts: list[tuple[str, int]] = []
    for c in right_cols:
        left_c = c
        right_c = f"{c}_new"
        neq_mask = ~(
            (pl.col(left_c) == pl.col(right_c))
            | (pl.col(left_c).is_null() & pl.col(right_c).is_null())
        )
        n = joined.filter(neq_mask).height
        if n > 0:
            diff_counts.append((c, n))

    if not diff_counts:
        print("- En IDs alineables, no hay diferencias columna a columna.")
        return {
            "only_legacy_ids": only_a,
            "only_new_ids": only_b,
            "ids_count_diff": cnt_diff,
            "aligned_ids_count": single_ids.height,
            "diff_counts": [],
            "samples_by_column": {},
        }

    diff_counts.sort(key=lambda x: x[1], reverse=True)
    print(f"- Columnas con diferencias en IDs alineables: {len(diff_counts)}")
    top_n = len(diff_counts) if sample_limit <= 0 else 15
    print(f"  Top {top_n} columnas con más diffs:")
    for c, n in diff_counts[:top_n]:
        print(f"  * {c}: {n:,} filas")

    sample_cols_n = len(diff_counts) if sample_limit <= 0 else min(len(diff_counts), max_columns_to_show)
    print(f"\n  Muestras por columna con `project_id` (top {sample_cols_n}):")
    samples_by_column: dict[str, pl.DataFrame] = {}
    for c, _ in diff_counts[:sample_cols_n]:
        left_c = c
        right_c = f"{c}_new"
        neq_mask = ~(
            (pl.col(left_c) == pl.col(right_c))
            | (pl.col(left_c).is_null() & pl.col(right_c).is_null())
        )
        sample = joined.filter(neq_mask).select(
            "project_id",
            pl.col(left_c).alias(f"{c}_legacy"),
            pl.col(right_c).alias(f"{c}_new"),
        )
        sample = _show_df(sample, sample_limit)
        samples_by_column[c] = sample
        print(f"  - {c} (muestra {sample.height}):")
        print(sample)

    return {
        "only_legacy_ids": only_a,
        "only_new_ids": only_b,
        "ids_count_diff": cnt_diff,
        "aligned_ids_count": single_ids.height,
        "diff_counts": diff_counts,
        "samples_by_column": samples_by_column,
    }


def _write_markdown_report(
    report_path: Path,
    *,
    partition: str,
    refresh_both: bool,
    sample_limit: int,
    df_legacy: pl.DataFrame,
    df_new: pl.DataFrame,
    only_legacy_cols: list[str],
    only_new_cols: list[str],
    bad_cols: list[tuple[str, pl.DataFrame]],
    rows_ok: bool,
    row_diff: pl.DataFrame,
    diagnostics: dict | None,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Comparativa Gold `lk_projects` vs `lk_projects_new`")
    lines.append("")
    lines.append(f"- Fecha de generación: `{datetime.now().isoformat(timespec='seconds')}`")
    lines.append(f"- Partición: `{partition}`")
    lines.append(f"- Refresh ambos upstreams: `{refresh_both}`")
    lines.append(f"- Límite de muestra: `{sample_limit}` (`0` = completo)")
    lines.append("")
    lines.append("## Resumen")
    lines.append("")
    lines.append(f"- Filas legacy: `{df_legacy.height}`")
    lines.append(f"- Filas new: `{df_new.height}`")
    lines.append(f"- Columnas legacy: `{df_legacy.width}`")
    lines.append(f"- Columnas new: `{df_new.width}`")
    lines.append(f"- Columnas con diferencias: `{len(bad_cols)}`")
    lines.append(f"- Diferencias fila a fila: `{'no' if rows_ok else 'sí'}`")
    lines.append("")
    lines.append("## Diferencias de esquema")
    lines.append("")
    lines.append(f"- Solo en legacy: `{len(only_legacy_cols)}`")
    if only_legacy_cols:
        lines.append("```")
        lines.append("\n".join(only_legacy_cols))
        lines.append("```")
    lines.append(f"- Solo en new: `{len(only_new_cols)}`")
    if only_new_cols:
        lines.append("```")
        lines.append("\n".join(only_new_cols))
        lines.append("```")
    lines.append("")
    lines.append("## Diferencias fila a fila (multiconjunto)")
    lines.append("")
    if rows_ok:
        lines.append("No hay diferencias fila a fila.")
    else:
        lines.append(f"Diferencias detectadas: `{row_diff.height}` combinaciones.")
        lines.append("")
        lines.append(_df_to_markdown(row_diff, limit=sample_limit))
    lines.append("")

    lines.append("## Diferencias columna a columna (resumen)")
    lines.append("")
    lines.append(f"Columnas con diferencias: `{len(bad_cols)}`")
    lines.append("")
    if bad_cols:
        lines.append("| columna | diferencias_detectadas |")
        lines.append("| --- | --- |")
        for col, diff in bad_cols:
            lines.append(f"| `{col}` | {diff.height} |")
        lines.append("")
        lines.append("_Detalle por `project_id` en la sección de diagnóstico._")
        lines.append("")

    if diagnostics is not None:
        lines.append("## Diagnóstico por `project_id` (legacy vs new)")
        lines.append("")
        only_a = diagnostics.get("only_legacy_ids", pl.DataFrame())
        only_b = diagnostics.get("only_new_ids", pl.DataFrame())
        cnt_diff = diagnostics.get("ids_count_diff", pl.DataFrame())
        diff_counts = diagnostics.get("diff_counts", [])
        samples_by_column = diagnostics.get("samples_by_column", {})
        aligned_n = diagnostics.get("aligned_ids_count", 0)

        lines.append(f"- IDs solo en legacy: `{only_a.height}`")
        if only_a.height:
            lines.append(_df_to_markdown(only_a, limit=sample_limit))
            lines.append("")
        lines.append(f"- IDs solo en new: `{only_b.height}`")
        if only_b.height:
            lines.append(_df_to_markdown(only_b, limit=sample_limit))
            lines.append("")
        lines.append(f"- IDs con distinta cantidad de filas: `{cnt_diff.height}`")
        if cnt_diff.height:
            lines.append(_df_to_markdown(cnt_diff, limit=sample_limit))
            lines.append("")
        lines.append(f"- IDs alineables (1 fila por lado): `{aligned_n}`")
        lines.append("")
        if diff_counts:
            lines.append("### Columnas con diferencias en IDs alineables")
            lines.append("")
            lines.append("| columna | filas_con_diferencia |")
            lines.append("| --- | --- |")
            for c, n in diff_counts:
                lines.append(f"| `{c}` | {n} |")
            lines.append("")
            lines.append("### Muestras por columna (`project_id`, `legacy`, `new`)")
            lines.append("")
            for col, sample in samples_by_column.items():
                lines.append(f"#### `{col}`")
                lines.append("")
                lines.append(_df_to_markdown(sample, limit=sample_limit))
                lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partition", default=_default_partition(), help="Partición YYYY-MM-DD (default: ayer).")
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=SAMPLE_LIMIT,
        help="Límite de filas en muestras impresas. Usa 0 para mostrar completo.",
    )
    parser.add_argument(
        "--refresh-both-upstreams",
        action="store_true",
        help=(
            "Refresca explícitamente upstreams legacy y new antes de comparar. "
            "Recomendado para reducir desfases por snapshots."
        ),
    )
    parser.add_argument(
        "--report-md",
        default="",
        help=(
            "Ruta del reporte markdown. Si no se indica, se genera en "
            "lakehouse-sdk/tests/lk_projects_migration/reports/."
        ),
    )
    args = parser.parse_args()
    partition = args.partition
    sample_limit = args.sample_limit

    _ensure_import_paths()
    from lakehouse_sdk.loader import execute_asset, clear_cache

    print("=" * 90)
    print(f"Validación gold vs gold: gold_lk_projects vs gold_lk_projects_new | partition={partition}")
    print("=" * 90)
    if args.report_md:
        report_path = Path(args.report_md).resolve()
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = (
            Path(__file__).resolve().parent
            / "reports"
            / f"gold_lk_projects_comparison_{partition}_{ts}.md"
        )

    # Evita resultados de ejecuciones previas
    clear_cache()

    # 1) Refrescar bronze NEW primero (fuente de verdad para stg_*_new).
    # Los stg_*_new hechos con make_silver_stg_asset leen bronze desde S3.
    # Si no refrescamos raw_*_new, podemos comparar contra snapshots viejos.
    print("\n▶ Refrescando bronze del flujo NEW (S3)...")
    for asset_name in NEW_RAW_ASSETS:
        execute_asset(asset_name, partition_key=partition, use_cache=False)
    print(f"  ✅ Bronze NEW refrescado ({len(NEW_RAW_ASSETS)} assets).")

    # 1b) Refrescar silver/core NEW para que gold_lk_projects_new lea S3 fresco.
    print("\n▶ Refrescando silver/core del flujo NEW (S3)...")
    for asset_name in NEW_UPSTREAM_ASSETS:
        execute_asset(asset_name, partition_key=partition, use_cache=False)
    print(f"  ✅ Silver/Core NEW refrescado ({len(NEW_UPSTREAM_ASSETS)} assets).")

    # 1c) Opcional: refrescar también upstream legacy en la misma corrida.
    # Esto reduce el riesgo de comparar contra snapshots con distinto "momento".
    if args.refresh_both_upstreams:
        print("\n▶ Refrescando upstream del flujo legacy (mismo run)...")
        for asset_name in LEGACY_UPSTREAM_ASSETS:
            execute_asset(asset_name, partition_key=partition, use_cache=True)
        print(f"  ✅ Upstream legacy refrescado ({len(LEGACY_UPSTREAM_ASSETS)} assets).")

    # 2) Ejecutar gold legacy y gold new
    print("\n▶ Ejecutando gold_lk_projects (legacy)...")
    # Si ya refrescamos legacy, reutilizamos cache para no volver a consultar fuentes.
    legacy_use_cache = True if args.refresh_both_upstreams else False
    df_legacy = execute_asset("gold_lk_projects", partition_key=partition, use_cache=legacy_use_cache)
    print(f"  legacy -> filas={df_legacy.height:,}, cols={df_legacy.width}")

    print("\n▶ Ejecutando gold_lk_projects_new...")
    # gold_lk_projects_new siempre lee silvers desde S3 al ejecutarse.
    df_new = execute_asset("gold_lk_projects_new", partition_key=partition, use_cache=False)
    print(f"  new    -> filas={df_new.height:,}, cols={df_new.width}")

    # 3) Comparación de columnas (sin orden), excluyendo audit
    cols_legacy = {c for c in df_legacy.columns if c not in AUDIT_COLS}
    cols_new = {c for c in df_new.columns if c not in AUDIT_COLS}
    only_legacy = sorted(cols_legacy - cols_new)
    only_new = sorted(cols_new - cols_legacy)

    print("\n" + "-" * 90)
    print("1) Columnas (sin orden, excluyendo audit)")
    print("-" * 90)
    if only_legacy:
        print(f"  ❌ Solo en legacy ({len(only_legacy)}): {only_legacy}")
    if only_new:
        print(f"  ❌ Solo en new ({len(only_new)}): {only_new}")
    if not only_legacy and not only_new:
        print(f"  ✅ Mismo set de columnas ({len(cols_legacy)})")

    common_cols = sorted(cols_legacy & cols_new)
    if not common_cols:
        print("\nRESULTADO FINAL: ❌ Sin columnas comunes para comparar.")
        sys.exit(1)

    a = _to_comparable(df_legacy, common_cols)
    b = _to_comparable(df_new, common_cols)

    # 4) Columna por columna
    print("\n" + "-" * 90)
    print("2) Columna por columna (multiconjunto de valores)")
    print("-" * 90)
    bad_cols: list[tuple[str, pl.DataFrame]] = []
    for i, col in enumerate(common_cols, start=1):
        ok, diff = _compare_col_multiset(a, b, col)
        if ok:
            print(f"  ✅ [{i:>3}/{len(common_cols)}] {col}")
        else:
            print(f"  ❌ [{i:>3}/{len(common_cols)}] {col} ({diff.height:,} diferencias)")
            bad_cols.append((col, diff))

    # 5) Fila por fila
    print("\n" + "-" * 90)
    print("3) Fila por fila (multiconjunto de filas, sin orden)")
    print("-" * 90)
    rows_ok, row_diff = _compare_row_multiset(a, b, common_cols)
    if rows_ok:
        print("  ✅ Filas equivalentes (incluyendo duplicados).")
    else:
        print(f"  ❌ Diferencias en filas: {row_diff.height:,} combinaciones.")
        print(f"  Muestra ({_display_limit_text(sample_limit, row_diff.height)}):")
        print(_show_df(row_diff, sample_limit))

    print("\n" + "=" * 90)
    compatible = (
        not only_legacy and
        not only_new and
        len(bad_cols) == 0 and
        rows_ok
    )
    if compatible:
        print("RESULTADO FINAL: ✅ 100% compatibles (gold vs gold, excluyendo audit).")
        sys.exit(0)

    print("RESULTADO FINAL: ❌ NO compatibles.")
    if bad_cols:
        print(f"- Columnas con diferencias: {len(bad_cols)}")
        bad_cols_n = len(bad_cols) if sample_limit <= 0 else 10
        for col, diff in bad_cols[:bad_cols_n]:
            print(f"  * {col}: {diff.height:,} diferencias")
            print(_show_df(diff, sample_limit))
        if len(bad_cols) > bad_cols_n:
            print(f"  ... y {len(bad_cols) - bad_cols_n} columnas más con diferencias.")
    need_diagnostics = (len(bad_cols) > 0) or (not rows_ok)
    if need_diagnostics:
        if not rows_ok:
            print("- También hay diferencias fila a fila.")
        else:
            print("- Hay diferencias por columna; mostrando diagnóstico por `project_id`.")
        diagnostics = _print_detailed_diagnostics(
            a, b, common_cols, sample_limit=sample_limit, max_columns_to_show=20
        )
    else:
        diagnostics = None

    _write_markdown_report(
        report_path,
        partition=partition,
        refresh_both=args.refresh_both_upstreams,
        sample_limit=sample_limit,
        df_legacy=df_legacy,
        df_new=df_new,
        only_legacy_cols=only_legacy,
        only_new_cols=only_new,
        bad_cols=bad_cols,
        rows_ok=rows_ok,
        row_diff=row_diff,
        diagnostics=diagnostics,
    )
    print(f"📝 Reporte markdown guardado en: {report_path}")
    print("=" * 90)
    sys.exit(1)


if __name__ == "__main__":
    main()
