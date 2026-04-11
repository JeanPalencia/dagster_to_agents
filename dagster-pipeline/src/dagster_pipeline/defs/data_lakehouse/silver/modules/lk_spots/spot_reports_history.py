"""Módulo reutilizable: historial de reportes por spot (JSON alineado a MySQL, último reporte, flag)."""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import polars as pl

from dagster_pipeline.defs.data_lakehouse.silver.utils import case_when
from dagster_pipeline.defs.data_lakehouse.silver.modules.lk_spots.dictionaries import REPORT_REASON_MAP


def _json_default(o: Any) -> str:
    if isinstance(o, (datetime, date)):
        return o.isoformat(sep=" ")
    raise TypeError


def _series_to_datetime_us(s: pl.Series) -> pl.Series:
    """Coerce MySQL/Object/string timestamps to Datetime[μs] for sort/group."""
    if s.dtype == pl.Datetime:
        return s.cast(pl.Datetime("us"))
    if s.dtype == pl.Object:
        out: list[Any] = []
        for v in s:
            if v is None:
                out.append(None)
            elif isinstance(v, datetime):
                out.append(v)
            elif isinstance(v, date):
                out.append(datetime.combine(v, datetime.min.time()))
            elif isinstance(v, str):
                try:
                    out.append(datetime.fromisoformat(v.replace("Z", "+00:00").split("+")[0]))
                except ValueError:
                    out.append(None)
            else:
                out.append(None)
        return pl.Series(s.name, out, dtype=pl.Datetime("us"))
    return s.cast(pl.Utf8, strict=False).str.to_datetime(strict=False).cast(pl.Datetime("us"))


def _report_history_row_to_obj(row: dict[str, Any], reason_map: dict) -> dict[str, Any]:
    rid = row.get("reason_id")
    label = None
    if rid is not None:
        try:
            label = reason_map.get(int(rid))
        except (TypeError, ValueError):
            label = None
    return {
        "spot_report_id": row.get("id"),
        "spot_reason_id": rid,
        "spot_reason": label,
        "user_reporter_id": row.get("reported_by"),
        "spot_reporter_created_at": row.get("created_at"),
        "spot_reporter_updated_at": row.get("updated_at"),
        "spot_reporter_deleted_at": row.get("deleted_at"),
    }


def _build_spot_reports_json_group(g: pl.DataFrame) -> pl.DataFrame:
    """One group = one spot_id; JSON array matches stg_lk_spots.sql JSON_ARRAYAGG(JSON_OBJECT(...))."""
    sid = g["spot_id"][0]
    reason_map = REPORT_REASON_MAP
    objs = [_report_history_row_to_obj(dict(row), reason_map) for row in g.iter_rows(named=True)]
    return pl.DataFrame(
        {
            "spot_id": [sid],
            "spot_reports_full_history": [json.dumps(objs, default=_json_default, ensure_ascii=False)],
        }
    )


def build_spot_reports_summary(
    spot_reports: pl.DataFrame,
    report_history: pl.DataFrame,
    report_reason_map: dict | None = None,
) -> pl.DataFrame:
    """
    Resumen de reportes por spot_id: último reporte activo, historial JSON, flag spot_has_active_report.
    report_history DF is not used (SQL compatibility); kept for signature stability.
    """
    reason_map = report_reason_map or REPORT_REASON_MAP
    report_reason_expr = case_when("reason_id", reason_map, default=None)
    sr_filtered = spot_reports.filter(pl.col("deleted_at").is_null()).filter(pl.col("reason_id").is_not_null())
    _ts_cols = [c for c in ("created_at", "updated_at", "deleted_at") if c in sr_filtered.columns]
    if _ts_cols:
        sr_filtered = sr_filtered.with_columns(
            [_series_to_datetime_us(sr_filtered.get_column(c)).alias(c) for c in _ts_cols]
        )
    if sr_filtered.height > 0:
        latest_ranked = (
            sr_filtered.sort("created_at", descending=True)
            .with_columns(
                report_reason_expr.cast(pl.Utf8).alias("spot_last_active_report_reason")
            )
            .with_columns(pl.int_range(1, pl.len() + 1).over("spot_id").alias("rn"))
            .filter(pl.col("rn") == 1)
        )
        latest_active_reports = latest_ranked.select([
            pl.col("spot_id"),
            pl.col("reason_id").alias("spot_last_active_report_reason_id"),
            pl.col("spot_last_active_report_reason"),
            pl.col("reported_by").alias("spot_last_active_report_user_id"),
            pl.col("created_at").alias("spot_last_active_report_created_at"),
        ])
        ordered_reports = sr_filtered.sort(["spot_id", "created_at"], descending=[False, True])
        report_history_df = ordered_reports.group_by("spot_id", maintain_order=True).map_groups(
            _build_spot_reports_json_group
        )
        spot_reports_summary = (
            report_history_df.join(latest_active_reports, on="spot_id", how="left")
            .with_columns(
                pl.when(pl.col("spot_last_active_report_reason_id").is_not_null())
                .then(1)
                .otherwise(0)
                .alias("spot_has_active_report")
            )
        )
    else:
        spot_reports_summary = pl.DataFrame(schema={
            "spot_id": pl.Int64,
            "spot_reports_full_history": pl.Utf8,
            "spot_last_active_report_reason_id": pl.Int64,
            "spot_last_active_report_reason": pl.Utf8,
            "spot_last_active_report_user_id": pl.Int64,
            "spot_last_active_report_created_at": pl.Datetime,
            "spot_has_active_report": pl.Int64,
        })
    return spot_reports_summary
