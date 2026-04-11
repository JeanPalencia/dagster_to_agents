"""Reusable module: latest exchange rate. Any table with prices can use it.

If USD looks wrong (e.g. stale TC), the issue is usually **upstream**: the exchanges
DataFrame from S3 must be a fresh full extract. This module only picks a row from it.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional, Tuple

import polars as pl


def _created_at_date_expr(created_col: str) -> pl.Expr:
    """Normalize created_at to Date for comparisons (Utf8 or Datetime from MySQL/Parquet)."""
    # .cast(Datetime) on strings like "2026-03-22 10:00:00" can yield null; Utf8 → parse is reliable.
    c = pl.col(created_col)
    return c.cast(pl.Utf8, strict=False).str.to_datetime(strict=False).dt.date()


def _scalar_to_decimal_exchange(v: object) -> Decimal:
    """
    Build Decimal from the picked exchange_rate cell without float pollution when avoidable.

    Prefer string from extract; Parquet often yields Float64 — str(v) is then the best local fix.
    """
    if v is None:
        raise ValueError("null exchange_rate")
    if isinstance(v, Decimal):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            raise ValueError("empty exchange_rate string")
        return Decimal(s)
    if isinstance(v, bool):
        raise ValueError("boolean exchange_rate")
    if isinstance(v, int) and not isinstance(v, bool):
        return Decimal(v)
    if isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("non-finite exchange_rate float")
        return Decimal(str(v))
    return Decimal(str(v))


def select_latest_exchange_row(
    exchanges: pl.DataFrame,
    *,
    max_effective_date: Optional[date] = None,
) -> Tuple[pl.DataFrame, Optional[str]]:
    """
    One-row DataFrame used for USD conversion, plus optional warning text.

    - If `exchange_type` exists, keep rows with exchange_type == 1. If that removes
      all rows, fall back to the full frame.
    - If `max_effective_date` is set, only rows whose `created_at` **date** is on or
      before that day are considered (partition run N uses data as-of N-1; TC must
      not pick rows created on partition day N). If that filter removes every row,
      falls back to the pre-filter frame; the second return value is a warning for the caller.
    - Sort: `created_at` DESC (stg_lk_spots `te` CTE), then `id` ASC on ties.
    - If no created-at column, sort by `id` DESC only (date cap is ignored).
    """
    if exchanges.height == 0 or "exchange_rate" not in exchanges.columns:
        return pl.DataFrame(), None

    df = exchanges
    if "id" not in df.columns and "Id" in df.columns:
        df = df.rename({"Id": "id"})

    if "exchange_type" in df.columns:
        filtered = df.filter(pl.col("exchange_type") == 1)
        if filtered.height > 0:
            df = filtered

    date_cap_fallback_warn: Optional[str] = None
    created_col = "created_at" if "created_at" in df.columns else None
    if created_col is None:
        created_col = next((c for c in df.columns if "created" in c.lower()), None)

    if max_effective_date is not None and created_col is not None:
        date_ok = (
            pl.col(created_col).is_not_null()
            & (_created_at_date_expr(created_col) <= pl.lit(max_effective_date))
        )
        before_cap = df
        scoped = before_cap.filter(date_ok)
        if scoped.height > 0:
            df = scoped
        elif before_cap.height > 0:
            date_cap_fallback_warn = (
                f"No exchange row with created_at.date() <= {max_effective_date}; "
                "using latest row in extract (uncapped fallback)."
            )

    if created_col is None:
        if "id" in df.columns:
            return df.sort("id", descending=True, nulls_last=True).head(1), date_cap_fallback_warn
        return df.head(1), date_cap_fallback_warn

    sort_cols = [created_col]
    descending = [True]
    if "id" in df.columns:
        sort_cols.append("id")
        descending.append(False)
    out = df.sort(sort_cols, descending=descending, nulls_last=True).head(1)
    return out, date_cap_fallback_warn


def get_exchange_rate_decimal(
    exchanges: pl.DataFrame,
    *,
    max_effective_date: Optional[date] = None,
) -> Decimal:
    """Scalar rate as Decimal (exact as upstream allows). Defaults to Decimal('1') if missing."""
    row, _ = select_latest_exchange_row(exchanges, max_effective_date=max_effective_date)
    if row.height == 0:
        return Decimal("1")
    try:
        d = _scalar_to_decimal_exchange(row["exchange_rate"][0])
        if d <= 0:
            return Decimal("1")
        return d
    except Exception:
        return Decimal("1")


def get_exchange_rate(
    exchanges: pl.DataFrame,
    *,
    max_effective_date: Optional[date] = None,
) -> float:
    """Scalar rate for callers that still need Float64 (e.g. legacy hooks)."""
    return float(get_exchange_rate_decimal(exchanges, max_effective_date=max_effective_date))
