# defs/funnel_ptd/silver/silver_shared.py
"""
Shared transformation functions and constants used by multiple Core assets.

Only functions genuinely reused by 2+ Core assets belong here.
Single-use functions must live inside their respective asset file (SOLID SRP).
"""
from __future__ import annotations

from datetime import date, timedelta

import polars as pl


WINDOW_JOIN_KEYS = ["as_of_date", "ptd_type", "period_start", "period_end"]

PROJECTION_JOIN_KEYS = [
    "counting_method", "as_of_date", "ptd_type",
    "period_start", "period_end", "cohort_type",
]


def _build_projects_base(
    leads: pl.DataFrame,
    projects: pl.DataFrame,
) -> pl.DataFrame:
    """Join leads + projects and compute cohort_date_real."""
    df = projects.join(leads, on="lead_id", how="inner")

    df = df.with_columns(
        pl.when(
            pl.col("project_created_at").is_not_null()
            & (pl.col("project_created_at") >= (pl.col("lead_first_datetime") + timedelta(days=30)))
        )
        .then(pl.col("project_created_at").cast(pl.Date))
        .otherwise(pl.col("lead_first_date"))
        .alias("cohort_date_real")
    )
    return df


def _build_ptd_windows(ref_date: date) -> pl.DataFrame:
    """Generate Month-to-Date and Quarter-to-Date windows for the current year."""
    year_start = ref_date.replace(month=1, day=1)
    current_month_start = ref_date.replace(day=1)

    rows = []
    m = year_start
    while m <= current_month_start:
        next_m = (m.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = next_m - timedelta(days=1)
        period_end = ref_date if m == current_month_start else month_end
        rows.append({
            "as_of_date": ref_date,
            "ptd_type": "Month to Date",
            "period_start": m,
            "period_end": period_end,
        })
        m = next_m

    q_month = ((ref_date.month - 1) // 3) * 3 + 1
    q_start = ref_date.replace(month=q_month, day=1)
    rows.append({
        "as_of_date": ref_date,
        "ptd_type": "Quarter to Date",
        "period_start": q_start,
        "period_end": ref_date,
    })

    return pl.DataFrame(rows).cast({
        "as_of_date": pl.Date,
        "period_start": pl.Date,
        "period_end": pl.Date,
    })
