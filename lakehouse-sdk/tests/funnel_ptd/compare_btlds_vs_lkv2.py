#!/usr/bin/env python3
"""
Diagnostic: compare bt_lds_lead_spots data vs lk_leads_v2 + lk_projects_v2
to identify root causes of discrepancies in the funnel_ptd pipeline.

Usage (from dagster-pipeline directory):
    uv run python ../lakehouse-sdk/tests/funnel_ptd/compare_btlds_vs_lkv2.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

DAGSTER_SRC = Path(__file__).resolve().parents[3] / "dagster-pipeline" / "src"
sys.path.insert(0, str(DAGSTER_SRC))

from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

SEPARATOR = "=" * 80


def section(title: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


# ---------------------------------------------------------------------------
# 1. LEADS comparison
# ---------------------------------------------------------------------------

LEADS_BTLDS = """
SELECT lead_id, lds_event_at AS lead_first_datetime
FROM bt_lds_lead_spots
WHERE lds_event_id = 1
ORDER BY lead_id
"""

LEADS_LKV2 = """
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
    primera_fecha_lead AS lead_first_datetime
FROM leads_base
WHERE primera_fecha_lead >= TIMESTAMP '2021-01-01'
  AND primera_fecha_lead < CURRENT_DATE
  AND primera_fecha_lead < TIMESTAMP '9999-12-31'
ORDER BY lead_id
"""


def compare_leads() -> None:
    section("1. LEADS: bt_lds event 1 vs lk_leads_v2 LEAST()")

    df_btlds = query_bronze_source(LEADS_BTLDS, source_type="geospot_postgres")
    df_lkv2 = query_bronze_source(LEADS_LKV2, source_type="geospot_postgres")

    # Apply the same filters as stg_gs_fptd_leads to bt_lds
    from datetime import date, datetime
    MIN_DATE = datetime(2021, 1, 1)
    df_btlds = df_btlds.with_columns(
        pl.col("lead_first_datetime").cast(pl.Datetime("us"), strict=False)
    ).filter(
        pl.col("lead_first_datetime").is_not_null()
        & (pl.col("lead_first_datetime") >= MIN_DATE)
        & (pl.col("lead_first_datetime") < pl.lit(datetime.combine(date.today(), datetime.min.time())))
    )

    df_lkv2 = df_lkv2.with_columns(
        pl.col("lead_first_datetime").cast(pl.Datetime("us"), strict=False)
    )

    print(f"bt_lds event 1 (filtered): {df_btlds.height:,} leads")
    print(f"lk_leads_v2 LEAST():       {df_lkv2.height:,} leads")

    ids_btlds = set(df_btlds["lead_id"].to_list())
    ids_lkv2 = set(df_lkv2["lead_id"].to_list())

    only_btlds = ids_btlds - ids_lkv2
    only_lkv2 = ids_lkv2 - ids_btlds
    common = ids_btlds & ids_lkv2

    print(f"Common:         {len(common):,}")
    print(f"Only in bt_lds: {len(only_btlds):,}")
    print(f"Only in lk_v2:  {len(only_lkv2):,}")

    if only_btlds:
        print(f"  bt_lds exclusive IDs (first 10): {sorted(only_btlds)[:10]}")
    if only_lkv2:
        print(f"  lk_v2 exclusive IDs (first 10): {sorted(only_lkv2)[:10]}")

    # Compare dates for common leads
    merged = df_btlds.select(["lead_id", "lead_first_datetime"]).rename(
        {"lead_first_datetime": "dt_btlds"}
    ).join(
        df_lkv2.select(["lead_id", "lead_first_datetime"]).rename(
            {"lead_first_datetime": "dt_lkv2"}
        ),
        on="lead_id",
        how="inner",
    )
    diffs = merged.filter(pl.col("dt_btlds") != pl.col("dt_lkv2"))
    print(f"Date differences in common leads: {diffs.height:,}")
    if diffs.height > 0:
        print(diffs.head(10))


# ---------------------------------------------------------------------------
# 2. PROJECTS comparison
# ---------------------------------------------------------------------------

PROJECTS_BTLDS = """
SELECT
    lead_id,
    project_id,
    lds_event_id,
    lds_event_at,
    lds_cohort_type,
    lds_cohort_at
FROM bt_lds_lead_spots
WHERE lds_event_id IN (2, 4, 5, 6, 7, 10)
ORDER BY lead_id, project_id, lds_event_id
"""

PROJECTS_LKV2 = """
SELECT
    lead_id,
    project_id,
    project_created_at,
    project_funnel_visit_created_date AS visit_scheduled_at,
    project_funnel_visit_confirmed_at AS visit_confirmed_at,
    project_funnel_visit_realized_at  AS visit_completed_at,
    project_funnel_loi_date,
    project_won_date
FROM lk_projects_v2
WHERE project_id IS NOT NULL
ORDER BY lead_id, project_id
"""


def compare_projects() -> None:
    section("2. PROJECTS: bt_lds pivot vs lk_projects_v2")

    df_btlds_raw = query_bronze_source(PROJECTS_BTLDS, source_type="geospot_postgres")
    df_lkv2 = query_bronze_source(PROJECTS_LKV2, source_type="geospot_postgres")

    print(f"bt_lds raw event rows: {df_btlds_raw.height:,}")
    print(f"lk_projects_v2 rows:   {df_lkv2.height:,}")

    # Pivot bt_lds to project level (same logic as stg_gs_fptd_projects)
    df_btlds_raw = df_btlds_raw.with_columns([
        pl.col("lead_id").cast(pl.Int64, strict=False),
        pl.col("project_id").cast(pl.Int64, strict=False),
        pl.col("lds_event_id").cast(pl.Int32, strict=False),
        pl.col("lds_event_at").cast(pl.Datetime("us"), strict=False),
        pl.col("lds_cohort_at").cast(pl.Date, strict=False),
    ])

    pivoted = df_btlds_raw.group_by(["lead_id", "project_id"]).agg([
        pl.col("lds_event_at").filter(pl.col("lds_event_id") == 2).min().alias("project_created_at"),
        pl.col("lds_event_at").filter(pl.col("lds_event_id") == 4).min().cast(pl.Date).alias("visit_scheduled_at"),
        pl.col("lds_event_at").filter(pl.col("lds_event_id") == 5).min().alias("visit_confirmed_at"),
        pl.col("lds_event_at").filter(pl.col("lds_event_id") == 6).min().alias("visit_completed_at"),
        pl.col("lds_event_at").filter(pl.col("lds_event_id") == 7).min().cast(pl.Date).alias("project_funnel_loi_date"),
        pl.col("lds_event_at").filter(pl.col("lds_event_id") == 10).min().cast(pl.Date).alias("project_won_date"),
        pl.col("lds_cohort_at").filter(pl.col("lds_event_id") == 2).first().alias("cohort_date_real"),
        pl.col("lds_cohort_type").filter(pl.col("lds_event_id") == 2).first().alias("cohort_type"),
    ])

    print(f"bt_lds pivoted projects: {pivoted.height:,}")

    # Compare project IDs
    df_lkv2 = df_lkv2.with_columns([
        pl.col("lead_id").cast(pl.Int64, strict=False),
        pl.col("project_id").cast(pl.Int64, strict=False),
    ])

    ids_btlds = set(pivoted.select(["lead_id", "project_id"]).unique().iter_rows())
    ids_lkv2 = set(df_lkv2.select(["lead_id", "project_id"]).unique().iter_rows())

    only_btlds = ids_btlds - ids_lkv2
    only_lkv2 = ids_lkv2 - ids_btlds
    common = ids_btlds & ids_lkv2

    print(f"\nProject pairs (lead_id, project_id):")
    print(f"  bt_lds pivoted: {len(ids_btlds):,}")
    print(f"  lk_projects_v2: {len(ids_lkv2):,}")
    print(f"  Common:         {len(common):,}")
    print(f"  Only in bt_lds: {len(only_btlds):,}")
    print(f"  Only in lk_v2:  {len(only_lkv2):,}")

    if only_btlds:
        print(f"  bt_lds exclusive (first 5): {sorted(only_btlds)[:5]}")
    if only_lkv2:
        print(f"  lk_v2 exclusive (first 5): {sorted(only_lkv2)[:5]}")

    # Compare column values for common projects
    compare_project_columns(pivoted, df_lkv2)


def compare_project_columns(pivoted: pl.DataFrame, lkv2: pl.DataFrame) -> None:
    """Column-by-column comparison for common projects."""
    merged = pivoted.join(lkv2, on=["lead_id", "project_id"], how="inner", suffix="_lk")

    cols_to_compare = [
        ("project_created_at", "project_created_at_lk"),
        ("visit_scheduled_at", "visit_scheduled_at_lk"),
        ("visit_confirmed_at", "visit_confirmed_at_lk"),
        ("visit_completed_at", "visit_completed_at_lk"),
        ("project_funnel_loi_date", "project_funnel_loi_date_lk"),
        ("project_won_date", "project_won_date_lk"),
    ]

    section("2b. Column-by-column comparison (common projects)")
    print(f"Common projects: {merged.height:,}")

    for bt_col, lk_col in cols_to_compare:
        bt_cast = merged[bt_col].cast(pl.Date, strict=False)
        lk_cast = merged[lk_col].cast(pl.Date, strict=False)

        bt_null = bt_cast.is_null().sum()
        lk_null = lk_cast.is_null().sum()

        bt_notnull = bt_cast.is_not_null()
        lk_notnull = lk_cast.is_not_null()

        both_notnull = bt_notnull & lk_notnull
        both_null = bt_cast.is_null() & lk_cast.is_null()
        bt_only = bt_notnull & lk_cast.is_null()
        lk_only = bt_cast.is_null() & lk_notnull

        n_both = both_notnull.sum()
        match_mask = bt_cast == lk_cast
        n_match = (both_notnull & match_mask).sum()
        n_diff = n_both - n_match

        col_name = bt_col.replace("_lk", "")
        print(f"\n  {col_name}:")
        print(f"    bt_lds nulls: {bt_null:,}, lk_v2 nulls: {lk_null:,}")
        print(f"    Both null:    {both_null.sum():,}")
        print(f"    bt_lds only:  {bt_only.sum():,}")
        print(f"    lk_v2 only:   {lk_only.sum():,}")
        print(f"    Both non-null: {n_both:,}")
        print(f"    Matching:     {n_match:,}")
        print(f"    Different:    {n_diff:,}")

        if n_diff > 0:
            diff_rows = merged.filter(
                both_notnull & ~match_mask
            ).select(["lead_id", "project_id", bt_col, lk_col]).head(5)
            print(f"    Sample diffs:")
            print(diff_rows)

        if lk_only.sum() > 0:
            lk_only_rows = merged.filter(
                bt_cast.is_null() & lk_notnull
            ).select(["lead_id", "project_id", bt_col, lk_col]).head(5)
            print(f"    lk_v2 has value, bt_lds null (sample):")
            print(lk_only_rows)


# ---------------------------------------------------------------------------
# 3. LOI deep-dive
# ---------------------------------------------------------------------------

LOI_BTLDS = """
SELECT lead_id, project_id, MIN(lds_event_at)::date AS loi_date
FROM bt_lds_lead_spots
WHERE lds_event_id = 7
GROUP BY lead_id, project_id
ORDER BY lead_id, project_id
"""

LOI_LKV2 = """
SELECT lead_id, project_id, project_funnel_loi_date::date AS loi_date
FROM lk_projects_v2
WHERE project_funnel_loi_date IS NOT NULL
  AND project_id IS NOT NULL
ORDER BY lead_id, project_id
"""


def compare_lois() -> None:
    section("3. LOIs: bt_lds event 7 vs lk_projects_v2.project_funnel_loi_date")

    df_btlds = query_bronze_source(LOI_BTLDS, source_type="geospot_postgres")
    df_lkv2 = query_bronze_source(LOI_LKV2, source_type="geospot_postgres")

    df_btlds = df_btlds.with_columns([
        pl.col("lead_id").cast(pl.Int64, strict=False),
        pl.col("project_id").cast(pl.Int64, strict=False),
        pl.col("loi_date").cast(pl.Date, strict=False),
    ])
    df_lkv2 = df_lkv2.with_columns([
        pl.col("lead_id").cast(pl.Int64, strict=False),
        pl.col("project_id").cast(pl.Int64, strict=False),
        pl.col("loi_date").cast(pl.Date, strict=False),
    ])

    print(f"bt_lds event 7 LOIs:                {df_btlds.height:,}")
    print(f"lk_projects_v2 LOIs (non-null):     {df_lkv2.height:,}")

    ids_btlds = set(df_btlds.select(["lead_id", "project_id"]).iter_rows())
    ids_lkv2 = set(df_lkv2.select(["lead_id", "project_id"]).iter_rows())

    only_btlds = ids_btlds - ids_lkv2
    only_lkv2 = ids_lkv2 - ids_btlds
    common = ids_btlds & ids_lkv2

    print(f"Common:         {len(common):,}")
    print(f"Only in bt_lds: {len(only_btlds):,}")
    print(f"Only in lk_v2:  {len(only_lkv2):,}")

    if only_lkv2:
        print(f"\n  *** lk_v2 has {len(only_lkv2)} LOI records NOT in bt_lds event 7 ***")
        only_lkv2_sorted = sorted(only_lkv2)[:10]
        print(f"  First 10: {only_lkv2_sorted}")
        only_lkv2_df = df_lkv2.filter(
            pl.struct(["lead_id", "project_id"]).is_in(
                [{"lead_id": r[0], "project_id": r[1]} for r in only_lkv2_sorted]
            )
        )
        print(only_lkv2_df)

    if common:
        merged = df_btlds.rename({"loi_date": "loi_btlds"}).join(
            df_lkv2.rename({"loi_date": "loi_lkv2"}),
            on=["lead_id", "project_id"],
            how="inner",
        )
        diffs = merged.filter(pl.col("loi_btlds") != pl.col("loi_lkv2"))
        print(f"\nDate differences in common LOIs: {diffs.height:,}")
        if diffs.height > 0:
            print(diffs.head(10))


# ---------------------------------------------------------------------------
# 4. COHORT_DATE_REAL comparison
# ---------------------------------------------------------------------------

COHORT_BTLDS = """
SELECT lead_id, project_id, lds_cohort_at AS cohort_date_real
FROM bt_lds_lead_spots
WHERE lds_event_id = 2
ORDER BY lead_id, project_id
"""

COHORT_LKV2 = """
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
),
leads_base_filt AS (
    SELECT lead_id, primera_fecha_lead
    FROM leads_base
    WHERE primera_fecha_lead >= TIMESTAMP '2021-01-01'
      AND primera_fecha_lead < CURRENT_DATE
      AND primera_fecha_lead < TIMESTAMP '9999-12-31'
)
SELECT
    lb.lead_id,
    p.project_id,
    (CASE
        WHEN p.project_created_at IS NOT NULL
         AND p.project_created_at >= lb.primera_fecha_lead + INTERVAL '30 days'
            THEN p.project_created_at
        ELSE lb.primera_fecha_lead
    END)::date AS cohort_date_real
FROM leads_base_filt lb
JOIN lk_projects_v2 p USING (lead_id)
WHERE p.project_id IS NOT NULL
ORDER BY lb.lead_id, p.project_id
"""


def compare_cohort_date() -> None:
    section("4. COHORT_DATE_REAL: bt_lds lds_cohort_at vs SQL CASE formula")

    df_btlds = query_bronze_source(COHORT_BTLDS, source_type="geospot_postgres")
    df_lkv2 = query_bronze_source(COHORT_LKV2, source_type="geospot_postgres")

    df_btlds = df_btlds.with_columns([
        pl.col("lead_id").cast(pl.Int64, strict=False),
        pl.col("project_id").cast(pl.Int64, strict=False),
        pl.col("cohort_date_real").cast(pl.Date, strict=False),
    ])
    df_lkv2 = df_lkv2.with_columns([
        pl.col("lead_id").cast(pl.Int64, strict=False),
        pl.col("project_id").cast(pl.Int64, strict=False),
        pl.col("cohort_date_real").cast(pl.Date, strict=False),
    ])

    print(f"bt_lds cohort records (event 2): {df_btlds.height:,}")
    print(f"lk_v2 cohort records:            {df_lkv2.height:,}")

    merged = df_btlds.rename({"cohort_date_real": "cohort_btlds"}).join(
        df_lkv2.rename({"cohort_date_real": "cohort_lkv2"}),
        on=["lead_id", "project_id"],
        how="inner",
    )

    print(f"Common (lead_id, project_id):    {merged.height:,}")

    diffs = merged.filter(pl.col("cohort_btlds") != pl.col("cohort_lkv2"))
    print(f"Cohort date differences:         {diffs.height:,}")

    if diffs.height > 0:
        print("\nSample differences:")
        print(diffs.head(10))

    # Check for projects in lk_v2 but not in bt_lds event 2
    ids_btlds = set(df_btlds.select(["lead_id", "project_id"]).iter_rows())
    ids_lkv2 = set(df_lkv2.select(["lead_id", "project_id"]).iter_rows())
    only_lkv2 = ids_lkv2 - ids_btlds
    only_btlds = ids_btlds - ids_lkv2
    print(f"\nOnly in bt_lds: {len(only_btlds):,}")
    print(f"Only in lk_v2:  {len(only_lkv2):,}")


# ---------------------------------------------------------------------------
# 5. COHORT_TYPE comparison
# ---------------------------------------------------------------------------

COHORT_TYPE_BTLDS = """
SELECT lead_id, project_id, lds_cohort_type AS cohort_type
FROM bt_lds_lead_spots
WHERE lds_event_id = 2
ORDER BY lead_id, project_id
"""


def compare_cohort_type() -> None:
    section("5. COHORT_TYPE: bt_lds lds_cohort_type vs SQL computed")

    df_btlds = query_bronze_source(COHORT_TYPE_BTLDS, source_type="geospot_postgres")
    df_btlds = df_btlds.with_columns([
        pl.col("lead_id").cast(pl.Int64, strict=False),
        pl.col("project_id").cast(pl.Int64, strict=False),
    ])

    print(f"bt_lds cohort_type distribution:")
    print(df_btlds.group_by("cohort_type").agg(pl.len().alias("count")).sort("count", descending=True))


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    print(SEPARATOR)
    print("  DIAGNOSTIC: bt_lds_lead_spots vs lk_leads_v2 + lk_projects_v2")
    print(SEPARATOR)

    compare_leads()
    compare_projects()
    compare_lois()
    compare_cohort_date()
    compare_cohort_type()

    section("DONE")
    print("Review the output above to identify root causes of discrepancies.")


if __name__ == "__main__":
    main()
