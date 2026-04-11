#!/usr/bin/env python3
"""
Diagnostic: identify exactly why 65 leads exist in lk_leads (legacy)
but not in lk_leads_v2 (our pipeline).

Strategy:
1. Query GeoSpot lk_leads (legacy) with funnel filter → get filtered universe
2. Query GeoSpot lk_leads_v2 with equivalent filter → get filtered universe
3. Identify the exclusive leads in each
4. For the exclusive leads, check their FULL record in both GeoSpot tables
5. For the exclusive leads, check their CURRENT state in MySQL (clients)

Usage (from dagster-pipeline directory):
    uv run python ../lakehouse-sdk/tests/funnel_ptd/diagnose_65_leads.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

DAGSTER_SRC = Path(__file__).resolve().parents[3] / "dagster-pipeline" / "src"
sys.path.insert(0, str(DAGSTER_SRC))

from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source

SEP = "=" * 80


def section(title: str) -> None:
    print(f"\n{SEP}\n  {title}\n{SEP}")


# ── Step 1: Get filtered universes from GeoSpot ──

LEGACY_FILTERED = """
SELECT lead_id, lead_l0, lead_l1, lead_l2, lead_l3, lead_l4,
       lead_domain, lead_deleted_at,
       lead_lead0_at, lead_lead1_at, lead_lead2_at, lead_lead3_at, lead_lead4_at
FROM lk_leads
WHERE (lead_l0 = 1 OR lead_l1 = 1 OR lead_l2 = 1 OR lead_l3 = 1 OR lead_l4 = 1)
  AND (lead_domain NOT IN ('spot2.mx') OR lead_domain IS NULL)
  AND lead_deleted_at IS NULL
ORDER BY lead_id
"""

V2_FILTERED = """
SELECT lead_id, lead_l0, lead_l1, lead_l2, lead_l3, lead_l4,
       lead_domain, lead_deleted_at,
       lead_lead0_at, lead_lead1_at, lead_lead2_at, lead_lead3_at, lead_lead4_at
FROM lk_leads_v2
WHERE (lead_l0 OR lead_l1 OR lead_l2 OR lead_l3 OR lead_l4)
  AND (lead_domain NOT IN ('spot2.mx') OR lead_domain IS NULL)
  AND lead_deleted_at IS NULL
ORDER BY lead_id
"""


def get_exclusive_ids() -> tuple[list[int], list[int]]:
    section("1. Filtered universes from GeoSpot")

    df_legacy = query_bronze_source(LEGACY_FILTERED, source_type="geospot_postgres")
    df_v2 = query_bronze_source(V2_FILTERED, source_type="geospot_postgres")

    print(f"lk_leads (legacy) filtered: {df_legacy.height:,}")
    print(f"lk_leads_v2 filtered:       {df_v2.height:,}")

    ids_legacy = set(df_legacy["lead_id"].to_list())
    ids_v2 = set(df_v2["lead_id"].to_list())

    only_legacy = sorted(ids_legacy - ids_v2)
    only_v2 = sorted(ids_v2 - ids_legacy)

    print(f"Only in legacy:  {len(only_legacy)}")
    print(f"Only in v2:      {len(only_v2)}")

    return only_legacy, only_v2


# ── Step 2: Full record in both GeoSpot tables for exclusive leads ──

def check_geospot_records(lead_ids: list[int], label: str) -> None:
    if not lead_ids:
        print(f"  No exclusive leads in {label}")
        return

    ids_str = ",".join(str(i) for i in lead_ids)

    section(f"2a. Full record in lk_leads (legacy) for {len(lead_ids)} exclusive leads")
    q_legacy = f"""
    SELECT lead_id, lead_l0, lead_l1, lead_l2, lead_l3, lead_l4,
           lead_domain, lead_deleted_at,
           lead_lead0_at, lead_lead1_at, lead_lead2_at, lead_lead3_at, lead_lead4_at,
           lead_created_at
    FROM lk_leads
    WHERE lead_id IN ({ids_str})
    ORDER BY lead_id
    """
    df_legacy = query_bronze_source(q_legacy, source_type="geospot_postgres")
    print(f"Found in lk_leads: {df_legacy.height}")
    print(df_legacy)

    section(f"2b. Full record in lk_leads_v2 for the same {len(lead_ids)} leads")
    q_v2 = f"""
    SELECT lead_id, lead_l0, lead_l1, lead_l2, lead_l3, lead_l4,
           lead_domain, lead_deleted_at,
           lead_lead0_at, lead_lead1_at, lead_lead2_at, lead_lead3_at, lead_lead4_at,
           lead_created_at
    FROM lk_leads_v2
    WHERE lead_id IN ({ids_str})
    ORDER BY lead_id
    """
    df_v2 = query_bronze_source(q_v2, source_type="geospot_postgres")
    print(f"Found in lk_leads_v2: {df_v2.height}")
    if df_v2.height > 0:
        print(df_v2)
    else:
        print("  *** These leads DO NOT EXIST in lk_leads_v2 at all ***")

    # Check which exist in v2 but fail the filter
    if df_v2.height > 0:
        section("2c. Why do they fail the v2 filter?")
        for row in df_v2.iter_rows(named=True):
            lid = row["lead_id"]
            reasons = []
            has_flag = any([row.get("lead_l0"), row.get("lead_l1"), row.get("lead_l2"),
                           row.get("lead_l3"), row.get("lead_l4")])
            if not has_flag:
                reasons.append("no lead flags (l0-l4 all false/null)")
            if row.get("lead_domain") == "spot2.mx":
                reasons.append("domain = spot2.mx")
            if row.get("lead_deleted_at") is not None:
                reasons.append(f"deleted_at = {row['lead_deleted_at']}")
            if not reasons:
                reasons.append("PASSES FILTER (should be in filtered set!)")
            print(f"  lead_id={lid}: {'; '.join(reasons)}")


# ── Step 3: Current state in MySQL (source of truth) ──

def check_mysql_source(lead_ids: list[int]) -> None:
    if not lead_ids:
        return

    section(f"3. Current state in MySQL (clients table) for {len(lead_ids)} leads")
    ids_str = ",".join(str(i) for i in lead_ids)

    q_mysql = f"""
    SELECT
        id AS lead_id,
        IF(lead0_at IS NOT NULL, 1, 0) AS lead_l0,
        IF(lead1_at IS NOT NULL, 1, 0) AS lead_l1,
        IF(lead2_at IS NOT NULL, 1, 0) AS lead_l2,
        IF(lead3_at IS NOT NULL, 1, 0) AS lead_l3,
        IF(lead4_at IS NOT NULL, 1, 0) AS lead_l4,
        LOWER(TRIM(SUBSTRING_INDEX(email, '@', -1))) AS lead_domain,
        deleted_at AS lead_deleted_at,
        lead0_at, lead1_at, lead2_at, lead3_at, lead4_at,
        created_at, updated_at
    FROM clients
    WHERE id IN ({ids_str})
    ORDER BY id
    """
    df_mysql = query_bronze_source(q_mysql, source_type="mysql_prod")
    print(f"Found in MySQL: {df_mysql.height}")

    if df_mysql.height == 0:
        print("  *** These leads DO NOT EXIST in MySQL (deleted from source) ***")
        return

    print(df_mysql)

    section("3b. Current MySQL state vs legacy filter criteria")
    for row in df_mysql.iter_rows(named=True):
        lid = row["lead_id"]
        has_flag = any([row.get("lead_l0"), row.get("lead_l1"), row.get("lead_l2"),
                       row.get("lead_l3"), row.get("lead_l4")])
        is_deleted = row.get("lead_deleted_at") is not None
        is_spot2 = row.get("lead_domain") == "spot2.mx"

        would_pass_legacy = has_flag and not is_spot2 and not is_deleted
        status_parts = []
        if not has_flag:
            status_parts.append("NO lead flags")
        if is_deleted:
            status_parts.append(f"DELETED ({row['lead_deleted_at']})")
        if is_spot2:
            status_parts.append("domain=spot2.mx")
        if would_pass_legacy:
            status_parts.append("WOULD PASS filter (unchanged)")

        print(f"  lead_id={lid}: {'; '.join(status_parts)}")


# ── Step 4: Summary ──

def summarize(lead_ids: list[int]) -> None:
    if not lead_ids:
        return

    section("4. Summary: compare legacy GeoSpot record vs current MySQL state")
    ids_str = ",".join(str(i) for i in lead_ids)

    # Get legacy state
    q_legacy = f"""
    SELECT lead_id, lead_l0, lead_l1, lead_l2, lead_l3, lead_l4,
           lead_domain, lead_deleted_at
    FROM lk_leads WHERE lead_id IN ({ids_str})
    """
    df_legacy = query_bronze_source(q_legacy, source_type="geospot_postgres")

    # Get current MySQL state
    q_mysql = f"""
    SELECT id AS lead_id,
           IF(lead0_at IS NOT NULL, 1, 0) AS lead_l0_now,
           IF(lead1_at IS NOT NULL, 1, 0) AS lead_l1_now,
           IF(lead2_at IS NOT NULL, 1, 0) AS lead_l2_now,
           IF(lead3_at IS NOT NULL, 1, 0) AS lead_l3_now,
           IF(lead4_at IS NOT NULL, 1, 0) AS lead_l4_now,
           LOWER(TRIM(SUBSTRING_INDEX(email, '@', -1))) AS lead_domain_now,
           deleted_at AS lead_deleted_at_now
    FROM clients WHERE id IN ({ids_str})
    """
    df_mysql = query_bronze_source(q_mysql, source_type="mysql_prod")

    if df_mysql.height == 0:
        print("All leads were HARD-DELETED from MySQL (no longer in clients table)")
        return

    merged = df_legacy.join(
        df_mysql, on="lead_id", how="left"
    )

    categories = {"deleted": [], "flags_removed": [], "domain_changed": [],
                  "not_in_mysql": [], "unchanged": []}

    for row in merged.iter_rows(named=True):
        lid = row["lead_id"]

        if row.get("lead_l0_now") is None:
            categories["not_in_mysql"].append(lid)
            continue

        was_deleted = row.get("lead_deleted_at") is not None
        is_deleted_now = row.get("lead_deleted_at_now") is not None

        had_flags = any([row.get("lead_l0"), row.get("lead_l1"), row.get("lead_l2"),
                        row.get("lead_l3"), row.get("lead_l4")])
        has_flags_now = any([row.get("lead_l0_now"), row.get("lead_l1_now"),
                            row.get("lead_l2_now"), row.get("lead_l3_now"),
                            row.get("lead_l4_now")])

        old_domain = row.get("lead_domain")
        new_domain = row.get("lead_domain_now")

        if not was_deleted and is_deleted_now:
            categories["deleted"].append(lid)
        elif had_flags and not has_flags_now:
            categories["flags_removed"].append(lid)
        elif old_domain != "spot2.mx" and new_domain == "spot2.mx":
            categories["domain_changed"].append(lid)
        else:
            categories["unchanged"].append(lid)

    print(f"\n  Cause breakdown for {len(lead_ids)} exclusive legacy leads:")
    print(f"    Soft-deleted since legacy run:     {len(categories['deleted'])}")
    print(f"    Lead flags removed (l0-l4→null):   {len(categories['flags_removed'])}")
    print(f"    Domain changed to spot2.mx:        {len(categories['domain_changed'])}")
    print(f"    Hard-deleted from MySQL:            {len(categories['not_in_mysql'])}")
    print(f"    Unchanged (still should pass):      {len(categories['unchanged'])}")

    if categories["deleted"]:
        print(f"\n  Deleted IDs: {categories['deleted'][:20]}")
    if categories["flags_removed"]:
        print(f"  Flags removed IDs: {categories['flags_removed'][:20]}")
    if categories["domain_changed"]:
        print(f"  Domain changed IDs: {categories['domain_changed'][:20]}")
    if categories["not_in_mysql"]:
        print(f"  Hard-deleted IDs: {categories['not_in_mysql'][:20]}")
    if categories["unchanged"]:
        print(f"  *** UNEXPECTED - unchanged IDs: {categories['unchanged'][:20]}")
        print("  These need manual investigation!")


def main() -> None:
    print(SEP)
    print("  DIAGNOSTIC: Why 65 leads in lk_leads but not lk_leads_v2?")
    print(SEP)

    only_legacy, only_v2 = get_exclusive_ids()

    if only_legacy:
        check_geospot_records(only_legacy, "legacy")
        try:
            check_mysql_source(only_legacy)
            summarize(only_legacy)
        except Exception as e:
            print(f"\n  MySQL not accessible (VPN/tunnel required): {type(e).__name__}")
            print("  Skipping MySQL checks — GeoSpot analysis above is sufficient.")

    if only_v2:
        section("EXTRA: leads only in v2 (not expected)")
        print(f"  IDs: {only_v2[:20]}")

    section("DONE")


if __name__ == "__main__":
    main()
