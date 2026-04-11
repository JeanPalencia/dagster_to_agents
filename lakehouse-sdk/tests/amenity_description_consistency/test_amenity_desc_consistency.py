#!/usr/bin/env python3
"""
Test: Amenity-Description Consistency Analysis.

Materializes Bronze -> STG -> Core assets locally, then prints:
  1. Summary statistics by category
  2. Ranking of most-omitted amenities
  3. Control samples (5 random spots per category) for manual validation

Usage:
    cd dagster-pipeline
    uv run python ../lakehouse-sdk/tests/amenity_description_consistency/test_amenity_desc_consistency.py
"""
from __future__ import annotations

import random
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl


def _ensure_import_paths() -> None:
    root = Path(__file__).resolve().parents[3]
    dagster_src = root / "dagster-pipeline" / "src"
    if str(dagster_src) not in sys.path:
        sys.path.insert(0, str(dagster_src))


def _separator(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}\n")


def main() -> None:
    _ensure_import_paths()

    from dagster import build_asset_context
    from dagster_pipeline.defs.amenity_description_consistency.bronze.raw_gs_lk_spots import (
        adc_raw_gs_lk_spots,
    )
    from dagster_pipeline.defs.amenity_description_consistency.bronze.raw_gs_bt_spot_amenities import (
        adc_raw_gs_bt_spot_amenities,
    )
    from dagster_pipeline.defs.amenity_description_consistency.silver.stg.stg_gs_lk_spots import (
        adc_stg_gs_lk_spots,
    )
    from dagster_pipeline.defs.amenity_description_consistency.silver.stg.stg_gs_bt_spot_amenities import (
        adc_stg_gs_bt_spot_amenities,
    )
    from dagster_pipeline.defs.amenity_description_consistency.silver.core.core_amenity_desc_consistency import (
        core_amenity_desc_consistency,
    )

    t0 = time.time()
    ctx = build_asset_context()

    # --- Bronze ---
    _separator("BRONZE")
    t_bronze = time.time()
    df_spots = adc_raw_gs_lk_spots(ctx)
    df_amenities = adc_raw_gs_bt_spot_amenities(ctx)
    print(f"  Bronze duration: {time.time() - t_bronze:.1f}s")

    # --- STG ---
    _separator("SILVER STG")
    t_stg = time.time()
    df_stg_spots = adc_stg_gs_lk_spots(ctx, df_spots)
    df_stg_amenities = adc_stg_gs_bt_spot_amenities(ctx, df_amenities)
    print(f"  STG duration: {time.time() - t_stg:.1f}s")

    # --- Core ---
    _separator("SILVER CORE")
    t_core = time.time()
    df_core = core_amenity_desc_consistency(ctx, df_stg_spots, df_stg_amenities)
    core_duration = time.time() - t_core
    print(f"  Core duration: {core_duration:.1f}s")

    total_duration = time.time() - t0
    print(f"\n  TOTAL duration: {total_duration:.1f}s")

    # --- Summary Statistics ---
    _separator("SUMMARY BY CATEGORY")
    total = df_core.height
    categories = [
        (1, "All mentioned"),
        (2, "Partial omission"),
        (3, "Total omission"),
    ]
    for cat_id, cat_label in categories:
        subset = df_core.filter(pl.col("adc_category_id") == cat_id)
        n = subset.height
        pct = n / total * 100 if total > 0 else 0
        print(f"  {cat_label:25s}: {n:>6,} spots ({pct:5.1f}%)")
    print(f"  {'TOTAL':25s}: {total:>6,} spots")

    # --- Amenity omission ranking ---
    _separator("AMENITY OMISSION RANKING")
    amenity_stats: dict[str, dict] = {}
    for row in df_core.iter_rows(named=True):
        for am in row["adc_tagged_amenities"].split(", "):
            am = am.strip()
            if not am:
                continue
            if am not in amenity_stats:
                amenity_stats[am] = {"tagged": 0, "mentioned": 0, "omitted": 0}
            amenity_stats[am]["tagged"] += 1
        mentioned_list = row["adc_mentioned_amenities"]
        if mentioned_list:
            for am in mentioned_list.split(", "):
                am = am.strip()
                if am and am in amenity_stats:
                    amenity_stats[am]["mentioned"] += 1
        omitted_list = row["adc_omitted_amenities"]
        if omitted_list:
            for am in omitted_list.split(", "):
                am = am.strip()
                if am and am in amenity_stats:
                    amenity_stats[am]["omitted"] += 1

    ranking = sorted(
        amenity_stats.items(),
        key=lambda x: x[1]["omitted"],
        reverse=True,
    )
    print(f"  {'Amenity':<28s} {'Tagged':>9s} {'Mentioned':>11s} {'Omitted':>8s} {'% Omission':>11s}")
    print(f"  {'-'*28} {'-'*9} {'-'*11} {'-'*8} {'-'*11}")
    for am, stats in ranking:
        pct = stats["omitted"] / stats["tagged"] * 100 if stats["tagged"] > 0 else 0
        print(
            f"  {am:<28s} {stats['tagged']:>9,} {stats['mentioned']:>11,} "
            f"{stats['omitted']:>8,} {pct:>10.1f}%"
        )

    # --- Control Samples ---
    _separator("CONTROL SAMPLES (5 random spots per category)")
    random.seed(42)

    report_lines: list[str] = []
    report_lines.append(f"# rpt_amenity_description_consistency: Control Samples")
    report_lines.append(f"")
    report_lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}")
    report_lines.append(f"Total spots: {total:,}")
    report_lines.append(f"Duration: {total_duration:.1f}s")
    report_lines.append(f"")

    for cat_id, cat_label in categories:
        subset = df_core.filter(pl.col("adc_category_id") == cat_id)
        n = subset.height
        if n == 0:
            continue

        sample_size = min(5, n)
        indices = random.sample(range(n), sample_size)
        sample = subset[indices]

        header = f"--- {cat_label} ({n:,} spots) ---"
        print(header)
        report_lines.append(f"## {cat_label} ({n:,} spots)")
        report_lines.append(f"")

        for row in sample.iter_rows(named=True):
            desc_preview = row["spot_description"][:300]
            if len(row["spot_description"]) > 300:
                desc_preview += "..."

            block = [
                f"  spot_id: {row['spot_id']}",
                f"  type: {row['spot_type']} | status: {row['spot_status_full']}",
                f"  tagged ({row['adc_total_tagged']}): {row['adc_tagged_amenities']}",
                f"  mentioned ({row['adc_total_mentioned']}): {row['adc_mentioned_amenities'] or '(none)'}",
                f"  omitted ({row['adc_total_omitted']}): {row['adc_omitted_amenities'] or '(none)'}",
                f"  rate: {row['adc_mention_rate']:.0%}",
                f"  description: {desc_preview}",
            ]
            for line in block:
                print(line)
            print()

            report_lines.append(f"### spot_id: {row['spot_id']}")
            report_lines.append(f"")
            report_lines.append(f"- Type: {row['spot_type']} | Status: {row['spot_status_full']}")
            report_lines.append(f"- Tagged ({row['adc_total_tagged']}): {row['adc_tagged_amenities']}")
            report_lines.append(f"- Mentioned ({row['adc_total_mentioned']}): {row['adc_mentioned_amenities'] or '(none)'}")
            report_lines.append(f"- Omitted ({row['adc_total_omitted']}): {row['adc_omitted_amenities'] or '(none)'}")
            report_lines.append(f"- Rate: {row['adc_mention_rate']:.0%}")
            report_lines.append(f"- Description: {desc_preview}")
            report_lines.append(f"")

    # Save report
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = (
        Path(__file__).resolve().parent / "reports" / f"control_samples_{ts}.md"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"\nReporte guardado en: {report_path}")


if __name__ == "__main__":
    main()
