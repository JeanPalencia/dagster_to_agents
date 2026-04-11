# defs/effective_supply/silver/core/core_lk_effective_supply_rules.py
"""
Silver Core Final: Unifies Rent and Sale hierarchical rules with model metadata
and run_id. Grain: market_universe x rule_id.
"""
from datetime import date

import dagster as dg
import polars as pl


@dg.asset(
    group_name="effective_supply_silver",
    description="Core Final: hierarchical rules (Rent + Sale) with model metadata and run_id.",
)
def core_lk_effective_supply_rules(
    context: dg.AssetExecutionContext,
    core_ml_rules: dict,
    core_ml_train: dict,
    stg_gs_effective_supply_run_id: int,
) -> pl.DataFrame:
    run_meta = core_ml_train["run_metadata"]
    model_version = run_meta["model_version"]

    today = date.today()
    window_end_date = today.replace(day=1)

    frames = []
    for universe in ["rent", "sale"]:
        rules_df: pl.DataFrame = core_ml_rules[universe]

        enriched = rules_df.with_columns([
            pl.lit(model_version).alias("model_version"),
            pl.lit(window_end_date).alias("window_end_date"),
        ])
        frames.append(enriched)

        context.log.info(f"  {universe}: {enriched.height} rules")

    nullable_str_cols = [
        "spot_sector", "spot_type", "spot_municipality", "spot_corridor",
        "price_sqm_range", "price_sqm_pctl_range", "price_sqm_is_discriminant",
        "area_range", "area_pctl_range", "area_is_discriminant", "strength",
    ]
    for i, frame in enumerate(frames):
        for col in nullable_str_cols:
            if col in frame.columns:
                frames[i] = frames[i].with_columns(pl.col(col).cast(pl.Utf8))

    df = pl.concat(frames)
    df = df.with_columns(
        pl.lit(stg_gs_effective_supply_run_id).cast(pl.Int64).alias("run_id")
    )

    context.log.info(
        f"core_lk_effective_supply_rules: "
        f"{df.height:,} rows, {df.width} columns, run_id={stg_gs_effective_supply_run_id}"
    )
    return df
