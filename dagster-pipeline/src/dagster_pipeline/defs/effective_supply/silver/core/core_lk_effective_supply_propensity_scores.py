# defs/effective_supply/silver/core/core_lk_effective_supply_propensity_scores.py
"""
Silver Core Final: Unifies Rent and Sale propensity scores with model metadata
and run_id. Grain: spot_id x market_universe.
"""
from datetime import date

import dagster as dg
import polars as pl


@dg.asset(
    group_name="effective_supply_silver",
    description="Core Final: propensity scores (Rent + Sale) with model metadata and run_id.",
)
def core_lk_effective_supply_propensity_scores(
    context: dg.AssetExecutionContext,
    core_ml_score: dict,
    core_ml_train: dict,
    stg_gs_effective_supply_run_id: int,
) -> pl.DataFrame:
    run_meta = core_ml_train["run_metadata"]
    model_version = run_meta["model_version"]
    window_months = run_meta["window_months"]

    today = date.today()
    window_end_date = today.replace(day=1)

    frames = []
    for universe in ["rent", "sale"]:
        scored_df: pl.DataFrame = core_ml_score[universe]
        variant = core_ml_train[universe]["variant"]

        enriched = scored_df.with_columns([
            pl.lit(model_version).alias("model_version"),
            pl.lit(window_end_date).alias("window_end_date"),
            pl.lit(window_months).cast(pl.Int64).alias("window_months"),
            pl.lit(variant).alias("model_variant"),
        ])
        frames.append(enriched)

        context.log.info(
            f"  {universe}: {enriched.height:,} rows, "
            f"variant={variant}, p_top_p80 mean={enriched['p_top_p80'].mean():.4f}"
        )

    df = pl.concat(frames)
    df = df.with_columns(
        pl.lit(stg_gs_effective_supply_run_id).cast(pl.Int64).alias("run_id")
    )

    context.log.info(
        f"core_lk_effective_supply_propensity_scores: "
        f"{df.height:,} rows, {df.width} columns, run_id={stg_gs_effective_supply_run_id}"
    )
    return df
