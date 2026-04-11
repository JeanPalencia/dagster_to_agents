# defs/effective_supply/ml/ml_build_universes.py
"""
Core ML: Build Rent and Sale universe DataFrames from core_effective_supply.

Each universe contains only the columns needed for modelling (spot attributes)
with leakage columns (counts, scores) removed. Price columns are aliased to
generic names (price_total, price_sqm) so downstream assets are universe-agnostic.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.effective_supply.silver.core.ml.constants import (
    LEAKAGE_COLS,
    POPULATION_COL,
    PRICE_COLS,
    TARGET_COL,
    UNIVERSES,
)


def _build_universe(
    df: pl.DataFrame,
    universe: str,
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Filter to universe population and prepare columns."""
    pop_col = POPULATION_COL[universe]
    target_col = TARGET_COL[universe]
    prices = PRICE_COLS[universe]

    # Filter to population
    uni_df = df.filter(pl.col(pop_col) == 1)

    # Alias price columns to generic names
    uni_df = uni_df.with_columns([
        pl.col(prices["price_total"]).alias("price_total"),
        pl.col(prices["price_sqm"]).alias("price_sqm"),
    ])

    # Add universe label
    uni_df = uni_df.with_columns(pl.lit(universe.capitalize()).alias("market_universe"))

    # Keep only needed columns: spot_id, target, features sources, generic prices
    # String name columns are included for downstream readability (rules, category effects)
    keep_cols = [
        "spot_id",
        target_col,
        "market_universe",
        # Categorical feature sources (IDs + string names)
        "spot_sector_id", "spot_sector",
        "spot_type_id", "spot_type",
        "spot_modality_id", "spot_modality",
        "spot_state_id", "spot_state",
        "spot_municipality_id", "spot_municipality",
        "spot_corridor_id", "spot_corridor",
        # Numeric feature sources
        "price_total",
        "price_sqm",
        "spot_area_in_sqm",
    ]

    uni_df = uni_df.select([c for c in keep_cols if c in uni_df.columns])

    # Rename target to generic name
    uni_df = uni_df.rename({target_col: "target"})

    context.log.info(
        f"  {universe}: {uni_df.height:,} spots, "
        f"target=1: {uni_df.filter(pl.col('target') == 1).height:,} "
        f"({uni_df.filter(pl.col('target') == 1).height / uni_df.height * 100:.1f}%)"
    )

    return uni_df


@dg.asset(
    group_name="effective_supply_ml",
    description="Core ML: Build Rent and Sale universe DataFrames from core (no leakage columns).",
)
def core_ml_build_universes(
    context: dg.AssetExecutionContext,
    core_effective_supply: pl.DataFrame,
) -> dict:
    """
    Build universe-specific DataFrames for ML training.

    Returns dict with keys 'rent' and 'sale', each a pl.DataFrame with:
    - spot_id, target (0/1), market_universe
    - Categorical IDs: sector, type, modality, state, municipality, corridor
    - Numeric: price_total, price_sqm, spot_area_in_sqm
    """
    context.log.info(
        f"Input core: {core_effective_supply.height:,} rows, "
        f"{core_effective_supply.width} columns"
    )

    for col in LEAKAGE_COLS:
        assert col in core_effective_supply.columns, (
            f"Expected leakage column {col} in core — needed to verify exclusion"
        )

    result = {}
    for universe in UNIVERSES:
        result[universe] = _build_universe(
            core_effective_supply, universe, context
        )

    return result
