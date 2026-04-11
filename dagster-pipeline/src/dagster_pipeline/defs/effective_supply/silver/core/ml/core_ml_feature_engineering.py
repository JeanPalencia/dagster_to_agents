# defs/effective_supply/ml/ml_feature_engineering.py
"""
ML asset: Feature engineering for each universe.

Transforms:
1. Geographic encoding for municipality and corridor (high cardinality):
   - Coverage-based per state: 90% coverage, floor 3, global min support 10 spots
2. Cast categorical IDs to string (for CatBoost categorical handling)
3. Log1p transforms for numeric features (price_sqm, area)
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.effective_supply.silver.core.ml.constants import (
    ALL_FEATURES,
    CAT_FEATURES,
    HIGH_CARD_COLS,
    NULL_TOKEN,
    NUM_FEATURES,
    OTHER_TOKEN,
    STATE_COL,
    GEO_ENC_COVERAGE,
    GEO_ENC_FLOOR,
    GEO_ENC_MIN_SUPPORT,
    UNIVERSES,
)


def _geo_encode_per_state(
    df: pl.DataFrame,
    source_col: str,
    target_col: str,
    state_col: str,
    coverage: float,
    floor: int,
    min_support: int,
) -> tuple[pl.DataFrame, list[str]]:
    """
    Coverage-based per-state geographic encoding with global minimum support.

    For each state, retain the most frequent values of *source_col* until
    cumulative spot coverage >= *coverage*, with a minimum of *floor* values
    per state (or all if the state has fewer).

    Additionally, a value is only retained if it has >= *min_support* spots
    globally across all states.  This prevents noise from micro-categories
    that would cause overfitting in the model.

    Values not retained are mapped to OTHER_TOKEN; nulls to NULL_TOKEN.

    Returns (transformed df, sorted list of retained IDs as strings).
    """
    # Pre-compute global frequency to enforce min_support
    non_null = df.filter(
        pl.col(source_col).is_not_null() & pl.col(state_col).is_not_null()
    )
    global_freq = (
        non_null.group_by(source_col)
        .agg(pl.len().alias("_gfreq"))
    )
    eligible_ids: set[str] = set(
        global_freq.filter(pl.col("_gfreq") >= min_support)[source_col]
        .cast(pl.Utf8)
        .to_list()
    )

    retained: set[str] = set()
    state_ids = non_null[state_col].unique().to_list()

    for st_id in state_ids:
        st_freq = (
            non_null.filter(pl.col(state_col) == st_id)
            .group_by(source_col)
            .agg(pl.len().alias("_freq"))
            .sort("_freq", descending=True)
        )
        st_total = st_freq["_freq"].sum()
        if st_total == 0:
            continue

        cum = 0
        kept = 0
        for row in st_freq.iter_rows(named=True):
            val_str = str(row[source_col])
            if val_str not in eligible_ids:
                continue
            cum += row["_freq"]
            kept += 1
            retained.add(val_str)
            if cum / st_total >= coverage and kept >= floor:
                break
        # Ensure at least floor eligible values
        if kept < floor:
            for row in st_freq.iter_rows(named=True):
                if kept >= floor:
                    break
                val_str = str(row[source_col])
                if val_str not in eligible_ids:
                    continue
                retained.add(val_str)
                kept += 1

    top_ids = sorted(retained)

    # Map: null -> NULL_TOKEN, not retained -> OTHER_TOKEN, else str(id)
    df = df.with_columns(
        pl.when(pl.col(source_col).is_null())
        .then(pl.lit(NULL_TOKEN))
        .when(pl.col(source_col).cast(pl.Utf8).is_in(top_ids))
        .then(pl.col(source_col).cast(pl.Utf8))
        .otherwise(pl.lit(OTHER_TOKEN))
        .alias(target_col)
    )

    return df, top_ids


def _engineer_universe(
    df: pl.DataFrame,
    universe: str,
    context: dg.AssetExecutionContext,
) -> dict:
    """Apply feature engineering to one universe DataFrame."""

    geo_enc_metadata: dict[str, list[str]] = {}

    # --- 1) Coverage-based per-state geographic encoding for high-cardinality categoricals ---
    for source_col, target_col in HIGH_CARD_COLS.items():
        df, top_ids = _geo_encode_per_state(
            df, source_col, target_col,
            state_col=STATE_COL,
            coverage=GEO_ENC_COVERAGE,
            floor=GEO_ENC_FLOOR,
            min_support=GEO_ENC_MIN_SUPPORT,
        )
        geo_enc_metadata[target_col] = top_ids

        other_count = df.filter(pl.col(target_col) == OTHER_TOKEN).height
        null_count = df.filter(pl.col(target_col) == NULL_TOKEN).height
        context.log.info(
            f"  {universe}/{target_col}: {len(top_ids)} retained "
            f"(coverage={GEO_ENC_COVERAGE:.0%}, floor={GEO_ENC_FLOOR}), "
            f"{other_count:,} OTHER, {null_count:,} NULL"
        )

    # --- 2) Cast low-cardinality categorical IDs to string ---
    low_card_cats = [
        c for c in CAT_FEATURES
        if c not in HIGH_CARD_COLS.values()
    ]
    for col in low_card_cats:
        df = df.with_columns(
            pl.when(pl.col(col).is_null())
            .then(pl.lit(NULL_TOKEN))
            .otherwise(pl.col(col).cast(pl.Utf8))
            .alias(col)
        )

    # --- 3) Log1p transforms for numerics (price_total excluded: redundant with price_sqm * area) ---
    df = df.with_columns([
        pl.col("price_sqm").fill_null(0.0).map_batches(
            lambda s: s.to_frame().select(pl.col(s.name).cast(pl.Float64) + 1.0).to_series().log()
        ).alias("log_price_sqm"),
        pl.col("spot_area_in_sqm").fill_null(0.0).map_batches(
            lambda s: s.to_frame().select(pl.col(s.name).cast(pl.Float64) + 1.0).to_series().log()
        ).alias("log_area_sqm"),
    ])

    # --- 4) Select final feature columns + metadata ---
    final_cols = ["spot_id", "target", "market_universe"] + ALL_FEATURES
    df_final = df.select([c for c in final_cols if c in df.columns])

    context.log.info(
        f"  {universe}: {df_final.height:,} rows, "
        f"{df_final.width} feature columns"
    )

    return {
        "df": df_final,
        "geo_enc_metadata": geo_enc_metadata,
    }


@dg.asset(
    group_name="effective_supply_ml",
    description="Core ML: Feature engineering (geographic encoding, log transforms) per universe.",
)
def core_ml_feature_engineering(
    context: dg.AssetExecutionContext,
    core_ml_build_universes: dict,
) -> dict:
    """
    Apply feature engineering to each universe.

    Uses coverage-based per-state geographic encoding (90% coverage, floor=3)
    for municipality and corridor.

    Returns dict with keys 'rent' and 'sale', each containing:
    - 'df': pl.DataFrame with spot_id, target, market_universe, + ALL_FEATURES
    - 'geo_enc_metadata': dict mapping encoded column -> list of retained IDs
    """
    context.log.info("Feature engineering for ML universes...")

    result = {}
    for universe in UNIVERSES:
        context.log.info(f"Processing {universe}...")
        result[universe] = _engineer_universe(
            core_ml_build_universes[universe], universe, context
        )

    return result
