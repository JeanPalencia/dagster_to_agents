# defs/effective_supply/ml/ml_category_effects.py
"""
ML asset: Category effects (lift analysis) per universe.

For each categorical feature (and discretised numeric bins), computes:
- support_n: number of spots in the category
- p_top: rate of is_top_p80 = 1 in the category
- lift: p_top / p_top_global  (>1 means better than average)

Uses the original IDs and resolves them to human-readable names.
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.effective_supply.silver.core.ml.constants import (
    CAT_EFFECT_FEATURES,
    ID_TO_NAME_COL,
    MIN_SUPPORT,
    NUM_EFFECT_FEATURES,
    NUM_QUINTILE_BINS,
    UNIVERSES,
)

# Friendly display names for feature columns
_FEATURE_DISPLAY_NAMES: dict[str, str] = {
    "spot_sector_id": "sector",
    "spot_type_id": "tipo",
    "spot_modality_id": "modalidad",
    "spot_state_id": "estado",
    "spot_municipality_id": "municipio",
    "spot_corridor_id": "corredor",
    "price_sqm": "precio_m²",
    "spot_area_in_sqm": "área_m²",
}


def _build_id_to_name_maps(df: pl.DataFrame) -> dict[str, dict[str, str]]:
    """Build {id_col: {id_value_str: name_str}} from the DataFrame."""
    maps: dict[str, dict[str, str]] = {}
    for id_col, name_col in ID_TO_NAME_COL.items():
        if id_col not in df.columns or name_col not in df.columns:
            continue
        pairs = (
            df.select([id_col, name_col])
            .filter(pl.col(id_col).is_not_null())
            .unique()
        )
        mapping = {}
        for row in pairs.iter_rows():
            mapping[str(row[0])] = str(row[1]) if row[1] is not None else "Sin nombre"
        maps[id_col] = mapping
    return maps


def _compute_cat_effects_for_feature(
    df: pl.DataFrame,
    feature_col: str,
    target_col: str,
    p_top_global: float,
    min_support: int,
    name_map: dict[str, str],
) -> pl.DataFrame:
    """Compute category effects for a single categorical feature."""
    grouped = (
        df.filter(pl.col(feature_col).is_not_null())
        .group_by(feature_col)
        .agg([
            pl.len().cast(pl.Int64).alias("support_n"),
            pl.col(target_col).mean().alias("p_top"),
        ])
        .filter(pl.col("support_n") >= min_support)
    )

    if grouped.height == 0:
        return pl.DataFrame(schema={
            "feature_name": pl.Utf8,
            "category_id": pl.Utf8,
            "category_name": pl.Utf8,
            "support_n": pl.Int64,
            "p_top": pl.Float64,
            "lift": pl.Float64,
        })

    feat_display = _FEATURE_DISPLAY_NAMES.get(feature_col, feature_col)

    result = grouped.with_columns([
        pl.lit(feat_display).alias("feature_name"),
        pl.col(feature_col).cast(pl.Utf8).alias("category_id"),
        (pl.col("p_top") / p_top_global).alias("lift"),
    ])

    # Resolve ID → name
    if name_map:
        result = result.with_columns(
            pl.col("category_id").map_elements(
                lambda x: name_map.get(x, x), return_dtype=pl.Utf8
            ).alias("category_name")
        )
    else:
        result = result.with_columns(
            pl.col("category_id").alias("category_name")
        )

    result = result.select([
        "feature_name", "category_id", "category_name", "support_n", "p_top", "lift"
    ])

    return result.sort("lift", descending=True)


def _compute_numeric_bin_effects(
    df: pl.DataFrame,
    num_col: str,
    target_col: str,
    p_top_global: float,
    n_bins: int,
    min_support: int,
) -> pl.DataFrame:
    """Discretise a numeric column into quintiles and compute effects per bin."""
    valid = df.filter(pl.col(num_col).is_not_null() & (pl.col(num_col) > 0))
    if valid.height < n_bins * min_support:
        return pl.DataFrame(schema={
            "feature_name": pl.Utf8,
            "category_id": pl.Utf8,
            "category_name": pl.Utf8,
            "support_n": pl.Int64,
            "p_top": pl.Float64,
            "lift": pl.Float64,
        })

    quantiles = [i / n_bins for i in range(n_bins + 1)]
    q_values = [float(valid[num_col].quantile(q)) for q in quantiles]

    feat_display = _FEATURE_DISPLAY_NAMES.get(num_col, num_col)
    is_price = "price" in num_col

    rows = []
    for i in range(n_bins):
        lo, hi = q_values[i], q_values[i + 1]
        if i < n_bins - 1:
            band = valid.filter((pl.col(num_col) >= lo) & (pl.col(num_col) < hi))
        else:
            band = valid.filter(pl.col(num_col) >= lo)

        if band.height >= min_support:
            p_top = band[target_col].mean()
            lift = p_top / p_top_global if p_top_global > 0 else 0.0

            bin_id = f"Q{i+1}"
            if is_price:
                if i < n_bins - 1:
                    bin_name = f"${lo:,.0f} – ${hi:,.0f}"
                else:
                    bin_name = f"${lo:,.0f}+"
            else:
                if i < n_bins - 1:
                    bin_name = f"{lo:,.0f} – {hi:,.0f} m²"
                else:
                    bin_name = f"{lo:,.0f}+ m²"

            rows.append({
                "feature_name": feat_display,
                "category_id": bin_id,
                "category_name": bin_name,
                "support_n": band.height,
                "p_top": float(p_top),
                "lift": float(lift),
            })

    if not rows:
        return pl.DataFrame(schema={
            "feature_name": pl.Utf8,
            "category_id": pl.Utf8,
            "category_name": pl.Utf8,
            "support_n": pl.Int64,
            "p_top": pl.Float64,
            "lift": pl.Float64,
        })

    return pl.DataFrame(rows).sort("lift", descending=True)


def _compute_universe_effects(
    uni_df: pl.DataFrame,
    universe: str,
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Compute category effects for all features in one universe."""
    target_col = "target"
    p_top_global = float(uni_df[target_col].mean())

    # Build ID → name maps
    id_name_maps = _build_id_to_name_maps(uni_df)

    context.log.info(
        f"  {universe}: {uni_df.height:,} spots, "
        f"p_top_global={p_top_global:.4f}"
    )

    frames = []

    # Categorical features
    for feat in CAT_EFFECT_FEATURES:
        if feat in uni_df.columns:
            name_map = id_name_maps.get(feat, {})
            effects = _compute_cat_effects_for_feature(
                uni_df, feat, target_col, p_top_global, MIN_SUPPORT, name_map
            )
            if effects.height > 0:
                frames.append(effects)
                context.log.info(
                    f"  {universe}/{feat}: {effects.height} categories (support >= {MIN_SUPPORT})"
                )

    # Numeric features (discretised)
    for feat in NUM_EFFECT_FEATURES:
        if feat in uni_df.columns:
            effects = _compute_numeric_bin_effects(
                uni_df, feat, target_col, p_top_global, NUM_QUINTILE_BINS, MIN_SUPPORT
            )
            if effects.height > 0:
                frames.append(effects)
                context.log.info(
                    f"  {universe}/{feat}: {effects.height} bins"
                )

    if not frames:
        return pl.DataFrame(schema={
            "market_universe": pl.Utf8,
            "feature_name": pl.Utf8,
            "category_id": pl.Utf8,
            "category_name": pl.Utf8,
            "support_n": pl.Int64,
            "p_top": pl.Float64,
            "lift": pl.Float64,
        })

    result = pl.concat(frames)
    result = result.with_columns(
        pl.lit(universe.capitalize()).alias("market_universe")
    )

    return result


@dg.asset(
    group_name="effective_supply_ml",
    description="Core ML: Category effects (lift per feature value) per universe.",
)
def core_ml_category_effects(
    context: dg.AssetExecutionContext,
    core_ml_build_universes: dict,
) -> dict:
    """
    Compute lift per category for each feature in each universe.

    Returns dict with keys 'rent'/'sale', each a pl.DataFrame with:
    market_universe, feature_name, category_id, category_name, support_n, p_top, lift
    """
    context.log.info("Computing category effects...")

    result = {}
    for universe in UNIVERSES:
        result[universe] = _compute_universe_effects(
            core_ml_build_universes[universe], universe, context
        )

    return result
