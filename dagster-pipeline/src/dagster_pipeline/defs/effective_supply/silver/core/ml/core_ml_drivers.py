# defs/effective_supply/ml/ml_drivers.py
"""
ML asset: Compute feature importance via permutation importance.

Uses sklearn's permutation_importance on the full dataset.
Falls back to CatBoost's built-in importance if permutation fails.
"""
import dagster as dg
import numpy as np
import polars as pl
from catboost import Pool

from dagster_pipeline.defs.effective_supply.silver.core.ml.constants import (
    PERM_IMPORTANCE_REPEATS,
    RANDOM_SEED,
    UNIVERSES,
)


def _compute_drivers(
    feat_data: dict,
    train_result: dict,
    universe: str,
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Compute feature importance for one universe."""
    df: pl.DataFrame = feat_data["df"]
    model = train_result["model"]
    active_features = train_result["active_features"]
    cat_feature_indices = train_result["cat_feature_indices"]

    # Use full dataset
    X = df.select(active_features).to_pandas()
    y = df["target"].to_numpy()

    try:
        # Try permutation importance (n_jobs=1 to avoid pickle issues with CatBoost)
        from sklearn.inspection import permutation_importance
        perm_result = permutation_importance(
            model, X, y,
            scoring="roc_auc",
            n_repeats=PERM_IMPORTANCE_REPEATS,
            random_state=RANDOM_SEED,
            n_jobs=1,
        )
        importances = perm_result.importances_mean
        method = "permutation"
        context.log.info(f"  {universe}: permutation importance computed successfully")
    except Exception as e:
        # Fall back to CatBoost's built-in feature importance
        context.log.warning(
            f"  {universe}: permutation importance failed ({e}), "
            f"using CatBoost built-in importance"
        )
        importances = model.get_feature_importance()
        method = "catboost_builtin"

    # Normalise to 0–1
    max_imp = importances.max() if importances.max() > 0 else 1.0
    normalised = importances / max_imp

    drivers_df = pl.DataFrame({
        "market_universe": [universe.capitalize()] * len(active_features),
        "feature_name": active_features,
        "importance": normalised.tolist(),
        "importance_method": [method] * len(active_features),
    })

    # Add rank (1 = most important)
    drivers_df = drivers_df.sort("importance", descending=True).with_row_index(
        name="rank", offset=1
    ).cast({"rank": pl.Int64})

    context.log.info(f"  {universe} top-3 drivers ({method}):")
    for row in drivers_df.head(3).iter_rows(named=True):
        context.log.info(
            f"    #{row['rank']} {row['feature_name']}: "
            f"{row['importance']:.4f}"
        )

    return drivers_df


@dg.asset(
    group_name="effective_supply_ml",
    description="Core ML: Feature importance (drivers) per universe.",
)
def core_ml_drivers(
    context: dg.AssetExecutionContext,
    core_ml_feature_engineering: dict,
    core_ml_train: dict,
) -> dict:
    """
    Compute feature importance for each universe.

    Returns dict with keys 'rent'/'sale', each a pl.DataFrame with:
    market_universe, feature_name, importance, importance_method, rank
    """
    context.log.info("Computing feature drivers...")

    result = {}
    for universe in UNIVERSES:
        result[universe] = _compute_drivers(
            core_ml_feature_engineering[universe],
            core_ml_train[universe],
            universe,
            context,
        )

    return result
