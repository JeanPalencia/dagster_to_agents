# defs/effective_supply/ml/ml_score.py
"""
ML asset: Generate propensity scores for every spot in each universe.

Scores the full feature-engineered DataFrame (not just train/test) using
the trained CatBoost model to produce P(is_top_p80 = 1) per spot.
"""
import dagster as dg
import polars as pl
from catboost import Pool

from dagster_pipeline.defs.effective_supply.silver.core.ml.constants import UNIVERSES


def _score_universe(
    feat_data: dict,
    train_result: dict,
    universe: str,
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """Score all spots in one universe."""
    df: pl.DataFrame = feat_data["df"]
    model = train_result["model"]
    active_features = train_result["active_features"]
    cat_feature_indices = train_result["cat_feature_indices"]

    # Prepare features
    X = df.select(active_features).to_pandas()
    pool = Pool(X, cat_features=cat_feature_indices)

    # Predict probabilities
    y_proba = model.predict_proba(pool)[:, 1]

    # Build output DataFrame
    scored = pl.DataFrame({
        "spot_id": df["spot_id"],
        "market_universe": df["market_universe"],
        "p_top_p80": y_proba,
    })

    context.log.info(
        f"  {universe}: {scored.height:,} spots scored, "
        f"p_top_p80 mean={scored['p_top_p80'].mean():.4f}, "
        f"median={scored['p_top_p80'].median():.4f}, "
        f"p95={scored['p_top_p80'].quantile(0.95):.4f}"
    )

    return scored


@dg.asset(
    group_name="effective_supply_ml",
    description="Core ML: Propensity scores P(is_top_p80=1) for all spots per universe.",
)
def core_ml_score(
    context: dg.AssetExecutionContext,
    core_ml_feature_engineering: dict,
    core_ml_train: dict,
) -> dict:
    """
    Score every spot in each universe.

    Returns dict with keys 'rent'/'sale', each a pl.DataFrame with:
    spot_id, market_universe, p_top_p80
    """
    context.log.info("Scoring all spots...")

    result = {}
    for universe in UNIVERSES:
        result[universe] = _score_universe(
            core_ml_feature_engineering[universe],
            core_ml_train[universe],
            universe,
            context,
        )

    return result
