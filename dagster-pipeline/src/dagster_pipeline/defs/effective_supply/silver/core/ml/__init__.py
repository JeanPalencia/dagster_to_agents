# defs/effective_supply/silver/core/ml/__init__.py
"""
Core ML sub-pipeline for Effective Supply propensity scoring.

Trains separate CatBoost models for Rent and Sale populations to predict
P(is_top_p80 = 1) using only spot attributes (no leakage from counts/scores).
"""

from dagster_pipeline.defs.effective_supply.silver.core.ml.core_ml_build_universes import (
    core_ml_build_universes,
)
from dagster_pipeline.defs.effective_supply.silver.core.ml.core_ml_feature_engineering import (
    core_ml_feature_engineering,
)
from dagster_pipeline.defs.effective_supply.silver.core.ml.core_ml_train import (
    core_ml_train,
)
from dagster_pipeline.defs.effective_supply.silver.core.ml.core_ml_score import (
    core_ml_score,
)
from dagster_pipeline.defs.effective_supply.silver.core.ml.core_ml_drivers import (
    core_ml_drivers,
)
from dagster_pipeline.defs.effective_supply.silver.core.ml.core_ml_category_effects import (
    core_ml_category_effects,
)
from dagster_pipeline.defs.effective_supply.silver.core.ml.core_ml_rules import (
    core_ml_rules,
)

__all__ = [
    "core_ml_build_universes",
    "core_ml_feature_engineering",
    "core_ml_train",
    "core_ml_score",
    "core_ml_drivers",
    "core_ml_category_effects",
    "core_ml_rules",
]
