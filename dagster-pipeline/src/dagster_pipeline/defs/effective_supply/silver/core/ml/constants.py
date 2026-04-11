# defs/effective_supply/ml/constants.py
"""
Constants for the ML effective supply pipeline.

All tunable parameters are centralised here for easy adjustment.
"""
RANDOM_SEED: int = 42

# =============================================================================
# Universe definitions
# =============================================================================
RENT_MODALITIES: list[str] = ["Rent", "Rent & Sale"]
SALE_MODALITIES: list[str] = ["Sale", "Rent & Sale"]

TARGET_COL: dict[str, str] = {
    "rent": "is_top_p80_rent_id",
    "sale": "is_top_p80_sale_id",
}
POPULATION_COL: dict[str, str] = {
    "rent": "is_for_rent_id",
    "sale": "is_for_sale_id",
}

UNIVERSES: list[str] = ["rent", "sale"]

# =============================================================================
# Feature lists
# =============================================================================

# Categorical features (CatBoost native handling)
CAT_FEATURES: list[str] = [
    "spot_sector_id",
    "spot_type_id",
    "spot_modality_id",
    "spot_state_id",
    "municipality_enc",
    "corridor_enc",
]

# Numeric features (universe-specific price columns are aliased to these)
# NOTE: log_price_total removed — redundant with log_price_sqm + log_area_sqm
NUM_FEATURES: list[str] = [
    "log_price_sqm",
    "log_area_sqm",
]

# All features used by the model
ALL_FEATURES: list[str] = CAT_FEATURES + NUM_FEATURES

# Universe-specific price column mapping
PRICE_COLS: dict[str, dict[str, str]] = {
    "rent": {
        "price_total": "spot_price_total_mxn_rent",
        "price_sqm": "spot_price_sqm_mxn_rent",
    },
    "sale": {
        "price_total": "spot_price_total_mxn_sale",
        "price_sqm": "spot_price_sqm_mxn_sale",
    },
}

# =============================================================================
# Leakage columns (must NOT be used as features)
# =============================================================================
LEAKAGE_COLS: list[str] = [
    "view_count", "contact_count", "project_count",
    "view_score", "contact_score", "project_score",
    "total_score",
]

# =============================================================================
# Geographic encoding (coverage-based per state)
# =============================================================================
# For each state, retain the most frequent municipalities/corridors until
# cumulative coverage >= GEO_ENC_COVERAGE, with a minimum of GEO_ENC_FLOOR
# per state.  A value is only retained if it has >= GEO_ENC_MIN_SUPPORT spots
# globally in the universe, preventing noise from micro-categories.
GEO_ENC_COVERAGE: float = 0.90
GEO_ENC_FLOOR: int = 3
GEO_ENC_MIN_SUPPORT: int = 10
STATE_COL: str = "spot_state_id"

HIGH_CARD_COLS: dict[str, str] = {
    "spot_municipality_id": "municipality_enc",
    "spot_corridor_id": "corridor_enc",
}

NULL_TOKEN: str = "__NULL__"
OTHER_TOKEN: str = "__OTHER__"

# =============================================================================
# ID → name mapping (for human-readable outputs)
# =============================================================================
ID_TO_NAME_COL: dict[str, str] = {
    "spot_sector_id": "spot_sector",
    "spot_type_id": "spot_type",
    "spot_modality_id": "spot_modality",
    "spot_state_id": "spot_state",
    "spot_municipality_id": "spot_municipality",
    "spot_corridor_id": "spot_corridor",
}

# =============================================================================
# Train / validation / test split
# =============================================================================
TRAIN_RATIO: float = 0.70
VAL_RATIO: float = 0.15
TEST_RATIO: float = 0.15
GROUP_HOLDOUT_FRAC: float = 0.20

# =============================================================================
# CatBoost hyper-parameters
# =============================================================================
CATBOOST_BASE: dict = {
    "loss_function": "Logloss",
    "eval_metric": "AUC",
    "iterations": 2000,
    "learning_rate": 0.05,
    "depth": 6,
    "l2_leaf_reg": 6,
    "min_data_in_leaf": 50,
    "random_seed": RANDOM_SEED,
    "od_type": "Iter",
    "od_wait": 100,
    "verbose": False,
    "allow_writing_files": False,
}

CATBOOST_FALLBACK_1: dict = {
    **CATBOOST_BASE,
    "depth": 4,
    "l2_leaf_reg": 12,
    "min_data_in_leaf": 100,
}

# =============================================================================
# Quality gates
# =============================================================================
GATE_AUC_GROUP_MIN: float = 0.60
GATE_GAP_AUC_MAX: float = 0.05

# =============================================================================
# Fallback training configurations
# =============================================================================
FALLBACK_CONFIGS: list[dict] = [
    {"variant": "base",            "params": CATBOOST_BASE,       "drop_features": []},
    {"variant": "reg",             "params": CATBOOST_FALLBACK_1, "drop_features": []},
    {"variant": "no_muni",         "params": CATBOOST_FALLBACK_1, "drop_features": ["municipality_enc"]},
    {"variant": "no_muni_no_corr", "params": CATBOOST_FALLBACK_1, "drop_features": ["municipality_enc", "corridor_enc"]},
]

# =============================================================================
# Permutation importance
# =============================================================================
PERM_IMPORTANCE_REPEATS: int = 3

# =============================================================================
# Category effects (Fase 2)
# =============================================================================
MIN_SUPPORT: int = 30

# Categorical features for lift analysis (uses original IDs, not encoded)
CAT_EFFECT_FEATURES: list[str] = [
    "spot_sector_id",
    "spot_type_id",
    "spot_modality_id",
    "spot_state_id",
    "spot_municipality_id",
    "spot_corridor_id",
]

# Numeric features to discretise into quintile bins
# NOTE: price_total removed — redundant with price_sqm and area
NUM_EFFECT_FEATURES: list[str] = [
    "price_sqm",
    "spot_area_in_sqm",
]

NUM_QUINTILE_BINS: int = 5

# =============================================================================
# Rules extraction (Fase 3) — Hierarchical Drill-Down + Marascuilo fusion
# =============================================================================
RULES_MIN_SUPPORT: int = 30
RULES_MIN_LIFT: float = 1.5            # Minimum lift to keep a rule (filters out weak state-only rules)
RULES_MIN_LIFT_INCREMENT: float = 0.05  # 5% minimum lift improvement to keep a feature
RULES_MARASCUILO_ALPHA: float = 0.05   # Significance level for Marascuilo proportion test

# Sliding-window range evaluation (replaces fixed quintile bins for rules)
RULES_RANGE_STEP: float = 0.05         # Percentile step for sliding window (5 pp)
RULES_RANGE_MIN_PCTL: float = 0.05     # Lower bound: P5
RULES_RANGE_MAX_PCTL: float = 0.95     # Upper bound: P95

# Strength categories (based on lift)
STRENGTH_THRESHOLDS: list[tuple[float, str]] = [
    (3.0, "Very High"),
    (2.0, "High"),
    (1.5, "Moderate"),
]
