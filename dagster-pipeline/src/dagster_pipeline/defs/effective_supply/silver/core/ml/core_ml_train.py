# defs/effective_supply/ml/ml_train.py
"""
ML asset: Train CatBoost models with QA gate and automatic fallback.

For each universe (Rent / Sale):
1. Stratified split: 70% train / 15% val / 15% test
2. Group holdout: 20% of municipality_enc values for stress test
3. Train CatBoost with early stopping on val AUC
4. Evaluate metrics on random test + group test
5. QA gate: auc_group_test >= 0.60, gap_auc <= 0.05
6. Fallback cascade if gate fails
"""
from datetime import date

import dagster as dg
import numpy as np
import polars as pl
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import brier_score_loss, roc_auc_score
from sklearn.metrics import average_precision_score
from sklearn.model_selection import train_test_split

from dagster_pipeline.defs.effective_supply.silver.core.ml.constants import (
    ALL_FEATURES,
    CAT_FEATURES,
    FALLBACK_CONFIGS,
    GATE_AUC_GROUP_MIN,
    GATE_GAP_AUC_MAX,
    GROUP_HOLDOUT_FRAC,
    NUM_FEATURES,
    OTHER_TOKEN,
    NULL_TOKEN,
    RANDOM_SEED,
    TEST_RATIO,
    TRAIN_RATIO,
    UNIVERSES,
    VAL_RATIO,
)


def _compute_metrics(y_true: np.ndarray, y_proba: np.ndarray) -> dict:
    """Compute AUC-ROC, PR-AUC, and Brier score."""
    return {
        "auc_roc": float(roc_auc_score(y_true, y_proba)),
        "pr_auc": float(average_precision_score(y_true, y_proba)),
        "brier": float(brier_score_loss(y_true, y_proba)),
    }


def _train_universe(
    feat_data: dict,
    universe: str,
    context: dg.AssetExecutionContext,
) -> dict:
    """Train model for one universe with gate + fallback."""

    df: pl.DataFrame = feat_data["df"]
    target = df["target"].to_numpy()

    context.log.info(
        f"  {universe}: {df.height:,} samples, "
        f"target=1: {target.sum():,} ({target.mean()*100:.1f}%)"
    )

    # =========================================================================
    # 1) Stratified split: train / val / test
    # =========================================================================
    indices = np.arange(len(target))
    val_test_ratio = VAL_RATIO + TEST_RATIO

    idx_train, idx_valtest, y_train_split, y_valtest = train_test_split(
        indices, target,
        test_size=val_test_ratio,
        stratify=target,
        random_state=RANDOM_SEED,
    )
    test_frac_of_valtest = TEST_RATIO / val_test_ratio
    idx_val, idx_test, _, _ = train_test_split(
        idx_valtest, y_valtest,
        test_size=test_frac_of_valtest,
        stratify=y_valtest,
        random_state=RANDOM_SEED,
    )

    context.log.info(
        f"  {universe} split: train={len(idx_train):,}, "
        f"val={len(idx_val):,}, test={len(idx_test):,}"
    )

    # =========================================================================
    # 2) Group holdout: 20% of municipality_enc values
    # =========================================================================
    if "municipality_enc" in df.columns:
        unique_munis = df["municipality_enc"].unique()
        muni_values = [
            v for v in unique_munis.to_list()
            if v is not None and v not in (OTHER_TOKEN, NULL_TOKEN)
        ]
    else:
        muni_values = []

    rng = np.random.RandomState(RANDOM_SEED)
    n_holdout = max(1, int(len(muni_values) * GROUP_HOLDOUT_FRAC))
    holdout_munis = set(rng.choice(muni_values, size=n_holdout, replace=False).tolist()) if muni_values else set()

    if "municipality_enc" in df.columns:
        group_mask = df["municipality_enc"].is_in(list(holdout_munis)).to_numpy()
    else:
        group_mask = np.zeros(len(df), dtype=bool)

    idx_group_test = np.where(group_mask)[0]
    idx_group_train = np.where(~group_mask)[0]

    context.log.info(
        f"  {universe} group holdout: {len(holdout_munis)} munis, "
        f"group_train={len(idx_group_train):,}, group_test={len(idx_group_test):,}"
    )

    # =========================================================================
    # 3) Train with fallback cascade
    # =========================================================================
    best_result = None

    for fb_cfg in FALLBACK_CONFIGS:
        variant = fb_cfg["variant"]
        params = fb_cfg["params"]
        drop_features = fb_cfg["drop_features"]

        # Determine active features
        active_features = [f for f in ALL_FEATURES if f not in drop_features]
        active_cat = [f for f in CAT_FEATURES if f in active_features and f in df.columns]
        active_num = [f for f in NUM_FEATURES if f in active_features and f in df.columns]
        active_all = active_cat + active_num

        if not active_all:
            context.log.warning(f"  {universe}/{variant}: no features left, skipping")
            continue

        # Prepare numpy arrays
        X_all = df.select(active_all).to_pandas()
        y_all = target

        # CatBoost pool indices
        cat_feature_indices = list(range(len(active_cat)))

        X_train_arr = X_all.iloc[idx_train]
        y_train_arr = y_all[idx_train]
        X_val_arr = X_all.iloc[idx_val]
        y_val_arr = y_all[idx_val]
        X_test_arr = X_all.iloc[idx_test]
        y_test_arr = y_all[idx_test]

        train_pool = Pool(X_train_arr, y_train_arr, cat_features=cat_feature_indices)
        val_pool = Pool(X_val_arr, y_val_arr, cat_features=cat_feature_indices)
        test_pool = Pool(X_test_arr, cat_features=cat_feature_indices)

        # Train
        model = CatBoostClassifier(**params)
        model.fit(train_pool, eval_set=val_pool, use_best_model=True)

        # Evaluate on random test
        y_proba_test = model.predict_proba(test_pool)[:, 1]
        metrics_random = _compute_metrics(y_test_arr, y_proba_test)

        # Evaluate on group test
        if len(idx_group_test) > 0:
            X_group_test = X_all.iloc[idx_group_test]
            y_group_test = y_all[idx_group_test]
            group_pool = Pool(X_group_test, cat_features=cat_feature_indices)
            y_proba_group = model.predict_proba(group_pool)[:, 1]
            metrics_group = _compute_metrics(y_group_test, y_proba_group)
        else:
            metrics_group = {"auc_roc": 0.0, "pr_auc": 0.0, "brier": 1.0}

        gap_auc = metrics_random["auc_roc"] - metrics_group["auc_roc"]

        context.log.info(
            f"  {universe}/{variant}: "
            f"AUC_random={metrics_random['auc_roc']:.4f}, "
            f"AUC_group={metrics_group['auc_roc']:.4f}, "
            f"gap={gap_auc:.4f}, "
            f"PR-AUC={metrics_random['pr_auc']:.4f}, "
            f"Brier={metrics_random['brier']:.4f}"
        )

        # Check gate
        passed = (
            metrics_group["auc_roc"] >= GATE_AUC_GROUP_MIN
            and gap_auc <= GATE_GAP_AUC_MAX
        )

        if passed:
            context.log.info(f"  {universe}/{variant}: PASSED gate ✅")
            best_result = {
                "model": model,
                "variant": variant,
                "active_features": active_all,
                "cat_feature_indices": cat_feature_indices,
                "metrics_random": metrics_random,
                "metrics_group": metrics_group,
                "gap_auc": gap_auc,
                "best_iteration": model.get_best_iteration(),
            }
            break
        else:
            context.log.info(
                f"  {universe}/{variant}: FAILED gate ❌ "
                f"(auc_group>={GATE_AUC_GROUP_MIN}? "
                f"{metrics_group['auc_roc']:.4f}, "
                f"gap<={GATE_GAP_AUC_MAX}? {gap_auc:.4f})"
            )
            # Keep as candidate in case nothing passes
            if best_result is None or metrics_random["auc_roc"] > best_result["metrics_random"]["auc_roc"]:
                best_result = {
                    "model": model,
                    "variant": variant,
                    "active_features": active_all,
                    "cat_feature_indices": cat_feature_indices,
                    "metrics_random": metrics_random,
                    "metrics_group": metrics_group,
                    "gap_auc": gap_auc,
                    "best_iteration": model.get_best_iteration(),
                    "gate_passed": False,
                }

    if best_result and "gate_passed" not in best_result:
        best_result["gate_passed"] = True

    context.log.info(
        f"  {universe}: selected variant='{best_result['variant']}', "
        f"gate_passed={best_result.get('gate_passed', False)}"
    )

    return best_result


@dg.asset(
    group_name="effective_supply_ml",
    description="Core ML: Train CatBoost models (Rent + Sale) with QA gate and fallback.",
)
def core_ml_train(
    context: dg.AssetExecutionContext,
    core_ml_feature_engineering: dict,
) -> dict:
    """
    Train CatBoost for each universe. Returns dict with keys 'rent'/'sale',
    each containing: model, variant, metrics, active_features, etc.

    Also includes 'run_metadata' with model_version and window info.
    """
    context.log.info("Training ML models...")

    today = date.today()
    model_version = today.strftime("%Y%m")

    results = {"run_metadata": {"model_version": model_version, "window_months": 6}}

    for universe in UNIVERSES:
        context.log.info(f"Training {universe}...")
        results[universe] = _train_universe(
            core_ml_feature_engineering[universe], universe, context
        )

    return results
