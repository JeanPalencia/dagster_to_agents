# defs/effective_supply/silver/core/core_lk_effective_supply_model_metrics.py
"""
Silver Core Final: Persists ML model evaluation metrics with run_id.
Grain: market_universe x model_version.
"""
from datetime import date

import dagster as dg
import polars as pl


@dg.asset(
    group_name="effective_supply_silver",
    description="Core Final: model evaluation metrics (Rent + Sale) with run_id.",
)
def core_lk_effective_supply_model_metrics(
    context: dg.AssetExecutionContext,
    core_ml_train: dict,
    stg_gs_effective_supply_run_id: int,
) -> pl.DataFrame:
    run_meta = core_ml_train["run_metadata"]
    model_version = run_meta["model_version"]
    window_months = run_meta["window_months"]

    today = date.today()
    window_end_date = today.replace(day=1)

    rows = []
    for universe in ["rent", "sale"]:
        uni_data = core_ml_train[universe]
        metrics_random = uni_data["metrics_random"]
        metrics_group = uni_data["metrics_group"]

        rows.append({
            "market_universe": universe.capitalize(),
            "model_version": model_version,
            "model_variant": uni_data["variant"],
            "gate_passed_id": 1 if uni_data.get("gate_passed", True) else 0,
            "gate_passed": "Yes" if uni_data.get("gate_passed", True) else "No",
            "auc_roc_random": metrics_random["auc_roc"],
            "pr_auc_random": metrics_random["pr_auc"],
            "brier_random": metrics_random["brier"],
            "auc_roc_group": metrics_group["auc_roc"],
            "pr_auc_group": metrics_group["pr_auc"],
            "brier_group": metrics_group["brier"],
            "gap_auc": uni_data["gap_auc"],
            "best_iteration": uni_data.get("best_iteration"),
            "window_end_date": window_end_date,
            "window_months": window_months,
        })

        context.log.info(
            f"  {universe}: variant={uni_data['variant']}, "
            f"gate={'PASS' if uni_data.get('gate_passed', True) else 'FAIL'}, "
            f"AUC_random={metrics_random['auc_roc']:.4f}, "
            f"AUC_group={metrics_group['auc_roc']:.4f}, "
            f"gap={uni_data['gap_auc']:.4f}"
        )

    df = pl.DataFrame(rows)
    df = df.with_columns(
        pl.lit(stg_gs_effective_supply_run_id).cast(pl.Int64).alias("run_id")
    )

    context.log.info(
        f"core_lk_effective_supply_model_metrics: "
        f"{df.height} rows, {df.width} columns, run_id={stg_gs_effective_supply_run_id}"
    )
    return df
