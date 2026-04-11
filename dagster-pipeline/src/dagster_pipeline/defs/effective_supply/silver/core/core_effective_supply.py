# defs/effective_supply/silver/core/core_effective_supply.py
"""
Silver Core: Effective Supply - Summarized by spot_id with relevance scores,
spot attributes, top P80 flags, and modality flags.

This asset:
1. Pivots event counts from core_effective_supply_events into columns
   (view_count, contact_count, project_count) per spot_id
2. Computes relevance scores using log-normalized p95-clipped method
3. INNER JOINs with lk_spots to enrich with spot attributes
4. Computes is_top_p80 flag (global + per modality population)
5. Computes modality flags (is_for_rent, is_for_sale)

Output: One row per spot_id with event counts, scores, flags, and spot attributes.
"""
import math

import dagster as dg
import polars as pl

# =============================================================================
# Score weights (must sum to 1.0)
# Adjust W_PROJECT and W_CONTACT; W_VIEW is the complement.
# =============================================================================
W_PROJECT: float = 0.7
W_CONTACT: float = 0.2
W_VIEW: float = 1.0 - W_PROJECT - W_CONTACT  # 0.1


def _score_expr(col_name: str, p95_val: float) -> pl.Expr:
    """
    Build a Polars expression for log-normalized, p95-clipped score.

    Formula: min(1.0, ln(1 + X) / ln(1 + p95_X))
    Returns 0.0 when p95 is 0 (no positive values exist).
    """
    score_name = col_name.replace("_count", "_score")
    if p95_val > 0:
        return (
            (pl.col(col_name).cast(pl.Float64) + 1.0).log() / math.log(1.0 + p95_val)
        ).clip(0.0, 1.0).alias(score_name)
    else:
        return pl.lit(0.0).alias(score_name)


@dg.asset(
    group_name="effective_supply_silver",
    description=(
        "Silver Core: pivots event types into columns per spot_id, "
        "computes relevance scores (log + p95 + clipping), "
        "then INNER JOINs with lk_spots for spot attributes."
    ),
)
def core_effective_supply(
    context: dg.AssetExecutionContext,
    core_effective_supply_events: pl.DataFrame,
    stg_gs_lk_spots: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds the core effective supply table summarized by spot_id.

    Steps:
    1. Pivot event counts by event_type_id into columns:
       - view_count (event_type_id == 2)
       - contact_count (event_type_id == 1)
       - project_count (event_type_id == 3)
    2. Compute relevance scores per count (log + p95 + clipping)
       and total_score (weighted sum)
    3. INNER JOIN with stg_gs_lk_spots by spot_id for enrichment

    Schema (29 columns):
    - spot_id
    - Counts: view_count, contact_count, project_count
    - Scores: view_score, contact_score, project_score, total_score
    - Spot attributes from lk_spots (21 columns)
    """
    # =========================================================================
    # 1. Pivot: one row per spot_id with count columns per event type
    # =========================================================================
    df_pivoted = (
        core_effective_supply_events
        .group_by("spot_id")
        .agg([
            pl.col("event_count")
            .filter(pl.col("event_type_id") == 2)
            .sum()
            .alias("view_count"),

            pl.col("event_count")
            .filter(pl.col("event_type_id") == 1)
            .sum()
            .alias("contact_count"),

            pl.col("event_count")
            .filter(pl.col("event_type_id") == 3)
            .sum()
            .alias("project_count"),
        ])
        # Fill nulls with 0 (spots that only have some event types)
        .with_columns([
            pl.col("view_count").fill_null(0).cast(pl.Int64),
            pl.col("contact_count").fill_null(0).cast(pl.Int64),
            pl.col("project_count").fill_null(0).cast(pl.Int64),
        ])
    )
    context.log.info(
        f"Pivoted events: {df_pivoted.height:,} unique spots "
        f"(from {core_effective_supply_events.height:,} event rows)"
    )

    # =========================================================================
    # 2. Relevance scores: log-normalized, p95-clipped, range [0.0, 1.0]
    # =========================================================================
    # Compute p95 on values > 0 (scalar)
    p95_view = (
        df_pivoted.filter(pl.col("view_count") > 0)["view_count"]
        .quantile(0.95, "linear") or 0.0
    )
    p95_contact = (
        df_pivoted.filter(pl.col("contact_count") > 0)["contact_count"]
        .quantile(0.95, "linear") or 0.0
    )
    p95_project = (
        df_pivoted.filter(pl.col("project_count") > 0)["project_count"]
        .quantile(0.95, "linear") or 0.0
    )
    context.log.info(
        f"Score p95 thresholds: view={p95_view}, contact={p95_contact}, project={p95_project}"
    )

    # Individual scores
    df_pivoted = df_pivoted.with_columns([
        _score_expr("view_count", float(p95_view)),
        _score_expr("contact_count", float(p95_contact)),
        _score_expr("project_count", float(p95_project)),
    ])

    # Weighted total score + sqrt adjustment to reduce skew toward 0
    df_pivoted = df_pivoted.with_columns(
        (
            W_VIEW * pl.col("view_score")
            + W_CONTACT * pl.col("contact_score")
            + W_PROJECT * pl.col("project_score")
        ).clip(0.0, 1.0).pow(0.4).clip(0.0, 1.0).alias("total_score")
    )
    context.log.info(
        f"Score weights: view={W_VIEW}, contact={W_CONTACT}, project={W_PROJECT} "
        f"(sqrt-adjusted)"
    )

    # =========================================================================
    # 3. INNER JOIN with lk_spots for enrichment
    # =========================================================================
    # Ensure spot_id types match for join (Int64)
    df_spots = stg_gs_lk_spots.with_columns(
        pl.col("spot_id").cast(pl.Int64).alias("spot_id")
    )

    df_result = df_pivoted.join(
        df_spots,
        on="spot_id",
        how="inner",
    )

    context.log.info(
        f"core_effective_supply: {df_result.height:,} rows after INNER JOIN with lk_spots "
        f"({df_pivoted.height - df_result.height:,} rows dropped - spots not in lk_spots)"
    )

    # =========================================================================
    # 4. is_top_p80: all spots are marketable (complexes excluded in stg).
    #    Ties at the p80 threshold are included (>=).
    # =========================================================================
    p80_threshold = df_result["total_score"].quantile(0.80, "nearest")
    context.log.info(
        f"is_top_p80 threshold: {p80_threshold:.6f} "
        f"(computed on {df_result.height:,} spots)"
    )

    is_top_expr = pl.col("total_score") >= p80_threshold
    df_result = df_result.with_columns([
        pl.when(is_top_expr).then(1).otherwise(0).cast(pl.Int64).alias("is_top_p80_id"),
        pl.when(is_top_expr).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("is_top_p80"),
    ])

    top_count = df_result.filter(pl.col("is_top_p80_id") == 1).height
    context.log.info(f"is_top_p80=True: {top_count:,} spots")

    # =========================================================================
    # 5. Modality flags: is_for_rent / is_for_sale
    #    "Rent & Sale" belongs to BOTH populations.
    # =========================================================================
    rent_expr = pl.col("spot_modality").is_in(["Rent", "Rent & Sale"])
    sale_expr = pl.col("spot_modality").is_in(["Sale", "Rent & Sale"])

    df_result = df_result.with_columns([
        pl.when(rent_expr).then(1).otherwise(0).cast(pl.Int64).alias("is_for_rent_id"),
        pl.when(rent_expr).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("is_for_rent"),
        pl.when(sale_expr).then(1).otherwise(0).cast(pl.Int64).alias("is_for_sale_id"),
        pl.when(sale_expr).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("is_for_sale"),
    ])

    rent_pop = df_result.filter(pl.col("is_for_rent_id") == 1)
    sale_pop = df_result.filter(pl.col("is_for_sale_id") == 1)
    context.log.info(
        f"Modality populations: rent={rent_pop.height:,}, sale={sale_pop.height:,}"
    )

    # =========================================================================
    # 6. Top P80 per modality population (rent / sale)
    #    Spots outside the population always get 0/No.
    # =========================================================================
    p80_rent = rent_pop["total_score"].quantile(0.80, "nearest")
    p80_sale = sale_pop["total_score"].quantile(0.80, "nearest")
    context.log.info(
        f"is_top_p80_rent threshold: {p80_rent:.6f} "
        f"(on {rent_pop.height:,} rent spots)"
    )
    context.log.info(
        f"is_top_p80_sale threshold: {p80_sale:.6f} "
        f"(on {sale_pop.height:,} sale spots)"
    )

    is_top_rent_expr = (pl.col("is_for_rent_id") == 1) & (pl.col("total_score") >= p80_rent)
    is_top_sale_expr = (pl.col("is_for_sale_id") == 1) & (pl.col("total_score") >= p80_sale)

    df_result = df_result.with_columns([
        pl.when(is_top_rent_expr).then(1).otherwise(0).cast(pl.Int64).alias("is_top_p80_rent_id"),
        pl.when(is_top_rent_expr).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("is_top_p80_rent"),
        pl.when(is_top_sale_expr).then(1).otherwise(0).cast(pl.Int64).alias("is_top_p80_sale_id"),
        pl.when(is_top_sale_expr).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("is_top_p80_sale"),
    ])

    top_rent_count = df_result.filter(pl.col("is_top_p80_rent_id") == 1).height
    top_sale_count = df_result.filter(pl.col("is_top_p80_sale_id") == 1).height
    context.log.info(f"is_top_p80_rent=Yes: {top_rent_count:,} spots")
    context.log.info(f"is_top_p80_sale=Yes: {top_sale_count:,} spots")

    return df_result
