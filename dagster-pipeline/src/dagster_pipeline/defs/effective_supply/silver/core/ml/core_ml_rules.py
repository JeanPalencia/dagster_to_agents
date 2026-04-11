# defs/effective_supply/ml/ml_rules.py
"""
ML asset: Hierarchical Drill-Down rules with Marascuilo fusion.

Algorithm:
1. Start with estado (always first, highest importance)
2. For each state with n >= RULES_MIN_SUPPORT:
   a. Evaluate municipio AND corredor independently (each generates its
      own rule branches if lift increment >= 5%)
   b. Generate GRANULAR candidate rules: one per combination of
      geo_value x tipo x sector that beats the parent p_top
   c. Attach precio_sqm_range and area_range as context
3. Fusion phase (Marascuilo):
   a. Within each group sharing (estado, precio_sqm_range, area_range),
      apply Marascuilo test to identify statistically indistinguishable rules
   b. Build maximal cliques (greedy, ordered by p_top desc)
   c. Merge clique members: union categorical values, recalculate p_top/n
4. Output: one row per merged rule with individual columns per variable

modalidad is NOT included — already implied by market_universe (Rent/Sale).
Municipio/corredor: coverage-based per-state geographic encoding (90% coverage,
floor 3, min_support 10).
"""
import math

import dagster as dg
import polars as pl
from scipy.stats import chi2

from dagster_pipeline.defs.effective_supply.silver.core.ml.constants import (
    ID_TO_NAME_COL,
    POPULATION_COL,
    RULES_MARASCUILO_ALPHA,
    RULES_MIN_LIFT,
    RULES_MIN_LIFT_INCREMENT,
    RULES_MIN_SUPPORT,
    RULES_RANGE_MAX_PCTL,
    RULES_RANGE_MIN_PCTL,
    RULES_RANGE_STEP,
    STATE_COL,
    STRENGTH_THRESHOLDS,
    GEO_ENC_COVERAGE,
    GEO_ENC_FLOOR,
    GEO_ENC_MIN_SUPPORT,
    UNIVERSES,
)


# ── Feature configuration ──────────────────────────────────────────────────

# Mapping from driver feature_name to (id_col, name_col, output_col)
# NOTE: spot_modality_id excluded — redundant with market_universe split
_FEATURE_MAP: dict[str, tuple[str, str, str]] = {
    "spot_state_id": ("spot_state_id", "spot_state", "spot_state"),
    "spot_sector_id": ("spot_sector_id", "spot_sector", "spot_sector"),
    "spot_type_id": ("spot_type_id", "spot_type", "spot_type"),
    "municipality_enc": ("spot_municipality_id", "spot_municipality", "spot_municipality"),
    "corridor_enc": ("spot_corridor_id", "spot_corridor", "spot_corridor"),
}

_NUM_FEATURES: list[tuple[str, str, str]] = [
    # (driver_feature_name, source_col, output_col)
    ("log_price_sqm", "price_sqm", "price_sqm_range"),
    ("log_area_sqm", "spot_area_in_sqm", "area_range"),
]

# Categorical features eligible for Marascuilo fusion
# (tipo, sector, and geo — only if they share estado + price + area context)
_FUSABLE_COLS: set[str] = {"spot_type", "spot_sector", "spot_municipality", "spot_corridor"}

# All output columns for the rules table (in order)
RULE_COLUMNS: list[str] = [
    "market_universe", "rule_id",
    "spot_state", "spot_sector", "spot_type", "spot_municipality", "spot_corridor",
    "price_sqm_range", "price_sqm_pctl_range",
    "price_sqm_is_discriminant_id", "price_sqm_is_discriminant",
    "area_range", "area_pctl_range",
    "area_is_discriminant_id", "area_is_discriminant",
    "rule_text", "support_n", "p_top", "lift",
    "avg_score_percentile", "strength",
]


# ── Numeric discretisation helpers ──────────────────────────────────────────

def _format_range(col: str, lo: float, hi: float | None) -> str:
    """Format a numeric range for display."""
    is_price = "price" in col
    if hi is None:
        return f"${lo:,.0f}+" if is_price else f"{lo:,.0f}+ m²"
    if is_price:
        return f"${lo:,.0f} – ${hi:,.0f}"
    return f"{lo:,.0f} – {hi:,.0f} m²"


def _format_pctl_range(lo_pctl: float, hi_pctl: float) -> str:
    """Format a percentile range for display, e.g. 'P25-P70'."""
    return f"P{int(lo_pctl * 100)}-P{int(hi_pctl * 100)}"


def _observed_range_label(series: pl.Series, col: str) -> str | None:
    """Compute the observed P5-P95 range for a numeric series, formatted."""
    valid = series.filter(series.is_not_null() & (series > 0))
    if valid.len() < 10:
        return None
    lo = float(valid.quantile(RULES_RANGE_MIN_PCTL))
    hi = float(valid.quantile(RULES_RANGE_MAX_PCTL))
    return _format_range(col, lo, hi)


def _best_sliding_window(
    values: pl.Series,
    targets: pl.Series,
    p_top_parent: float,
    min_support: int,
) -> tuple[float, float, float, int, float, float] | None:
    """Find the best contiguous percentile window that maximises lift.

    Uses a sliding-window approach over percentile cut-points with cumulative
    sums for efficient evaluation of all (lo, hi) pairs.

    Parameters
    ----------
    values : pl.Series
        Numeric feature values (price_sqm or area) for the candidate subgroup.
    targets : pl.Series
        Binary target (0/1) aligned with *values*.
    p_top_parent : float
        The p_top of the candidate *before* applying any numeric filter.
        Used to check whether the best window provides incremental lift.
    min_support : int
        Minimum number of spots required inside the window.

    Returns
    -------
    tuple(lo_val, hi_val, p_top, n, lo_pctl, hi_pctl) if a window with
    lift > parent was found, or None if no window beats the parent or has
    enough support. lo_pctl/hi_pctl are the percentile boundaries (e.g. 0.25, 0.70).

    Algorithm
    ---------
    1. Filter to valid (non-null, >0) values.
    2. Compute 19 percentile cut-points: P5, P10, ..., P95 of the subgroup.
    3. Sort spots by value; build a cumulative-sum array of the target column.
    4. For each of the 171 pairs (P_lo, P_hi):
       - Use binary search on the sorted values to find index boundaries.
       - Derive n and sum_target by subtracting cumulative sums.
       - Compute p_top = sum_target / n.
    5. Return the (lo, hi) pair that maximises p_top, subject to n >= min_support.
    """
    import numpy as np

    mask = (values.is_not_null() & (values > 0)).to_numpy()
    vals_arr = values.to_numpy()[mask]
    tgt_arr = targets.to_numpy()[mask]

    if len(vals_arr) < min_support:
        return None

    order = np.argsort(vals_arr)
    vals_sorted = vals_arr[order]
    tgt_sorted = tgt_arr[order]

    cum_target = np.concatenate(([0.0], np.cumsum(tgt_sorted)))

    pctl_points = np.arange(
        RULES_RANGE_MIN_PCTL,
        RULES_RANGE_MAX_PCTL + RULES_RANGE_STEP / 2,
        RULES_RANGE_STEP,
    )
    cut_values = np.quantile(vals_sorted, pctl_points)

    best_ptop = -1.0
    best_lo = 0.0
    best_hi = 0.0
    best_n = 0
    best_lo_pctl = 0.0
    best_hi_pctl = 0.0

    for i in range(len(cut_values)):
        lo_val = cut_values[i]
        idx_lo = int(np.searchsorted(vals_sorted, lo_val, side="left"))

        for j in range(i + 1, len(cut_values)):
            hi_val = cut_values[j]
            idx_hi = int(np.searchsorted(vals_sorted, hi_val, side="right"))

            n = idx_hi - idx_lo
            if n < min_support:
                continue

            sum_target = cum_target[idx_hi] - cum_target[idx_lo]
            p_top = sum_target / n

            if p_top > best_ptop:
                best_ptop = p_top
                best_lo = lo_val
                best_hi = hi_val
                best_n = n
                best_lo_pctl = float(pctl_points[i])
                best_hi_pctl = float(pctl_points[j])

    if best_n < min_support or best_ptop <= 0:
        return None

    return (
        float(best_lo), float(best_hi), float(best_ptop), int(best_n),
        best_lo_pctl, best_hi_pctl,
    )


# ── Geographic encoding (coverage-based per state) ────────────────────────

def _geo_encode_series(
    df: pl.DataFrame,
    id_col: str,
    name_col: str,
    state_col: str,
    coverage: float,
    floor: int,
    min_support: int,
) -> pl.Series:
    """Coverage-based per-state geographic encoding with global min support.

    Returns a name series with 'Others' for non-retained values.
    """
    non_null = df.filter(
        pl.col(id_col).is_not_null() & pl.col(state_col).is_not_null()
    )
    # Global frequency filter
    global_freq = non_null.group_by(id_col).agg(pl.len().alias("_gfreq"))
    eligible_ids: set[str] = set(
        global_freq.filter(pl.col("_gfreq") >= min_support)[id_col]
        .cast(pl.Utf8)
        .to_list()
    )

    retained: set[str] = set()
    for st_id in non_null[state_col].unique().to_list():
        st_freq = (
            non_null.filter(pl.col(state_col) == st_id)
            .group_by(id_col)
            .agg(pl.len().alias("_freq"))
            .sort("_freq", descending=True)
        )
        st_total = st_freq["_freq"].sum()
        if st_total == 0:
            continue

        cum = 0
        kept = 0
        for row in st_freq.iter_rows(named=True):
            val_str = str(row[id_col])
            if val_str not in eligible_ids:
                continue
            cum += row["_freq"]
            kept += 1
            retained.add(val_str)
            if cum / st_total >= coverage and kept >= floor:
                break
        if kept < floor:
            for row in st_freq.iter_rows(named=True):
                if kept >= floor:
                    break
                val_str = str(row[id_col])
                if val_str not in eligible_ids:
                    continue
                retained.add(val_str)
                kept += 1

    id_to_name: dict[str, str] = {}
    if name_col in df.columns:
        pairs = df.filter(pl.col(id_col).is_not_null()).select([id_col, name_col]).unique()
        for row in pairs.iter_rows():
            id_to_name[str(row[0])] = str(row[1]) if row[1] is not None else str(row[0])

    values = []
    for row_id in df[id_col].to_list():
        if row_id is None:
            values.append(None)
        elif str(row_id) in retained:
            values.append(id_to_name.get(str(row_id), str(row_id)))
        else:
            values.append("Others")

    return pl.Series("_geo_enc", values)


# ── Helpers for granular candidate generation ──────────────────────────────

def _category_stats(
    df: pl.DataFrame,
    cat_col: str,
    target_col: str,
    min_support: int,
) -> list[tuple]:
    """Return (value, p_top, n) for each category with enough support."""
    stats = (
        df.filter(pl.col(cat_col).is_not_null())
        .group_by(cat_col)
        .agg([
            pl.col(target_col).mean().alias("p_top"),
            pl.len().cast(pl.Int64).alias("n"),
        ])
        .filter(pl.col("n") >= min_support)
        .sort("p_top", descending=True)
    )
    return [(row[0], row[1], row[2]) for row in stats.iter_rows()]


def _best_categories(
    df: pl.DataFrame,
    cat_col: str,
    target_col: str,
    p_top_parent: float,
    min_support: int,
) -> list[str]:
    """Return categories whose p_top > parent p_top and have enough support."""
    stats = _category_stats(df, cat_col, target_col, min_support)
    return [val for val, p, n in stats if p > p_top_parent]


def _compute_lift_with_feature(
    df: pl.DataFrame,
    filter_values: list,
    filter_col: str,
    target_col: str,
) -> float:
    """Compute p_top after filtering to given values."""
    filtered = df.filter(pl.col(filter_col).is_in(filter_values))
    if filtered.height == 0:
        return 0.0
    return float(filtered[target_col].mean())


# ── Marascuilo fusion ──────────────────────────────────────────────────────

def _marascuilo_groups(
    candidates: list[dict],
    alpha: float,
) -> list[list[int]]:
    """
    Group candidate rules by Marascuilo test for k proportions.

    Each candidate must have 'p_top' (float) and 'support_n' (int).
    Returns list of groups, where each group is a list of candidate indices.
    Groups are built greedily as maximal cliques (no transitivity bridge).
    """
    k = len(candidates)
    if k <= 1:
        return [list(range(k))]

    # Chi-squared critical value for k-1 degrees of freedom
    chi2_crit = float(chi2.ppf(1 - alpha, k - 1))

    # Pre-compute pairwise indistinguishability
    indist = [[False] * k for _ in range(k)]
    for i in range(k):
        indist[i][i] = True
        pi, ni = candidates[i]["p_top"], candidates[i]["support_n"]
        for j in range(i + 1, k):
            pj, nj = candidates[j]["p_top"], candidates[j]["support_n"]
            r_ij = math.sqrt(
                pi * (1 - pi) / ni + pj * (1 - pj) / nj
            ) * math.sqrt(chi2_crit)
            if abs(pi - pj) < r_ij:
                indist[i][j] = True
                indist[j][i] = True

    # Greedy clique building (ordered by p_top descending)
    order = sorted(range(k), key=lambda i: -candidates[i]["p_top"])
    assigned = [False] * k
    groups: list[list[int]] = []

    for seed in order:
        if assigned[seed]:
            continue
        group = [seed]
        assigned[seed] = True

        for candidate_idx in order:
            if assigned[candidate_idx]:
                continue
            # Check that candidate is indistinguishable with ALL current group members
            if all(indist[candidate_idx][g] for g in group):
                group.append(candidate_idx)
                assigned[candidate_idx] = True

        groups.append(group)

    return groups


def _merge_candidates(
    candidates: list[dict],
    group_indices: list[int],
    working_df: pl.DataFrame,
    target_col: str,
    p_top_global: float,
) -> dict:
    """
    Merge a group of candidate rules into one rule.

    - Union categorical values (spot_type, spot_sector, spot_municipality, spot_corridor)
    - Recalculate p_top and n from the union of matching rows
    - Keep shared context (spot_state, price_sqm_range, area_range)
    """
    members = [candidates[i] for i in group_indices]

    # Start from the first member (all share estado + price + area context)
    merged = dict(members[0])

    # Union fusable columns
    for col in _FUSABLE_COLS:
        all_vals: list[str] = []
        for m in members:
            val = m.get(col)
            if val is not None:
                for v in val.split(", "):
                    if v not in all_vals:
                        all_vals.append(v)
        merged[col] = ", ".join(all_vals) if all_vals else None

    # Build combined filter to recalculate metrics from actual data
    mask = pl.lit(False)
    for m in members:
        m_mask = pl.lit(True)
        if m.get("_filter_col") and m.get("_filter_vals"):
            m_mask = m_mask & pl.col(m["_filter_col"]).is_in(m["_filter_vals"])
        if m.get("_tipo_val") is not None:
            m_mask = m_mask & (pl.col("spot_type_id") == m["_tipo_val"])
        if m.get("_sector_val") is not None:
            m_mask = m_mask & (pl.col("spot_sector_id") == m["_sector_val"])
        mask = mask | m_mask

    union_df = working_df.filter(mask)

    merged["support_n"] = union_df.height
    if union_df.height > 0:
        merged["p_top"] = float(union_df[target_col].mean())
    else:
        merged["p_top"] = members[0]["p_top"]
    merged["lift"] = merged["p_top"] / p_top_global if p_top_global > 0 else 0.0

    # Clean internal keys
    for key in ("_filter_col", "_filter_vals", "_tipo_val", "_sector_val"):
        merged.pop(key, None)

    return merged


# ── Core algorithm ──────────────────────────────────────────────────────────

def _compute_universe_rules(
    uni_df: pl.DataFrame,
    universe: str,
    drivers_df: pl.DataFrame,
    context: dg.AssetExecutionContext,
    gold_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Generate hierarchical rules with Marascuilo fusion for one universe."""
    target_col = "target"
    p_top_global = float(uni_df[target_col].mean())

    if p_top_global == 0:
        context.log.warning(f"  {universe}: p_top_global=0, skipping")
        return _empty_rules_df()

    # ── Feature importance order from drivers ─────────────────────────
    importance_order = (
        drivers_df.sort("importance", descending=True)["feature_name"].to_list()
    )

    context.log.info(
        f"  {universe}: p_top_global={p_top_global:.4f}, "
        f"importance order: {importance_order}"
    )

    # ── Build id→name maps ────────────────────────────────────────────
    cat_name_maps: dict[str, dict[str, str]] = {}
    for feat_name, (id_col, name_col, _) in _FEATURE_MAP.items():
        if id_col in uni_df.columns and name_col in uni_df.columns:
            pairs = (
                uni_df.filter(pl.col(id_col).is_not_null())
                .select([id_col, name_col])
                .unique()
            )
            nm = {}
            for row in pairs.iter_rows():
                nm[str(row[0])] = str(row[1]) if row[1] is not None else str(row[0])
            cat_name_maps[id_col] = nm

    state_names = cat_name_maps.get("spot_state_id", {})

    # ── Pre-compute coverage-based per-state encoding for municipality and corridor
    muni_enc = _geo_encode_series(
        uni_df, "spot_municipality_id", "spot_municipality",
        state_col=STATE_COL, coverage=GEO_ENC_COVERAGE, floor=GEO_ENC_FLOOR,
        min_support=GEO_ENC_MIN_SUPPORT,
    )
    corr_enc = _geo_encode_series(
        uni_df, "spot_corridor_id", "spot_corridor",
        state_col=STATE_COL, coverage=GEO_ENC_COVERAGE, floor=GEO_ENC_FLOOR,
        min_support=GEO_ENC_MIN_SUPPORT,
    )
    df = uni_df.with_columns([
        muni_enc.alias("_muni_enc"),
        corr_enc.alias("_corr_enc"),
    ])

    # ── Determine which non-geo categorical features to explore ───────
    # Exclude state (always first), geo (handled separately), and modalidad
    geo_features = {"municipality_enc", "corridor_enc"}
    drillable_cats: list[str] = []
    for feat_name in importance_order:
        if feat_name in geo_features or feat_name == "spot_state_id":
            continue
        if feat_name == "spot_modality_id":
            continue
        if feat_name in _FEATURE_MAP:
            drillable_cats.append(feat_name)

    # ── Step 1: All states with enough support ───────────────────────
    # We evaluate ALL states (not just lift > 1) because a state with
    # low overall p_top can still contain niche combinations with high lift.
    # The final lift filter (RULES_MIN_LIFT) handles quality control.
    state_stats = _category_stats(
        df, "spot_state_id", target_col, RULES_MIN_SUPPORT
    )
    state_cats = [val for val, _p, _n in state_stats]
    context.log.info(f"  {universe}: {len(state_cats)} states with n >= {RULES_MIN_SUPPORT}")

    # ── Step 2: Generate granular candidates per state ────────────────
    all_candidates: list[dict] = []

    for state_id in state_cats:
        state_name = state_names.get(str(state_id), str(state_id))
        state_df = df.filter(pl.col("spot_state_id") == state_id)

        if state_df.height < RULES_MIN_SUPPORT:
            continue

        p_top_state = float(state_df[target_col].mean())

        # ── Choose municipio vs corredor ──────────────────────────
        best_munis = _best_categories(
            state_df, "_muni_enc", target_col, p_top_state, RULES_MIN_SUPPORT
        )
        best_corrs = _best_categories(
            state_df, "_corr_enc", target_col, p_top_state, RULES_MIN_SUPPORT
        )

        p_muni = (
            _compute_lift_with_feature(state_df, best_munis, "_muni_enc", target_col)
            if best_munis else 0.0
        )
        p_corr = (
            _compute_lift_with_feature(state_df, best_corrs, "_corr_enc", target_col)
            if best_corrs else 0.0
        )

        muni_lift_inc = (
            (p_muni - p_top_state) / p_top_state
            if p_top_state > 0 and p_muni > 0 else 0
        )
        corr_lift_inc = (
            (p_corr - p_top_state) / p_top_state
            if p_top_state > 0 and p_corr > 0 else 0
        )

        # Generate geo items for BOTH municipio and corredor independently.
        # Each geo value produces its own rule branch; the lift/support
        # filters downstream will keep only the good ones.
        geo_items: list[tuple] = []
        if muni_lift_inc >= RULES_MIN_LIFT_INCREMENT and best_munis:
            geo_items.extend(
                [(gv, "_muni_enc", "municipio") for gv in best_munis]
            )
        if corr_lift_inc >= RULES_MIN_LIFT_INCREMENT and best_corrs:
            geo_items.extend(
                [(gv, "_corr_enc", "corredor") for gv in best_corrs]
            )
        if not geo_items:
            geo_items = [(None, None, None)]

        # Determine drillable cat values per feature
        # We compute which individual values beat the state p_top
        cat_items: dict[str, list] = {}
        for feat_name in drillable_cats:
            id_col = _FEATURE_MAP[feat_name][0]
            if id_col not in state_df.columns:
                continue
            good_vals = _best_categories(
                state_df, id_col, target_col, p_top_state, RULES_MIN_SUPPORT
            )
            if good_vals:
                cat_items[feat_name] = good_vals

        # ── Generate candidates: combinatorial expansion ─────────
        # Build lists for tipo and sector (the fusable categoricals)
        tipo_vals = cat_items.get("spot_type_id", [None])
        sector_vals = cat_items.get("spot_sector_id", [None])
        if not tipo_vals:
            tipo_vals = [None]
        if not sector_vals:
            sector_vals = [None]

        for geo_val, geo_col, geo_out in geo_items:
            # Filter to this geo value
            if geo_col is not None:
                geo_df = state_df.filter(pl.col(geo_col) == geo_val)
            else:
                geo_df = state_df

            if geo_df.height < RULES_MIN_SUPPORT:
                continue

            for tipo_val in tipo_vals:
                if tipo_val is not None:
                    tipo_df = geo_df.filter(pl.col("spot_type_id") == tipo_val)
                else:
                    tipo_df = geo_df

                if tipo_df.height < RULES_MIN_SUPPORT:
                    continue

                for sector_val in sector_vals:
                    if sector_val is not None:
                        cand_df = tipo_df.filter(
                            pl.col("spot_sector_id") == sector_val
                        )
                    else:
                        cand_df = tipo_df

                    if cand_df.height < RULES_MIN_SUPPORT:
                        continue

                    cand_p = float(cand_df[target_col].mean())

                    # Only keep if better than state baseline
                    if cand_p <= p_top_state:
                        continue

                    # ── Compute numeric ranges (sliding window) ───
                    price_range = None
                    price_pctl = None
                    price_disc = 0
                    area_range_val = None
                    area_pctl = None
                    area_disc = 0
                    working = cand_df

                    for _, src_col, out_col in _NUM_FEATURES:
                        if src_col not in working.columns:
                            continue

                        is_discriminant = False
                        window = _best_sliding_window(
                            working[src_col],
                            working[target_col],
                            cand_p,
                            RULES_MIN_SUPPORT,
                        )

                        if window is not None:
                            w_lo, w_hi, w_ptop, w_n, w_lo_p, w_hi_p = window
                            lift_inc = (
                                (w_ptop - cand_p) / cand_p
                                if cand_p > 0 else 0
                            )
                            if lift_inc >= RULES_MIN_LIFT_INCREMENT:
                                working = working.filter(
                                    (pl.col(src_col) >= w_lo)
                                    & (pl.col(src_col) <= w_hi)
                                )
                                cand_p = w_ptop
                                label = _format_range(src_col, w_lo, w_hi)
                                pctl_label = _format_pctl_range(w_lo_p, w_hi_p)
                                if out_col == "price_sqm_range":
                                    price_range = label
                                    price_pctl = pctl_label
                                    price_disc = 1
                                else:
                                    area_range_val = label
                                    area_pctl = pctl_label
                                    area_disc = 1
                                is_discriminant = True

                        if not is_discriminant:
                            obs_label = _observed_range_label(
                                working[src_col], src_col
                            )
                            if obs_label:
                                label_star = f"{obs_label} *"
                                pctl_obs = _format_pctl_range(
                                    RULES_RANGE_MIN_PCTL, RULES_RANGE_MAX_PCTL
                                )
                                if out_col == "price_sqm_range":
                                    price_range = label_star
                                    price_pctl = pctl_obs
                                    price_disc = 0
                                else:
                                    area_range_val = label_star
                                    area_pctl = pctl_obs
                                    area_disc = 0

                    # Final metrics for this candidate
                    final_n = working.height
                    if final_n < RULES_MIN_SUPPORT:
                        continue

                    final_p = float(working[target_col].mean())
                    final_lift = final_p / p_top_global if p_top_global > 0 else 0

                    # Resolve names
                    tipo_name = None
                    if tipo_val is not None:
                        tipo_name = cat_name_maps.get(
                            "spot_type_id", {}
                        ).get(str(tipo_val), str(tipo_val))

                    sector_name = None
                    if sector_val is not None:
                        sector_name = cat_name_maps.get(
                            "spot_sector_id", {}
                        ).get(str(sector_val), str(sector_val))

                    geo_name = None
                    if geo_val is not None:
                        geo_name = str(geo_val)

                    candidate = {
                        "spot_state": state_name,
                        "spot_sector": sector_name,
                        "spot_type": tipo_name,
                        "spot_municipality": geo_name if geo_out == "municipio" else None,
                        "spot_corridor": geo_name if geo_out == "corredor" else None,
                        "price_sqm_range": price_range,
                        "price_sqm_pctl_range": price_pctl,
                        "price_sqm_is_discriminant_id": price_disc,
                        "price_sqm_is_discriminant": "Yes" if price_disc == 1 else "No",
                        "area_range": area_range_val,
                        "area_pctl_range": area_pctl,
                        "area_is_discriminant_id": area_disc,
                        "area_is_discriminant": "Yes" if area_disc == 1 else "No",
                        "support_n": final_n,
                        "p_top": final_p,
                        "lift": final_lift,
                        # Internal keys for merge recalculation
                        "_filter_col": geo_col,
                        "_filter_vals": [geo_val] if geo_val is not None else None,
                        "_tipo_val": tipo_val,
                        "_sector_val": sector_val,
                        "_state_df_key": state_id,
                    }
                    all_candidates.append(candidate)

        # Also add a state-level rule (no further drill-down) as fallback
        if state_df.height >= RULES_MIN_SUPPORT:
            state_p = float(state_df[target_col].mean())
            # Compute observed ranges for state level
            st_price = None
            st_price_pctl = None
            st_area = None
            st_area_pctl = None
            obs_pctl = _format_pctl_range(
                RULES_RANGE_MIN_PCTL, RULES_RANGE_MAX_PCTL
            )
            for _, src_col, out_col in _NUM_FEATURES:
                obs = _observed_range_label(state_df[src_col], src_col)
                if obs:
                    if out_col == "price_sqm_range":
                        st_price = f"{obs} *"
                        st_price_pctl = obs_pctl
                    else:
                        st_area = f"{obs} *"
                        st_area_pctl = obs_pctl

            all_candidates.append({
                "spot_state": state_name,
                "spot_sector": None,
                "spot_type": None,
                "spot_municipality": None,
                "spot_corridor": None,
                "price_sqm_range": st_price,
                "price_sqm_pctl_range": st_price_pctl,
                "price_sqm_is_discriminant_id": 0,
                "price_sqm_is_discriminant": "No",
                "area_range": st_area,
                "area_pctl_range": st_area_pctl,
                "area_is_discriminant_id": 0,
                "area_is_discriminant": "No",
                "support_n": state_df.height,
                "p_top": state_p,
                "lift": state_p / p_top_global if p_top_global > 0 else 0,
                "_filter_col": None,
                "_filter_vals": None,
                "_tipo_val": None,
                "_sector_val": None,
                "_state_df_key": state_id,
            })

    context.log.info(
        f"  {universe}: {len(all_candidates)} granular candidates generated"
    )

    if not all_candidates:
        context.log.warning(f"  {universe}: no candidates generated")
        return _empty_rules_df()

    # ── Step 3: Marascuilo fusion ────────────────────────────────────
    # Group candidates by (spot_state, price_sqm_range, area_range) for fusion
    fusion_key = lambda c: (
        c["spot_state"],
        c.get("price_sqm_range") or "",
        c.get("area_range") or "",
    )

    from collections import defaultdict
    fusion_groups: dict[tuple, list[int]] = defaultdict(list)
    for idx, cand in enumerate(all_candidates):
        fusion_groups[fusion_key(cand)].append(idx)

    merged_rules: list[dict] = []

    for group_key, cand_indices in fusion_groups.items():
        group_cands = [all_candidates[i] for i in cand_indices]

        if len(group_cands) == 1:
            # No fusion needed, just clean up internal keys
            rule = dict(group_cands[0])
            for k in ("_filter_col", "_filter_vals", "_tipo_val", "_sector_val", "_state_df_key"):
                rule.pop(k, None)
            merged_rules.append(rule)
            continue

        # Apply Marascuilo test
        cliques = _marascuilo_groups(group_cands, RULES_MARASCUILO_ALPHA)

        # Get the state DataFrame for metric recalculation
        state_id = group_cands[0]["_state_df_key"]
        state_df = df.filter(pl.col("spot_state_id") == state_id)

        for clique in cliques:
            if len(clique) == 1:
                rule = dict(group_cands[clique[0]])
                for k in ("_filter_col", "_filter_vals", "_tipo_val", "_sector_val", "_state_df_key"):
                    rule.pop(k, None)
                merged_rules.append(rule)
            else:
                merged = _merge_candidates(
                    group_cands, clique, state_df, target_col, p_top_global
                )
                for k in ("_state_df_key",):
                    merged.pop(k, None)
                merged_rules.append(merged)

    context.log.info(
        f"  {universe}: {len(merged_rules)} rules after Marascuilo fusion "
        f"(alpha={RULES_MARASCUILO_ALPHA})"
    )

    if not merged_rules:
        context.log.warning(f"  {universe}: no rules after fusion")
        return _empty_rules_df()

    # ── Step 4: Filter by minimum lift ────────────────────────────────
    merged_rules = [r for r in merged_rules if r.get("lift", 0) >= RULES_MIN_LIFT]
    context.log.info(
        f"  {universe}: {len(merged_rules)} rules after min-lift filter "
        f"(>= {RULES_MIN_LIFT}x)"
    )

    if not merged_rules:
        context.log.warning(f"  {universe}: no rules above min-lift threshold")
        return _empty_rules_df()

    # ── Step 5: Compute avg_score_percentile ──────────────────────────
    if gold_df is not None:
        pop_col = POPULATION_COL.get(universe)
        if pop_col and pop_col in gold_df.columns:
            uni_gold = gold_df.filter(pl.col(pop_col) == 1)
        else:
            uni_gold = gold_df

        ts = uni_gold["total_score"]
        ts_rank = ts.rank("ordinal") / ts.len()
        uni_gold = uni_gold.with_columns(ts_rank.alias("_pctl_rank"))
        state_name_to_id = {
            v: k for k, v in cat_name_maps.get("spot_state_id", {}).items()
        }

        for rule in merged_rules:
            mask = pl.lit(True)
            sid = state_name_to_id.get(rule["spot_state"])
            if sid is not None:
                mask = mask & (pl.col("spot_state_id") == int(sid))

            _skip_vals = {"Others", "__OTHER__", "__NULL__"}
            if rule.get("spot_municipality"):
                vals = [v.strip() for v in rule["spot_municipality"].split(",")]
                vals_clean = [v for v in vals if v not in _skip_vals]
                if vals_clean:
                    mask = mask & pl.col("spot_municipality").is_in(vals_clean)
            if rule.get("spot_corridor"):
                vals = [v.strip() for v in rule["spot_corridor"].split(",")]
                vals_clean = [v for v in vals if v not in _skip_vals]
                if vals_clean:
                    mask = mask & pl.col("spot_corridor").is_in(vals_clean)
            if rule.get("spot_type"):
                vals = [v.strip() for v in rule["spot_type"].split(",")]
                mask = mask & pl.col("spot_type").is_in(vals)
            if rule.get("spot_sector"):
                vals = [v.strip() for v in rule["spot_sector"].split(",")]
                mask = mask & pl.col("spot_sector").is_in(vals)

            matched = uni_gold.filter(mask)
            if matched.height > 0:
                avg_pctl = float(matched["_pctl_rank"].mean()) * 100.0
                rule["avg_score_percentile"] = round(avg_pctl, 1)
            else:
                rule["avg_score_percentile"] = None
    else:
        for rule in merged_rules:
            rule["avg_score_percentile"] = None

    # ── Step 6: Compute strength label ────────────────────────────────
    for rule in merged_rules:
        lift_val = rule.get("lift", 0)
        label = "Moderate"
        for threshold, cat_label in STRENGTH_THRESHOLDS:
            if lift_val >= threshold:
                label = cat_label
                break
        rule["strength"] = label

    # ── Step 7: Build rule_text, sort, assign IDs ────────────────────
    for rule in merged_rules:
        text_parts = [f"estado = {rule['spot_state']}"]
        if rule.get("spot_municipality"):
            text_parts.append(f"municipio ∈ {{{rule['spot_municipality']}}}")
        if rule.get("spot_corridor"):
            text_parts.append(f"corredor ∈ {{{rule['spot_corridor']}}}")
        if rule.get("spot_sector"):
            text_parts.append(f"sector ∈ {{{rule['spot_sector']}}}")
        if rule.get("spot_type"):
            text_parts.append(f"tipo ∈ {{{rule['spot_type']}}}")
        if rule.get("price_sqm_range"):
            text_parts.append(f"precio_m² ∈ {rule['price_sqm_range']}")
        if rule.get("area_range"):
            text_parts.append(f"área ∈ {rule['area_range']}")

        rule["rule_text"] = " → ".join(text_parts)
        rule["market_universe"] = universe.capitalize()

    # Remove duplicate rules (same rule_text)
    seen_texts: set[str] = set()
    deduped: list[dict] = []
    for r in merged_rules:
        if r["rule_text"] not in seen_texts:
            seen_texts.add(r["rule_text"])
            deduped.append(r)
    merged_rules = deduped

    # Sort by p_top descending, assign rule_id
    merged_rules.sort(key=lambda r: r["p_top"], reverse=True)
    for i, r in enumerate(merged_rules, 1):
        r["rule_id"] = i

    # Build DataFrame with only the output columns
    rules_df = pl.DataFrame(merged_rules).select(RULE_COLUMNS)

    context.log.info(
        f"  {universe}: {rules_df.height} final rules, "
        f"top p_top={rules_df['p_top'].max():.4f}, "
        f"top lift={rules_df['lift'].max():.2f}x"
    )

    for row in rules_df.head(5).iter_rows(named=True):
        context.log.info(
            f"    #{row['rule_id']}  lift={row['lift']:.2f}x  p={row['p_top']:.4f}  "
            f"pctl={row['avg_score_percentile']}  str={row['strength']}  "
            f"n={row['support_n']}  →  {row['rule_text']}"
        )

    return rules_df


def _empty_rules_df() -> pl.DataFrame:
    """Return an empty DataFrame with the correct schema."""
    _STR_COLS = {
        "market_universe", "spot_state", "spot_sector", "spot_type",
        "spot_municipality", "spot_corridor", "price_sqm_range",
        "price_sqm_pctl_range", "price_sqm_is_discriminant",
        "area_range", "area_pctl_range", "area_is_discriminant",
        "rule_text", "strength",
    }
    _INT_COLS = {
        "rule_id", "support_n",
        "price_sqm_is_discriminant_id", "area_is_discriminant_id",
    }
    return pl.DataFrame(
        schema={
            col: pl.Utf8 if col in _STR_COLS
            else pl.Int64 if col in _INT_COLS
            else pl.Float64
            for col in RULE_COLUMNS
        }
    )


# ── Dagster asset ──────────────────────────────────────────────────────────

@dg.asset(
    group_name="effective_supply_ml",
    description=(
        "Core ML: Hierarchical drill-down rules with Marascuilo fusion "
        "(estado-first, geo-aware, no modalidad). "
        "Enriched with avg_score_percentile and strength."
    ),
)
def core_ml_rules(
    context: dg.AssetExecutionContext,
    core_ml_build_universes: dict,
    core_ml_drivers: dict,
    core_effective_supply: pl.DataFrame,
) -> dict:
    """
    Generate interpretable hierarchical rules for each universe.

    Returns dict with keys 'rent'/'sale', each a pl.DataFrame with:
    market_universe, rule_id, spot_state, spot_sector, spot_type,
    spot_municipality, spot_corridor, price_sqm_range, price_sqm_pctl_range,
    price_sqm_is_discriminant_id, area_range, area_pctl_range,
    area_is_discriminant_id, rule_text, support_n, p_top, lift,
    avg_score_percentile, strength
    """
    context.log.info("Generating hierarchical rules with Marascuilo fusion...")

    result = {}
    for universe in UNIVERSES:
        context.log.info(f"Processing {universe}...")
        result[universe] = _compute_universe_rules(
            core_ml_build_universes[universe],
            universe,
            core_ml_drivers[universe],
            context,
            gold_df=core_effective_supply,
        )

    return result
