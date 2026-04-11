# defs/spot_state_transitions/processing.py
"""
Shared processing functions and data contracts for the Spot State Transitions pipeline.

Contains:
- TRANSITIONS_SCHEMA: canonical type contract between STG and Core layers
- FINAL_COLUMNS / FINAL_RENAME: output contract from Core to Gold
- Core transformation functions (dedup, prev/next, sstd IDs)

Ported from update_spot_status_history_v3.py (Pandas) to Polars.
All functions are pure transformations — no database queries.
"""
import polars as pl


# =============================================================================
# DATA CONTRACTS (shared between layers)
# =============================================================================

TRANSITIONS_SCHEMA = {
    "source_sst_id": pl.Int64,
    "spot_id": pl.Int64,
    "state": pl.Int64,
    "reason": pl.Int64,
    "source_id": pl.Int64,
    "source": pl.Utf8,
    "source_sst_created_at": pl.Datetime,
    "source_sst_updated_at": pl.Datetime,
}

FINAL_COLUMNS = [
    "source_sst_id", "spot_id", "state", "reason", "state_full",
    "prev_state", "prev_reason", "prev_state_full",
    "next_state", "next_reason", "next_state_full",
    "sstd_status_final_id", "sstd_status_id",
    "sstd_status_full_final_id", "sstd_status_full_id",
    "prev_source_sst_created_at", "next_source_sst_created_at",
    "minutes_since_prev_state", "minutes_until_next_state",
    "source_sst_created_at", "source_sst_updated_at",
    "source_id", "source",
]

FINAL_RENAME = {
    "state": "spot_status_id",
    "reason": "spot_status_reason_id",
    "prev_state": "prev_spot_status_id",
    "prev_reason": "prev_spot_status_reason_id",
    "state_full": "spot_status_full_id",
    "prev_state_full": "prev_spot_status_full_id",
    "next_state": "next_spot_status_id",
    "next_reason": "next_spot_status_reason_id",
    "next_state_full": "next_spot_status_full_id",
}


# =============================================================================
# CORE PROCESSING FUNCTIONS
# =============================================================================

def dedup_consecutive_runs(
    df: pl.DataFrame,
    *,
    spot_col: str = "spot_id",
    time_col: str = "source_sst_created_at",
    state_col: str = "state",
    reason_col: str = "reason",
) -> pl.DataFrame:
    """
    Removes consecutive duplicate (state, reason) pairs per spot_id,
    keeping the first (oldest) record of each run.

    Uses global shift on a sorted DataFrame (not over()) for deterministic ordering.
    Group boundaries are detected by comparing spot_id with the previous row.
    """
    if df.height == 0:
        return df

    df = df.with_row_index("_row_id")
    df = df.sort([spot_col, time_col, "_row_id"])

    state_filled = pl.col(state_col).cast(pl.Utf8).fill_null("__NA__")
    reason_filled = pl.col(reason_col).cast(pl.Utf8).fill_null("__NA__")

    df = df.with_columns(
        (state_filled + "|" + reason_filled).alias("_pair")
    )

    df = df.with_columns([
        pl.col("_pair").shift(1).alias("_prev_pair"),
        pl.col(spot_col).shift(1).alias("_prev_spot"),
    ])

    # A row is a "change" if: new spot group, or pair differs from previous
    df = df.with_columns(
        (
            pl.col("_prev_spot").is_null()
            | (pl.col(spot_col) != pl.col("_prev_spot"))
            | (pl.col("_pair") != pl.col("_prev_pair"))
        )
        .alias("_is_change")
    )

    cleaned = (
        df.filter(pl.col("_is_change"))
        .sort([spot_col, time_col, "_row_id"])
        .drop(["_row_id", "_pair", "_prev_pair", "_prev_spot", "_is_change"])
    )

    return cleaned


def add_prev_fields(
    df: pl.DataFrame,
    *,
    spot_col: str = "spot_id",
    time_col: str = "source_sst_created_at",
    state_col: str = "state",
    reason_col: str = "reason",
) -> pl.DataFrame:
    """Adds previous state fields per spot_id group.

    Uses global shift on a sorted DataFrame with spot boundary masking.
    """
    if df.height == 0:
        return df

    df = df.with_row_index("_orig_pos")
    df = df.sort([spot_col, time_col, "_orig_pos"])

    # Global shift — then null-out at group boundaries
    df = df.with_columns([
        pl.col(state_col).shift(1).alias("prev_state"),
        pl.col(reason_col).shift(1).alias("prev_reason"),
        pl.col(time_col).shift(1).alias("prev_source_sst_created_at"),
        pl.col(spot_col).shift(1).alias("_prev_spot"),
    ])

    is_boundary = (
        pl.col("_prev_spot").is_null()
        | (pl.col(spot_col) != pl.col("_prev_spot"))
    )
    df = df.with_columns([
        pl.when(is_boundary).then(None).otherwise(pl.col("prev_state")).alias("prev_state"),
        pl.when(is_boundary).then(None).otherwise(pl.col("prev_reason")).alias("prev_reason"),
        pl.when(is_boundary).then(None).otherwise(pl.col("prev_source_sst_created_at")).alias("prev_source_sst_created_at"),
    ]).drop("_prev_spot")

    df = df.with_columns(
        ((pl.col(time_col) - pl.col("prev_source_sst_created_at"))
         .dt.total_seconds() / 60.0)
        .alias("minutes_since_prev_state")
    )

    state_num = pl.col(state_col).cast(pl.Int64, strict=False)
    reason_num = pl.col(reason_col).cast(pl.Int64, strict=False).fill_null(0)
    prev_state_num = pl.col("prev_state").cast(pl.Int64, strict=False)
    prev_reason_num = pl.col("prev_reason").cast(pl.Int64, strict=False).fill_null(0)

    df = df.with_columns([
        pl.when(state_num.is_not_null())
        .then(state_num * 100 + reason_num)
        .otherwise(None)
        .cast(pl.Int64)
        .alias("state_full"),

        pl.when(prev_state_num.is_not_null())
        .then(prev_state_num * 100 + prev_reason_num)
        .otherwise(None)
        .cast(pl.Int64)
        .alias("prev_state_full"),
    ])

    df = df.sort("_orig_pos").drop("_orig_pos")
    return df


def add_next_fields(
    df: pl.DataFrame,
    *,
    spot_col: str = "spot_id",
    time_col: str = "source_sst_created_at",
    state_col: str = "state",
    reason_col: str = "reason",
) -> pl.DataFrame:
    """Adds next state fields per spot_id group.

    Uses global shift(-1) on a sorted DataFrame with spot boundary masking.
    """
    if df.height == 0:
        return df

    df = df.with_row_index("_orig_pos")
    df = df.sort([spot_col, time_col, "_orig_pos"])

    # Global shift(-1) — then null-out at group boundaries
    df = df.with_columns([
        pl.col(state_col).shift(-1).alias("next_state"),
        pl.col(reason_col).shift(-1).alias("next_reason"),
        pl.col(time_col).shift(-1).alias("next_source_sst_created_at"),
        pl.col(spot_col).shift(-1).alias("_next_spot"),
    ])

    is_boundary = (
        pl.col("_next_spot").is_null()
        | (pl.col(spot_col) != pl.col("_next_spot"))
    )
    df = df.with_columns([
        pl.when(is_boundary).then(None).otherwise(pl.col("next_state")).alias("next_state"),
        pl.when(is_boundary).then(None).otherwise(pl.col("next_reason")).alias("next_reason"),
        pl.when(is_boundary).then(None).otherwise(pl.col("next_source_sst_created_at")).alias("next_source_sst_created_at"),
    ]).drop("_next_spot")

    df = df.with_columns(
        ((pl.col("next_source_sst_created_at") - pl.col(time_col))
         .dt.total_seconds() / 60.0)
        .alias("minutes_until_next_state")
    )

    state_num = pl.col(state_col).cast(pl.Int64, strict=False)
    reason_num = pl.col(reason_col).cast(pl.Int64, strict=False).fill_null(0)
    next_state_num = pl.col("next_state").cast(pl.Int64, strict=False)
    next_reason_num = pl.col("next_reason").cast(pl.Int64, strict=False).fill_null(0)

    # state_full may already exist from add_prev_fields; only add if missing
    exprs = []
    if "state_full" not in df.columns:
        exprs.append(
            pl.when(state_num.is_not_null())
            .then(state_num * 100 + reason_num)
            .otherwise(None)
            .cast(pl.Int64)
            .alias("state_full")
        )

    exprs.append(
        pl.when(next_state_num.is_not_null())
        .then(next_state_num * 100 + next_reason_num)
        .otherwise(None)
        .cast(pl.Int64)
        .alias("next_state_full")
    )

    df = df.with_columns(exprs)
    df = df.sort("_orig_pos").drop("_orig_pos")
    return df


def add_sstd_ids(
    df: pl.DataFrame,
    *,
    status_col: str = "state",
    reason_col: str = "reason",
    prev_status_col: str = "prev_state",
    prev_reason_col: str = "prev_reason",
) -> pl.DataFrame:
    """Computes the 4 composite transition IDs (sstd_*)."""
    if df.height == 0:
        return df

    s = pl.col(status_col).cast(pl.Int64, strict=False).fill_null(0)
    r = pl.col(reason_col).cast(pl.Int64, strict=False).fill_null(0)
    ps = pl.col(prev_status_col).cast(pl.Int64, strict=False).fill_null(0)
    pr = pl.col(prev_reason_col).cast(pl.Int64, strict=False).fill_null(0)

    valid = pl.col(status_col).cast(pl.Int64, strict=False).fill_null(0) > 0

    # A. sstd_status_final_id = status * 100000
    sstd_final = s * 100000

    # B. sstd_status_id = status * 100000 + prev_status * 100  (exclude Public→Public)
    sstd_status = s * 100000 + ps * 100
    exclude_simple = (s == 1) & (ps == 1)

    # C. sstd_status_full_final_id = status * 100000 + reason * 1000
    sstd_full_final = s * 100000 + r * 1000

    # D. sstd_status_full_id (exclude identical self-transitions)
    full_current = s * 100000 + r * 1000
    full_prev = ps * 100000 + pr * 1000
    sstd_full = s * 100000 + r * 1000 + ps * 100 + pr
    exclude_full = full_current == full_prev

    df = df.with_columns([
        pl.when(valid).then(sstd_final).otherwise(None).cast(pl.Int64)
        .alias("sstd_status_final_id"),

        pl.when(valid & ~exclude_simple).then(sstd_status).otherwise(None).cast(pl.Int64)
        .alias("sstd_status_id"),

        pl.when(valid).then(sstd_full_final).otherwise(None).cast(pl.Int64)
        .alias("sstd_status_full_final_id"),

        pl.when(valid & ~exclude_full).then(sstd_full).otherwise(None).cast(pl.Int64)
        .alias("sstd_status_full_id"),
    ])

    return df
