"""Normalized spot prices (clean_prices, spots_prices, etc.)."""
from __future__ import annotations

import math
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

import polars as pl

from dagster_pipeline.defs.data_lakehouse.silver.utils import case_when
from dagster_pipeline.defs.data_lakehouse.silver.modules.lk_spots.dictionaries import PRICE_AREA_MAP, CURRENCY_MAP

# stg_lk_spots clean_prices: SQL is ORDER BY updated_at DESC only; legacy materializations often
# tie-break with highest id (newest row). Use id DESC so rn=1 matches that convention.
CLEAN_PRICES_TIE_USE_CREATED_AT = False

# Totals/maintenance: quantize trims Float64 tails (HALF_UP). Use 6 dp so USD/MXN totals stay closer to
# DECIMAL/TC math (e.g. exchanges ~6 fractional digits) before float storage. Do NOT round *_sqm_* here —
# per-m² values need finer precision; rounding them to 2 dp increased strict-compare diffs vs SQLite REAL.
SPOT_PRICE_TOTAL_DECIMALS = 6

_SPOT_PRICE_ROUND_TOTAL_MAINT_COLS = (
    "spot_price_total_mxn_rent",
    "spot_price_total_usd_rent",
    "spot_price_total_mxn_sale",
    "spot_price_total_usd_sale",
    "spot_maintenance_cost_mxn",
    "spot_maintenance_cost_usd",
)
_SPOT_SUB_PRICE_ROUND_TOTAL_COLS = (
    "spot_sub_min_price_total_mxn_rent",
    "spot_sub_max_price_total_mxn_rent",
    "spot_sub_mean_price_total_mxn_rent",
    "spot_sub_min_price_total_usd_rent",
    "spot_sub_max_price_total_usd_rent",
    "spot_sub_mean_price_total_usd_rent",
    "spot_sub_min_price_total_mxn_sale",
    "spot_sub_max_price_total_mxn_sale",
    "spot_sub_mean_price_total_mxn_sale",
    "spot_sub_min_price_total_usd_sale",
    "spot_sub_max_price_total_usd_sale",
    "spot_sub_mean_price_total_usd_sale",
)


def quantize_half_up(value: object, ndigits: int = SPOT_PRICE_TOTAL_DECIMALS) -> float | None:
    """
    MySQL ROUND(x, n) semantics: half away from zero (not Python banker's rounding).
    Use str(float) bridge so Polars Float64 matches legacy compare for totals / USD from MXN/TC.
    """
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        step = Decimal(10) ** -ndigits
        return float(Decimal(str(value)).quantize(step, rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError, TypeError, OverflowError):
        return None


def _round_float_columns(df: pl.DataFrame, names: tuple[str, ...], decimals: int) -> pl.DataFrame:
    exist = [n for n in names if n in df.columns]
    if not exist:
        return df
    return df.with_columns([
        pl.col(n).map_elements(
            lambda x, nd=decimals: quantize_half_up(x, nd),
            return_dtype=pl.Float64,
        ).alias(n)
        for n in exist
    ])


def _prices_row_id_column(df: pl.DataFrame) -> str | None:
    for name in ("id", "Id", "price_id", "PRICE_ID"):
        if name in df.columns:
            return name
    return None


def _currency_as_int(currency_type: object) -> int | None:
    if currency_type is None:
        return None
    try:
        return int(currency_type)
    except (TypeError, ValueError):
        try:
            return int(float(currency_type))
        except (TypeError, ValueError):
            return None


def _to_decimal_amount(amount: object) -> Decimal | None:
    if amount is None:
        return None
    if isinstance(amount, float) and (math.isnan(amount) or math.isinf(amount)):
        return None
    try:
        return Decimal(str(amount))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _fx_mxn_usd_for_amount(amount: object, currency_type: object, er: Decimal) -> tuple[Decimal | None, Decimal | None]:
    """
    Same branches as stg_lk_spots clean_prices (MySQL): MXN-listed → (rate, rate/er); USD-listed → (rate*er, rate).
    """
    d = _to_decimal_amount(amount)
    if d is None:
        return None, None
    if _currency_as_int(currency_type) == 2:
        return d * er, d
    return d, d / er


def _decimal_to_float_clean(d: Decimal | None) -> float | None:
    if d is None:
        return None
    return float(d)


def _struct_row_as_dict(row: object) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    try:
        return dict(row)  # type: ignore[arg-type]
    except Exception:
        pass
    keys = ("rate", "currency_type", "maintenance", "max_rate")
    if hasattr(row, "__getitem__"):
        return {k: row[k] for k in keys}  # type: ignore[index]
    raise TypeError(f"unexpected clean_prices struct row type: {type(row)!r}")


def _clean_prices_fx_row(row: object, er: Decimal) -> dict[str, float | None]:
    d = _struct_row_as_dict(row)
    cur = d.get("currency_type")
    mxn_r, usd_r = _fx_mxn_usd_for_amount(d.get("rate"), cur, er)
    mxn_m, usd_m = _fx_mxn_usd_for_amount(d.get("maintenance"), cur, er)
    mxn_x, usd_x = _fx_mxn_usd_for_amount(d.get("max_rate"), cur, er)
    return {
        "mxn_rate": _decimal_to_float_clean(mxn_r),
        "usd_rate": _decimal_to_float_clean(usd_r),
        "mxn_maintenance": _decimal_to_float_clean(mxn_m),
        "usd_maintenance": _decimal_to_float_clean(usd_m),
        "max_mxn_rate": _decimal_to_float_clean(mxn_x),
        "max_usd_rate": _decimal_to_float_clean(usd_x),
    }


_CLEAN_PRICES_STRUCT_DTYPE = pl.Struct(
    {
        "mxn_rate": pl.Float64,
        "usd_rate": pl.Float64,
        "mxn_maintenance": pl.Float64,
        "usd_maintenance": pl.Float64,
        "max_mxn_rate": pl.Float64,
        "max_usd_rate": pl.Float64,
    }
)


def build_clean_prices(
    prices: pl.DataFrame,
    exchange_rate: Decimal | float | int | str,
) -> pl.DataFrame:
    """
    One row per (spot_id, type): non-deleted, FX columns, rn=1 like clean_prices CTE.

    Uses Decimal for divisions/multiplications with `exchange_rate` so the applied TC matches
    `exchanges.exchange_rate` (as string/Decimal) rather than Float64 pollution.
    """
    if isinstance(exchange_rate, Decimal):
        er = exchange_rate
    else:
        er = Decimal(str(exchange_rate))
    if er <= 0:
        er = Decimal("1")

    p = prices.filter(pl.col("deleted_at").is_null())
    _pk = _prices_row_id_column(p)
    if _pk is not None and _pk != "id":
        p = p.rename({_pk: "id"})

    need = ("rate", "currency_type", "maintenance", "max_rate")
    for col in need:
        if col not in p.columns:
            p = p.with_columns(pl.lit(None).alias(col))

    clean_prices = p.with_columns(
        pl.struct(list(need))
        .map_elements(
            lambda r, _er=er: _clean_prices_fx_row(r, _er),
            return_dtype=_CLEAN_PRICES_STRUCT_DTYPE,
        )
        .alias("_clean_fx")
    ).unnest("_clean_fx")
    sort_cols = ["spot_id", "type", "updated_at"]
    sort_desc = [False, False, True]
    if CLEAN_PRICES_TIE_USE_CREATED_AT and "created_at" in clean_prices.columns:
        sort_cols.append("created_at")
        sort_desc.append(True)
    if "id" in clean_prices.columns:
        sort_cols.append("id")
        sort_desc.append(True)
    clean_prices = (
        clean_prices.sort(sort_cols, descending=sort_desc, nulls_last=True)
        .with_columns(
            (pl.col("spot_id").cast(pl.Utf8) + "_" + pl.col("type").cast(pl.Utf8)).alias("_part"),
        )
        .with_columns(pl.int_range(1, pl.len() + 1).over("_part").alias("rn"))
        .filter(pl.col("rn") == 1)
        .drop(["_part", "rn"])
    )
    return clean_prices


def build_spots_prices(
    clean_prices: pl.DataFrame,
    total_spots: pl.DataFrame,
) -> pl.DataFrame:
    """
    basic_spots_prices (join total_spots + rent/sale from clean_prices) + complex_price_stats + spots_prices.
    """
    cp_r = clean_prices.filter(pl.col("type") == 1).select([
        pl.col("spot_id"), pl.col("mxn_rate"), pl.col("usd_rate"), pl.col("currency_type"),
        pl.col("price_area"), pl.col("mxn_maintenance"), pl.col("usd_maintenance"),
    ])
    cp_s = clean_prices.filter(pl.col("type") == 2).select([
        pl.col("spot_id").alias("spot_id_s"), pl.col("mxn_rate").alias("s_mxn_rate"),
        pl.col("usd_rate").alias("s_usd_rate"), pl.col("currency_type").alias("s_currency_type"),
        pl.col("price_area").alias("s_price_area"),
    ])
    ts_joined = (
        total_spots.join(cp_r, left_on="id", right_on="spot_id", how="left", suffix="_r")
        .join(cp_s, left_on="id", right_on="spot_id_s", how="left", suffix="_s")
    )
    sq = pl.col("square_space") if "square_space" in ts_joined.columns else pl.lit(1.0)
    basic_spots_prices = ts_joined.with_columns([
        pl.col("id").alias("spot_id"),
        pl.col("parent_id").alias("spot_parent_id"),
        pl.col("source_table_id").alias("spot_type_id"),
        sq.alias("square_space"),
        pl.when(pl.col("mxn_rate").is_not_null() & pl.col("s_mxn_rate").is_null()).then(pl.lit(1))
        .when(pl.col("mxn_rate").is_null() & pl.col("s_mxn_rate").is_not_null()).then(pl.lit(2))
        .when(pl.col("mxn_rate").is_not_null() & pl.col("s_mxn_rate").is_not_null()).then(pl.lit(3))
        .otherwise(pl.lit(None)).alias("spot_modality_id"),
        pl.when(pl.col("mxn_rate").is_not_null() & pl.col("s_mxn_rate").is_null()).then(pl.lit("Rent"))
        .when(pl.col("mxn_rate").is_null() & pl.col("s_mxn_rate").is_not_null()).then(pl.lit("Sale"))
        .when(pl.col("mxn_rate").is_not_null() & pl.col("s_mxn_rate").is_not_null()).then(pl.lit("Rent & Sale"))
        .otherwise(pl.lit(None)).alias("spot_modality"),
        pl.col("price_area").alias("spot_price_area_rent_id"),
        case_when("price_area", PRICE_AREA_MAP, default=None).alias("spot_price_area_rent"),
        pl.when((pl.col("currency_type") == 1) | pl.col("currency_type").is_null()).then(pl.lit(1)).when(pl.col("currency_type") == 2).then(pl.lit(2)).alias("spot_currency_rent_id"),
        case_when(pl.col("currency_type").fill_null(1), CURRENCY_MAP, default=None).alias("spot_currency_rent"),
        pl.col("s_price_area").alias("spot_price_area_sale_id"),
        case_when("s_price_area", PRICE_AREA_MAP, default=None).alias("spot_price_area_sale"),
        pl.when((pl.col("s_currency_type") == 1) | pl.col("s_currency_type").is_null()).then(pl.lit(1)).when(pl.col("s_currency_type") == 2).then(pl.lit(2)).alias("spot_currency_sale_id"),
        case_when(pl.col("s_currency_type").fill_null(1), CURRENCY_MAP, default=None).alias("spot_currency_sale"),
        pl.when(pl.col("price_area") == 1).then(pl.col("mxn_rate")).when(pl.col("price_area") == 2).then(pl.col("mxn_rate") * sq).otherwise(None).alias("spot_price_total_mxn_rent"),
        pl.when(pl.col("price_area") == 1).then(pl.col("usd_rate")).when(pl.col("price_area") == 2).then(pl.col("usd_rate") * sq).otherwise(None).alias("spot_price_total_usd_rent"),
        pl.when((pl.col("price_area") == 1) & (sq > 0)).then(pl.col("mxn_rate") / sq).when(pl.col("price_area") == 2).then(pl.col("mxn_rate")).otherwise(None).alias("spot_price_sqm_mxn_rent"),
        pl.when((pl.col("price_area") == 1) & (sq > 0)).then(pl.col("usd_rate") / sq).when(pl.col("price_area") == 2).then(pl.col("usd_rate")).otherwise(None).alias("spot_price_sqm_usd_rent"),
        pl.when(pl.col("s_price_area") == 1).then(pl.col("s_mxn_rate")).when(pl.col("s_price_area") == 2).then(pl.col("s_mxn_rate") * sq).otherwise(None).alias("spot_price_total_mxn_sale"),
        pl.when(pl.col("s_price_area") == 1).then(pl.col("s_usd_rate")).when(pl.col("s_price_area") == 2).then(pl.col("s_usd_rate") * sq).otherwise(None).alias("spot_price_total_usd_sale"),
        pl.when((pl.col("s_price_area") == 1) & (sq > 0)).then(pl.col("s_mxn_rate") / sq).when(pl.col("s_price_area") == 2).then(pl.col("s_mxn_rate")).otherwise(None).alias("spot_price_sqm_mxn_sale"),
        pl.when((pl.col("s_price_area") == 1) & (sq > 0)).then(pl.col("s_usd_rate") / sq).when(pl.col("s_price_area") == 2).then(pl.col("s_usd_rate")).otherwise(None).alias("spot_price_sqm_usd_sale"),
        pl.col("mxn_maintenance").alias("spot_maintenance_cost_mxn"),
        pl.col("usd_maintenance").alias("spot_maintenance_cost_usd"),
    ])
    basic_spots_prices = _round_float_columns(
        basic_spots_prices, _SPOT_PRICE_ROUND_TOTAL_MAINT_COLS, SPOT_PRICE_TOTAL_DECIMALS
    )

    bsp_sub = basic_spots_prices.filter(pl.col("spot_type_id") == 3)
    if bsp_sub.height > 0 and "square_space" in bsp_sub.columns:
        complex_price_stats = bsp_sub.group_by("spot_parent_id").agg([
            pl.col("square_space").sum().alias("subspace_total_area"),
            pl.col("spot_id").count().alias("subspace_count"),
            pl.col("spot_price_total_mxn_rent").min().alias("min_price_total_mxn_rent"),
            pl.col("spot_price_total_mxn_rent").max().alias("max_price_total_mxn_rent"),
            pl.col("spot_price_total_mxn_rent").mean().alias("mean_price_total_mxn_rent"),
            pl.col("spot_price_sqm_mxn_rent").min().alias("min_price_sqm_mxn_rent"),
            pl.col("spot_price_sqm_mxn_rent").max().alias("max_price_sqm_mxn_rent"),
            (pl.col("spot_price_total_mxn_rent").sum() / pl.col("square_space").sum()).alias("mean_price_sqm_mxn_rent"),
            pl.col("spot_price_total_usd_rent").min().alias("min_price_total_usd_rent"),
            pl.col("spot_price_total_usd_rent").max().alias("max_price_total_usd_rent"),
            pl.col("spot_price_total_usd_rent").mean().alias("mean_price_total_usd_rent"),
            pl.col("spot_price_sqm_usd_rent").min().alias("min_price_sqm_usd_rent"),
            pl.col("spot_price_sqm_usd_rent").max().alias("max_price_sqm_usd_rent"),
            (pl.col("spot_price_total_usd_rent").sum() / pl.col("square_space").sum()).alias("mean_price_sqm_usd_rent"),
            pl.col("spot_price_total_mxn_sale").min().alias("min_price_total_mxn_sale"),
            pl.col("spot_price_total_mxn_sale").max().alias("max_price_total_mxn_sale"),
            pl.col("spot_price_total_mxn_sale").mean().alias("mean_price_total_mxn_sale"),
            pl.col("spot_price_sqm_mxn_sale").min().alias("min_price_sqm_mxn_sale"),
            pl.col("spot_price_sqm_mxn_sale").max().alias("max_price_sqm_mxn_sale"),
            (pl.col("spot_price_total_mxn_sale").sum() / pl.col("square_space").sum()).alias("mean_price_sqm_mxn_sale"),
            pl.col("spot_price_total_usd_sale").min().alias("min_price_total_usd_sale"),
            pl.col("spot_price_total_usd_sale").max().alias("max_price_total_usd_sale"),
            pl.col("spot_price_total_usd_sale").mean().alias("mean_price_total_usd_sale"),
            pl.col("spot_price_sqm_usd_sale").min().alias("min_price_sqm_usd_sale"),
            pl.col("spot_price_sqm_usd_sale").max().alias("max_price_sqm_usd_sale"),
            (pl.col("spot_price_total_usd_sale").sum() / pl.col("square_space").sum()).alias("mean_price_sqm_usd_sale"),
        ])
    else:
        complex_price_stats = pl.DataFrame(schema={"spot_parent_id": pl.Int64})

    if complex_price_stats.width > 1 and "spot_parent_id" in complex_price_stats.columns:
        cps_renamed = complex_price_stats.select([
            pl.col("spot_parent_id").alias("cps_spot_parent_id"),
            pl.col("min_price_total_mxn_rent").alias("spot_sub_min_price_total_mxn_rent"),
            pl.col("max_price_total_mxn_rent").alias("spot_sub_max_price_total_mxn_rent"),
            pl.col("mean_price_total_mxn_rent").alias("spot_sub_mean_price_total_mxn_rent"),
            pl.col("min_price_sqm_mxn_rent").alias("spot_sub_min_price_sqm_mxn_rent"),
            pl.col("max_price_sqm_mxn_rent").alias("spot_sub_max_price_sqm_mxn_rent"),
            pl.col("mean_price_sqm_mxn_rent").alias("spot_sub_mean_price_sqm_mxn_rent"),
            pl.col("min_price_total_usd_rent").alias("spot_sub_min_price_total_usd_rent"),
            pl.col("max_price_total_usd_rent").alias("spot_sub_max_price_total_usd_rent"),
            pl.col("mean_price_total_usd_rent").alias("spot_sub_mean_price_total_usd_rent"),
            pl.col("min_price_sqm_usd_rent").alias("spot_sub_min_price_sqm_usd_rent"),
            pl.col("max_price_sqm_usd_rent").alias("spot_sub_max_price_sqm_usd_rent"),
            pl.col("mean_price_sqm_usd_rent").alias("spot_sub_mean_price_sqm_usd_rent"),
            pl.col("min_price_total_mxn_sale").alias("spot_sub_min_price_total_mxn_sale"),
            pl.col("max_price_total_mxn_sale").alias("spot_sub_max_price_total_mxn_sale"),
            pl.col("mean_price_total_mxn_sale").alias("spot_sub_mean_price_total_mxn_sale"),
            pl.col("min_price_sqm_mxn_sale").alias("spot_sub_min_price_sqm_mxn_sale"),
            pl.col("max_price_sqm_mxn_sale").alias("spot_sub_max_price_sqm_mxn_sale"),
            pl.col("mean_price_sqm_mxn_sale").alias("spot_sub_mean_price_sqm_mxn_sale"),
            pl.col("min_price_total_usd_sale").alias("spot_sub_min_price_total_usd_sale"),
            pl.col("max_price_total_usd_sale").alias("spot_sub_max_price_total_usd_sale"),
            pl.col("mean_price_total_usd_sale").alias("spot_sub_mean_price_total_usd_sale"),
            pl.col("min_price_sqm_usd_sale").alias("spot_sub_min_price_sqm_usd_sale"),
            pl.col("max_price_sqm_usd_sale").alias("spot_sub_max_price_sqm_usd_sale"),
            pl.col("mean_price_sqm_usd_sale").alias("spot_sub_mean_price_sqm_usd_sale"),
        ])
        cps_renamed = _round_float_columns(
            cps_renamed, _SPOT_SUB_PRICE_ROUND_TOTAL_COLS, SPOT_PRICE_TOTAL_DECIMALS
        )
        spots_prices = basic_spots_prices.join(
            cps_renamed, left_on="spot_id", right_on="cps_spot_parent_id", how="left"
        )
        if "cps_spot_parent_id" in spots_prices.columns:
            spots_prices = spots_prices.drop("cps_spot_parent_id")
    else:
        sub_cols = [
            "spot_sub_min_price_total_mxn_rent", "spot_sub_max_price_total_mxn_rent", "spot_sub_mean_price_total_mxn_rent",
            "spot_sub_min_price_sqm_mxn_rent", "spot_sub_max_price_sqm_mxn_rent", "spot_sub_mean_price_sqm_mxn_rent",
            "spot_sub_min_price_total_usd_rent", "spot_sub_max_price_total_usd_rent", "spot_sub_mean_price_total_usd_rent",
            "spot_sub_min_price_sqm_usd_rent", "spot_sub_max_price_sqm_usd_rent", "spot_sub_mean_price_sqm_usd_rent",
            "spot_sub_min_price_total_mxn_sale", "spot_sub_max_price_total_mxn_sale", "spot_sub_mean_price_total_mxn_sale",
            "spot_sub_min_price_sqm_mxn_sale", "spot_sub_max_price_sqm_mxn_sale", "spot_sub_mean_price_sqm_mxn_sale",
            "spot_sub_min_price_total_usd_sale", "spot_sub_max_price_total_usd_sale", "spot_sub_mean_price_total_usd_sale",
            "spot_sub_min_price_sqm_usd_sale", "spot_sub_max_price_sqm_usd_sale", "spot_sub_mean_price_sqm_usd_sale",
        ]
        spots_prices = basic_spots_prices.with_columns([pl.lit(None).alias(c) for c in sub_cols])

    return spots_prices
