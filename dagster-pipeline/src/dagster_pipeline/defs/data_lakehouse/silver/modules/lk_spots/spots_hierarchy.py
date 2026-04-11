"""
Módulo reutilizable: jerarquía de spots (singles, complexes, subspaces) y total_spots.
Defines the backbone of the data (parent/child, _real fields, state_id_real2, status labels).
"""
import polars as pl

from dagster_pipeline.defs.data_lakehouse.silver.utils import (
    case_when,
    case_when_conditions,
    expr_mysql_numeric_int64,
)
from dagster_pipeline.defs.data_lakehouse.silver.modules.lk_spots.dictionaries import (
    STATUS_LABEL_MAP,
    STATUS_REASON_MAP,
)


def _status_reason_label(reason_col: str) -> pl.Expr:
    """CASE WHEN for state_reason -> label."""
    return case_when(reason_col, STATUS_REASON_MAP, default=None)


def build_total_spots(
    spots: pl.DataFrame,
    prices: pl.DataFrame,
    zip_codes: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds total_spots: union of singles, complexes and subspaces with _real fields,
    state_id_real2 and status columns (spot_parent_status, spot_status, etc.).
    Includes soft-deleted spots (spots.deleted_at may be set); downstream columns
    spot_deleted_at / spot_deleted_date carry the flag. Prices still use non-deleted
    price rows only (clean_prices / EXISTS logic).
    """
    if spots.height == 0:
        return spots

    valid_price_spot_ids = (
        prices.filter(pl.col("deleted_at").is_null())
        .filter(pl.col("type").is_in([1, 2]))
        .select(pl.col("spot_id").unique())
    )
    spot_type_ok = pl.col("spot_type_id").is_in([13, 11, 9, 15, 17, 18])

    parent_nulls = [pl.lit(None).alias(c) for c in [
        "parent_spot_type_id", "parent_street", "parent_ext_number", "parent_int_number",
        "parent_zip_code_id", "parent_latitude", "parent_longitude", "parent_quality_state",
        "parent_state_id", "parent_data_municipality_id", "parent_city_id", "parent_settlement_id",
        "parent_contact_id", "parent_spot_state", "parent_state_reason", "parent_zone_id",
        "parent_nearest_zone_id", "parent_construction_date", "parent_parking_space_by_area",
    ]]

    singles = spots.filter(
        pl.col("parent_id").is_null(),
        pl.col("is_complex") == 0,
        pl.col("id").is_in(valid_price_spot_ids.to_series()),
        spot_type_ok,
    )
    spots_singles = singles.with_columns([
        pl.col("parent_id").alias("parent_id_real"),
        *parent_nulls,
    ])

    complexes = spots.filter(
        pl.col("parent_id").is_null(),
        pl.col("is_complex") == 1,
        pl.col("id").is_in(valid_price_spot_ids.to_series()),
        spot_type_ok,
    )
    spots_complexes = complexes.with_columns([
        pl.col("parent_id").alias("parent_id_real"),
        *parent_nulls,
    ])

    parent_complex = spots.filter(
        pl.col("parent_id").is_null(),
        pl.col("is_complex") == 1,
        pl.col("id").is_in(valid_price_spot_ids.to_series()),
    ).select([
        pl.col("id").alias("parent_id_real"),
        pl.col("spot_type_id").alias("parent_spot_type_id"),
        pl.col("contact_id").alias("parent_contact_id"),
        pl.col("street").alias("parent_street"),
        pl.col("ext_number").alias("parent_ext_number"),
        pl.col("int_number").alias("parent_int_number"),
        pl.col("zip_code_id").alias("parent_zip_code_id"),
        pl.col("latitude").alias("parent_latitude"),
        pl.col("longitude").alias("parent_longitude"),
        pl.col("quality_state").alias("parent_quality_state"),
        pl.col("state_id").alias("parent_state_id"),
        pl.col("data_municipality_id").alias("parent_data_municipality_id"),
        pl.col("city_id").alias("parent_city_id"),
        pl.col("settlement_id").alias("parent_settlement_id"),
        pl.col("spot_state").alias("parent_spot_state"),
        pl.col("state_reason").alias("parent_state_reason"),
        pl.col("zone_id").alias("parent_zone_id"),
        pl.col("nearest_zone_id").alias("parent_nearest_zone_id"),
        pl.col("construction_date").alias("parent_construction_date"),
        pl.col("parking_space_by_area").alias("parent_parking_space_by_area"),
    ])
    spots_subspaces = spots.join(
        parent_complex, left_on="parent_id", right_on="parent_id_real", how="inner"
    )

    reals = [
        "spot_type_id", "street", "ext_number", "int_number", "zip_code_id",
        "latitude", "longitude", "quality_state", "state_id", "data_municipality_id",
        "city_id", "settlement_id", "zone_id", "nearest_zone_id", "contact_id",
        "spot_state", "state_reason", "construction_date", "parking_space_by_area",
    ]
    reals_expr_singles = [pl.col(c).alias(f"{c}_real") for c in reals if c in spots_singles.columns]
    reals_expr_complexes = [pl.col(c).alias(f"{c}_real") for c in reals if c in spots_complexes.columns]

    pts_singles = spots_singles.with_columns(reals_expr_singles + [pl.lit(1).alias("source_table_id")])
    pts_complexes = spots_complexes.with_columns(reals_expr_complexes + [pl.lit(2).alias("source_table_id")])
    pts_sub = spots_subspaces.with_columns([
        pl.coalesce(pl.col("parent_spot_type_id"), pl.col("spot_type_id")).alias("spot_type_id_real"),
        pl.coalesce(pl.col("parent_street"), pl.col("street")).alias("street_real"),
        pl.coalesce(pl.col("parent_ext_number"), pl.col("ext_number")).alias("ext_number_real"),
        pl.coalesce(pl.col("parent_int_number"), pl.col("int_number")).alias("int_number_real"),
        pl.coalesce(pl.col("parent_zip_code_id"), pl.col("zip_code_id")).alias("zip_code_id_real"),
        pl.coalesce(pl.col("parent_latitude"), pl.col("latitude")).alias("latitude_real"),
        pl.coalesce(pl.col("parent_longitude"), pl.col("longitude")).alias("longitude_real"),
        pl.coalesce(pl.col("parent_quality_state"), pl.col("quality_state")).alias("quality_state_real"),
        pl.coalesce(pl.col("parent_state_id"), pl.col("state_id")).alias("state_id_real"),
        pl.coalesce(pl.col("parent_data_municipality_id"), pl.col("data_municipality_id")).alias("data_municipality_id_real"),
        pl.coalesce(pl.col("parent_city_id"), pl.col("city_id")).alias("city_id_real"),
        pl.coalesce(pl.col("parent_settlement_id"), pl.col("settlement_id")).alias("settlement_id_real"),
        pl.coalesce(pl.col("parent_zone_id"), pl.col("zone_id")).alias("zone_id_real"),
        pl.coalesce(pl.col("parent_nearest_zone_id"), pl.col("nearest_zone_id")).alias("nearest_zone_id_real"),
        pl.coalesce(pl.col("parent_contact_id"), pl.col("contact_id")).alias("contact_id_real"),
        pl.col("parent_spot_state").alias("spot_state_real"),
        pl.col("parent_state_reason").alias("state_reason_real"),
        pl.coalesce(pl.col("parent_construction_date"), pl.col("construction_date")).alias("construction_date_real"),
        pl.coalesce(pl.col("parent_parking_space_by_area"), pl.col("parking_space_by_area")).alias("parking_space_by_area_real"),
        pl.lit(3).alias("source_table_id"),
    ])
    pre_total_spots = pl.concat([pts_singles, pts_complexes, pts_sub], how="diagonal_relaxed")

    _norm_state_cols = [
        expr_mysql_numeric_int64(c).alias(c)
        for c in ("spot_state", "spot_state_real", "state_reason", "state_reason_real")
        if c in pre_total_spots.columns
    ]
    if _norm_state_cols:
        pre_total_spots = pre_total_spots.with_columns(_norm_state_cols)

    if "spot_state_real" not in pre_total_spots.columns and "spot_state" in pre_total_spots.columns:
        pre_total_spots = pre_total_spots.with_columns(pl.col("spot_state").alias("spot_state_real"))
    if "state_reason_real" not in pre_total_spots.columns and "state_reason" in pre_total_spots.columns:
        pre_total_spots = pre_total_spots.with_columns(pl.col("state_reason").alias("state_reason_real"))

    state_real = "spot_state_real" if "spot_state_real" in pre_total_spots.columns else "spot_state"
    reason_real = "state_reason_real" if "state_reason_real" in pre_total_spots.columns else "state_reason"
    parent_full_list = [
        (pl.col(state_real) == 1, "Public"),
        ((pl.col(state_real) == 2) & (pl.col(reason_real) == 1), "Draft User"),
        ((pl.col(state_real) == 2) & (pl.col(reason_real) == 2), "Draft Onboarding API"),
        ((pl.col(state_real) == 2) & (pl.col(reason_real) == 3), "Draft Onboarding Scraping"),
        ((pl.col(state_real) == 2) & (pl.col(reason_real) == 4), "Draft QA"),
        ((pl.col(state_real) == 3) & (pl.col(reason_real) == 5), "Disabled Publisher"),
        ((pl.col(state_real) == 3) & (pl.col(reason_real) == 6), "Disabled Occupied"),
        ((pl.col(state_real) == 3) & (pl.col(reason_real) == 7), "Disabled Won"),
        ((pl.col(state_real) == 3) & (pl.col(reason_real) == 8), "Disabled QA"),
        ((pl.col(state_real) == 3) & (pl.col(reason_real) == 9), "Disabled Internal Use"),
        ((pl.col(state_real) == 4) & (pl.col(reason_real) == 10), "Archived Internal"),
        ((pl.col(state_real) == 4) & (pl.col(reason_real) == 11), "Archived Owner"),
        ((pl.col(state_real) == 4) & (pl.col(reason_real) == 12), "Archived Unpublished"),
        ((pl.col(state_real) == 4) & (pl.col(reason_real) == 13), "Archived Outdated"),
        ((pl.col(state_real) == 4) & (pl.col(reason_real) == 14), "Archived Quality"),
        # Legacy SQL: nested CASE with ELSE 'Draft' / 'Disabled' / 'Archived' (not Unknown)
        (pl.col(state_real) == 2, "Draft"),
        (pl.col(state_real) == 3, "Disabled"),
        (pl.col(state_real) == 4, "Archived"),
    ]
    spot_full_list = [
        (pl.col("spot_state") == 1, "Public"),
        ((pl.col("spot_state") == 2) & (pl.col("state_reason") == 1), "Draft User"),
        ((pl.col("spot_state") == 2) & (pl.col("state_reason") == 2), "Draft Onboarding API"),
        ((pl.col("spot_state") == 2) & (pl.col("state_reason") == 3), "Draft Onboarding Scraping"),
        ((pl.col("spot_state") == 2) & (pl.col("state_reason") == 4), "Draft QA"),
        ((pl.col("spot_state") == 3) & (pl.col("state_reason") == 5), "Disabled Publisher"),
        ((pl.col("spot_state") == 3) & (pl.col("state_reason") == 6), "Disabled Occupied"),
        ((pl.col("spot_state") == 3) & (pl.col("state_reason") == 7), "Disabled Won"),
        ((pl.col("spot_state") == 3) & (pl.col("state_reason") == 8), "Disabled QA"),
        ((pl.col("spot_state") == 3) & (pl.col("state_reason") == 9), "Disabled Internal Use"),
        ((pl.col("spot_state") == 4) & (pl.col("state_reason") == 10), "Archived Internal"),
        ((pl.col("spot_state") == 4) & (pl.col("state_reason") == 11), "Archived Owner"),
        ((pl.col("spot_state") == 4) & (pl.col("state_reason") == 12), "Archived Unpublished"),
        ((pl.col("spot_state") == 4) & (pl.col("state_reason") == 13), "Archived Outdated"),
        ((pl.col("spot_state") == 4) & (pl.col("state_reason") == 14), "Archived Quality"),
        (pl.col("spot_state") == 2, "Draft"),
        (pl.col("spot_state") == 3, "Disabled"),
        (pl.col("spot_state") == 4, "Archived"),
    ]
    total_spots_with_statuses = pre_total_spots.with_columns([
        pl.col(state_real).alias("spot_parent_status_id"),
        case_when(state_real, STATUS_LABEL_MAP, default="Unknown").alias("spot_parent_status"),
        pl.col(reason_real).alias("spot_parent_status_reason_id"),
        _status_reason_label(reason_real).alias("spot_parent_status_reason"),
        (100 * pl.col(state_real) + pl.col(reason_real).fill_null(0)).alias("spot_parent_status_full_id"),
        case_when_conditions(parent_full_list, default="Unknown").alias("spot_parent_status_full"),
        pl.col("spot_state").alias("spot_status_id"),
        case_when("spot_state", STATUS_LABEL_MAP, default="Unknown").alias("spot_status"),
        pl.col("state_reason").alias("spot_status_reason_id"),
        _status_reason_label("state_reason").alias("spot_status_reason"),
        (100 * pl.col("spot_state") + pl.col("state_reason").fill_null(0)).alias("spot_status_full_id"),
        case_when_conditions(spot_full_list, default="Unknown").alias("spot_status_full"),
    ])

    zc = zip_codes.select(pl.col("id").alias("zc_id"), pl.col("state_id").alias("zc_state_id"))
    total_spots = total_spots_with_statuses.join(
        zc, left_on="zip_code_id_real", right_on="zc_id", how="left"
    ).with_columns(
        pl.coalesce(pl.col("state_id_real"), pl.col("zc_state_id")).alias("state_id_real2")
    )
    drop_zc = [c for c in ["zc_id", "zc_state_id"] if c in total_spots.columns]
    if drop_zc:
        total_spots = total_spots.drop(drop_zc)

    return total_spots
