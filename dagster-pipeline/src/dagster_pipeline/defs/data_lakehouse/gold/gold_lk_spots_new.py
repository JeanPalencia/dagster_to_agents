# defs/data_lakehouse/gold/gold_lk_spots_new.py
"""
Gold layer: Final lk_spots table ready for consumption (NEW VERSION).

Canonical column order: PostgreSQL lk_spots (see GOLD_LK_SPOTS_GOVERNANCE_ORDER).
Reads stg_lk_spots_new from S3, aligns columns to PostgreSQL lk_spots (name + order),
adds filter/filter_sub and audit fields via gold.utils.add_audit_fields.
Column order matches PostgreSQL lk_spots (183 columns through aud_job).
"""
import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_GOLD
from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_gold_s3_key,
    write_polars_to_s3,
    read_silver_from_s3,
    load_to_geospot,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.notifications import send_stale_data_notification
from dagster_pipeline.defs.data_lakehouse.gold.utils import add_audit_fields


# Exact column order and names for PostgreSQL lk_spots (183 columns).
# Must match the table definition: Parquet → Geospot relies on this order/types.
GOLD_LK_SPOTS_GOVERNANCE_ORDER = (
    "spot_id", "spot_link", "spot_public_link",
    "spot_parent_status_id", "spot_parent_status",
    "spot_parent_status_reason_id", "spot_parent_status_reason",
    "spot_parent_status_full_id", "spot_parent_status_full",
    "spot_status_id", "spot_status",
    "spot_status_reason_id", "spot_status_reason",
    "spot_status_full_id", "spot_status_full",
    "spot_sector_id", "spot_sector", "spot_type_id", "spot_type",
    "spot_is_complex", "spot_parent_id",
    "spot_title", "spot_description",
    "spot_address", "spot_street", "spot_ext_number", "spot_int_number",
    "spot_settlement",
    "spot_data_settlement_id", "spot_data_settlement",
    "spot_municipality_id", "spot_municipality", "spot_data_municipality_id", "spot_data_municipality",
    "spot_state_id", "spot_state", "spot_data_state", "spot_region_id", "spot_region",
    "spot_corridor_id", "spot_corridor", "spot_nearest_corridor_id", "spot_nearest_corridor",
    "spot_latitude", "spot_longitude", "spot_zip_code_id", "spot_zip_code",
    "spot_settlement_type_id", "spot_settlement_type", "spot_settlement_type_en",
    "spot_zone_type_id", "spot_zone_type", "spot_zone_type_en",
    "spot_municipality_zip_code", "spot_state_zip_code",
    "spot_listing_id", "spot_listing_representative_status_id", "spot_listing_representative_status",
    "spot_listing_status_id", "spot_listing_status", "spot_listing_hierarchy", "spot_is_listing_id", "spot_is_listing",
    "spot_area_in_sqm", "spot_modality_id", "spot_modality",
    "spot_price_area_rent_id", "spot_price_area_rent", "spot_price_area_sale_id", "spot_price_area_sale",
    "spot_currency_rent_id", "spot_currency_rent", "spot_currency_sale_id", "spot_currency_sale",
    "spot_price_total_mxn_rent", "spot_price_total_usd_rent", "spot_price_sqm_mxn_rent", "spot_price_sqm_usd_rent",
    "spot_price_total_mxn_sale", "spot_price_total_usd_sale", "spot_price_sqm_mxn_sale", "spot_price_sqm_usd_sale",
    "spot_maintenance_cost_mxn", "spot_maintenance_cost_usd",
    "spot_sub_min_price_total_mxn_rent", "spot_sub_max_price_total_mxn_rent", "spot_sub_mean_price_total_mxn_rent",
    "spot_sub_min_price_sqm_mxn_rent", "spot_sub_max_price_sqm_mxn_rent", "spot_sub_mean_price_sqm_mxn_rent",
    "spot_sub_min_price_total_usd_rent", "spot_sub_max_price_total_usd_rent", "spot_sub_mean_price_total_usd_rent",
    "spot_sub_min_price_sqm_usd_rent", "spot_sub_max_price_sqm_usd_rent", "spot_sub_mean_price_sqm_usd_rent",
    "spot_sub_min_price_total_mxn_sale", "spot_sub_max_price_total_mxn_sale", "spot_sub_mean_price_total_mxn_sale",
    "spot_sub_min_price_sqm_mxn_sale", "spot_sub_max_price_sqm_mxn_sale", "spot_sub_mean_price_sqm_mxn_sale",
    "spot_sub_min_price_total_usd_sale", "spot_sub_max_price_total_usd_sale", "spot_sub_mean_price_total_usd_sale",
    "spot_sub_min_price_sqm_usd_sale", "spot_sub_max_price_sqm_usd_sale", "spot_sub_mean_price_sqm_usd_sale",
    "contact_id", "contact_email", "contact_domain", "contact_subgroup_id", "contact_subgroup",
    "contact_category_id", "contact_category", "contact_company",
    "user_id", "user_profile_id",
    "user_max_role_id", "user_max_role",
    "user_industria_role_id", "user_industria_role",
    "user_email", "user_domain",
    "user_affiliation_id", "user_affiliation",
    "user_level_id", "user_level", "user_broker_next_id", "user_broker_next",
    "spot_external_id", "spot_external_updated_at",
    "spot_quality_control_status_id", "spot_quality_control_status",
    "spot_photo_count", "spot_photo_platform_count",
    "spot_parent_photo_count", "spot_parent_photo_platform_count",
    "spot_photo_effective_count", "spot_photo_platform_effective_count",
    "spot_class_id", "spot_class", "spot_parking_spaces", "spot_parking_space_by_area",
    "spot_condition_id", "spot_condition", "spot_construction_date", "spot_score",
    "spot_exclusive_id", "spot_exclusive",
    "spot_landlord_exclusive_id", "spot_landlord_exclusive",
    "spot_is_area_in_range_id", "spot_is_area_in_range",
    "spot_is_rent_price_in_range_id", "spot_is_rent_price_in_range",
    "spot_is_sale_price_in_range_id", "spot_is_sale_price_in_range",
    "spot_is_maintenance_price_in_range_id", "spot_is_maintenance_price_in_range",
    "spot_origin_id", "spot_origin",
    "spot_last_active_report_reason_id", "spot_last_active_report_reason", "spot_last_active_report_user_id",
    "spot_last_active_report_created_at", "spot_has_active_report", "spot_reports_full_history",
    "spot_created_at", "spot_created_date", "spot_updated_at", "spot_updated_date",
    "spot_valid_through", "spot_valid_through_date", "spot_deleted_at", "spot_deleted_date",
    "filter", "filter_sub",
    "aud_inserted_at", "aud_inserted_date", "aud_updated_at", "aud_updated_date", "aud_job",
)


def _transform_lk_spots_new(stg_lk_spots_new: pl.DataFrame) -> pl.DataFrame:
    """
    Builds gold_lk_spots_new to match lk_spots: 183 columns in exact order.

    - Selects columns present in stg; adds null for missing governance columns.
    - Adds filter (spot_status_id=1 and spot_type_id!=3) and filter_sub (spot_status_id=1 and spot_type_id=3).
    - Adds audit fields with add_audit_fields(job_name="lk_spots").
    - Final select in GOLD_LK_SPOTS_GOVERNANCE_ORDER for Geospot COPY/Parquet load.
    """
    df = stg_lk_spots_new
    # Select columns present in stg (keep all that match legacy names)
    select_cols = [c for c in GOLD_LK_SPOTS_GOVERNANCE_ORDER if c in df.columns]
    if select_cols:
        df = df.select(select_cols)
    # filter / filter_sub (same logic as lk_spots.sql)
    if df.height == 0:
        df = df.with_columns([
            pl.lit(0).alias("filter"),
            pl.lit(0).alias("filter_sub"),
        ])
    else:
        status_public = pl.col("spot_status_id") == 1 if "spot_status_id" in df.columns else pl.lit(False)
        type_sub = pl.col("spot_type_id") == 3 if "spot_type_id" in df.columns else pl.lit(False)
        df = df.with_columns([
            pl.when(status_public & ~type_sub).then(1).otherwise(0).alias("filter"),
            pl.when(status_public & type_sub).then(1).otherwise(0).alias("filter_sub"),
        ])
    df = add_audit_fields(df, job_name="lk_spots")

    # spot_zip_code: 5-digit string (leading zeros), same idea as pandas to_numeric + pad
    if "spot_zip_code" in df.columns:
        zip_raw = pl.col("spot_zip_code")
        z = (
            pl.when(zip_raw.is_null())
            .then(pl.lit(None).cast(pl.Int64))
            .otherwise(
                zip_raw.cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
                .cast(pl.Int64, strict=False)
            )
        )
        df = df.with_columns(
            pl.when(z.is_null())
            .then(pl.lit(None).cast(pl.Utf8))
            .when((z >= 0) & (z <= 99_999))
            .then(z.cast(pl.Utf8).str.pad_start(5, "0"))
            .otherwise(pl.lit(None).cast(pl.Utf8))
            .alias("spot_zip_code")
        )
    # Legacy lk_spots: spot_parent_id is text (stringified id)
    if "spot_parent_id" in df.columns:
        _pid = pl.col("spot_parent_id")
        _pid_txt = pl.coalesce(
            _pid.cast(pl.Int64, strict=False).cast(pl.Utf8),
            _pid.cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).cast(pl.Utf8),
            _pid.cast(pl.Utf8, strict=False),
        )
        df = df.with_columns(
            pl.when(_pid.is_null()).then(pl.lit(None).cast(pl.Utf8)).otherwise(_pid_txt).alias("spot_parent_id")
        )
    if "spot_condition_id" in df.columns:
        # Legacy: text "0.0", "1.0", etc.
        df = df.with_columns(
            pl.when(pl.col("spot_condition_id").is_not_null())
            .then(pl.col("spot_condition_id").cast(pl.Utf8) + pl.lit(".0"))
            .otherwise(pl.lit(None).cast(pl.Utf8))
            .alias("spot_condition_id")
        )
    if "spot_construction_date" in df.columns:
        # Legacy: text (ISO date) or null; do not fill with default
        df = df.with_columns(
            pl.when(pl.col("spot_construction_date").is_not_null())
            .then(pl.col("spot_construction_date").cast(pl.Utf8))
            .otherwise(pl.lit(None).cast(pl.Utf8))
            .alias("spot_construction_date")
        )

    # Add missing governance columns as null.
    # Types aligned to legacy lk_spots / target Postgres (incl. bigint is_complex, text parent_id, timestamps).
    def _null_type_for(col: str):
        if col == "spot_external_updated_at":
            return pl.Datetime  # legacy lk_spots: timestamp
        if col in ("spot_valid_through", "spot_valid_through_date"):
            return pl.Date
        if col.endswith("_id") or "count" in col or "hierarchy" in col or col in ("filter", "filter_sub", "spot_has_active_report"):
            return pl.Int64
        if col in ("spot_parking_space_by_area", "spot_condition_id", "spot_construction_date", "spot_external_id"):
            return pl.Utf8  # schema: text
        if col.endswith("_date") and "aud_" not in col:
            return pl.Date
        if col.startswith("aud_"):
            return pl.Datetime if col.endswith("_at") else pl.Date
        if col.endswith("_at"):
            return pl.Datetime
        if any(x in col for x in ("price", "sqm", "cost", "score", "latitude", "longitude", "area")):
            return pl.Float64
        if col == "spot_reports_full_history":
            # Legacy type JSON; Polars Utf8 (valid JSON text for load to json/jsonb).
            return pl.Utf8
        return pl.Utf8

    for col in GOLD_LK_SPOTS_GOVERNANCE_ORDER:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(_null_type_for(col)).alias(col))

    # Columns that in Postgres are integer/bigint: force Int64 first (most governance *_id are bigint).
    # Exclude text ids / legacy text parent: spot_parent_id is Utf8 like legacy lk_spots.
    _int_cols = [
        c for c in df.columns
        if (
            c.endswith("_id")
            or "count" in c
            or c in ("filter", "filter_sub", "spot_has_active_report", "spot_listing_hierarchy", "spot_parking_spaces")
        )
        and c not in (
            "spot_condition_id",
            "spot_external_id",
            "spot_parking_space_by_area",
            "spot_is_complex",
            "spot_parent_id",
        )
    ]
    if _int_cols:
        # Float or numeric string -> Int64 (null preserved)
        df = df.with_columns([
            pl.when(pl.col(c).is_null())
            .then(pl.lit(None).cast(pl.Int64))
            .otherwise(pl.col(c).cast(pl.Float64).cast(pl.Int64))
            .alias(c)
            for c in _int_cols
        ])

    # Governance DDL: spot_status_id / spot_parent_status_id are INTEGER; legacy lk_spots uses BIGINT.
    for c in ("spot_status_id", "spot_parent_status_id"):
        if c in df.columns:
            df = df.with_columns(
                pl.when(pl.col(c).is_null())
                .then(pl.lit(None).cast(pl.Int32))
                .otherwise(pl.col(c).cast(pl.Int32, strict=False))
                .alias(c)
            )

    # spot_parking_space_by_area: schema text; keep as text (e.g. "1/9", "1/50").
    if "spot_parking_space_by_area" in df.columns:
        df = df.with_columns(pl.col("spot_parking_space_by_area").cast(pl.Utf8).alias("spot_parking_space_by_area"))

    # Legacy lk_spots: spot_is_complex as bigint (0/1), not boolean
    if "spot_is_complex" in df.columns:
        _sic = pl.col("spot_is_complex")
        df = df.with_columns(
            pl.when(_sic.is_null())
            .then(pl.lit(None).cast(pl.Int64))
            .otherwise(_sic.cast(pl.Int64, strict=False))
            .alias("spot_is_complex")
        )

    if "spot_external_updated_at" in df.columns:
        df = df.with_columns(
            pl.col("spot_external_updated_at").cast(pl.Datetime("us"), strict=False).alias("spot_external_updated_at")
        )
    for _dcol in ("spot_valid_through", "spot_valid_through_date"):
        if _dcol in df.columns:
            df = df.with_columns(pl.col(_dcol).cast(pl.Date, strict=False).alias(_dcol))

    df = df.select(GOLD_LK_SPOTS_GOVERNANCE_ORDER)
    return df


@dg.asset(
    name="gold_lk_spots_new",
    partitions_def=daily_partitions,
    group_name="gold",
    retry_policy=dg.RetryPolicy(max_retries=2),
    tags=TAGS_LK_SPOTS_GOLD,
    io_manager_key="s3_gold",
    description=(
        "Gold: lk_spots for PostgreSQL (183 cols, Geospot order). "
        "Reads stg_lk_spots_new from S3; filter/filter_sub + add_audit_fields; S3 parquet + load_to_geospot."
    ),
)
def gold_lk_spots_new(context):
    """
    Builds the gold lk_spots table from stg_lk_spots_new.

    Output column order matches PostgreSQL lk_spots (183 columns).
    Uses add_audit_fields from gold.utils for aud_*.
    """
    def body():
        stale_tables = []
        stg_lk_spots_new, meta = read_silver_from_s3("stg_lk_spots_new", context)
        if meta.get("is_stale"):
            stale_tables.append({
                "table_name": "stg_lk_spots_new",
                "expected_date": context.partition_key,
                "available_date": meta.get("file_date", ""),
                "layer": "silver",
                "file_path": meta.get("file_path", ""),
            })

        df = _transform_lk_spots_new(stg_lk_spots_new)
        context.log.info(
            f"gold_lk_spots_new: {df.height:,} rows, {df.width} columns for partition {context.partition_key}"
        )

        s3_key_full = build_gold_s3_key("lk_spots_new", context.partition_key, file_format="parquet")
        write_polars_to_s3(df, s3_key_full, context, file_format="parquet")

        context.log.info(
            f"📤 Loading to Geospot (single parquet, {df.height:,} rows): table=lk_spots"
        )
        load_to_geospot(
            s3_key=s3_key_full,
            table_name="lk_spots",
            mode="replace",
            context=context,
            timeout=600,
            max_retries=5,
        )
        context.log.info("✅ Geospot load completed for lk_spots")

        if stale_tables:
            send_stale_data_notification(
                context,
                stale_tables,
                current_asset_name="gold_lk_spots_new",
                current_layer="gold",
            )

        return df

    yield from iter_job_wrapped_compute(context, body)
