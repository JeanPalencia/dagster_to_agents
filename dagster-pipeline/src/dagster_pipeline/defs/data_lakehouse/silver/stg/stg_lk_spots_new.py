# defs/data_lakehouse/silver/stg/stg_lk_spots_new.py
"""
Silver aggregate: lk_spots view (replica of stg_lk_spots.sql).

Reads the 21 Silver tables from S3 (they must be materialized first in the same or a previous run)
and applies the full logic from Data_Lakehouse/sql/mysql/stg_lk_spots.sql. Output:
a single Silver table that feeds gold_lk_spots_new.

Upstream (deps; run before this asset):
- stg_s2p_exchanges_new, stg_s2p_prices_new, stg_s2p_spots_new, stg_s2p_zip_codes_new,
  stg_s2p_spot_reports_new, stg_s2p_report_history_new, stg_s2p_listings_spots_new,
  stg_s2p_listings_new, stg_s2p_photos_new,   stg_s2p_contacts_new, stg_s2p_users_new,
  stg_s2p_profiles_new, stg_s2p_model_has_roles_new, stg_s2p_data_states_new, stg_s2p_states_new,
  stg_s2p_data_municipalities_new, stg_s2p_cities_new, stg_s2p_data_settlements_new,
  stg_s2p_zones_new, stg_s2p_spot_photos_new, stg_s2p_spot_rankings_new.
"""
from __future__ import annotations

import gc
import os
import sys
from datetime import datetime, timedelta

try:
    import resource
except ImportError:
    resource = None  # Unix-only; unavailable on Windows

import dagster as dg
import polars as pl
from decimal import Decimal

from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_SILVER
from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    build_silver_s3_key,
    write_polars_to_s3,
    read_silver_from_s3,
)
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute
from dagster_pipeline.defs.data_lakehouse.silver.modules.lk_spots import (
    get_exchange_rate_decimal,
    select_latest_exchange_row,
    build_total_spots,
    build_clean_prices,
    build_spots_prices,
    build_spot_reports_summary,
    build_spot_listings_filtered,
    build_spot_photos,
    build_final_results,
)


# Names of the 21 silver tables stg_lk_spots_new reads from S3 (same order as before)
_STG_LK_SPOTS_UPSTREAM = (
    "stg_s2p_exchanges_new",
    "stg_s2p_prices_new",
    "stg_s2p_spots_new",
    "stg_s2p_zip_codes_new",
    "stg_s2p_spot_reports_new",
    "stg_s2p_report_history_new",
    "stg_s2p_listings_spots_new",
    "stg_s2p_listings_new",
    "stg_s2p_photos_new",
    "stg_s2p_contacts_new",
    "stg_s2p_users_new",
    "stg_s2p_profiles_new",
    "stg_s2p_model_has_roles_new",
    "stg_s2p_data_states_new",
    "stg_s2p_states_new",
    "stg_s2p_data_municipalities_new",
    "stg_s2p_cities_new",
    "stg_s2p_data_settlements_new",
    "stg_s2p_zones_new",
    "stg_s2p_spot_photos_new",
    "stg_s2p_spot_rankings_new",
)


def _load_silver_tables_from_s3(context) -> dict[str, pl.DataFrame]:
    """Load the 21 silver tables from S3 for the current partition."""
    result = {}
    for name in _STG_LK_SPOTS_UPSTREAM:
        df, _ = read_silver_from_s3(name, context)
        result[name] = df
    return result


def _log_memory_rss(context, label: str) -> None:
    """Log current process RSS in MB (ru_maxrss: bytes on macOS, KB on Linux). No-op on Windows (resource is Unix-only)."""
    if resource is None:
        context.log.debug(f"Memory RSS [{label}]: N/A (Windows)")
        return
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_mb = rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024
    context.log.info(f"Memory RSS [{label}]: {rss_mb:.1f} MB")


def _transform_stg_lk_spots(
    context,
    stg_s2p_exchanges_new: pl.DataFrame,
    stg_s2p_prices_new: pl.DataFrame,
    stg_s2p_spots_new: pl.DataFrame,
    stg_s2p_zip_codes_new: pl.DataFrame,
    stg_s2p_spot_reports_new: pl.DataFrame,
    stg_s2p_report_history_new: pl.DataFrame,
    stg_s2p_listings_spots_new: pl.DataFrame,
    stg_s2p_listings_new: pl.DataFrame,
    stg_s2p_photos_new: pl.DataFrame,
    stg_s2p_contacts_new: pl.DataFrame,
    stg_s2p_users_new: pl.DataFrame,
    stg_s2p_profiles_new: pl.DataFrame,
    stg_s2p_model_has_roles_new: pl.DataFrame,
    stg_s2p_data_states_new: pl.DataFrame,
    stg_s2p_states_new: pl.DataFrame,
    stg_s2p_data_municipalities_new: pl.DataFrame,
    stg_s2p_cities_new: pl.DataFrame,
    stg_s2p_data_settlements_new: pl.DataFrame,
    stg_s2p_zones_new: pl.DataFrame,
    stg_s2p_spot_photos_new: pl.DataFrame,
    stg_s2p_spot_rankings_new: pl.DataFrame,
) -> pl.DataFrame:
    """
    Replicates Data_Lakehouse/sql/mysql/stg_lk_spots.sql logic in Polars.
    Delegates to lk_spots modules: exchange_rates, hierarchy, prices, reports, listings, photos, presentation.
    Releases intermediates as soon as they are no longer used (del + gc.collect) to reduce memory peak.
    """
    spots = stg_s2p_spots_new
    if spots.height == 0:
        context.log.warning("stg_s2p_spots_new is empty; stg_lk_spots_new will be empty.")
        return spots

    # Tope para created_at del TC: filas con fecha <= tc_max_date; luego la más reciente.
    # - LK_SPOTS_TC_MAX_DATE_LAG_VS_PARTITION=0 (default): partition_key = fecha de cierre de datos →
    #   p.ej. partition 2026-03-23 incluye TC del 23 (no del 24).
    # - LK_SPOTS_TC_MAX_DATE_LAG_VS_PARTITION=1: partition_key = día de corrida y datos van hasta ayer →
    #   p.ej. partition 2026-03-24 usa TC hasta el 23 (evita TC “de hoy” si el extract ya lo trae).
    partition_day = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    lag_days = int(os.environ.get("LK_SPOTS_TC_MAX_DATE_LAG_VS_PARTITION", "0"))
    tc_max_date = partition_day - timedelta(days=lag_days)
    context.log.info(
        f"exchange_rate: max_effective_date={tc_max_date} "
        f"(partition={context.partition_key}, LK_SPOTS_TC_MAX_DATE_LAG_VS_PARTITION={lag_days})"
    )

    er_pick, er_cap_warn = select_latest_exchange_row(
        stg_s2p_exchanges_new, max_effective_date=tc_max_date
    )
    if er_cap_warn:
        context.log.warning(er_cap_warn)
    if er_pick.height > 0:
        eid = er_pick["id"][0] if "id" in er_pick.columns else None
        cat = er_pick["created_at"][0] if "created_at" in er_pick.columns else None
        er_val = er_pick["exchange_rate"][0]
        context.log.info(
            f"exchange_rate pick: id={eid} created_at={cat} rate={er_val} "
            f"(rows in stg_s2p_exchanges_new={stg_s2p_exchanges_new.height})"
        )
    er = get_exchange_rate_decimal(stg_s2p_exchanges_new, max_effective_date=tc_max_date)
    if er <= 0:
        context.log.warning("exchanges empty or invalid exchange_rate; using er=1 for prices.")
        er = Decimal("1")

    clean_prices = build_clean_prices(stg_s2p_prices_new, er)
    total_spots = build_total_spots(spots, stg_s2p_prices_new, stg_s2p_zip_codes_new)
    spots_prices = build_spots_prices(clean_prices, total_spots)
    # Release large DFs no longer needed to reduce memory peak (~80% on last step)
    _log_memory_rss(context, "before release clean_prices/total_spots")
    del clean_prices, total_spots
    gc.collect()
    _log_memory_rss(context, "after release clean_prices/total_spots")

    spot_reports_summary = build_spot_reports_summary(
        stg_s2p_spot_reports_new, stg_s2p_report_history_new
    )
    spot_listings_filtered = build_spot_listings_filtered(
        stg_s2p_listings_spots_new, stg_s2p_listings_new
    )
    spot_photos = build_spot_photos(stg_s2p_photos_new)

    out = build_final_results(
        spots_prices,
        stg_s2p_zip_codes_new,
        stg_s2p_states_new,
        stg_s2p_data_states_new,
        stg_s2p_data_municipalities_new,
        stg_s2p_cities_new,
        stg_s2p_data_settlements_new,
        stg_s2p_zones_new,
        stg_s2p_contacts_new,
        stg_s2p_users_new,
        stg_s2p_profiles_new,
        stg_s2p_model_has_roles_new,
        stg_s2p_spot_rankings_new,
        spot_reports_summary,
        spot_listings_filtered,
        spot_photos,
    )
    # Release intermediates before writing to S3 / returning
    _log_memory_rss(context, "before release spots_prices/summaries")
    del spots_prices, spot_reports_summary, spot_listings_filtered, spot_photos
    gc.collect()
    _log_memory_rss(context, "after release spots_prices/summaries")

    context.log.info(
        f"stg_lk_spots_new: final_results -> {out.height} rows, {out.width} cols."
    )
    return out


@dg.asset(
    name="stg_lk_spots_new",
    partitions_def=daily_partitions,
    group_name="silver",
    retry_policy=dg.RetryPolicy(max_retries=2),
    tags=TAGS_LK_SPOTS_SILVER,
    deps=list(_STG_LK_SPOTS_UPSTREAM),
    description=(
        "Silver aggregate: lk_spots view from 21 STG (replica of stg_lk_spots.sql). "
        "Reads upstream silver from S3. Feeds gold_lk_spots_new."
    ),
    io_manager_key="s3_silver",
)
def stg_lk_spots_new(context):
    """
    Builds the Silver view stg_lk_spots. Reads the 21 Silver tables from S3
    (upstream assets must be materialized first), then applies the same logic as before.
    """
    def body():
        tables = _load_silver_tables_from_s3(context)
        df_out = _transform_stg_lk_spots(
            context,
            stg_s2p_exchanges_new=tables["stg_s2p_exchanges_new"],
            stg_s2p_prices_new=tables["stg_s2p_prices_new"],
            stg_s2p_spots_new=tables["stg_s2p_spots_new"],
            stg_s2p_zip_codes_new=tables["stg_s2p_zip_codes_new"],
            stg_s2p_spot_reports_new=tables["stg_s2p_spot_reports_new"],
            stg_s2p_report_history_new=tables["stg_s2p_report_history_new"],
            stg_s2p_listings_spots_new=tables["stg_s2p_listings_spots_new"],
            stg_s2p_listings_new=tables["stg_s2p_listings_new"],
            stg_s2p_photos_new=tables["stg_s2p_photos_new"],
            stg_s2p_contacts_new=tables["stg_s2p_contacts_new"],
            stg_s2p_users_new=tables["stg_s2p_users_new"],
            stg_s2p_profiles_new=tables["stg_s2p_profiles_new"],
            stg_s2p_model_has_roles_new=tables["stg_s2p_model_has_roles_new"],
            stg_s2p_data_states_new=tables["stg_s2p_data_states_new"],
            stg_s2p_states_new=tables["stg_s2p_states_new"],
            stg_s2p_data_municipalities_new=tables["stg_s2p_data_municipalities_new"],
            stg_s2p_cities_new=tables["stg_s2p_cities_new"],
            stg_s2p_data_settlements_new=tables["stg_s2p_data_settlements_new"],
            stg_s2p_zones_new=tables["stg_s2p_zones_new"],
            stg_s2p_spot_photos_new=tables["stg_s2p_spot_photos_new"],
            stg_s2p_spot_rankings_new=tables["stg_s2p_spot_rankings_new"],
        )

        context.log.info(
            f"stg_lk_spots_new: {df_out.height:,} rows, {df_out.width} columns "
            f"for partition {context.partition_key}"
        )

        s3_key = build_silver_s3_key("stg_lk_spots_new", file_format="parquet")
        write_polars_to_s3(df_out, s3_key, context, file_format="parquet")
        gc.collect()  # Release write buffers before returning
        return df_out

    yield from iter_job_wrapped_compute(context, body)
