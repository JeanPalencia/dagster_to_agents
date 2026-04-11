# defs/data_lakehouse/silver/stg/stg_s2p_profiles_new.py
"""
Silver STG: Complete transformation of raw_s2p_profiles_new using reusable functions.

This asset maintains original bronze column names and only adds calculated columns when necessary.
Gets the most recent profile per user using window functions.
"""
import polars as pl

from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset
from dagster_pipeline.defs.data_lakehouse.lk_spots_concurrency import TAGS_LK_SPOTS_SILVER
from dagster_pipeline.defs.data_lakehouse.silver.utils import case_when


def _transform_s2p_profiles(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms raw_s2p_profiles_new maintaining original column names.
    Only adds calculated columns when necessary (CASE statements, concatenations, etc.).

    Replicates the logic:
    - CASE statements for person_type, tenant_type mappings
    - String concatenations for full name and phone
    """

    # CASE statements using reusable functions
    user_person_type_expr = case_when(
        "person_type",
        {1: "Legal Entity", 2: "Individual"},
        default="Unknown",
        alias="user_person_type"
    )

    user_industria_role_expr = case_when(
        "tenant_type",
        {1: "Tenant", 2: "Broker", 4: "Landlord", 5: "Developer"},
        default="Unknown",
        alias="user_industria_role"
    )

    # user_type_id: tenant_type -> 1 (Buyer) or 2 (Seller) or 0
    user_type_id_expr = (
        pl.when(pl.col("tenant_type") == 1)
        .then(1)
        .when(pl.col("tenant_type").is_in([2, 4, 5]))
        .then(2)
        .otherwise(0)
        .alias("user_type_id")
    )

    # user_type: tenant_type -> "Buyer" (1) or "Seller" (2,4,5) or "Unknown"
    user_type_expr = (
        pl.when(pl.col("tenant_type") == 1)
        .then(pl.lit("Buyer"))
        .when(pl.col("tenant_type").is_in([2, 4, 5]))
        .then(pl.lit("Seller"))
        .otherwise(pl.lit("Unknown"))
        .alias("user_type")
    )

    # COALESCE(phone_full_number, CONCAT(phone_indicator, phone_number))
    user_phone_full_number_expr = (
        pl.when(pl.col("phone_full_number").is_not_null())
        .then(pl.col("phone_full_number").cast(pl.String))
        .otherwise(
            (
                pl.col("phone_indicator").fill_null("").cast(pl.String)
                + pl.col("phone_number").fill_null("").cast(pl.String)
            )
        )
        .alias("user_phone_full_number")
    )

    # CONCAT_WS(' ', name, last_name, mothers_last_name)
    user_full_name_expr = pl.concat_str(
        [pl.col("name"), pl.col("last_name"), pl.col("mothers_last_name")],
        separator=" ",
    ).alias("user_full_name")

    # Maintain all original columns from bronze and add calculated ones
    # This preserves all original column names (id, name, last_name, etc.)
    # and only adds new calculated columns (user_* prefixed ones)
    return df.with_columns([
        # Add new calculated columns (do NOT replace original columns)
        # These are additional columns, not replacements
        user_full_name_expr,
        user_phone_full_number_expr,
        user_person_type_expr,
        user_industria_role_expr,
        user_type_id_expr,
        user_type_expr,

        # COALESCE(person_type, 0) AS user_person_type_id (new column)
        pl.col("person_type").fill_null(0).alias("user_person_type_id"),

        # COALESCE(tenant_type, 0) AS user_industria_role_id (new column)
        pl.col("tenant_type").fill_null(0).alias("user_industria_role_id"),

        # Date columns (derived from timestamps - new columns)
        pl.col("created_at").dt.date().alias("user_profile_created_date"),
        pl.col("updated_at").dt.date().alias("user_profile_updated_date"),
    ])


def _transform_s2p_profiles_with_dedupe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Deduplicates by user_id (most recent profile per user), then applies _transform_s2p_profiles.
    Equivalent to ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) = 1.
    """
    earliest_profiles = (
        df.sort(["user_id", "created_at"], descending=[False, True])
        .group_by("user_id", maintain_order=True)
        .head(1)
    )
    return _transform_s2p_profiles(earliest_profiles)


stg_s2p_profiles_new = make_silver_stg_asset(
    "raw_s2p_profiles_new",
    _transform_s2p_profiles_with_dedupe,
    tags=TAGS_LK_SPOTS_SILVER,
    description=(
        "Silver STG: Complete transformation of profiles from Spot2 Platform "
        "using reusable functions. Gets most recent profile per user. "
        "Maintains original column names and adds calculated fields."
    ),
    allow_row_loss=True,  # Expected: one row per user after deduplication
)
