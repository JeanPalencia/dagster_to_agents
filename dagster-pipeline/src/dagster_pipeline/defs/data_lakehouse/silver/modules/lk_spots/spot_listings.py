"""Reusable module: listings per spot (ranking by availability, one row per spot)."""
from __future__ import annotations

import polars as pl


def build_spot_listings_filtered(
    listings_spots: pl.DataFrame,
    listings: pl.DataFrame,
) -> pl.DataFrame:
    """
    Join listings_spots + listings, derived columns (status, hierarchy, is_listing),
    ranking by spot_id (prefer status_id=1, then by listing_created_at), one row per spot.

    SQL legacy: FROM listings_spots ls LEFT JOIN listings l ON ... WHERE l.deleted_at IS NULL.
    Similar: filter listings before join (exclude deleted and orphaned listings without match).
    """
    lid_col = "listing_id" if "listing_id" in listings_spots.columns else ([c for c in listings_spots.columns if "list" in c.lower()][0] if listings_spots.width > 0 else "listing_id")
    l_id = "id" if "id" in listings.columns else "id"

    # Bug 2 fix: filter listings BEFORE join; guard if deleted_at column does not exist.
    listings_active = listings.filter(pl.col("deleted_at").is_null()) if "deleted_at" in listings.columns else listings
    spot_listings = listings_spots.join(
        listings_active,
        left_on=lid_col,
        right_on=l_id,
        how="inner",
        suffix="_l",
    )

    rep_status = pl.col("status_id_l") if "status_id_l" in spot_listings.columns else pl.col("status_id")
    ls_status = pl.col("status_id") if "status_id" in spot_listings.columns else pl.lit(0)
    hierarchy_col = pl.col("hierarchy") if "hierarchy" in spot_listings.columns else pl.lit(0)

    # Bug 3 fix: legacy uses ls.created_at (listings_spots). Require column; no fallback to listings.
    if "created_at" not in listings_spots.columns:
        raise ValueError("listings_spots missing 'created_at' — required for listing ranking")
    created_col = pl.col("created_at")

    spot_listings = spot_listings.with_columns([
        pl.when(rep_status == 1).then(1).when(rep_status == 2).then(2).when(rep_status == 3).then(3).otherwise(0).alias("spot_listing_representative_status_id"),
        pl.when(rep_status == 1).then(pl.lit("Available")).when(rep_status == 2).then(pl.lit("Unavailable")).when(rep_status == 3).then(pl.lit("Deleted")).otherwise(pl.lit("Not applicable")).alias("spot_listing_representative_status"),
        pl.when(ls_status == 1).then(1).when(ls_status == 2).then(2).when(ls_status == 3).then(3).otherwise(0).alias("spot_listing_status_id"),
        pl.when(ls_status == 1).then(pl.lit("Created")).when(ls_status == 2).then(pl.lit("Verified")).when(ls_status == 3).then(pl.lit("Published")).otherwise(pl.lit("Unknown")).alias("spot_listing_status"),
        hierarchy_col.alias("spot_listing_hierarchy"),
        pl.when(hierarchy_col == 1).then(1).otherwise(0).alias("spot_is_listing_id"),
        pl.when(hierarchy_col == 1).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("spot_is_listing"),
        created_col.alias("listing_created_at"),
    ])

    sid_col = "spot_id" if "spot_id" in spot_listings.columns else (([c for c in spot_listings.columns if "spot" in c.lower() and "id" in c.lower()] or ["spot_id"])[0])
    select_exprs = [pl.col(sid_col).alias("spot_id")]
    if lid_col in spot_listings.columns:
        select_exprs.append(pl.col(lid_col).alias("spot_listing_id"))
    else:
        select_exprs.append(pl.lit(None).cast(pl.Int64).alias("spot_listing_id"))
    for c in ["spot_listing_representative_status_id", "spot_listing_representative_status", "spot_listing_status_id", "spot_listing_status", "spot_listing_hierarchy", "spot_is_listing_id", "spot_is_listing", "listing_created_at"]:
        if c in spot_listings.columns:
            select_exprs.append(pl.col(c))
    spot_listings = spot_listings.select(select_exprs)

    # Bug 1 fix: sort + group_by + head(1) — exact legacy order.
    spot_listings_filtered = (
        spot_listings
        .with_columns((pl.col("spot_listing_status_id") == 1).cast(pl.Int64).alias("_ord"))
        .sort(
            ["spot_id", "_ord", "listing_created_at", "spot_listing_id"],
            descending=[False, True, True, True],
        )
        .group_by("spot_id", maintain_order=True)
        .head(1)
        .drop("_ord")
    )
    return spot_listings_filtered
