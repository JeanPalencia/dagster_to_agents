"""Módulo reutilizable: conteos de fotos por spot (spot_photo_count, spot_photo_platform_count)."""
import polars as pl


def build_spot_photos(photos: pl.DataFrame) -> pl.DataFrame:
    """
    Groups by photoable_id with filters (Spot, not social, type, visible, not deleted).
    Devuelve spot_photo_count y spot_photo_platform_count por photo_spot_id.
    """
    if photos.height == 0 or "photoable_id" not in photos.columns:
        return pl.DataFrame(schema={"photo_spot_id": pl.Int64, "spot_photo_count": pl.UInt32, "spot_photo_platform_count": pl.UInt32})
    # Replica LIKE 'App\\Models\\Spot' del SQL legacy — filtra por el tipo exacto del modelo.
    # str.ends_with evita falsos positivos de tipos como SpotReport o SpotRanking.
    photoable_type_ok = pl.col("photoable_type").str.ends_with("Spot") if "photoable_type" in photos.columns else pl.lit(True)
    social_ok = pl.col("social_type").is_null() if "social_type" in photos.columns else pl.lit(True)
    type_ok = (pl.col("type") != 2) if "type" in photos.columns else pl.lit(True)
    visible_ok = (pl.col("is_visible") == True) if "is_visible" in photos.columns else pl.lit(True)
    del_ok = pl.col("deleted_at").is_null() if "deleted_at" in photos.columns else pl.lit(True)
    spot_photos = photos.group_by("photoable_id").agg([
        pl.col("id").count().alias("spot_photo_count"),
        (photoable_type_ok & social_ok & type_ok & visible_ok & del_ok).sum().alias("spot_photo_platform_count"),
    ]).rename({"photoable_id": "photo_spot_id"})
    return spot_photos
