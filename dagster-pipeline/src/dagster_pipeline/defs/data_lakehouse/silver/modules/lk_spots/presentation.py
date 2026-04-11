"""
Presentation layer: large join of spots_prices with dimensions (zip, states, contacts, users, etc.)
and construction of final output (out_cols with alias spot_*, contact_*, user_*, ID translation).
"""
import polars as pl

from dagster_pipeline.defs.data_lakehouse.silver.utils import case_when, expr_mysql_numeric_int64, expr_spot2_internal_email
from dagster_pipeline.defs.data_lakehouse.silver.modules.lk_spots.dictionaries import (
    SPOT_SECTOR_MAP,
    SPOT_TYPE_MAP,
    CONTACT_SUBGROUP_MAP,
    CONTACT_CATEGORY_MAP,
    USER_INDUSTRIA_ROLE_MAP,
    USER_LEVEL_MAP,
    SPOT_QUALITY_MAP,
    SPOT_CLASS_MAP,
    SPOT_CONDITION_MAP,
    SPOT_ORIGIN_MAP,
)


def _safe_drop(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    existing = [c for c in cols if c in df.columns]
    return df.drop(existing) if existing else df


def build_final_results(
    spots_prices: pl.DataFrame,
    zip_codes: pl.DataFrame,
    stg_s2p_states_new: pl.DataFrame,
    stg_s2p_data_states_new: pl.DataFrame,
    stg_s2p_data_municipalities_new: pl.DataFrame,
    stg_s2p_cities_new: pl.DataFrame,
    stg_s2p_data_settlements_new: pl.DataFrame,
    stg_s2p_zones_new: pl.DataFrame,
    stg_s2p_contacts_new: pl.DataFrame,
    stg_s2p_users_new: pl.DataFrame,
    stg_s2p_profiles_new: pl.DataFrame,
    stg_s2p_model_has_roles_new: pl.DataFrame,
    stg_s2p_spot_rankings_new: pl.DataFrame,
    spot_reports_summary: pl.DataFrame,
    spot_listings_filtered: pl.DataFrame,
    spot_photos: pl.DataFrame,
) -> pl.DataFrame:
    """
    Joins spots_prices with all dimensions and builds the final DataFrame
    with columns spot_*, contact_*, user_*, etc. (stg_lk_spots_new schema).
    """
    base = spots_prices
    id_col = "id" if "id" in base.columns else "spot_id"

    zc_select = [pl.col("id").alias("_zc_id")]
    if "code" in zip_codes.columns:
        zc_select.append(pl.col("code").alias("zip_c_code"))
    else:
        zc_select.append(pl.lit(None).cast(pl.Utf8).alias("zip_c_code"))
    for src, alias in [("settlement", "zip_c_spot_settlement"), ("settlement_type", "zip_c_spot_settlement_type"), ("zone", "zip_c_spot_zone_type"), ("municipality", "zip_c_municipality")]:
        if src in zip_codes.columns:
            zc_select.append(pl.col(src).alias(alias))
        else:
            zc_select.append(pl.lit(None).cast(pl.Utf8).alias(alias))
    if "state_id" in zip_codes.columns:
        zc_select.append(pl.col("state_id").alias("zip_c_state_id"))
    else:
        zc_select.append(pl.lit(None).cast(pl.Int64).alias("zip_c_state_id"))
    zip_c = zip_codes.select(zc_select)
    base = base.join(zip_c, left_on="zip_code_id_real", right_on="_zc_id", how="left")
    base = _safe_drop(base, ["_zc_id"])

    states_df = stg_s2p_states_new
    if states_df.height > 0 and "id" in states_df.columns and "name" in states_df.columns:
        states_c = states_df.select(pl.col("id").alias("_states_c_id"), pl.col("name").alias("states_c_name"))
        base = base.join(states_c, left_on="zip_c_state_id", right_on="_states_c_id", how="left")
        base = _safe_drop(base, ["_states_c_id"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Utf8).alias("states_c_name"))

    ds = stg_s2p_data_states_new
    if ds.height > 0 and "id" in ds.columns and "name" in ds.columns:
        ds_sel = ds.select(pl.col("id").alias("_ds_id"), pl.col("name").alias("data_state_name"))
        base = base.join(ds_sel, left_on="state_id_real2", right_on="_ds_id", how="left")
        base = _safe_drop(base, ["_ds_id"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Utf8).alias("data_state_name"))
    st = stg_s2p_states_new
    if st.height > 0 and "id" in st.columns and "name" in st.columns:
        st_sel = st.select(pl.col("id").alias("_st_id"), pl.col("name").alias("state_name"))
        base = base.join(st_sel, left_on="state_id_real2", right_on="_st_id", how="left")
        base = _safe_drop(base, ["_st_id"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Utf8).alias("state_name"))
    dm = stg_s2p_data_municipalities_new
    if dm.height > 0 and "id" in dm.columns and "name" in dm.columns:
        dm_sel = dm.select(pl.col("id").alias("_dm_id"), pl.col("name").alias("data_municipality_name"))
        base = base.join(dm_sel, left_on="data_municipality_id_real", right_on="_dm_id", how="left")
        base = _safe_drop(base, ["_dm_id"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Utf8).alias("data_municipality_name"))
    cities = stg_s2p_cities_new
    if cities.height > 0 and "id" in cities.columns and "name" in cities.columns:
        cities_sel = cities.select(pl.col("id").alias("_city_id"), pl.col("name").alias("city_name"))
        base = base.join(cities_sel, left_on="city_id_real", right_on="_city_id", how="left")
        base = _safe_drop(base, ["_city_id"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Utf8).alias("city_name"))
    dset = stg_s2p_data_settlements_new
    if dset.height > 0 and "id" in dset.columns and "name" in dset.columns:
        dset_sel = dset.select(pl.col("id").alias("_dset_id"), pl.col("name").alias("data_settlement_name"))
        base = base.join(dset_sel, left_on="settlement_id_real", right_on="_dset_id", how="left")
        base = _safe_drop(base, ["_dset_id"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Utf8).alias("data_settlement_name"))
    zones_df = stg_s2p_zones_new
    if zones_df.height > 0 and "id" in zones_df.columns and "name" in zones_df.columns:
        zones_sel = zones_df.select(pl.col("id").alias("_zone_id"), pl.col("name").alias("zone_name"))
        base = base.join(zones_sel, left_on="zone_id_real", right_on="_zone_id", how="left")
        base = _safe_drop(base, ["_zone_id"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Utf8).alias("zone_name"))
    if zones_df.height > 0 and "id" in zones_df.columns and "name" in zones_df.columns:
        nearest_sel = zones_df.select(pl.col("id").alias("_nz_id"), pl.col("name").alias("nearest_zone_name"))
        base = base.join(nearest_sel, left_on="nearest_zone_id_real", right_on="_nz_id", how="left")
        base = _safe_drop(base, ["_nz_id"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Utf8).alias("nearest_zone_name"))

    contacts_df = stg_s2p_contacts_new
    if contacts_df.height > 0 and "id" in contacts_df.columns:
        contact_cols = [pl.col("id").alias("contact_id_join")]
        for c in ["email", "subgroup", "category", "company"]:
            if c in contacts_df.columns:
                contact_cols.append(pl.col(c).alias(f"contact_{c}"))
            else:
                contact_cols.append(pl.lit(None).cast(pl.Utf8).alias(f"contact_{c}"))
        contacts_sel = contacts_df.select(contact_cols)
        base = base.join(contacts_sel, left_on="contact_id_real", right_on="contact_id_join", how="left")
        base = _safe_drop(base, ["contact_id_join"])
    else:
        for c in ["contact_email", "contact_subgroup", "contact_category", "contact_company"]:
            base = base.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))
    users_df = stg_s2p_users_new
    _uid_key = "id" if users_df.height > 0 and "id" in users_df.columns else ("user_id" if users_df.height > 0 and "user_id" in users_df.columns else None)
    if users_df.height > 0 and _uid_key is not None:
        user_cols = [pl.col(_uid_key).alias("user_id_join")]
        _email_src = "email" if "email" in users_df.columns else ("user_email" if "user_email" in users_df.columns else None)
        user_cols.append(
            pl.col(_email_src).alias("user_email_raw")
            if _email_src
            else pl.lit(None).cast(pl.Utf8).alias("user_email_raw")
        )
        user_cols.append(
            expr_mysql_numeric_int64("level_id").alias("user_level_id_raw")
            if "level_id" in users_df.columns
            else pl.lit(None).cast(pl.Int64).alias("user_level_id_raw")
        )
        users_sel = users_df.select(user_cols)
        base = base.join(users_sel, left_on="user_id", right_on="user_id_join", how="left")
        base = _safe_drop(base, ["user_id_join"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Utf8).alias("user_email_raw"), pl.lit(None).cast(pl.Int64).alias("user_level_id_raw"))
    profiles_df = stg_s2p_profiles_new
    if profiles_df.height > 0 and "user_id" in profiles_df.columns:
        prof_cols = [pl.col("user_id").alias("profile_user_id")]
        prof_cols.append(pl.col("id").alias("user_profile_id") if "id" in profiles_df.columns else pl.lit(None).cast(pl.Int64).alias("user_profile_id"))
        prof_cols.append(pl.col("tenant_type").alias("profile_tenant_type") if "tenant_type" in profiles_df.columns else pl.lit(None).cast(pl.Int64).alias("profile_tenant_type"))
        profiles_sel = profiles_df.select(prof_cols)
        base = base.join(profiles_sel, left_on="user_id", right_on="profile_user_id", how="left")
        base = _safe_drop(base, ["profile_user_id"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Int64).alias("user_profile_id"), pl.lit(None).cast(pl.Int64).alias("profile_tenant_type"))

    mhr_df = stg_s2p_model_has_roles_new
    if mhr_df.height > 0 and "model_id" in mhr_df.columns and "max_role" in mhr_df.columns:
        mhr_sel = mhr_df.select(
            pl.col("model_id").alias("user_id_join_mhr"),
            pl.col("max_role").alias("mhr_max_role"),
        )
        base = base.join(mhr_sel, left_on="user_id", right_on="user_id_join_mhr", how="left")
        base = _safe_drop(base, ["user_id_join_mhr"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Int64).alias("mhr_max_role"))

    spot_rankings_df = stg_s2p_spot_rankings_new
    if spot_rankings_df.height > 0 and "spot_id" in spot_rankings_df.columns and "score" in spot_rankings_df.columns:
        created_rn = "created_at" if "created_at" in spot_rankings_df.columns else spot_rankings_df.columns[0]
        sp_scores = (
            spot_rankings_df.sort(created_rn, descending=True)
            .with_columns(pl.lit(1).alias("_one"))
            .with_columns(pl.cum_count("_one").over("spot_id").alias("_rn"))
            .filter(pl.col("_rn") == 1)
            .drop(["_one", "_rn"])
            .select(pl.col("spot_id").alias("_sp_spot_id"), pl.col("score").alias("spot_score_join"))
        )
        base = base.join(sp_scores, left_on=id_col, right_on="_sp_spot_id", how="left")
        base = _safe_drop(base, ["_sp_spot_id"])
    else:
        base = base.with_columns(pl.lit(None).cast(pl.Float64).alias("spot_score_join"))

    sr_renamed = spot_reports_summary.rename({"spot_id": "_sr_id"})
    base = base.join(sr_renamed, left_on=id_col, right_on="_sr_id", how="left")
    base = _safe_drop(base, ["_sr_id"])
    if spot_listings_filtered.width > 0:
        sl_renamed = spot_listings_filtered.rename({"spot_id": "_sl_id"})
        base = base.join(sl_renamed, left_on=id_col, right_on="_sl_id", how="left")
        base = _safe_drop(base, ["_sl_id"])
    if spot_photos.width > 0:
        base = base.join(spot_photos, left_on=id_col, right_on="photo_spot_id", how="left")
        parent_phs = spot_photos.rename({
            "photo_spot_id": "_pp_id",
            "spot_photo_count": "spot_parent_photo_count",
            "spot_photo_platform_count": "spot_parent_photo_platform_count",
        })
        if "parent_id" in base.columns:
            base = base.join(parent_phs, left_on="parent_id", right_on="_pp_id", how="left")
            base = _safe_drop(base, ["_pp_id"])

    def _addr_part() -> pl.Expr:
        st = "street_real" if "street_real" in base.columns else "street"
        ex = "ext_number_real" if "ext_number_real" in base.columns else "ext_number"
        a = pl.when(pl.col(st).is_not_null() & pl.col(ex).is_not_null()).then(pl.col(st) + pl.lit(" ") + pl.col(ex).cast(pl.Utf8))
        a = a.when(pl.col(st).is_not_null()).then(pl.col(st))
        a = a.when(pl.col(ex).is_not_null()).then(pl.col(ex).cast(pl.Utf8))
        a = a.otherwise(pl.lit(None))
        return a
    city_or_zip = pl.coalesce(pl.col("city_name"), pl.col("zip_c_municipality")) if "city_name" in base.columns else (pl.col("zip_c_municipality") if "zip_c_municipality" in base.columns else pl.lit(None))
    cp_part = pl.when(pl.col("zip_c_code").is_not_null()).then(pl.lit("CP. ") + pl.col("zip_c_code").cast(pl.Utf8)).otherwise(pl.lit(None)) if "zip_c_code" in base.columns else pl.lit(None)
    spot_address_expr = pl.concat_str([_addr_part(), city_or_zip, pl.col("state_name") if "state_name" in base.columns else pl.lit(None), cp_part], separator=", ")

    _region_id = pl.when(pl.col("zip_c_state_id").is_in([7, 15, 12, 13, 17, 21, 29])).then(1)
    _region_id = _region_id.when(pl.col("zip_c_state_id").is_in([2, 3, 6, 8, 19, 26, 28])).then(2)
    _region_id = _region_id.when(pl.col("zip_c_state_id").is_in([1, 11, 22, 24, 32])).then(3)
    _region_id = _region_id.when(pl.col("zip_c_state_id").is_in([4, 5, 20, 23, 27, 30, 31])).then(4)
    _region_id = _region_id.when(pl.col("zip_c_state_id").is_in([9, 10, 14, 16, 18, 25])).then(5)
    _region_id = _region_id.otherwise(pl.lit(None))
    _region = pl.when(pl.col("zip_c_state_id").is_in([7, 15, 12, 13, 17, 21, 29])).then(pl.lit("Center"))
    _region = _region.when(pl.col("zip_c_state_id").is_in([2, 3, 6, 8, 19, 26, 28])).then(pl.lit("North"))
    _region = _region.when(pl.col("zip_c_state_id").is_in([1, 11, 22, 24, 32])).then(pl.lit("Shallows"))
    _region = _region.when(pl.col("zip_c_state_id").is_in([4, 5, 20, 23, 27, 30, 31])).then(pl.lit("Southeast"))
    _region = _region.when(pl.col("zip_c_state_id").is_in([9, 10, 14, 16, 18, 25])).then(pl.lit("West"))
    _region = _region.otherwise(pl.lit(None))

    settlement_type_map = {"Ranchería": 1, "Colonia": 2, "Fraccionamiento": 3, "Pueblo": 4, "Ejido": 5, "Rancho": 6, "Barrio": 7, "Unidad habitacional": 8, "Congregación": 9, "Condominio": 10, "Zona industrial": 11, "Poblado comunal": 12, "Granja": 13, "Conjunto habitacional": 14, "Hacienda": 15, "Equipamiento": 16, "Finca": 17, "Zona comercial": 18, "Paraje": 19, "Residencial": 20, "Ampliación": 21, "Aeropuerto": 22, "Zona federal": 23, "Exhacienda": 24, "Gran usuario": 25, "Parque industrial": 26, "Estación": 27, "Villa": 28, "Campamento": 29, "Puerto": 30, "Zona militar": 31, "Zona naval": 32}
    settlement_type_en = [("Ranchería", "Hamlet"), ("Colonia", "Neighborhood"), ("Fraccionamiento", "Residential subdivision"), ("Pueblo", "Village"), ("Ejido", "Ejido"), ("Rancho", "Ranch"), ("Barrio", "Quarter"), ("Unidad habitacional", "Housing complex"), ("Congregación", "Settlement (congregación)"), ("Condominio", "Condominium"), ("Zona industrial", "Industrial zone"), ("Poblado comunal", "Communal settlement"), ("Granja", "Farm"), ("Conjunto habitacional", "Residential complex"), ("Hacienda", "Hacienda"), ("Equipamiento", "Public facilities area"), ("Finca", "Country estate"), ("Zona comercial", "Commercial zone"), ("Paraje", "Locality (paraje)"), ("Residencial", "Residential area"), ("Ampliación", "Extension (ampliación)"), ("Aeropuerto", "Airport"), ("Zona federal", "Federal zone"), ("Exhacienda", "Former hacienda"), ("Gran usuario", "Large user facility"), ("Parque industrial", "Industrial park"), ("Estación", "Station"), ("Villa", "Town (villa)"), ("Campamento", "Camp"), ("Puerto", "Port"), ("Zona militar", "Military zone"), ("Zona naval", "Naval zone")]

    out_cols = [
        pl.col(id_col).alias("spot_id"),
        (pl.lit("https://platform.spot2.mx/admin/spots/") + pl.col(id_col).cast(pl.Utf8)).alias("spot_link"),
        (pl.lit("https://spot2.mx/spots/") + pl.col(id_col).cast(pl.Utf8)).alias("spot_public_link"),
    ]
    for c in ["spot_parent_status_id", "spot_parent_status", "spot_parent_status_reason_id", "spot_parent_status_reason", "spot_parent_status_full_id", "spot_parent_status_full", "spot_status_id", "spot_status", "spot_status_reason_id", "spot_status_reason", "spot_status_full_id", "spot_status_full"]:
        if c in base.columns:
            out_cols.append(pl.col(c))
    spot_type_id_real = "spot_type_id_real" if "spot_type_id_real" in base.columns else "spot_type_id"
    if spot_type_id_real in base.columns:
        out_cols.append(pl.col(spot_type_id_real).alias("spot_sector_id"))
        out_cols.append(case_when(spot_type_id_real, SPOT_SECTOR_MAP, default=None).alias("spot_sector"))
    if "source_table_id" in base.columns:
        out_cols.append(pl.col("source_table_id").alias("spot_type_id"))
        out_cols.append(case_when("source_table_id", SPOT_TYPE_MAP, default=None).alias("spot_type"))
    if "is_complex" in base.columns:
        _ic = pl.col("is_complex")
        out_cols.append(
            pl.when(_ic.is_null())
            .then(pl.lit(None).cast(pl.Int64))
            .otherwise(_ic.cast(pl.Int64, strict=False))
            .alias("spot_is_complex")
        )
    else:
        out_cols.append(pl.lit(None).cast(pl.Int64).alias("spot_is_complex"))
    if "parent_id" in base.columns:
        _pid = pl.col("parent_id")
        out_cols.append(
            pl.when(_pid.is_null())
            .then(pl.lit(None).cast(pl.Utf8))
            .otherwise(
                pl.coalesce(
                    _pid.cast(pl.Int64, strict=False).cast(pl.Utf8),
                    _pid.cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).cast(pl.Utf8),
                    _pid.cast(pl.Utf8, strict=False),
                )
            )
            .alias("spot_parent_id")
        )
    else:
        out_cols.append(pl.lit(None).cast(pl.Utf8).alias("spot_parent_id"))
    if "name" in base.columns:
        out_cols.append(pl.col("name").alias("spot_title"))
    if "description" in base.columns:
        out_cols.append(pl.col("description").alias("spot_description"))
    out_cols.append(spot_address_expr.alias("spot_address"))
    out_cols.append(pl.col("street_real").alias("spot_street") if "street_real" in base.columns else pl.col("street").alias("spot_street") if "street" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_street"))
    out_cols.append(pl.col("ext_number_real").alias("spot_ext_number") if "ext_number_real" in base.columns else pl.col("ext_number").alias("spot_ext_number") if "ext_number" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_ext_number"))
    out_cols.append(pl.col("int_number").alias("spot_int_number") if "int_number" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_int_number"))
    out_cols.append(pl.col("zip_c_spot_settlement").alias("spot_settlement") if "zip_c_spot_settlement" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_settlement"))
    out_cols.append(pl.col("settlement_id_real").alias("spot_data_settlement_id") if "settlement_id_real" in base.columns else pl.lit(None).cast(pl.Int64).alias("spot_data_settlement_id"))
    out_cols.append(pl.col("data_settlement_name").alias("spot_data_settlement") if "data_settlement_name" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_data_settlement"))
    out_cols.append(pl.col("city_id_real").alias("spot_municipality_id") if "city_id_real" in base.columns else pl.lit(None).cast(pl.Int64).alias("spot_municipality_id"))
    out_cols.append(pl.coalesce(pl.col("city_name"), pl.col("zip_c_municipality")).alias("spot_municipality") if "city_name" in base.columns or "zip_c_municipality" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_municipality"))
    out_cols.append(pl.col("data_municipality_id_real").alias("spot_data_municipality_id") if "data_municipality_id_real" in base.columns else pl.lit(None).cast(pl.Int64).alias("spot_data_municipality_id"))
    out_cols.append(pl.col("data_municipality_name").alias("spot_data_municipality") if "data_municipality_name" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_data_municipality"))
    out_cols.append(pl.col("state_id_real2").alias("spot_state_id") if "state_id_real2" in base.columns else pl.lit(None).cast(pl.Int64).alias("spot_state_id"))
    out_cols.append(pl.col("state_name").alias("spot_state") if "state_name" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_state"))
    out_cols.append(pl.col("data_state_name").alias("spot_data_state") if "data_state_name" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_data_state"))
    out_cols.append(_region_id.alias("spot_region_id"))
    out_cols.append(_region.alias("spot_region"))
    out_cols.append(pl.col("zone_id_real").alias("spot_corridor_id") if "zone_id_real" in base.columns else pl.lit(None).cast(pl.Int64).alias("spot_corridor_id"))
    out_cols.append(pl.col("zone_name").alias("spot_corridor") if "zone_name" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_corridor"))
    out_cols.append(pl.col("nearest_zone_id_real").alias("spot_nearest_corridor_id") if "nearest_zone_id_real" in base.columns else pl.lit(None).cast(pl.Int64).alias("spot_nearest_corridor_id"))
    out_cols.append(pl.col("nearest_zone_name").alias("spot_nearest_corridor") if "nearest_zone_name" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_nearest_corridor"))
    out_cols.append(pl.col("latitude_real").alias("spot_latitude") if "latitude_real" in base.columns else pl.col("latitude").alias("spot_latitude") if "latitude" in base.columns else pl.lit(None).cast(pl.Float64).alias("spot_latitude"))
    out_cols.append(pl.col("longitude_real").alias("spot_longitude") if "longitude_real" in base.columns else pl.col("longitude").alias("spot_longitude") if "longitude" in base.columns else pl.lit(None).cast(pl.Float64).alias("spot_longitude"))
    out_cols.append(pl.col("zip_code_id_real").alias("spot_zip_code_id") if "zip_code_id_real" in base.columns else pl.lit(None).cast(pl.Int64).alias("spot_zip_code_id"))
    out_cols.append(pl.col("zip_c_code").alias("spot_zip_code") if "zip_c_code" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_zip_code"))
    out_cols.append(pl.col("zip_c_municipality").alias("spot_municipality_zip_code") if "zip_c_municipality" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_municipality_zip_code"))
    out_cols.append(pl.col("states_c_name").alias("spot_state_zip_code") if "states_c_name" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_state_zip_code"))

    if "zip_c_spot_settlement_type" in base.columns:
        st_trim = pl.col("zip_c_spot_settlement_type").str.strip_chars()
        st_id = pl.lit(0)
        for k, v in settlement_type_map.items():
            st_id = pl.when(st_trim == k).then(v).otherwise(st_id)
        out_cols.append(st_id.alias("spot_settlement_type_id"))
        out_cols.append(pl.when(st_trim.is_not_null()).then(st_trim).otherwise(pl.lit("Unknown")).alias("spot_settlement_type"))
        st_en = pl.lit("Unknown")
        for k, en in settlement_type_en:
            st_en = pl.when(st_trim == k).then(pl.lit(en)).otherwise(st_en)
        out_cols.append(st_en.alias("spot_settlement_type_en"))
    else:
        out_cols.extend([pl.lit(0).alias("spot_settlement_type_id"), pl.lit("Unknown").alias("spot_settlement_type"), pl.lit("Unknown").alias("spot_settlement_type_en")])
    if "zip_c_spot_zone_type" in base.columns:
        zt_trim = pl.col("zip_c_spot_zone_type").str.strip_chars()
        out_cols.append(pl.when(zt_trim == "Rural").then(1).when(zt_trim == "Urbano").then(2).when(zt_trim == "Semiurbano").then(3).otherwise(0).alias("spot_zone_type_id"))
        out_cols.append(pl.when(zt_trim.is_not_null()).then(zt_trim).otherwise(pl.lit("Unknown")).alias("spot_zone_type"))
        out_cols.append(pl.when(zt_trim == "Rural").then(pl.lit("Rural")).when(zt_trim == "Urbano").then(pl.lit("Urban")).when(zt_trim == "Semiurbano").then(pl.lit("Semi-urban")).otherwise(pl.lit("Unknown")).alias("spot_zone_type_en"))
    else:
        out_cols.extend([pl.lit(0).alias("spot_zone_type_id"), pl.lit("Unknown").alias("spot_zone_type"), pl.lit("Unknown").alias("spot_zone_type_en")])

    listing_cols = ["spot_listing_id", "spot_listing_representative_status_id", "spot_listing_representative_status", "spot_listing_status_id", "spot_listing_status", "spot_listing_hierarchy", "spot_is_listing_id", "spot_is_listing"]
    for c in listing_cols:
        if c in base.columns:
            out_cols.append(pl.col(c))
    if "square_space" in base.columns:
        out_cols.append(pl.col("square_space").alias("spot_area_in_sqm"))
    price_cols = [c for c in base.columns if (c.startswith("spot_price") or c.startswith("spot_modality") or c.startswith("spot_currency") or c.startswith("spot_sub_") or c in ("spot_maintenance_cost_mxn", "spot_maintenance_cost_usd")) and c != "spot_listing_id"]
    for c in price_cols:
        out_cols.append(pl.col(c))
    out_cols.append(pl.col("contact_id_real").alias("contact_id") if "contact_id_real" in base.columns else pl.lit(None).cast(pl.Int64).alias("contact_id"))
    if "contact_email" in base.columns:
        out_cols.append(pl.col("contact_email"))
        out_cols.append(pl.col("contact_email").str.to_lowercase().str.strip_chars().str.split("@").list.get(-1).alias("contact_domain"))
    else:
        out_cols.append(pl.lit(None).cast(pl.Utf8).alias("contact_email"))
        out_cols.append(pl.lit(None).cast(pl.Utf8).alias("contact_domain"))
    out_cols.append(pl.col("contact_subgroup").alias("contact_subgroup_id") if "contact_subgroup" in base.columns else pl.lit(None).cast(pl.Utf8).alias("contact_subgroup_id"))
    out_cols.append(case_when("contact_subgroup", CONTACT_SUBGROUP_MAP, default=None).alias("contact_subgroup") if "contact_subgroup" in base.columns else pl.lit(None).cast(pl.Utf8).alias("contact_subgroup"))
    out_cols.append(pl.col("contact_category").alias("contact_category_id") if "contact_category" in base.columns else pl.lit(None).cast(pl.Int64).alias("contact_category_id"))
    out_cols.append(case_when("contact_category", CONTACT_CATEGORY_MAP, default=None).alias("contact_category") if "contact_category" in base.columns else pl.lit(None).cast(pl.Utf8).alias("contact_category"))
    out_cols.append(pl.col("contact_company").alias("contact_company") if "contact_company" in base.columns else pl.lit(None).cast(pl.Utf8).alias("contact_company"))
    out_cols.append(pl.col("user_id").alias("user_id") if "user_id" in base.columns else pl.lit(None).cast(pl.Int64).alias("user_id"))
    out_cols.append(pl.col("user_profile_id").alias("user_profile_id") if "user_profile_id" in base.columns else pl.lit(None).cast(pl.Int64).alias("user_profile_id"))
    _mr_raw = (
        pl.col("mhr_max_role").cast(pl.Int64, strict=False)
        if "mhr_max_role" in base.columns
        else pl.lit(None).cast(pl.Int64)
    )
    out_cols.append(pl.coalesce(_mr_raw, pl.lit(0)).alias("user_max_role_id"))
    out_cols.append(
        pl.when(_mr_raw == 1)
        .then(pl.lit("Admin"))
        .when(_mr_raw == 2)
        .then(pl.lit("Tenant"))
        .when(_mr_raw == 3)
        .then(pl.lit("External Broker"))
        .when(_mr_raw == 4)
        .then(pl.lit("Landlord"))
        .when(_mr_raw == 5)
        .then(pl.lit("Internal Broker"))
        .otherwise(pl.lit("Unknown"))
        .alias("user_max_role")
    )
    out_cols.append(pl.coalesce(pl.col("profile_tenant_type").cast(pl.Int64), pl.lit(0)).alias("user_industria_role_id") if "profile_tenant_type" in base.columns else pl.lit(0).alias("user_industria_role_id"))
    out_cols.append(case_when("profile_tenant_type", USER_INDUSTRIA_ROLE_MAP, default="Unknown").alias("user_industria_role") if "profile_tenant_type" in base.columns else pl.lit("Unknown").alias("user_industria_role"))
    if "user_email_raw" in base.columns:
        ue = pl.col("user_email_raw").str.to_lowercase().str.strip_chars()
        out_cols.append(ue.alias("user_email"))
        out_cols.append(ue.str.split("@").list.get(-1).alias("user_domain"))
        _internal_user = expr_spot2_internal_email("user_email_raw")
        out_cols.append(pl.when(_internal_user).then(1).otherwise(0).alias("user_affiliation_id"))
        out_cols.append(pl.when(_internal_user).then(pl.lit("Internal User")).otherwise(pl.lit("External User")).alias("user_affiliation"))
    else:
        out_cols.extend([pl.lit(None).cast(pl.Utf8).alias("user_email"), pl.lit(None).cast(pl.Utf8).alias("user_domain"), pl.lit(0).alias("user_affiliation_id"), pl.lit("External User").alias("user_affiliation")])
    out_cols.append(pl.when(pl.col("user_level_id_raw").is_in([1, 2, 3])).then(pl.col("user_level_id_raw")).otherwise(0).alias("user_level_id") if "user_level_id_raw" in base.columns else pl.lit(0).alias("user_level_id"))
    out_cols.append(case_when("user_level_id_raw", USER_LEVEL_MAP, default="Others").alias("user_level") if "user_level_id_raw" in base.columns else pl.lit("Others").alias("user_level"))
    if "user_email_raw" in base.columns:
        ue = pl.col("user_email_raw").str.to_lowercase().str.strip_chars()
        out_cols.append(pl.when(ue.str.ends_with("@nextagents.mx")).then(1).otherwise(0).alias("user_broker_next_id"))
        out_cols.append(pl.when(ue.str.ends_with("@nextagents.mx")).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("user_broker_next"))
    else:
        out_cols.extend([pl.lit(0).alias("user_broker_next_id"), pl.lit("No").alias("user_broker_next")])
    out_cols.append(pl.col("external_id").alias("spot_external_id") if "external_id" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_external_id"))
    if "external_updated_at" in base.columns:
        out_cols.append(
            pl.col("external_updated_at").cast(pl.Datetime("us"), strict=False).alias("spot_external_updated_at")
        )
    else:
        out_cols.append(pl.lit(None).cast(pl.Datetime("us")).alias("spot_external_updated_at"))
    _qcol = "quality_state_real" if "quality_state_real" in base.columns else "quality_state"
    out_cols.append(pl.col(_qcol).alias("spot_quality_control_status_id") if _qcol in base.columns else pl.lit(None).cast(pl.Int64).alias("spot_quality_control_status_id"))
    out_cols.append(case_when(_qcol, SPOT_QUALITY_MAP, default=None).alias("spot_quality_control_status") if _qcol in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_quality_control_status"))
    out_cols.append(pl.coalesce(pl.col("spot_photo_count").cast(pl.Int64), pl.lit(0)).alias("spot_photo_count") if "spot_photo_count" in base.columns else pl.lit(0).alias("spot_photo_count"))
    out_cols.append(pl.coalesce(pl.col("spot_photo_platform_count").cast(pl.Int64), pl.lit(0)).alias("spot_photo_platform_count") if "spot_photo_platform_count" in base.columns else pl.lit(0).alias("spot_photo_platform_count"))
    out_cols.append(pl.coalesce(pl.col("spot_parent_photo_count").cast(pl.Int64), pl.lit(0)).alias("spot_parent_photo_count") if "spot_parent_photo_count" in base.columns else pl.lit(0).alias("spot_parent_photo_count"))
    out_cols.append(pl.coalesce(pl.col("spot_parent_photo_platform_count").cast(pl.Int64), pl.lit(0)).alias("spot_parent_photo_platform_count") if "spot_parent_photo_platform_count" in base.columns else pl.lit(0).alias("spot_parent_photo_platform_count"))
    if "spot_photo_count" in base.columns:
        _photo_cnt = pl.coalesce(pl.col("spot_photo_count").cast(pl.Int64), pl.lit(0))
        _parent_photo_cnt = pl.coalesce(pl.col("spot_parent_photo_count").cast(pl.Int64), pl.lit(0)) if "spot_parent_photo_count" in base.columns else pl.lit(0)
        out_cols.append(pl.when(_photo_cnt > 0).then(_photo_cnt).otherwise(_parent_photo_cnt).alias("spot_photo_effective_count"))
    else:
        out_cols.append(pl.lit(0).alias("spot_photo_effective_count"))
    if "spot_photo_platform_count" in base.columns:
        _platform_cnt = pl.coalesce(pl.col("spot_photo_platform_count").cast(pl.Int64), pl.lit(0))
        _parent_platform_cnt = pl.coalesce(pl.col("spot_parent_photo_platform_count").cast(pl.Int64), pl.lit(0)) if "spot_parent_photo_platform_count" in base.columns else pl.lit(0)
        out_cols.append(pl.when(_platform_cnt > 0).then(_platform_cnt).otherwise(_parent_platform_cnt).alias("spot_photo_platform_effective_count"))
    else:
        out_cols.append(pl.lit(0).alias("spot_photo_platform_effective_count"))
    out_cols.append(pl.col("class").alias("spot_class_id") if "class" in base.columns else pl.lit(None).cast(pl.Int64).alias("spot_class_id"))
    out_cols.append(case_when("class", SPOT_CLASS_MAP, default=None).alias("spot_class") if "class" in base.columns else pl.lit(None).cast(pl.Utf8).alias("spot_class"))
    if "parking_spaces" in base.columns:
        _pk = pl.col("parking_spaces").cast(pl.Float64, strict=False)
        out_cols.append(
            pl.when(_pk.is_null())
            .then(pl.lit(None).cast(pl.Int64))
            .otherwise(_pk.round(0).cast(pl.Int64, strict=False))
            .alias("spot_parking_spaces")
        )
    else:
        out_cols.append(pl.lit(None).cast(pl.Int64).alias("spot_parking_spaces"))
    out_cols.append(pl.col("parking_space_by_area_real").alias("spot_parking_space_by_area") if "parking_space_by_area_real" in base.columns else pl.col("parking_space_by_area").alias("spot_parking_space_by_area") if "parking_space_by_area" in base.columns else pl.lit(None).cast(pl.Float64).alias("spot_parking_space_by_area"))
    out_cols.append(pl.col("spot_condition").alias("spot_condition_id") if "spot_condition" in base.columns else pl.lit(None).cast(pl.Int64).alias("spot_condition_id"))
    out_cols.append(case_when("spot_condition", SPOT_CONDITION_MAP, default="Unknown").alias("spot_condition") if "spot_condition" in base.columns else pl.lit("Unknown").alias("spot_condition"))
    out_cols.append(pl.col("construction_date_real").alias("spot_construction_date") if "construction_date_real" in base.columns else pl.col("construction_date").alias("spot_construction_date") if "construction_date" in base.columns else pl.lit(None).cast(pl.Date).alias("spot_construction_date"))
    out_cols.append(pl.col("spot_score_join").alias("spot_score") if "spot_score_join" in base.columns else pl.lit(None).cast(pl.Float64).alias("spot_score"))
    out_cols.append(pl.coalesce(pl.col("is_exclusive").cast(pl.Int64), pl.lit(0)).alias("spot_exclusive_id") if "is_exclusive" in base.columns else pl.lit(0).alias("spot_exclusive_id"))
    out_cols.append(pl.when(pl.col("is_exclusive") == 1).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("spot_exclusive") if "is_exclusive" in base.columns else pl.lit("No").alias("spot_exclusive"))
    out_cols.append(pl.coalesce(pl.col("landlord_exclusive").cast(pl.Int64), pl.lit(0)).alias("spot_landlord_exclusive_id") if "landlord_exclusive" in base.columns else pl.lit(0).alias("spot_landlord_exclusive_id"))
    out_cols.append(pl.when(pl.col("landlord_exclusive") == 1).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("spot_landlord_exclusive") if "landlord_exclusive" in base.columns else pl.lit("No").alias("spot_landlord_exclusive"))

    st_real = pl.col(spot_type_id_real) if spot_type_id_real in base.columns else pl.lit(None)
    sq = pl.col("square_space") if "square_space" in base.columns else pl.lit(None)
    sp_rent = pl.col("spot_price_sqm_mxn_rent") if "spot_price_sqm_mxn_rent" in base.columns else pl.lit(None)
    sp_sale = pl.col("spot_price_sqm_mxn_sale") if "spot_price_sqm_mxn_sale" in base.columns else pl.lit(None)
    sp_maint = pl.col("spot_maintenance_cost_mxn") if "spot_maintenance_cost_mxn" in base.columns else pl.lit(None)
    src_tbl = pl.col("source_table_id") if "source_table_id" in base.columns else pl.lit(None)
    _area_ok_id = pl.when(sq.is_null()).then(pl.lit(None)).when((src_tbl.is_in([1, 3])) & (st_real == 11) & (sq >= 30) & (sq <= 2000)).then(1).when((src_tbl.is_in([1, 3])) & (st_real == 13) & (sq >= 20) & (sq <= 15000)).then(1).when((src_tbl.is_in([1, 3])) & (st_real == 9) & (sq >= 100) & (sq <= 50000)).then(1).when((src_tbl.is_in([1, 3])) & (st_real == 15) & (sq >= 200) & (sq <= 1000000)).then(1).when((src_tbl == 2) & (st_real == 11) & (sq >= 250) & (sq <= 25000)).then(1).when((src_tbl == 2) & (st_real == 13) & (sq >= 20) & (sq <= 20000)).then(1).when((src_tbl == 2) & (st_real == 9) & (sq >= 1000) & (sq <= 150000)).then(1).when((src_tbl == 2) & (st_real == 15) & (sq >= 430) & (sq <= 1394.26406871985)).then(1).otherwise(0)
    _area_ok = pl.when(sq.is_null()).then(pl.lit(None)).when((src_tbl.is_in([1, 3])) & (st_real == 11) & (sq >= 30) & (sq <= 2000)).then(pl.lit("Yes")).when((src_tbl.is_in([1, 3])) & (st_real == 13) & (sq >= 20) & (sq <= 15000)).then(pl.lit("Yes")).when((src_tbl.is_in([1, 3])) & (st_real == 9) & (sq >= 100) & (sq <= 50000)).then(pl.lit("Yes")).when((src_tbl.is_in([1, 3])) & (st_real == 15) & (sq >= 200) & (sq <= 1000000)).then(pl.lit("Yes")).when((src_tbl == 2) & (st_real == 11) & (sq >= 250) & (sq <= 25000)).then(pl.lit("Yes")).when((src_tbl == 2) & (st_real == 13) & (sq >= 20) & (sq <= 20000)).then(pl.lit("Yes")).when((src_tbl == 2) & (st_real == 9) & (sq >= 1000) & (sq <= 150000)).then(pl.lit("Yes")).when((src_tbl == 2) & (st_real == 15) & (sq >= 430) & (sq <= 1394.26406871985)).then(pl.lit("Yes")).otherwise(pl.lit("No"))
    out_cols.append(_area_ok_id.alias("spot_is_area_in_range_id"))
    out_cols.append(_area_ok.alias("spot_is_area_in_range"))
    _rent_ok_id = pl.when(sp_rent.is_null()).then(pl.lit(None)).when((st_real == 11) & (sp_rent >= 80) & (sp_rent <= 900)).then(1).when((st_real == 13) & (sp_rent >= 45) & (sp_rent <= 1200)).then(1).when((st_real == 9) & (sp_rent >= 50) & (sp_rent <= 300)).then(1).when((st_real == 15) & (sp_rent >= 50) & (sp_rent <= 250)).then(1).otherwise(0)
    out_cols.append(_rent_ok_id.alias("spot_is_rent_price_in_range_id"))
    out_cols.append(pl.when(sp_rent.is_null()).then(pl.lit(None)).when((st_real == 11) & (sp_rent >= 80) & (sp_rent <= 900)).then(pl.lit("Yes")).when((st_real == 13) & (sp_rent >= 45) & (sp_rent <= 1200)).then(pl.lit("Yes")).when((st_real == 9) & (sp_rent >= 50) & (sp_rent <= 300)).then(pl.lit("Yes")).when((st_real == 15) & (sp_rent >= 50) & (sp_rent <= 250)).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("spot_is_rent_price_in_range"))
    _sale_ok_id = pl.when(sp_sale.is_null()).then(pl.lit(None)).when((st_real == 11) & (sp_sale >= 12000) & (sp_sale <= 90000)).then(1).when((st_real == 13) & (sp_sale >= 6000) & (sp_sale <= 150000)).then(1).when((st_real == 9) & (sp_sale >= 5000) & (sp_sale <= 40000)).then(1).when((st_real == 15) & (sp_sale >= 100) & (sp_sale <= 250000)).then(1).otherwise(0)
    out_cols.append(_sale_ok_id.alias("spot_is_sale_price_in_range_id"))
    out_cols.append(pl.when(sp_sale.is_null()).then(pl.lit(None)).when((st_real == 11) & (sp_sale >= 12000) & (sp_sale <= 90000)).then(pl.lit("Yes")).when((st_real == 13) & (sp_sale >= 6000) & (sp_sale <= 150000)).then(pl.lit("Yes")).when((st_real == 9) & (sp_sale >= 5000) & (sp_sale <= 40000)).then(pl.lit("Yes")).when((st_real == 15) & (sp_sale >= 100) & (sp_sale <= 250000)).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("spot_is_sale_price_in_range"))
    _maint_ok_id = pl.when(sp_maint.is_null()).then(pl.lit(None)).when(st_real == 15).then(pl.lit(None)).when((st_real == 11) & (sp_maint >= 15) & (sp_maint <= 135)).then(1).when((st_real == 13) & (sp_maint >= 6) & (sp_maint <= 180)).then(1).when((st_real == 9) & (sp_maint >= 7) & (sp_maint <= 45)).then(1).otherwise(0)
    out_cols.append(_maint_ok_id.alias("spot_is_maintenance_price_in_range_id"))
    out_cols.append(pl.when(sp_maint.is_null()).then(pl.lit(None)).when(st_real == 15).then(pl.lit(None)).when((st_real == 11) & (sp_maint >= 15) & (sp_maint <= 135)).then(pl.lit("Yes")).when((st_real == 13) & (sp_maint >= 6) & (sp_maint <= 180)).then(pl.lit("Yes")).when((st_real == 9) & (sp_maint >= 7) & (sp_maint <= 45)).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("spot_is_maintenance_price_in_range"))
    out_cols.append(pl.coalesce(pl.col("origin").cast(pl.Int64), pl.lit(0)).alias("spot_origin_id") if "origin" in base.columns else pl.lit(0).alias("spot_origin_id"))
    out_cols.append(case_when("origin", SPOT_ORIGIN_MAP, default="Unknown").alias("spot_origin") if "origin" in base.columns else pl.lit("Unknown").alias("spot_origin"))
    # Replica COALESCE del legacy:
    #   COALESCE(sr.spot_last_active_report_reason_id, 0)
    #   COALESCE(sr.spot_last_active_report_reason, 'No active reports')
    #   COALESCE(sr.spot_has_active_report, 0)
    # Spots with no reports have NULL after the LEFT JOIN — coalesce same as in SQL.
    if "spot_last_active_report_reason_id" in base.columns:
        out_cols.append(pl.coalesce(pl.col("spot_last_active_report_reason_id").cast(pl.Int64), pl.lit(0)).alias("spot_last_active_report_reason_id"))
    else:
        out_cols.append(pl.lit(0).alias("spot_last_active_report_reason_id"))
    if "spot_last_active_report_reason" in base.columns:
        out_cols.append(pl.coalesce(pl.col("spot_last_active_report_reason"), pl.lit("No active reports")).alias("spot_last_active_report_reason"))
    else:
        out_cols.append(pl.lit("No active reports").alias("spot_last_active_report_reason"))
    for c, dtype in [("spot_last_active_report_user_id", pl.Int64), ("spot_last_active_report_created_at", pl.Datetime), ("spot_reports_full_history", pl.Utf8)]:
        out_cols.append(pl.col(c) if c in base.columns else pl.lit(None).cast(dtype).alias(c))
    if "spot_has_active_report" in base.columns:
        out_cols.append(pl.coalesce(pl.col("spot_has_active_report").cast(pl.Int64), pl.lit(0)).alias("spot_has_active_report"))
    else:
        out_cols.append(pl.lit(0).alias("spot_has_active_report"))
    out_cols.append(pl.col("created_at").alias("spot_created_at") if "created_at" in base.columns else pl.lit(None).cast(pl.Datetime).alias("spot_created_at"))
    out_cols.append(pl.col("created_at").dt.date().alias("spot_created_date") if "created_at" in base.columns else pl.lit(None).cast(pl.Date).alias("spot_created_date"))
    out_cols.append(pl.col("updated_at").alias("spot_updated_at") if "updated_at" in base.columns else pl.lit(None).cast(pl.Datetime).alias("spot_updated_at"))
    out_cols.append(pl.col("updated_at").dt.date().alias("spot_updated_date") if "updated_at" in base.columns else pl.lit(None).cast(pl.Date).alias("spot_updated_date"))
    if "valid_through" in base.columns:
        _vt = pl.col("valid_through").cast(pl.Datetime("us"), strict=False).dt.date()
        out_cols.append(_vt.alias("spot_valid_through"))
        out_cols.append(_vt.alias("spot_valid_through_date"))
    else:
        out_cols.extend([
            pl.lit(None).cast(pl.Date).alias("spot_valid_through"),
            pl.lit(None).cast(pl.Date).alias("spot_valid_through_date"),
        ])
    out_cols.append(pl.col("deleted_at").alias("spot_deleted_at") if "deleted_at" in base.columns else pl.lit(None).cast(pl.Datetime).alias("spot_deleted_at"))
    out_cols.append(pl.col("deleted_at").dt.date().alias("spot_deleted_date") if "deleted_at" in base.columns else pl.lit(None).cast(pl.Date).alias("spot_deleted_date"))
    return base.select(out_cols)
