# defs/data_lakehouse/silver/stg/stg_s2p_user_profiles.py
import dagster as dg
import polars as pl
from datetime import date

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _transform_s2p_user_profiles(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma el DataFrame de perfiles (ya filtrado al perfil más reciente
    por usuario) para obtener la misma selección de campos que la query MySQL.
    """

    # -------------------------
    # Expresiones auxiliares
    # -------------------------

    # person_type -> user_person_type
    pt = pl.col("person_type")
    user_person_type_expr = (
        pl.when(pt == 1)
        .then(pl.lit("Legal Entity"))
        .when(pt == 2)
        .then(pl.lit("Individual"))
        .otherwise(pl.lit("Unknown"))
        .alias("user_person_type")
    )

    # tenant_type -> user_industria_role
    tt = pl.col("tenant_type")
    user_industria_role_expr = (
        pl.when(tt == 1)
        .then(pl.lit("Tenant"))
        .when(tt == 2)
        .then(pl.lit("Broker"))
        .when(tt == 4)
        .then(pl.lit("Landlord"))
        .when(tt == 5)
        .then(pl.lit("Developer"))
        .otherwise(pl.lit("Unknown"))
        .alias("user_industria_role")
    )

    # tenant_type -> user_type_id
    user_type_id_expr = (
        pl.when(tt == 1)
        .then(1)
        .when(tt.is_in([2, 4, 5]))
        .then(2)
        .otherwise(0)
        .alias("user_type_id")
    )

    # tenant_type -> user_type
    user_type_expr = (
        pl.when(tt == 1)
        .then(pl.lit("Buyer"))
        .when(tt.is_in([2, 4, 5]))
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

    # -------------------------
    # Nuevo DataFrame
    # -------------------------
    return df.select(
        [
            pl.col("id").alias("user_profile_id"),
            pl.col("user_id"),
            pl.col("name").alias("user_name"),
            pl.col("last_name").alias("user_last_name"),
            pl.col("mothers_last_name").alias("user_mothers_last_name"),
            user_full_name_expr,
            pl.col("phone_indicator").alias("user_phone_indicator"),
            pl.col("phone_number").alias("user_phone_number"),
            user_phone_full_number_expr,
            pl.col("has_whatsapp").alias("user_has_whatsapp"),

            # COALESCE(person_type, 0) AS user_person_type_id
            pl.col("person_type").fill_null(0).alias("user_person_type_id"),
            user_person_type_expr,

            # COALESCE(tenant_type, 0) AS user_industria_role_id
            pl.col("tenant_type").fill_null(0).alias("user_industria_role_id"),
            user_industria_role_expr,
            user_type_id_expr,
            user_type_expr,

            pl.col("created_at").alias("user_profile_created_at"),
            pl.col("created_at").dt.date().alias("user_profile_created_date"),
            pl.col("updated_at").alias("user_profile_updated_at"),
            pl.col("updated_at").dt.date().alias("user_profile_updated_date"),
        ]
    )


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    description=(
        "Silver STG: perfil más reciente de cada usuario from Spot2 Platform, "
        "con campos enriquecidos a partir de profiles."
    ),
)
def stg_s2p_user_profiles(
    context: dg.AssetExecutionContext,
    raw_s2p_profiles: pl.DataFrame,
):
    """
    Emula:

    WITH earliest_profiles AS (
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn
            FROM profiles
        ) p
        WHERE p.rn = 1
    )
    SELECT ... FROM earliest_profiles;
    """

    def body():
        # Equivalente a:
        # ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) = 1
        # -> ordenamos por user_id, created_at DESC y tomamos la primera fila por grupo
        earliest_profiles = (
            raw_s2p_profiles
            .sort(["user_id", "created_at"], descending=[False, True])
            .group_by("user_id", maintain_order=True)
            .head(1)
        )

        df_profiles = _transform_s2p_user_profiles(earliest_profiles)

        context.log.info(
            f"stg_s2p_user_profiles: {df_profiles.height} rows for partition {context.partition_key}"
        )
        return df_profiles

    yield from iter_job_wrapped_compute(context, body)
