# defs/data_lakehouse/silver/stg/stg_s2p_projects.py
import dagster as dg
import polars as pl
from datetime import date

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _transform_s2p_projects(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma raw_s2p_project_requirements (project_requirements en MySQL)
    replicando la query stg_s2p_projects.
    """

    # --------------------------------
    # Expresiones auxiliares (CASE)
    # --------------------------------

    # spot_currency_sale_id (CASE currency_type ...)
    ct = pl.col("currency_type")
    spot_currency_sale_id_expr = (
        pl.when((ct == 1) | ct.is_null())
        .then(1)
        .when(ct == 2)
        .then(2)
        .otherwise(None)
        .alias("spot_currency_sale_id")
    )

    # spot_currency_sale (CASE currency_type ...)
    spot_currency_sale_expr = (
        pl.when((ct == 1) | ct.is_null())
        .then(pl.lit("MXN"))
        .when(ct == 2)
        .then(pl.lit("USD"))
        .otherwise(pl.lit(None))
        .alias("spot_currency_sale")
    )

    # spot_price_sqm_mxn_sale_min
    price_area = pl.col("price_area")
    max_sq = pl.col("max_square_space")
    min_sale_price = pl.col("min_sale_price")
    max_sale_price = pl.col("max_sale_price")
    min_rent_price = pl.col("min_rent_price")
    max_rent_price = pl.col("max_rent_price")

    spot_price_sqm_mxn_sale_min_expr = (
        pl.when((price_area == 1) & (max_sq != 0))
        .then(min_sale_price / max_sq)
        .when(price_area == 2)
        .then(min_sale_price)
        .otherwise(None)
        .alias("spot_price_sqm_mxn_sale_min")
    )

    # spot_price_sqm_mxn_sale_max
    spot_price_sqm_mxn_sale_max_expr = (
        pl.when((price_area == 1) & (max_sq != 0))
        .then(max_sale_price / max_sq)
        .when(price_area == 2)
        .then(max_sale_price)
        .otherwise(None)
        .alias("spot_price_sqm_mxn_sale_max")
    )

    # spot_price_sqm_mxn_rent_min
    spot_price_sqm_mxn_rent_min_expr = (
        pl.when((price_area == 1) & (max_sq != 0))
        .then(min_rent_price / max_sq)
        .when(price_area == 2)
        .then(min_rent_price)
        .otherwise(None)
        .alias("spot_price_sqm_mxn_rent_min")
    )

    # spot_price_sqm_mxn_rent_max
    spot_price_sqm_mxn_rent_max_expr = (
        pl.when((price_area == 1) & (max_sq != 0))
        .then(max_rent_price / max_sq)
        .when(price_area == 2)
        .then(max_rent_price)
        .otherwise(None)
        .alias("spot_price_sqm_mxn_rent_max")
    )

    # project_last_spot_stage (CASE last_spot_stage ...)
    lss = pl.col("last_spot_stage")
    project_last_spot_stage_expr = (
        pl.when(lss == 1)
        .then(pl.lit("Project"))
        .when(lss == 2)
        .then(pl.lit("Visit in Progress"))
        .when(lss == 3)
        .then(pl.lit("Intention Letter"))
        .when(lss == 4)
        .then(pl.lit("Documentation"))
        .when(lss == 5)
        .then(pl.lit("Contract"))
        .when(lss == 6)
        .then(pl.lit("Won"))
        .when(lss == 7)
        .then(pl.lit("Rejected"))
        .when(lss == 8)
        .then(pl.lit("Visit Completed"))
        .otherwise(pl.lit("Unknown"))
        .alias("project_last_spot_stage")
    )

    # project_enable (CASE enable ...)
    en = pl.col("enable")
    project_enable_expr = (
        pl.when(en == 1)
        .then(pl.lit("Yes"))
        .when(en == 0)
        .then(pl.lit("No"))
        .otherwise(pl.lit(None))
        .alias("project_enable")
    )

    # project_disable_reason_id: COALESCE(reason_id, 0)
    project_disable_reason_id_expr = (
        pl.col("reason_id").fill_null(0).alias("project_disable_reason_id")
    )

    # project_disable_reason (CASE reason_id ...)
    rid = pl.col("reason_id")
    project_disable_reason_expr = (
        pl.when(rid == 1)
        .then(pl.lit("Won"))
        .when(rid == 2)
        .then(pl.lit("Stopped responding"))
        .when(rid == 3)
        .then(pl.lit("Already found"))
        .when(rid == 4)
        .then(pl.lit("Canceled request"))
        .when(rid == 5)
        .then(pl.lit("Spaces do not meet requirements"))
        .when(rid == 6)
        .then(pl.lit("Other"))
        .when(rid == 7)
        .then(pl.lit("Project deleted"))
        .when(rid == 8)
        .then(pl.lit("Low budget"))
        .when(rid == 9)
        .then(pl.lit("Missing documentation"))
        .when(rid == 10)
        .then(pl.lit("Standby"))
        .otherwise(pl.lit("Not applicable"))
        .alias("project_disable_reason")
    )

    # --------------------------------
    # Campos de relevancia (sin restricción de fecha)
    # --------------------------------
    fecha_corte_fin = date.today()

    created_date = pl.col("created_at").dt.date()
    is_project_relevant = created_date <= fecha_corte_fin

    project_funnel_relevant_id_expr = (
        pl.when(is_project_relevant)
        .then(1)
        .otherwise(0)
        .alias("project_funnel_relevant_id")
    )

    project_funnel_relevant_expr = (
        pl.when(is_project_relevant)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("project_funnel_relevant")
    )

    # project_funnel_won_relevant_date: DATE(won_date) solo si no es futuro
    won_date_as_date = pl.col("won_date").dt.date()
    is_won_relevant = (
        pl.col("won_date").is_not_null()
        & (won_date_as_date <= fecha_corte_fin)
    )

    project_funnel_won_relevant_date_expr = (
        pl.when(is_won_relevant)
        .then(won_date_as_date)
        .otherwise(None)
        .alias("project_funnel_won_relevant_date")
    )

    # --------------------------------
    # Nuevo DataFrame: select ordenado
    # --------------------------------
    return df.select(
        [
            pl.col("id").alias("project_id"),
            pl.col("name").alias("project_name"),
            pl.col("spot_type_id").alias("spot_sector_id"),
            pl.when(pl.col("states").cast(pl.Utf8, strict=False).str.starts_with('"'))
              .then(pl.col("states").cast(pl.Utf8, strict=False).str.strip_chars('"'))
              .otherwise(pl.col("states").cast(pl.Utf8, strict=False))
              .alias("project_state_ids"),
            pl.col("min_square_space").alias("project_min_square_space"),
            pl.col("max_square_space").alias("project_max_square_space"),
            pl.col("currency_type").alias("project_currency_id"),
            pl.col("price_area").alias("spot_price_sqm"),
            pl.col("min_rent_price").alias("project_min_rent_price"),
            pl.col("max_rent_price").alias("project_max_rent_price"),
            pl.col("min_sale_price").alias("project_min_sale_price"),
            pl.col("max_sale_price").alias("project_max_sale_price"),
            pl.col("rent_months").alias("project_rent_months"),
            pl.col("commission").alias("project_commission"),
            pl.col("client_id").alias("lead_id"),
            pl.col("user_id").alias("user_id"),

            spot_currency_sale_id_expr,
            spot_currency_sale_expr,
            spot_price_sqm_mxn_sale_min_expr,
            spot_price_sqm_mxn_sale_max_expr,
            spot_price_sqm_mxn_rent_min_expr,
            spot_price_sqm_mxn_rent_max_expr,
            pl.col("last_spot_stage").alias("project_last_spot_stage_id"),
            project_last_spot_stage_expr,
            pl.col("enable").alias("project_enable_id"),
            project_enable_expr,
            project_disable_reason_id_expr,
            project_disable_reason_expr,
            # Campos de relevancia para funnel de proyectos
            project_funnel_relevant_id_expr,
            project_funnel_relevant_expr,
            pl.col("won_date").dt.date().alias("project_won_date"),
            project_funnel_won_relevant_date_expr,
            # Timestamps finales
            pl.col("created_at").alias("project_created_at"),
            pl.col("created_at").dt.date().alias("project_created_date"),
            pl.col("updated_at").alias("project_updated_at"),
            pl.col("updated_at").dt.date().alias("project_updated_date"),
        ]
    )


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    description="Silver STG: transformation de project_requirements to projects Spot2.",
)
def stg_s2p_projects(
    context: dg.AssetExecutionContext,
    raw_s2p_project_requirements: pl.DataFrame,
):
    """
    Transforma raw_s2p_project_requirements con lógica de negocio básica,
    emulando la query stg_s2p_projects en MySQL.
    """
    def body():
        df_projects = _transform_s2p_projects(raw_s2p_project_requirements)
        context.log.info(
            f"stg_s2p_projects: {df_projects.height} rows for partition {context.partition_key}"
        )
        return df_projects

    yield from iter_job_wrapped_compute(context, body)
