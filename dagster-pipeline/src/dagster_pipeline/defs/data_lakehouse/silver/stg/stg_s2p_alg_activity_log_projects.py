# defs/data_lakehouse/silver/stg/stg_s2p_alg_activity_log_projects.py
import dagster as dg
import polars as pl
from datetime import date

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute

# ============ Constantes de eventos de proyecto ============

# Eventos LOI (Letter of Intent)
LOI_EVENTS = [
    "projectSpotIntentionLetter",
    "projectSpotDocumentation",
    "projectSpotContract",
    "projectSpotWon",
]

# Eventos de contrato (subconjunto de LOI_EVENTS)
CONTRACT_EVENTS = [
    "projectSpotContract",
    "projectSpotWon",
]


def _transform_s2p_alg_activity_log_projects(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma raw_s2p_activity_log_projects (activity_log filtrado a proyectos)
    con clasificación de eventos LOI y contrato.
    """

    # Fechas de corte para relevancia
    fecha_corte_inicio = date(2024, 1, 1)
    fecha_corte_fin = date.today()

    # -------------------------
    # project_id y spot_id: extraer del JSON (todos los registros son de proyecto)
    # -------------------------
    project_id_expr = (
        pl.col("properties")
        .str.json_path_match("$.project_id")
        .cast(pl.Int64)
        .alias("project_id")
    )

    spot_id_expr = (
        pl.col("properties")
        .str.json_path_match("$.spot_id")
        .cast(pl.Int64)
        .alias("spot_id")
    )

    # -------------------------
    # LOI (Letter of Intent)
    # -------------------------
    is_loi_event = pl.col("event").is_in(LOI_EVENTS)

    alg_loi_event_id_expr = (
        pl.when(is_loi_event)
        .then(1)
        .otherwise(0)
        .alias("alg_loi_event_id")
    )

    alg_loi_event_expr = (
        pl.when(is_loi_event)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("alg_loi_event")
    )

    # -------------------------
    # Contract
    # -------------------------
    is_contract_event = pl.col("event").is_in(CONTRACT_EVENTS)

    alg_contract_event_id_expr = (
        pl.when(is_contract_event)
        .then(1)
        .otherwise(0)
        .alias("alg_contract_event_id")
    )

    alg_contract_event_expr = (
        pl.when(is_contract_event)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("alg_contract_event")
    )

    # -------------------------
    # Relevancia (filtro de fecha + tipo de evento)
    # -------------------------
    created_date = pl.col("created_at").dt.date()
    is_in_date_range = (created_date >= fecha_corte_inicio) & (created_date <= fecha_corte_fin)

    # LOI relevante para funnel de proyectos
    is_loi_relevant = is_in_date_range & is_loi_event

    alg_project_funnel_loi_relevant_id_expr = (
        pl.when(is_loi_relevant)
        .then(1)
        .otherwise(0)
        .alias("alg_project_funnel_loi_relevant_id")
    )

    alg_project_funnel_loi_relevant_expr = (
        pl.when(is_loi_relevant)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("alg_project_funnel_loi_relevant")
    )

    # Contract relevante para funnel de proyectos
    is_contract_relevant = is_in_date_range & is_contract_event

    alg_project_funnel_contract_relevant_id_expr = (
        pl.when(is_contract_relevant)
        .then(1)
        .otherwise(0)
        .alias("alg_project_funnel_contract_relevant_id")
    )

    alg_project_funnel_contract_relevant_expr = (
        pl.when(is_contract_relevant)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("alg_project_funnel_contract_relevant")
    )

    # -------------------------
    # SELECT final
    # -------------------------
    return df.select(
        [
            pl.col("id").alias("alg_id"),
            project_id_expr,
            spot_id_expr,
            pl.col("event").alias("alg_event"),
            # LOI
            alg_loi_event_id_expr,
            alg_loi_event_expr,
            # Contract
            alg_contract_event_id_expr,
            alg_contract_event_expr,
            # Relevancia LOI para funnel de proyectos
            alg_project_funnel_loi_relevant_id_expr,
            alg_project_funnel_loi_relevant_expr,
            # Relevancia Contract para funnel de proyectos
            alg_project_funnel_contract_relevant_id_expr,
            alg_project_funnel_contract_relevant_expr,
            # Timestamps
            pl.col("created_at").alias("alg_created_at"),
            pl.col("created_at").dt.date().alias("alg_created_date"),
            pl.col("updated_at").alias("alg_updated_at"),
            pl.col("updated_at").dt.date().alias("alg_updated_date"),
        ]
    )


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    description=(
        "Silver STG: activity_log de proyectos con clasificación LOI/Contract "
        "y flags de relevancia temporal."
    ),
)
def stg_s2p_alg_activity_log_projects(
    context: dg.AssetExecutionContext,
    raw_s2p_activity_log_projects: pl.DataFrame,
):
    """
    Transforma raw_s2p_activity_log_projects con clasificación de eventos:
    - alg_loi_event: eventos de Letter of Intent
    - alg_contract_event: eventos de contrato
    - alg_*_relevant: filtros de relevancia (fecha >= 2024-01-01)
    """
    def body():
        df_alg = _transform_s2p_alg_activity_log_projects(raw_s2p_activity_log_projects)

        context.log.info(
            f"stg_s2p_alg_activity_log_projects: {df_alg.height} rows for partition {context.partition_key}"
        )
        return df_alg

    yield from iter_job_wrapped_compute(context, body)
