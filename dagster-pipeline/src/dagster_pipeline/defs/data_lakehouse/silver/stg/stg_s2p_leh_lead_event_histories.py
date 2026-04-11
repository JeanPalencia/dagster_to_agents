# defs/data_lakehouse/silver/stg/stg_s2p_leh_lead_event_histories.py
import dagster as dg
import polars as pl
from datetime import datetime

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


FLOW_EVENTS = [
    "whatsappInteraction",
    "contactInteraction",
    "accountCreation",
    "officeLandingCreation",
    "retailLandingCreation",
    "industrialLandingCreation",
    "levelsContactWhatsappInteraction",
    "levelsContactEmailInteraction",
    "levelsContactPhoneInteraction",
    "chatbotConfirmSpotInterest",
    "chatbotProfilingCompleted",
]


# Orden de intención para desempate: mayor = más intención (L0=1, L1=2, L2=3, Levels Brokers=4, Bot=5)
FLOW_INTENT = {
    "accountCreation": 1,
    "officeLandingCreation": 2,
    "retailLandingCreation": 2,
    "industrialLandingCreation": 2,
    "whatsappInteraction": 3,
    "contactInteraction": 3,
    "levelsContactWhatsappInteraction": 4,
    "levelsContactEmailInteraction": 4,
    "levelsContactPhoneInteraction": 4,
    "chatbotConfirmSpotInterest": 5,
    "chatbotProfilingCompleted": 5,
}


def _transform_s2p_leh_lead_event_histories(df: pl.DataFrame) -> pl.DataFrame:
    """
    Emula la query:

    WITH client_event_histories_rn AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created_at ASC)  AS rn_first,
               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created_at DESC) AS rn_last
        FROM client_event_histories
    ),
    flow_events_rn AS (
        SELECT id AS flow_event_id,
               client_id,
               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created_at ASC) AS rn_flow_first
        FROM client_event_histories
        WHERE event_name IN (...) AND created_at >= '2025-08-01'
    )
    SELECT ...
    FROM client_event_histories_rn ceh
    LEFT JOIN flow_events_rn fer ON ceh.id = fer.flow_event_id;
    """

    # 1) Añadimos, por cliente, la fecha del primer y último evento
    df_with_bounds = df.with_columns(
        [
            pl.col("created_at").min().over("client_id").alias(
                "client_first_created_at"
            ),
            pl.col("created_at").max().over("client_id").alias(
                "client_last_created_at"
            ),
        ]
    )

    # 2) Subset de eventos de "flow" con fecha >= 2025-08-01
    #    Selección determinística del primer flow por cliente (main):
    #    ORDER BY created_at ASC, id ASC. Nuestra mejora: desempate por mayor intención (L1 > L0) cuando hay mismo created_at.
    flow_cutoff = datetime(2025, 8, 1)

    flow_events = (
        df_with_bounds.filter(
            (pl.col("event_name").is_in(FLOW_EVENTS))
            & (pl.col("created_at") >= flow_cutoff)
        )
        .with_columns(
            pl.col("event_name").replace(FLOW_INTENT).alias("_flow_intent")
        )
        .sort(["client_id", "created_at", "_flow_intent", "id"], descending=[False, False, True, False])
    )

    first_flow_ids = (
        flow_events
        .group_by("client_id")
        .agg(pl.col("id").first().alias("flow_event_id"))
        .with_columns(pl.lit(True).alias("is_first_flow"))
    )

    # 3) Equivalente al LEFT JOIN flow_events_rn fer ON ceh.id = fer.flow_event_id
    #    Traemos sólo el flag de primer evento de flow
    flow_flags = first_flow_ids.select(["flow_event_id", "is_first_flow"])

    joined = df_with_bounds.join(
        flow_flags,
        left_on="id",
        right_on="flow_event_id",
        how="left",
    )

    # 4) Expresiones CASE para lhe_flow_id y lhe_flow
    ev = pl.col("event_name")

    lhe_flow_id_expr = (
        pl.when(ev.is_in(["whatsappInteraction", "contactInteraction"]))
        .then(1)
        .when(ev.is_in(["accountCreation"]))
        .then(2)
        .when(
            ev.is_in(
                [
                    "officeLandingCreation",
                    "retailLandingCreation",
                    "industrialLandingCreation",
                ]
            )
        )
        .then(3)
        .when(
            ev.is_in(
                [
                    "levelsContactWhatsappInteraction",
                    "levelsContactEmailInteraction",
                    "levelsContactPhoneInteraction",
                ]
            )
        )
        .then(4)
        .when(ev.is_in(["chatbotConfirmSpotInterest", "chatbotProfilingCompleted"]))
        .then(5)
        .otherwise(None)
        .alias("lhe_flow_id")
    )

    lhe_flow_expr = (
        pl.when(ev.is_in(["whatsappInteraction", "contactInteraction"]))
        .then(pl.lit("L2"))
        .when(ev.is_in(["accountCreation"]))
        .then(pl.lit("L0"))
        .when(
            ev.is_in(
                [
                    "officeLandingCreation",
                    "retailLandingCreation",
                    "industrialLandingCreation",
                ]
            )
        )
        .then(pl.lit("L1"))
        .when(
            ev.is_in(
                [
                    "levelsContactWhatsappInteraction",
                    "levelsContactEmailInteraction",
                    "levelsContactPhoneInteraction",
                ]
            )
        )
        .then(pl.lit("Levels Brokers"))
        .when(ev.is_in(["chatbotConfirmSpotInterest", "chatbotProfilingCompleted"]))
        .then(pl.lit("Bot interested"))
        .otherwise(pl.lit(None))
        .alias("lhe_flow")
    )

    # 5) Flags de primer/último evento general
    is_first_event = (
        pl.col("created_at") == pl.col("client_first_created_at")
    )
    is_last_event = (
        pl.col("created_at") == pl.col("client_last_created_at")
    )

    # 6) Flags de primer evento del flow (usa is_first_flow, que será null para eventos sin flow)
    is_first_flow = pl.col("is_first_flow")

    lhe_first_event_id_expr = (
        pl.when(is_first_event).then(1).otherwise(0).alias("lhe_first_event_id")
    )
    lhe_first_event_expr = (
        pl.when(is_first_event).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias(
            "lhe_first_event"
        )
    )

    lhe_last_event_id_expr = (
        pl.when(is_last_event).then(1).otherwise(0).alias("lhe_last_event_id")
    )
    lhe_last_event_expr = (
        pl.when(is_last_event).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias(
            "lhe_last_event"
        )
    )

    lhe_first_project_funnel_event_id_expr = (
        pl.when(is_first_flow).then(1).otherwise(0).alias("lhe_first_project_funnel_event_id")
    )
    lhe_first_project_funnel_event_expr = (
        pl.when(is_first_flow)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("lhe_first_project_funnel_event")
    )

    # 7) Proyección final (SELECT ...)
    return joined.select(
        [
            pl.col("id").alias("leh_id"),
            pl.col("client_id").alias("lead_id"),
            pl.col("event_name").alias("lhe_event"),
            lhe_flow_id_expr,
            lhe_flow_expr,
            lhe_first_event_id_expr,
            lhe_first_event_expr,
            lhe_last_event_id_expr,
            lhe_last_event_expr,
            lhe_first_project_funnel_event_id_expr,
            lhe_first_project_funnel_event_expr,
            pl.col("created_at").alias("lhe_created_at"),
            pl.col("updated_at").alias("lhe_updated_at"),
        ]
    )


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    description=(
        "Silver STG: historiales de eventos de leads (client_event_histories) "
        "con flags de primer/último evento y primer evento de flow."
    ),
)
def stg_s2p_leh_lead_event_histories(
    context: dg.AssetExecutionContext,
    raw_s2p_client_event_histories: pl.DataFrame,
):
    """
    Transforma raw_s2p_client_event_histories replicando la lógica
    de stg_s2p_leh_lead_event_histories en MySQL.
    """
    def body():
        df_leh = _transform_s2p_leh_lead_event_histories(raw_s2p_client_event_histories)

        context.log.info(
            f"stg_s2p_leh_lead_event_histories: {df_leh.height} rows for partition {context.partition_key}"
        )
        return df_leh

    yield from iter_job_wrapped_compute(context, body)
