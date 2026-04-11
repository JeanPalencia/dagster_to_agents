# defs/data_lakehouse/silver/stg/stg_s2p_leh_lead_event_histories_new.py
"""
Silver STG: Complete transformation of raw_s2p_client_event_histories_new using reusable functions.

This asset maintains original bronze column names and only adds calculated columns when necessary.
"""
import polars as pl
from datetime import datetime

from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset
from dagster_pipeline.defs.data_lakehouse.silver.utils import case_when


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


# Intent order for tie-break: higher = more intent (L0=1, L1=2, L2=3, Levels Brokers=4, Bot=5)
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


def _transform_s2p_leh_lead_event_histories_new(df: pl.DataFrame) -> pl.DataFrame:
    # 1) Add, per client, the date of first and last event
    df_with_bounds = df.with_columns([
        pl.col("created_at").min().over("client_id").alias("client_first_created_at"),
        pl.col("created_at").max().over("client_id").alias("client_last_created_at"),
    ])
    # 2) Subset de eventos de "flow" con fecha >= 2025-08-01 (alineado con legacy y main).
    #    Primer flow por cliente: ORDER BY created_at ASC, mayor intención (desempate), id ASC.
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

    # 3) Equivalent to LEFT JOIN flow_events_rn fer ON ceh.id = fer.flow_event_id
    #    Bring only the flag of first flow event (estructura main).
    flow_flags = first_flow_ids.select([
        "flow_event_id",
        "is_first_flow",
    ])

    joined = df_with_bounds.join(
        flow_flags,
        left_on="id",
        right_on="flow_event_id",
        how="left",
    )

    # 4) CASE expressions for lhe_flow_id and lhe_flow using case_when
    ev = pl.col("event_name")

    # lhe_flow_id: Map events to flow IDs
    lhe_flow_id_expr = (
        pl.when(ev.is_in(["whatsappInteraction", "contactInteraction"]))
        .then(1)
        .when(ev.is_in(["accountCreation"]))
        .then(2)
        .when(ev.is_in([
            "officeLandingCreation",
            "retailLandingCreation",
            "industrialLandingCreation",
        ]))
        .then(3)
        .when(ev.is_in([
            "levelsContactWhatsappInteraction",
            "levelsContactEmailInteraction",
            "levelsContactPhoneInteraction",
        ]))
        .then(4)
        .when(ev.is_in(["chatbotConfirmSpotInterest", "chatbotProfilingCompleted"]))
        .then(5)
        .otherwise(None)
        .alias("lhe_flow_id")
    )

    # lhe_flow: Map events to flow text using case_when
    flow_mapping = {
        "whatsappInteraction": "L2",
        "contactInteraction": "L2",
        "accountCreation": "L0",
        "officeLandingCreation": "L1",
        "retailLandingCreation": "L1",
        "industrialLandingCreation": "L1",
        "levelsContactWhatsappInteraction": "Levels Brokers",
        "levelsContactEmailInteraction": "Levels Brokers",
        "levelsContactPhoneInteraction": "Levels Brokers",
        "chatbotConfirmSpotInterest": "Bot interested",
        "chatbotProfilingCompleted": "Bot interested",
    }

    lhe_flow_expr = case_when(
        "event_name",
        flow_mapping,
        default=None,
        alias="lhe_flow"
    )

    # 5) Flags for first/last general event
    is_first_event = pl.col("created_at") == pl.col("client_first_created_at")
    is_last_event = pl.col("created_at") == pl.col("client_last_created_at")

    # 6) Flags for first flow event (uses is_first_flow, which will be null for events without flow)
    is_first_flow = pl.col("is_first_flow")

    lhe_first_event_id_expr = (
        pl.when(is_first_event).then(1).otherwise(0).alias("lhe_first_event_id")
    )
    lhe_first_event_expr = (
        pl.when(is_first_event)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("lhe_first_event")
    )

    lhe_last_event_id_expr = (
        pl.when(is_last_event).then(1).otherwise(0).alias("lhe_last_event_id")
    )
    lhe_last_event_expr = (
        pl.when(is_last_event)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("lhe_last_event")
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

    # 7) Maintain all original columns and add calculated ones
    return joined.with_columns([
        # Rename key columns
        pl.col("id").alias("leh_id"),
        pl.col("client_id").alias("lead_id"),
        pl.col("event_name").alias("lhe_event"),

        # Flow columns
        lhe_flow_id_expr,
        lhe_flow_expr,

        # Event flags
        lhe_first_event_id_expr,
        lhe_first_event_expr,
        lhe_last_event_id_expr,
        lhe_last_event_expr,
        lhe_first_project_funnel_event_id_expr,
        lhe_first_project_funnel_event_expr,
    ])


stg_s2p_leh_lead_event_histories_new = make_silver_stg_asset(
    "raw_s2p_client_event_histories_new",
    _transform_s2p_leh_lead_event_histories_new,
    silver_asset_name="stg_s2p_leh_lead_event_histories_new",
    description=(
        "Silver STG: Complete transformation of client_event_histories from Spot2 Platform "
        "using reusable functions. Maintains original column names and adds calculated fields."
    ),
)
