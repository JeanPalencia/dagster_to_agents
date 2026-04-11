# defs/data_lakehouse/silver/stg/stg_s2p_clients_new.py
"""
Silver STG: Full transformation of raw_s2p_clients_new using reusable functions.
Keeps original bronze column names and adds calculated columns when needed.
"""
import polars as pl
from datetime import date, datetime

from dagster_pipeline.defs.data_lakehouse.silver.utils import (
    case_when,
    case_when_conditions,
)
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset


def _transform_s2p_clients(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms raw_s2p_clients_new maintaining original column names.
    Only adds calculated columns when necessary.
    """
    fecha_sentinela = date(9999, 12, 31)
    lead_type_date_expr = (
        pl.min_horizontal(
            *[
                pl.col(c).dt.date().fill_null(fecha_sentinela)
                for c in ["lead0_at", "lead1_at", "lead2_at", "lead3_at", "lead4_at"]
            ]
        ).alias("lead_type_date_start")
    )
    datetime_sentinela = datetime(9999, 12, 31, 23, 59, 59)
    lead_type_datetime_expr = (
        pl.min_horizontal(
            *[
                pl.col(c).fill_null(datetime_sentinela)
                for c in ["lead0_at", "lead1_at", "lead2_at", "lead3_at", "lead4_at"]
            ]
        ).alias("lead_type_datetime_start")
    )
    patron_campania = "BOFU|MOFU|TOFU|LOFU|Conversiones|BrandTerms"
    is_paid_campaign_expr = (
        pl.col("hs_utm_campaign")
        .fill_null("")
        .str.to_uppercase()
        .str.contains(patron_campania)
    )
    is_paid_origin_expr = pl.col("origin").is_in([6, 7, 8])
    is_paid_expr = is_paid_campaign_expr | is_paid_origin_expr

    origin_text_expr = case_when(
        "origin", {1: "Spot2", 2: "Hubspot"}, default=None, alias="origin_text"
    )
    client_type_text_expr = case_when(
        "client_type", {1: "Tenant", 2: "Broker", 3: "Landlord"}, default=None, alias="client_type_text"
    )
    client_state_text_expr = case_when(
        "client_state",
        {
            1: "New Client", 2: "Reactivated", 3: "Open Requirement", 4: "Sales Nurturing",
            5: "Stand By", 6: "Residential Search", 7: "Low Budget", 8: "Off-Market Footage",
            9: "Other Reason", 10: "Budget Not Met", 11: "Footage Requirements Not Met",
            12: "No Offer", 13: "Never Replied", 14: "Stopped Replying", 15: "No Longer Searching",
            16: "Other Nurturing", 17: "Contact Attempt", 18: "No Profile", 19: "Spot Not Available",
            20: "Documentation Missing", 21: "Visit Not Allowed", 22: "Wrong Phone Number",
            23: "Duplicated Client",
        },
        default=None,
        alias="client_state_text",
    )
    hs_client_source_text_expr = case_when(
        "hs_client_source",
        {
            1: "Created by Spot Agent", 2: "Paid social media", 3: "Platform",
            4: "Trebel", 5: "Created by Supply Agent", 6: "Paid search",
        },
        default=None,
        alias="hs_client_source_text",
    )
    spot_type_text_expr = case_when(
        "spot_type_id",
        {13: "Retail", 11: "Office", 9: "Industrial", 15: "Land"},
        default="Retail",
        alias="spot_type_text",
    )
    lead_max_type_expr = case_when_conditions(
        [
            (pl.col("lead4_at").is_not_null(), "L4"),
            (pl.col("lead3_at").is_not_null(), "L3"),
            (pl.col("lead2_at").is_not_null(), "L2"),
            (pl.col("lead1_at").is_not_null(), "L1"),
            (pl.col("lead0_at").is_not_null(), "L0"),
        ],
        default="Others",
        alias="lead_max_type",
    )

    # Same logic as lk_matches_visitors_to_leads/queries/clients.sql for correlation with VTL pipeline.
    _min_lead_dt = pl.min_horizontal(
        *[pl.col(c).fill_null(datetime_sentinela) for c in ["lead0_at", "lead1_at", "lead2_at", "lead3_at", "lead4_at"]]
    )
    lead_min_at_expr = (
        pl.when(_min_lead_dt == datetime_sentinela)
        .then(pl.lit(None).cast(pl.Datetime))
        .otherwise(_min_lead_dt)
        .alias("lead_min_at")
    )
    lead_min_type_expr = (
        pl.when(pl.col("lead_min_at").eq(pl.col("lead0_at"))).then(pl.lit("L0"))
        .when(pl.col("lead_min_at").eq(pl.col("lead1_at"))).then(pl.lit("L1"))
        .when(pl.col("lead_min_at").eq(pl.col("lead2_at"))).then(pl.lit("L2"))
        .when(pl.col("lead_min_at").eq(pl.col("lead3_at"))).then(pl.lit("L3"))
        .when(pl.col("lead_min_at").eq(pl.col("lead4_at"))).then(pl.lit("L4"))
        .otherwise(None)
        .alias("lead_min_type")
    )
    lead_type_vtl_expr = (
        pl.when(pl.col("lead4_at").is_not_null()).then(pl.lit("L4"))
        .when(pl.col("lead3_at").is_not_null()).then(pl.lit("L3"))
        .when(pl.col("lead2_at").is_not_null()).then(pl.lit("L2"))
        .when(pl.col("lead1_at").is_not_null()).then(pl.lit("L1"))
        .when(pl.col("lead0_at").is_not_null()).then(pl.lit("L0"))
        .otherwise(pl.lit("others"))
        .alias("lead_type")
    )

    industrial_region = pl.col("hs_industrial_profiling").str.json_path_match("$[0].region")
    industrial_state = pl.col("hs_industrial_profiling").str.json_path_match("$[0].state")
    industrial_size = pl.col("hs_industrial_profiling").str.json_path_match("$[0].size")
    industrial_budget = pl.col("hs_industrial_profiling").str.json_path_match("$[0].budget")
    office_region = pl.col("hs_office_profiling").str.json_path_match("$[0].region")
    office_state = pl.col("hs_office_profiling").str.json_path_match("$[0].state")
    office_size = pl.col("hs_office_profiling").str.json_path_match("$[0].size")
    office_budget = pl.col("hs_office_profiling").str.json_path_match("$[0].budget")

    industrial_profile_size_id_expr = (
        pl.when(industrial_size == "1").then(1).when(industrial_size == "2").then(2)
        .when(industrial_size == "3").then(3).when(industrial_size == "4").then(4)
        .when(industrial_size == "5").then(5).when(industrial_size == "6").then(6)
        .when(industrial_size == "7").then(7).when(industrial_size == "8").then(8)
        .when(industrial_size == "9").then(9).when(industrial_size == "10").then(10)
        .otherwise(None).cast(pl.Int64).alias("industrial_profile_size_id")
    )
    industrial_profile_size_text_expr = (
        pl.when(industrial_size == "1").then(pl.lit("Less than 1,000m²"))
        .when(industrial_size == "2").then(pl.lit("From 1,000m² to 3,000m²"))
        .when(industrial_size == "3").then(pl.lit("From 3,000m² to 10,000m²"))
        .when(industrial_size == "4").then(pl.lit("From 10,000m² to 20,000m²"))
        .when(industrial_size == "5").then(pl.lit("More than 20,000m²"))
        .when(industrial_size == "6").then(pl.lit("Less than 700m²"))
        .when(industrial_size == "7").then(pl.lit("From 700m² to 2,000m²"))
        .when(industrial_size == "8").then(pl.lit("From 2,000m² to 10,000m²"))
        .when(industrial_size == "9").then(pl.lit("More than 100,000m²"))
        .when(industrial_size == "10").then(pl.lit("Less than 10,000m²"))
        .otherwise(None).alias("industrial_profile_size_text")
    )
    industrial_profile_budget_id_expr = (
        pl.when(industrial_budget == "1").then(1).when(industrial_budget == "2").then(2)
        .when(industrial_budget == "3").then(3).when(industrial_budget == "4").then(4)
        .when(industrial_budget == "5").then(5).when(industrial_budget == "6").then(6)
        .when(industrial_budget == "7").then(7).when(industrial_budget == "8").then(8)
        .when(industrial_budget == "9").then(9).when(industrial_budget == "10").then(10)
        .otherwise(None).cast(pl.Int64).alias("industrial_profile_budget_id")
    )
    industrial_profile_budget_text_expr = (
        pl.when(industrial_budget == "1").then(pl.lit("Less than $100,000 MXN"))
        .when(industrial_budget == "2").then(pl.lit("From $100,000 to $300,000 MXN"))
        .when(industrial_budget == "3").then(pl.lit("From $300,000 to $1,000,000 MXN"))
        .when(industrial_budget == "4").then(pl.lit("More than $1,000,000 MXN"))
        .when(industrial_budget == "5").then(pl.lit("Less than $60,000 MXN"))
        .when(industrial_budget == "6").then(pl.lit("From $60,000 to $150,000 MXN"))
        .when(industrial_budget == "7").then(pl.lit("From $150,000 to $300,000 MXN"))
        .when(industrial_budget == "8").then(pl.lit("More than $300,000 MXN"))
        .when(industrial_budget == "9").then(pl.lit("Less than $30,000 MXN"))
        .when(industrial_budget == "10").then(pl.lit("From $30,000 to $100,000 MXN"))
        .otherwise(None).alias("industrial_profile_budget_text")
    )
    office_profile_size_id_expr = (
        pl.when(office_size == "1").then(1).when(office_size == "2").then(2)
        .when(office_size == "3").then(3).when(office_size == "4").then(4)
        .when(office_size == "5").then(5).when(office_size == "6").then(6)
        .otherwise(None).cast(pl.Int64).alias("office_profile_size_id")
    )
    office_profile_size_text_expr = (
        pl.when(office_size == "1").then(pl.lit("Less than 100m²"))
        .when(office_size == "2").then(pl.lit("From 100 to 200m²"))
        .when(office_size == "3").then(pl.lit("Less than 200m²"))
        .when(office_size == "4").then(pl.lit("From 200 to 500m²"))
        .when(office_size == "5").then(pl.lit("From 500 to 1000m²"))
        .when(office_size == "6").then(pl.lit("More than 1000m²"))
        .otherwise(None).alias("office_profile_size_text")
    )
    office_profile_budget_id_expr = (
        pl.when(office_budget == "1").then(1).when(office_budget == "2").then(2)
        .when(office_budget == "3").then(3).when(office_budget == "4").then(4)
        .when(office_budget == "5").then(5).when(office_budget == "6").then(6)
        .when(office_budget == "7").then(7).when(office_budget == "8").then(8)
        .otherwise(None).cast(pl.Int64).alias("office_profile_budget_id")
    )
    office_profile_budget_text_expr = (
        pl.when(office_budget == "1").then(pl.lit("Less than $25,000 MXN"))
        .when(office_budget == "2").then(pl.lit("From $25,000 to $75,000 MXN"))
        .when(office_budget == "3").then(pl.lit("From $75,000 to $150,000 MXN"))
        .when(office_budget == "4").then(pl.lit("More than $150,000 MXN"))
        .when(office_budget == "5").then(pl.lit("Less than $50,000 MXN"))
        .when(office_budget == "6").then(pl.lit("From $50,000 to $100,000 MXN"))
        .when(office_budget == "7").then(pl.lit("From $100,000 to $200,000 MXN"))
        .when(office_budget == "8").then(pl.lit("More than $200,000 MXN"))
        .otherwise(None).alias("office_profile_budget_text")
    )

    # Align with legacy stg_s2p_leads: no date filter for lead relevance (has_any_lead_at & not deleted & not spot2)
    has_any_lead_at = (
        pl.col("lead0_at").is_not_null() | pl.col("lead1_at").is_not_null()
        | pl.col("lead2_at").is_not_null() | pl.col("lead3_at").is_not_null()
        | pl.col("lead4_at").is_not_null()
    )
    lead_min_date = pl.min_horizontal(
        *[pl.col(c).dt.date().fill_null(fecha_sentinela) for c in ["lead0_at", "lead1_at", "lead2_at", "lead3_at", "lead4_at"]]
    )
    is_not_deleted = pl.col("deleted_at").is_null()
    is_not_spot2_email = ~pl.col("email").fill_null("").str.contains(r"@spot2\.mx$")
    is_lead_relevant = has_any_lead_at & is_not_deleted & is_not_spot2_email
    project_funnel_relevant_id_expr = pl.when(is_lead_relevant).then(1).otherwise(0).alias("project_funnel_relevant_id")
    project_funnel_relevant_text_expr = pl.when(is_lead_relevant).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("project_funnel_relevant")
    is_lds_relevant = has_any_lead_at & is_not_deleted & is_not_spot2_email
    lds_relevant_id_expr = pl.when(is_lds_relevant).then(1).otherwise(0).alias("lds_relevant_id")
    lds_relevant_text_expr = pl.when(is_lds_relevant).then(pl.lit("Yes")).otherwise(pl.lit("No")).alias("lds_relevant")

    # Normalizar "NA", "N/A", "null" (string) a NULL para alinear con legacy (lk_leads_v2).
    result = df.with_columns([
        pl.col("name").str.replace_all(r"[\n\r]+", " ").str.strip_chars(),
        pl.col("lastname").str.replace_all(r"[\n\r]+", " ").str.strip_chars(),
        pl.col("mothers_lastname").str.replace_all(r"[\n\r]+", " ").str.strip_chars(),
        pl.col("email").str.split("@").list.get(-1).str.strip_chars().str.to_lowercase().alias("email_domain"),
        pl.when(pl.col("phone_indicator") != "")
        .then(pl.col("phone_indicator").cast(pl.String) + pl.lit(" ") + pl.col("phone_number").cast(pl.String))
        .otherwise(pl.col("phone_number").cast(pl.String))
        .alias("full_phone_number"),
        origin_text_expr,
        client_type_text_expr,
        client_state_text_expr,
        hs_client_source_text_expr,
        spot_type_text_expr,
        lead_max_type_expr,
        pl.col("lead0_at").is_not_null().alias("has_lead0"),
        pl.col("lead1_at").is_not_null().alias("has_lead1"),
        pl.col("lead2_at").is_not_null().alias("has_lead2"),
        pl.col("lead3_at").is_not_null().alias("has_lead3"),
        pl.col("lead4_at").is_not_null().alias("has_lead4"),
        pl.col("supply_at").is_not_null().alias("has_supply"),
        pl.col("supply0_at").is_not_null().alias("has_supply0"),
        pl.col("supply1_at").is_not_null().alias("has_supply1"),
        pl.col("supply3_at").is_not_null().alias("has_supply3"),
        pl.when(pl.col("lead4_at").is_not_null()).then(4).when(pl.col("lead3_at").is_not_null()).then(3)
        .when(pl.col("lead2_at").is_not_null()).then(2).when(pl.col("lead1_at").is_not_null()).then(1)
        .when(pl.col("lead0_at").is_not_null()).then(0).otherwise(99).alias("lead_max_type_id"),
        (pl.col("client_agent_by_api") == 1).alias("client_agent_by_api"),
        pl.when(pl.col("spot_type_id").is_in([13, 11, 9, 15])).then(pl.col("spot_type_id")).otherwise(13).alias("spot_sector_id"),
        pl.when(is_paid_expr).then(0).otherwise(1).alias("campaign_type_id"),
        pl.when(is_paid_expr).then(pl.lit("Paid")).otherwise(pl.lit("Organic")).alias("campaign_type"),
        # Perfil industrial/oficina: alinear con legacy (stg_s2p_leads).
        # En legacy, lead_*_profile_state = JSON region (estado), lead_*_profile_zone = JSON state (zona/colonia).
        industrial_region.alias("industrial_profile_state"),
        industrial_state.alias("industrial_profile_region"),
        industrial_profile_size_id_expr,
        industrial_profile_size_text_expr,
        industrial_profile_budget_id_expr,
        industrial_profile_budget_text_expr,
        office_region.alias("office_profile_state"),
        office_state.alias("office_profile_region"),
        office_profile_size_id_expr,
        office_profile_size_text_expr,
        office_profile_budget_id_expr,
        office_profile_budget_text_expr,
        project_funnel_relevant_id_expr,
        project_funnel_relevant_text_expr,
        lds_relevant_id_expr,
        lds_relevant_text_expr,
        lead_type_date_expr,
        lead_type_datetime_expr,
        lead_min_at_expr,
        pl.col("created_at").dt.date().alias("created_date"),
        pl.col("updated_at").dt.date().alias("updated_date"),
        pl.col("deleted_at").dt.date().alias("deleted_date"),
    ]).with_columns([
        lead_min_type_expr,
        lead_type_vtl_expr,
    ])
    norm_cols = [c for c in ["mothers_lastname", "company", "hs_utm_campaign"] if c in result.columns]
    if norm_cols:
        result = result.with_columns([
            pl.when(pl.col(c).fill_null("").str.to_uppercase().str.strip_chars().is_in(["NA", "N/A", "NULL", ""]))
            .then(pl.lit(None).cast(pl.Utf8))
            .otherwise(pl.col(c))
            .alias(c)
            for c in norm_cols
        ])
    return result


stg_s2p_clients_new = make_silver_stg_asset(
    "raw_s2p_clients_new",
    _transform_s2p_clients,
    description="Silver STG: complete transformation de clients from Spot2 Platform using reusable functions.",
    partitioned=False,
)
