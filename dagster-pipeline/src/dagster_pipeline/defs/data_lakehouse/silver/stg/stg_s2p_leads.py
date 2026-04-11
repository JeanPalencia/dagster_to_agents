# defs/data_lakehouse/silver/stg/stg_s2p_leads.py
import dagster as dg
import polars as pl
from datetime import date, datetime

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _transform_s2p_leads(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforma el DataFrame de clientes (raw_s2p_clients)
    para obtener la misma selección de campos que la query MySQL.
    """

    # -------------------------
    # Expresiones auxiliares
    # -------------------------
    fecha_sentinela = date(9999, 12, 31)

    lead_type_date_expr = (
        pl.min_horizontal(
            *[
                pl.col(c).dt.date().fill_null(fecha_sentinela)
                for c in ["lead0_at", "lead1_at", "lead2_at", "lead3_at", "lead4_at"]
            ]
        ).alias("lead_type_date_start")
    )

    # lead_type_datetime_start: same as above but preserving timestamp
    datetime_sentinela = datetime(9999, 12, 31, 23, 59, 59)
    lead_type_datetime_expr = (
        pl.min_horizontal(
            *[
                pl.col(c).fill_null(datetime_sentinela)
                for c in ["lead0_at", "lead1_at", "lead2_at", "lead3_at", "lead4_at"]
            ]
        ).alias("lead_type_datetime_start")
    )

    # Patrón para campañas "pagadas" por utm (simula LIKE '%...%')
    patron_campania = "BOFU|MOFU|TOFU|LOFU|Conversiones|BrandTerms"

    is_paid_campaign_expr = (
        pl.col("hs_utm_campaign")
        .fill_null("")
        .str.to_uppercase()
        .str.contains(patron_campania)
    )

    is_paid_origin_expr = pl.col("origin").is_in([6, 7, 8])
    is_paid_expr = is_paid_campaign_expr | is_paid_origin_expr

    # lead_max_type (tal cual CASE del SQL)
    lead_max_type_expr = (
        pl.when(pl.col("lead4_at").is_not_null())
        .then(pl.lit("L4"))
        .when(pl.col("lead3_at").is_not_null())
        .then(pl.lit("L3"))
        .when(pl.col("lead2_at").is_not_null())
        .then(pl.lit("L2"))
        .when(pl.col("lead1_at").is_not_null())
        .then(pl.lit("L1"))
        .when(pl.col("lead0_at").is_not_null())
        .then(pl.lit("L0"))
        .otherwise(pl.lit("Others"))
        .alias("lead_max_type")
    )

    # lead_origin (CASE origin WHEN 1 THEN 'Spot2' WHEN 2 THEN 'Hubspot' END)
    lead_origin_expr = (
        pl.when(pl.col("origin") == 1)
        .then(pl.lit("Spot2"))
        .when(pl.col("origin") == 2)
        .then(pl.lit("Hubspot"))
        .otherwise(pl.lit(None))
        .alias("lead_origin")
    )

    # lead_client_type (CASE client_type ...)
    lead_client_type_expr = (
        pl.when(pl.col("client_type") == 1)
        .then(pl.lit("Tenant"))
        .when(pl.col("client_type") == 2)
        .then(pl.lit("Broker"))
        .when(pl.col("client_type") == 3)
        .then(pl.lit("Landlord"))
        .otherwise(pl.lit(None))
        .alias("lead_client_type")
    )

    # lead_status (CASE client_state ...)
    cs = pl.col("client_state")
    lead_status_expr = (
        pl.when(cs == 1)
        .then(pl.lit("New Client"))
        .when(cs == 2)
        .then(pl.lit("Reactivated"))
        .when(cs == 3)
        .then(pl.lit("Open Requirement"))
        .when(cs == 4)
        .then(pl.lit("Sales Nurturing"))
        .when(cs == 5)
        .then(pl.lit("Stand By"))
        .when(cs == 6)
        .then(pl.lit("Residential Search"))
        .when(cs == 7)
        .then(pl.lit("Low Budget"))
        .when(cs == 8)
        .then(pl.lit("Off-Market Footage"))
        .when(cs == 9)
        .then(pl.lit("Other Reason"))
        .when(cs == 10)
        .then(pl.lit("Budget Not Met"))
        .when(cs == 11)
        .then(pl.lit("Footage Requirements Not Met"))
        .when(cs == 12)
        .then(pl.lit("No Offer"))
        .when(cs == 13)
        .then(pl.lit("Never Replied"))
        .when(cs == 14)
        .then(pl.lit("Stopped Replying"))
        .when(cs == 15)
        .then(pl.lit("No Longer Searching"))
        .when(cs == 16)
        .then(pl.lit("Other Nurturing"))
        .when(cs == 17)
        .then(pl.lit("Contact Attempt"))
        .when(cs == 18)
        .then(pl.lit("No Profile"))
        .when(cs == 19)
        .then(pl.lit("Spot Not Available"))
        .when(cs == 20)
        .then(pl.lit("Documentation Missing"))
        .when(cs == 21)
        .then(pl.lit("Visit Not Allowed"))
        .when(cs == 22)
        .then(pl.lit("Wrong Phone Number"))
        .when(cs == 23)
        .then(pl.lit("Duplicated Client"))
        .otherwise(pl.lit(None))
        .alias("lead_status")
    )

    # lead_sourse (CASE hs_client_source ...)
    hcs = pl.col("hs_client_source")
    lead_sourse_expr = (
        pl.when(hcs == 1)
        .then(pl.lit("Created by Spot Agent"))
        .when(hcs == 2)
        .then(pl.lit("Paid social media"))
        .when(hcs == 3)
        .then(pl.lit("Platform"))
        .when(hcs == 4)
        .then(pl.lit("Trebel"))
        .when(hcs == 5)
        .then(pl.lit("Created by Supply Agent"))
        .when(hcs == 6)
        .then(pl.lit("Paid search"))
        .otherwise(pl.lit(None))
        .alias("lead_sourse")
    )

    # spot_sector (CASE spot_type_id ... ELSE 'Retail')
    st = pl.col("spot_type_id")
    spot_sector_expr = (
        pl.when(st == 13)
        .then(pl.lit("Retail"))
        .when(st == 11)
        .then(pl.lit("Office"))
        .when(st == 9)
        .then(pl.lit("Industrial"))
        .when(st == 15)
        .then(pl.lit("Land"))
        .otherwise(pl.lit("Retail"))
        .alias("spot_sector")
    )

    # JSON helpers
    industrial_region = pl.col("hs_industrial_profiling").str.json_path_match(
        "$[0].region"
    )
    industrial_state = pl.col("hs_industrial_profiling").str.json_path_match(
        "$[0].state"
    )
    industrial_size = pl.col("hs_industrial_profiling").str.json_path_match(
        "$[0].size"
    )
    industrial_budget = pl.col("hs_industrial_profiling").str.json_path_match(
        "$[0].budget"
    )

    office_region = pl.col("hs_office_profiling").str.json_path_match("$[0].region")
    office_state = pl.col("hs_office_profiling").str.json_path_match("$[0].state")
    office_size = pl.col("hs_office_profiling").str.json_path_match("$[0].size")
    office_budget = pl.col("hs_office_profiling").str.json_path_match("$[0].budget")

    # lead_industrial_profile_size_id (CASE JSON_UNQUOTE(size))
    lead_industrial_profile_size_id_expr = (
        pl.when(industrial_size == "1")
        .then(1)
        .when(industrial_size == "2")
        .then(2)
        .when(industrial_size == "3")
        .then(3)
        .when(industrial_size == "4")
        .then(4)
        .when(industrial_size == "5")
        .then(5)
        .when(industrial_size == "6")
        .then(6)
        .when(industrial_size == "7")
        .then(7)
        .when(industrial_size == "8")
        .then(8)
        .when(industrial_size == "9")
        .then(9)
        .when(industrial_size == "10")
        .then(10)
        .otherwise(None)
        .cast(pl.Int64)
        .alias("lead_industrial_profile_size_id")
    )

    # lead_industrial_profile_size (texto)
    lead_industrial_profile_size_expr = (
        pl.when(industrial_size == "1")
        .then(pl.lit("Less than 1,000m²"))
        .when(industrial_size == "2")
        .then(pl.lit("From 1,000m² to 3,000m²"))
        .when(industrial_size == "3")
        .then(pl.lit("From 3,000m² to 10,000m²"))
        .when(industrial_size == "4")
        .then(pl.lit("From 10,000m² to 20,000m²"))
        .when(industrial_size == "5")
        .then(pl.lit("More than 20,000m²"))
        .when(industrial_size == "6")
        .then(pl.lit("Less than 700m²"))
        .when(industrial_size == "7")
        .then(pl.lit("From 700m² to 2,000m²"))
        .when(industrial_size == "8")
        .then(pl.lit("From 2,000m² to 10,000m²"))
        .when(industrial_size == "9")
        .then(pl.lit("More than 100,000m²"))
        .when(industrial_size == "10")
        .then(pl.lit("Less than 10,000m²"))
        .otherwise(pl.lit(None))
        .alias("lead_industrial_profile_size")
    )

    # lead_industrial_profile_budget_id
    lead_industrial_profile_budget_id_expr = (
        pl.when(industrial_budget == "1")
        .then(1)
        .when(industrial_budget == "2")
        .then(2)
        .when(industrial_budget == "3")
        .then(3)
        .when(industrial_budget == "4")
        .then(4)
        .when(industrial_budget == "5")
        .then(5)
        .when(industrial_budget == "6")
        .then(6)
        .when(industrial_budget == "7")
        .then(7)
        .when(industrial_budget == "8")
        .then(8)
        .when(industrial_budget == "9")
        .then(9)
        .when(industrial_budget == "10")
        .then(10)
        .otherwise(None)
        .cast(pl.Int64)
        .alias("lead_industrial_profile_budget_id")
    )

    # lead_industrial_profile_budget (texto)
    lead_industrial_profile_budget_expr = (
        pl.when(industrial_budget == "1")
        .then(pl.lit("Less than $100,000 MXN"))
        .when(industrial_budget == "2")
        .then(pl.lit("From $100,000 to $300,000 MXN"))
        .when(industrial_budget == "3")
        .then(pl.lit("From $300,000 to $1,000,000 MXN"))
        .when(industrial_budget == "4")
        .then(pl.lit("More than $1,000,000 MXN"))
        .when(industrial_budget == "5")
        .then(pl.lit("Less than $60,000 MXN"))
        .when(industrial_budget == "6")
        .then(pl.lit("From $60,000 to $150,000 MXN"))
        .when(industrial_budget == "7")
        .then(pl.lit("From $150,000 to $300,000 MXN"))
        .when(industrial_budget == "8")
        .then(pl.lit("More than $300,000 MXN"))
        .when(industrial_budget == "9")
        .then(pl.lit("Less than $30,000 MXN"))
        .when(industrial_budget == "10")
        .then(pl.lit("From $30,000 to $100,000 MXN"))
        .otherwise(pl.lit(None))
        .alias("lead_industrial_profile_budget")
    )

    # lead_office_profile_size_id
    lead_office_profile_size_id_expr = (
        pl.when(office_size == "1")
        .then(1)
        .when(office_size == "2")
        .then(2)
        .when(office_size == "3")
        .then(3)
        .when(office_size == "4")
        .then(4)
        .when(office_size == "5")
        .then(5)
        .when(office_size == "6")
        .then(6)
        .otherwise(None)
        .cast(pl.Int64)
        .alias("lead_office_profile_size_id")
    )

    # lead_office_profile_size
    lead_office_profile_size_expr = (
        pl.when(office_size == "1")
        .then(pl.lit("Less than 100m²"))
        .when(office_size == "2")
        .then(pl.lit("From 100 to 200m²"))
        .when(office_size == "3")
        .then(pl.lit("Less than 200m²"))
        .when(office_size == "4")
        .then(pl.lit("From 200 to 500m²"))
        .when(office_size == "5")
        .then(pl.lit("From 500 to 1000m²"))
        .when(office_size == "6")
        .then(pl.lit("More than 1000m²"))
        .otherwise(pl.lit(None))
        .alias("lead_office_profile_size")
    )

    # lead_office_profile_budget_id
    lead_office_profile_budget_id_expr = (
        pl.when(office_budget == "1")
        .then(1)
        .when(office_budget == "2")
        .then(2)
        .when(office_budget == "3")
        .then(3)
        .when(office_budget == "4")
        .then(4)
        .when(office_budget == "5")
        .then(5)
        .when(office_budget == "6")
        .then(6)
        .when(office_budget == "7")
        .then(7)
        .when(office_budget == "8")
        .then(8)
        .otherwise(None)
        .cast(pl.Int64)
        .alias("lead_office_profile_budget_id")
    )

    # lead_office_profile_budget
    lead_office_profile_budget_expr = (
        pl.when(office_budget == "1")
        .then(pl.lit("Less than $25,000 MXN"))
        .when(office_budget == "2")
        .then(pl.lit("From $25,000 to $75,000 MXN"))
        .when(office_budget == "3")
        .then(pl.lit("From $75,000 to $150,000 MXN"))
        .when(office_budget == "4")
        .then(pl.lit("More than $150,000 MXN"))
        .when(office_budget == "5")
        .then(pl.lit("Less than $50,000 MXN"))
        .when(office_budget == "6")
        .then(pl.lit("From $50,000 to $100,000 MXN"))
        .when(office_budget == "7")
        .then(pl.lit("From $100,000 to $200,000 MXN"))
        .when(office_budget == "8")
        .then(pl.lit("More than $200,000 MXN"))
        .otherwise(pl.lit(None))
        .alias("lead_office_profile_budget")
    )

    # -------------------------
    # lead_relevant: filtro categórico para leads relevantes
    # -------------------------

    # Condición 1: al menos un leadX_at no es null
    has_any_lead_at = (
        pl.col("lead0_at").is_not_null()
        | pl.col("lead1_at").is_not_null()
        | pl.col("lead2_at").is_not_null()
        | pl.col("lead3_at").is_not_null()
        | pl.col("lead4_at").is_not_null()
    )

    # Condición 2: deleted_at IS NULL
    is_not_deleted = pl.col("deleted_at").is_null()

    # Condición 3: email NOT REGEXP '@spot2\.mx$' OR email IS NULL
    is_not_spot2_email = ~pl.col("email").fill_null("").str.contains(r"@spot2\.mx$")

    # Expresión combinada para lead relevante (sin restricción de fecha)
    is_lead_relevant = (
        has_any_lead_at
        & is_not_deleted
        & is_not_spot2_email
    )

    # lead_project_funnel_relevant_id (1 o 0)
    lead_project_funnel_relevant_id_expr = (
        pl.when(is_lead_relevant)
        .then(1)
        .otherwise(0)
        .alias("lead_project_funnel_relevant_id")
    )

    # lead_project_funnel_relevant ('Yes' o 'No')
    lead_project_funnel_relevant_expr = (
        pl.when(is_lead_relevant)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("lead_project_funnel_relevant")
    )

    # =====================================================================
    # LEAD LDS RELEVANT (para tabla BT_LDS_LEAD_SPOTS)
    # Mismas condiciones que lead_project_funnel_relevant EXCEPTO fecha:
    # - has_any_lead_at (al menos un leadX_at no es null)
    # - is_not_deleted (deleted_at IS NULL)
    # - is_not_spot2_email (email NOT like '@spot2.mx')
    # =====================================================================
    is_lead_lds_relevant = has_any_lead_at & is_not_deleted & is_not_spot2_email

    # lead_lds_relevant_id (1 o 0)
    lead_lds_relevant_id_expr = (
        pl.when(is_lead_lds_relevant)
        .then(1)
        .otherwise(0)
        .alias("lead_lds_relevant_id")
    )

    # lead_lds_relevant ('Yes' o 'No')
    lead_lds_relevant_expr = (
        pl.when(is_lead_lds_relevant)
        .then(pl.lit("Yes"))
        .otherwise(pl.lit("No"))
        .alias("lead_lds_relevant")
    )

    # ----------------------------------
    # Nuevo DataFrame: df.select([...])
    # ----------------------------------
    return df.select(
        [
            # id AS lead_id
            pl.col("id").alias("lead_id"),
            
            # Clean newlines from name fields (prevents CSV import issues)
            pl.col("name")
            .str.replace_all(r"[\n\r]+", " ")
            .str.strip_chars()
            .alias("lead_name"),
            
            pl.col("lastname")
            .str.replace_all(r"[\n\r]+", " ")
            .str.strip_chars()
            .alias("lead_last_name"),
            
            pl.col("mothers_lastname")
            .str.replace_all(r"[\n\r]+", " ")
            .str.strip_chars()
            .alias("lead_mothers_last_name"),
            
            pl.col("email").alias("lead_email"),

            # LOWER(TRIM(SUBSTRING_INDEX(email, '@', -1))) AS lead_domain
            pl.col("email")
            .str.split("@")
            .list.get(-1)
            .str.strip_chars()
            .str.to_lowercase()
            .alias("lead_domain"),

            pl.col("phone_indicator").alias("lead_phone_indicator"),
            pl.col("phone_number").alias("lead_phone_number"),

            # CONCAT(IF(phone_indicator != '', CONCAT(phone_indicator, ' '), ''), phone_number)
            pl.when(pl.col("phone_indicator") != "")
            .then(
                pl.col("phone_indicator").cast(pl.String)
                + pl.lit(" ")
                + pl.col("phone_number").cast(pl.String)
            )
            .otherwise(pl.col("phone_number").cast(pl.String))
            .alias("lead_full_phone_number"),

            pl.col("company").alias("lead_company"),
            pl.col("position").alias("lead_position"),

            pl.col("origin").alias("lead_origin_id"),
            lead_origin_expr,

            # IF(leadX_at IS NOT NULL, TRUE, FALSE)
            pl.col("lead0_at").is_not_null().alias("lead_l0"),
            pl.col("lead1_at").is_not_null().alias("lead_l1"),
            pl.col("lead2_at").is_not_null().alias("lead_l2"),
            pl.col("lead3_at").is_not_null().alias("lead_l3"),
            pl.col("lead4_at").is_not_null().alias("lead_l4"),

            pl.col("supply_at").is_not_null().alias("lead_supply"),
            pl.col("supply0_at").is_not_null().alias("lead_s0"),
            pl.col("supply1_at").is_not_null().alias("lead_s1"),
            pl.col("supply3_at").is_not_null().alias("lead_s3"),

            # lead_max_type_id
            pl.when(pl.col("lead4_at").is_not_null())
            .then(4)
            .when(pl.col("lead3_at").is_not_null())
            .then(3)
            .when(pl.col("lead2_at").is_not_null())
            .then(2)
            .when(pl.col("lead1_at").is_not_null())
            .then(1)
            .when(pl.col("lead0_at").is_not_null())
            .then(0)
            .otherwise(99)
            .alias("lead_max_type_id"),

            # lead_max_type
            lead_max_type_expr,

            pl.col("client_type").alias("lead_client_type_id"),
            lead_client_type_expr,

            pl.col("user_id").alias("user_id"),
            pl.col("client_agent_id").alias("agent_id"),
            # Convert TINYINT (0/1) to proper boolean for PostgreSQL CSV compatibility
            (pl.col("client_agent_by_api") == 1).alias("lead_agent_by_api"),
            pl.col("kam_agent_id").alias("agent_kam_id"),

            # spot_sector_id
            pl.when(pl.col("spot_type_id").is_in([13, 11, 9, 15]))
            .then(pl.col("spot_type_id"))
            .otherwise(13)
            .alias("spot_sector_id"),

            # spot_sector
            spot_sector_expr,

            pl.col("client_state").alias("lead_status_id"),
            lead_status_expr,

            pl.col("client_first_contact_time").alias("lead_first_contact_time"),

            pl.col("min_square_space").alias("spot_min_square_space"),
            pl.col("max_square_space").alias("spot_max_square_space"),

            pl.col("hs_client_source").alias("lead_sourse_id"),
            lead_sourse_expr,

            pl.col("hs_first_conversion").alias("lead_hs_first_conversion"),
            pl.col("hs_utm_campaign").alias("lead_hs_utm_campaign"),

            # lead_campaign_type_id
            pl.when(is_paid_expr)
            .then(0)
            .otherwise(1)
            .alias("lead_campaign_type_id"),

            # lead_campaign_type
            pl.when(is_paid_expr)
            .then(pl.lit("Paid"))
            .otherwise(pl.lit("Organic"))
            .alias("lead_campaign_type"),

            pl.col("hs_analytics_source_data_1").alias("lead_hs_analytics_source_data_1"),
            pl.col("hs_recent_conversion").alias("lead_hs_recent_conversion"),
            pl.col("hs_recent_conversion_date").alias("lead_hs_recent_conversion_date"),

            pl.col("hs_industrial_profiling").alias("lead_hs_industrial_profiling"),

            industrial_region.alias("lead_industrial_profile_state"),
            industrial_state.alias("lead_industrial_profile_zone"),
            lead_industrial_profile_size_id_expr,
            lead_industrial_profile_size_expr,
            lead_industrial_profile_budget_id_expr,
            lead_industrial_profile_budget_expr,

            pl.col("hs_office_profiling").alias("lead_hs_office_profiling"),

            office_region.alias("lead_office_profile_state"),
            office_state.alias("lead_office_profile_zone"),
            lead_office_profile_size_id_expr,
            lead_office_profile_size_expr,
            lead_office_profile_budget_id_expr,
            lead_office_profile_budget_expr,

            # lead_project_funnel_relevant: filtro categórico para funnel de proyectos
            lead_project_funnel_relevant_id_expr,
            lead_project_funnel_relevant_expr,

            # lead_lds_relevant: filtro para tabla BT_LDS_LEAD_SPOTS
            lead_lds_relevant_id_expr,
            lead_lds_relevant_expr,

            # timestamps originales renombrados
            pl.col("lead0_at").alias("lead_lead0_at"),
            pl.col("lead1_at").alias("lead_lead1_at"),
            pl.col("lead2_at").alias("lead_lead2_at"),
            pl.col("lead3_at").alias("lead_lead3_at"),
            pl.col("lead4_at").alias("lead_lead4_at"),

            pl.col("supply_at").alias("lead_supply_at"),
            pl.col("supply0_at").alias("lead_supply0_at"),
            pl.col("supply1_at").alias("lead_supply1_at"),
            pl.col("supply3_at").alias("lead_supply3_at"),

            pl.col("client_at").alias("lead_client_at"),

            # DATE(LEAST(COALESCE(...)))
            lead_type_date_expr,
            lead_type_datetime_expr,  # Same but with full timestamp

            pl.col("profiling_completed_at").alias("lead_profiling_completed_at"),

            pl.col("created_at").alias("lead_created_at"),
            pl.col("created_at").dt.date().alias("lead_created_date"),

            pl.col("updated_at").alias("lead_updated_at"),
            pl.col("updated_at").dt.date().alias("lead_updated_date"),

            pl.col("deleted_at").alias("lead_deleted_at"),
            pl.col("deleted_at").dt.date().alias("lead_deleted_date"),
        ]
    )


@dg.asset(
    partitions_def=daily_partitions,
    group_name="silver",
    description="Silver STG: simple transformation de clients to leads from Spot2 Platform.",
)
def stg_s2p_leads(
    context: dg.AssetExecutionContext,
    raw_s2p_clients: pl.DataFrame,
):
    """
    Transforma raw_s2p_clients con lógica de negocio básica.
    Este asset es reutilizado en etapas posteriores del pipeline.
    """
    def body():
        df_leads = _transform_s2p_leads(raw_s2p_clients)
        context.log.info(
            f"stg_s2p_leads: {df_leads.height} rows for partition {context.partition_key}"
        )
        return df_leads

    yield from iter_job_wrapped_compute(context, body)
