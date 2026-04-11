# defs/data_lakehouse/gold/gold_bt_lds_lead_spots.py
"""
Gold layer: BT_LDS_LEAD_SPOTS table (Event Log structure).

This table stores the complete history of leads and their projects as events.
Each row represents an event in the lead/project lifecycle.

Structure (funnel - each level adds more detail):
- lead_id: Lead identifier
- project_id: Project identifier (null for lead-level events)
- spot_id: Spot identifier (null for lead/project events)
- appointment_id: Appointment identifier (null for non-visit events)
- apd_id: Appointment date ID (null until confirmed/realized events)
- alg_id: Activity log ID (null until LOI/contract events)
- appointment_last_date_status_id: Last status ID of appointment
- appointment_last_status_at: Date when last status was assigned (MIN of updated_at)
- apd_status_id: Status ID of specific appointment date
- alg_event: Activity log event name (null until LOI/contract events)
- lds_event_id: Event type ID
- lds_event: Event type description
- lds_event_at: Event timestamp

Segmentation fields:
- lead_max_type_id: Maximum lead type ID (1-4, 99)
- lead_max_type: Maximum lead type (L1, L2, L3, L4, Others)
- spot_sector_id: Spot sector ID
- spot_sector: Spot sector (Industrial, Retail, Office, Flex)
- user_industria_role_id: Industry role ID
- user_industria_role: Industry role (Tenant, Landlord, Broker, Developer, Unknown)

Cohort fields:
- lds_cohort_type_id: 1 = new (project within 30 days of lead), 2 = reactivated
- lds_cohort_type: "New" or "Reactivated"
- lds_cohort_at: Cohort date (lead date if new, project date if reactivated)

Event Types:
1. Lead Created
2. Project Created
3. Spot Added
4. Visit Created
5. Visit Confirmed (status IN 3,4,6,7,8,9,18,19)
6. Visit Realized (status = 4)
7. LOI (Letter of Intent)
8. Contract
9. Spot Won (projectSpotWon - spot level)
10. Transaction (project level - project_won_date)

Filters:
- Leads: lead_lds_relevant_id == 1 (has_any_lead_at, not deleted, not spot2 email)
- Projects: No filter (all projects for relevant leads).
  Known anomaly: leads with user_id NULL (e.g. lead_id 12553) can get project_id from source
  (client_id) that does not exist in MySQL, causing phantom project and wrong lead_max_type;
  fix should be in source/silver or by validating project_id against canonical project list.
"""
import dagster as dg
import polars as pl
from datetime import datetime, date

from dagster_pipeline.defs.data_lakehouse.shared import daily_partitions
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute


def _transform_bt_lds_lead_spots(
    stg_s2p_leads: pl.DataFrame,
    stg_s2p_projects: pl.DataFrame,
    stg_s2p_prs_project_spots: pl.DataFrame,
    stg_s2p_appointments: pl.DataFrame,
    stg_s2p_apd_appointment_dates: pl.DataFrame,
    stg_s2p_alg_activity_log_projects: pl.DataFrame,
    stg_s2p_user_profiles: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds the gold_bt_lds_lead_spots event log table.
    
    Event blocks (funnel structure - each block filters from the previous):
    1. Lead Created - One event per relevant lead
    2. Project Created - One event per project linked to relevant leads
    3. Spot Added - One event per spot added to a project
    4. Visit Created - One event per visit/appointment created
    5. Visit Confirmed - Visits with status IN (3,4,6,7,8,9,18,19)
    6. Visit Realized - Visits with status = 4 (Visited)
    7. LOI - Letter of Intent events (alg_loi_event_id == 1)
    8. Contract - Contract events (alg_contract_event_id == 1)
    9. Spot Won - Spot-level won events (alg_event == "projectSpotWon")
    10. Transaction - Project-level won (project_won_date not null)
    """

    # =========================================================================
    # BLOCK 1: LEAD CREATED EVENTS
    # =========================================================================
    # One event per relevant lead with lead_type_date_start as event date

    df_lead_events = (
        stg_s2p_leads
        .filter(pl.col("lead_lds_relevant_id") == 1)
        .select([
            pl.col("lead_id"),
            pl.lit(None).cast(pl.Int64).alias("project_id"),
            pl.lit(None).cast(pl.Int64).alias("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(1).alias("lds_event_id"),
            pl.lit("Lead Created").alias("lds_event"),
            pl.col("lead_type_datetime_start").alias("lds_event_at"),
        ])
    )

    # =========================================================================
    # BLOCK 2: PROJECT CREATED EVENTS
    # =========================================================================
    # One event per project linked to relevant leads

    # Get relevant lead_ids
    relevant_lead_ids = (
        stg_s2p_leads
        .filter(pl.col("lead_lds_relevant_id") == 1)
        .select("lead_id")
    )

    df_project_events = (
        stg_s2p_projects
        .join(relevant_lead_ids, on="lead_id", how="inner")  # Only projects from relevant leads
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.lit(None).cast(pl.Int64).alias("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(2).alias("lds_event_id"),
            pl.lit("Project Created").alias("lds_event"),
            pl.col("project_created_at").alias("lds_event_at"),
        ])
    )

    # =========================================================================
    # BASE: RELEVANT SPOTS (used for blocks 3, 4, etc.)
    # =========================================================================
    # Projects from relevant leads with their spots

    relevant_projects = (
        stg_s2p_projects
        .join(relevant_lead_ids, on="lead_id", how="inner")
        .select(["project_id", "lead_id"])
    )

    df_relevant_spots = (
        stg_s2p_prs_project_spots
        .join(relevant_projects, on="project_id", how="inner")
        # Base fields: lead_id, project_id, spot_id + project_spot fields
    )

    # =========================================================================
    # BLOCK 3: SPOT ADDED EVENTS
    # =========================================================================
    # One event per spot added to a project (from relevant leads)

    df_spot_events = (
        df_relevant_spots
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(3).alias("lds_event_id"),
            pl.lit("Spot Added").alias("lds_event"),
            pl.col("project_spot_created_at").alias("lds_event_at"),
        ])
    )

    # =========================================================================
    # BLOCK 4: VISIT CREATED EVENTS
    # =========================================================================
    # One event per visit/appointment created for relevant project-spot combinations

    # Calculate the date when the last status was assigned
    # Uses the MINIMUM of updated_at from both tables as a "double check"
    # to reduce the probability that one was updated for a different reason
    df_last_status_date = (
        stg_s2p_appointments
        .join(stg_s2p_apd_appointment_dates, on="appointment_id", how="inner")
        .filter(
            pl.col("apd_status_id") == pl.col("appointment_last_date_status_id")
        )
        .with_columns(
            # Take the minimum of both updated_at (more likely to be the status change)
            pl.min_horizontal("appointment_updated_at", "apd_updated_at")
              .alias("status_updated_at")
        )
        .group_by("appointment_id")
        .agg(
            pl.col("status_updated_at").max().alias("appointment_last_status_at")
        )
    )

    # Base for visit events (used for blocks 4, 5, 6)
    # Inner join: only spots with appointments (funnel filter)
    df_visits_base = (
        df_relevant_spots
        .select(["lead_id", "project_id", "spot_id"])
        .join(
            stg_s2p_appointments,
            on=["project_id", "spot_id"],
            how="inner"
        )
        .join(
            df_last_status_date,
            on="appointment_id",
            how="left"  # LEFT to not lose appointments without matching status
        )
    )

    df_visit_created_events = (
        df_visits_base
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.col("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.col("appointment_last_date_status_id"),
            pl.col("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(4).alias("lds_event_id"),
            pl.lit("Visit Created").alias("lds_event"),
            pl.col("appointment_created_at").alias("lds_event_at"),
        ])
    )

    # =========================================================================
    # BLOCK 5: VISIT CONFIRMED EVENTS
    # =========================================================================
    # Visits with status IN (3=AgentConfirmed, 4=Visited, 6=Canceled, 7=ClientConfirmed,
    #                        8=ClientCanceled, 9=ContactConfirmed, 18=ContactCanceled,
    #                        19=RescheduledCanceled)
    # Event date: apd_at (appointment date)

    confirmed_statuses = [3, 4, 6, 7, 8, 9, 18, 19]

    df_visit_confirmed_events = (
        df_visits_base
        .join(
            stg_s2p_apd_appointment_dates,
            on="appointment_id",
            how="inner"
        )
        .filter(pl.col("apd_status_id").is_in(confirmed_statuses))
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.col("appointment_id"),
            pl.col("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.col("appointment_last_date_status_id"),
            pl.col("appointment_last_status_at"),
            pl.col("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(5).alias("lds_event_id"),
            pl.lit("Visit Confirmed").alias("lds_event"),
            pl.col("apd_at").alias("lds_event_at"),
        ])
    )

    # =========================================================================
    # BLOCK 6: VISIT REALIZED EVENTS
    # =========================================================================
    # Visits with status = 4 (Visited)
    # Event date: apd_at (appointment date)

    df_visit_realized_events = (
        df_visits_base
        .join(
            stg_s2p_apd_appointment_dates,
            on="appointment_id",
            how="inner"
        )
        .filter(pl.col("apd_status_id") == 4)
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.col("appointment_id"),
            pl.col("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.col("appointment_last_date_status_id"),
            pl.col("appointment_last_status_at"),
            pl.col("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(6).alias("lds_event_id"),
            pl.lit("Visit Realized").alias("lds_event"),
            pl.col("apd_at").alias("lds_event_at"),
        ])
    )

    # =========================================================================
    # BLOCK 7: LOI (LETTER OF INTENT) EVENTS
    # =========================================================================
    # LOI events from activity_log (alg_loi_event_id == 1)
    # Multiple rows per (lead_id, project_id, spot_id) are intentional (one per alg_id / log event)
    df_loi_events = (
        df_relevant_spots
        .select(["lead_id", "project_id", "spot_id"])
        .unique()
        .join(
            stg_s2p_alg_activity_log_projects.filter(pl.col("alg_loi_event_id") == 1),
            on=["project_id", "spot_id"],
            how="inner"
        )
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.col("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.col("alg_event").cast(pl.Utf8),
            pl.lit(7).alias("lds_event_id"),
            pl.lit("LOI").alias("lds_event"),
            pl.col("alg_created_at").alias("lds_event_at"),
        ])
    )

    # =========================================================================
    # BLOCK 8: CONTRACT EVENTS
    # =========================================================================
    # Contract events from activity_log (alg_contract_event_id == 1)
    # Multiple rows per (lead_id, project_id, spot_id) are intentional (one per alg_id)
    df_contract_events = (
        df_relevant_spots
        .select(["lead_id", "project_id", "spot_id"])
        .unique()
        .join(
            stg_s2p_alg_activity_log_projects.filter(pl.col("alg_contract_event_id") == 1),
            on=["project_id", "spot_id"],
            how="inner"
        )
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.col("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.col("alg_event").cast(pl.Utf8),
            pl.lit(8).alias("lds_event_id"),
            pl.lit("Contract").alias("lds_event"),
            pl.col("alg_created_at").alias("lds_event_at"),
        ])
    )

    # =========================================================================
    # BLOCK 9: SPOT WON EVENTS
    # =========================================================================
    # Spot won events from activity_log (alg_event == "projectSpotWon")
    # Multiple rows per (lead_id, project_id, spot_id) are intentional (one per alg_id)
    df_spot_won_events = (
        df_relevant_spots
        .select(["lead_id", "project_id", "spot_id"])
        .unique()
        .join(
            stg_s2p_alg_activity_log_projects.filter(pl.col("alg_event") == "projectSpotWon"),
            on=["project_id", "spot_id"],
            how="inner"
        )
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.col("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.col("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.col("alg_event").cast(pl.Utf8),
            pl.lit(9).alias("lds_event_id"),
            pl.lit("Spot Won").alias("lds_event"),
            pl.col("alg_created_at").alias("lds_event_at"),
        ])
    )

    # =========================================================================
    # BLOCK 10: TRANSACTION (PROJECT LEVEL)
    # =========================================================================
    # Transaction at project level - projects with won_date not null
    # Base: unique combinations of lead_id, project_id from block 7 (LOI)
    # spot_id and activity log fields are null for this block

    df_transaction_events = (
        df_relevant_spots
        .select(["lead_id", "project_id"])
        .unique()
        .join(
            stg_s2p_projects.filter(pl.col("project_won_date").is_not_null()),
            on="project_id",
            how="inner"
        )
        .select([
            pl.col("lead_id"),
            pl.col("project_id"),
            pl.lit(None).cast(pl.Int64).alias("spot_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_id"),
            pl.lit(None).cast(pl.Int64).alias("apd_id"),
            pl.lit(None).cast(pl.Int64).alias("alg_id"),
            pl.lit(None).cast(pl.Int64).alias("appointment_last_date_status_id"),
            pl.lit(None).cast(pl.Datetime).alias("appointment_last_status_at"),
            pl.lit(None).cast(pl.Int64).alias("apd_status_id"),
            pl.lit(None).cast(pl.String).alias("alg_event"),
            pl.lit(10).alias("lds_event_id"),
            pl.lit("Transaction").alias("lds_event"),
            pl.col("project_won_date").cast(pl.Datetime).alias("lds_event_at"),
        ])
    )

    # =========================================================================
    # UNION ALL EVENTS
    # =========================================================================

    df_result = pl.concat([
        df_lead_events,
        df_project_events,
        df_spot_events,
        df_visit_created_events,
        df_visit_confirmed_events,
        df_visit_realized_events,
        df_loi_events,
        df_contract_events,
        df_spot_won_events,
        df_transaction_events,
    ])

    # =========================================================================
    # ADD SEGMENTATION FIELDS
    # =========================================================================
    # Join with leads to get lead_max_type and spot_sector
    # Join with user_profiles (via user_id from leads) to get industry role

    df_lead_segments = (
        stg_s2p_leads
        .select([
            "lead_id",
            "user_id",
            "lead_max_type_id",
            "lead_max_type",
            "spot_sector_id",
            "spot_sector",
            "lead_type_datetime_start",  # For cohort calculation
        ])
    )

    df_user_segments = (
        stg_s2p_user_profiles
        .select([
            "user_id",
            "user_industria_role_id",
            "user_industria_role",
        ])
    )

    # Join leads with user profiles to get industry role
    df_lead_with_profile = (
        df_lead_segments
        .join(df_user_segments, on="user_id", how="left")
        .drop("user_id")
    )

    # Join result with segmentation fields
    df_result = df_result.join(
        df_lead_with_profile,
        on="lead_id",
        how="left"
    )

    # =========================================================================
    # ADD PROJECT DATES FOR COHORT CALCULATION
    # =========================================================================

    df_project_dates = (
        stg_s2p_projects
        .select(["project_id", "project_created_date"])
    )

    df_result = df_result.join(
        df_project_dates,
        on="project_id",
        how="left"
    )

    # =========================================================================
    # CALCULATE COHORT FIELDS
    # =========================================================================
    # lds_cohort_type_id: 1 = new (project within 30 days of lead), 2 = reactivated
    # lds_cohort_type: "New" or "Reactivated"
    # lds_cohort_at: lead date if new, project date if reactivated

    lead_date = pl.col("lead_type_datetime_start").dt.date()
    project_date = pl.col("project_created_date")
    days_diff = (project_date - lead_date).dt.total_days()

    # Flag: 1 if project_id is null (block 1) OR if difference <= 30 days
    is_recent = (
        pl.col("project_id").is_null() |
        (days_diff <= 30)
    )

    df_result = df_result.with_columns([
        # lds_cohort_type_id: 1 = new, 2 = reactivated
        pl.when(is_recent).then(1).otherwise(2).alias("lds_cohort_type_id"),

        # lds_cohort_type: catalog
        pl.when(is_recent)
            .then(pl.lit("New"))
            .otherwise(pl.lit("Reactivated"))
            .alias("lds_cohort_type"),

        # lds_cohort_at: lead date if new, project date if reactivated
        pl.when(is_recent)
            .then(lead_date)
            .otherwise(project_date)
            .alias("lds_cohort_at"),
    ])

    # Clean up auxiliary columns
    df_result = df_result.drop(["lead_type_datetime_start", "project_created_date"])

    # =========================================================================
    # ADD AUDIT FIELDS
    # =========================================================================

    now = datetime.now()
    today = date.today()

    df_result = df_result.with_columns([
        pl.lit(now).alias("aud_inserted_at"),
        pl.lit(today).alias("aud_inserted_date"),
        pl.lit(now).alias("aud_updated_at"),
        pl.lit(today).alias("aud_updated_date"),
        pl.lit("bt_lds_lead_spots_bck").alias("aud_job"),
    ])

    return df_result


@dg.asset(
    name="bt_lds_lead_spots_bck",
    partitions_def=daily_partitions,
    group_name="gold",
    description=(
        "Gold: BT_LDS_LEAD_SPOTS (legacy). Loaded by lakehouse_daily_job into bt_lds_lead_spots_bck. "
        "Combines relevant leads, projects, and spots as events."
    ),
)
def gold_bt_lds_lead_spots(
    context: dg.AssetExecutionContext,
    stg_s2p_leads: pl.DataFrame,
    stg_s2p_projects: pl.DataFrame,
    stg_s2p_prs_project_spots: pl.DataFrame,
    stg_s2p_appointments: pl.DataFrame,
    stg_s2p_apd_appointment_dates: pl.DataFrame,
    stg_s2p_alg_activity_log_projects: pl.DataFrame,
    stg_s2p_user_profiles: pl.DataFrame,
) -> pl.DataFrame:
    """
    Gold asset: BT_LDS_LEAD_SPOTS (Event Log).
    
    Dependencies:
    - stg_s2p_leads: Lead data with lead_lds_relevant_id filter
    - stg_s2p_projects: Project data (all projects)
    - stg_s2p_prs_project_spots: Project-Spot relationships
    - stg_s2p_appointments: Appointments/visits data
    - stg_s2p_apd_appointment_dates: Appointment dates (for confirmed/realized status)
    - stg_s2p_alg_activity_log_projects: Activity log for LOI/Contract events
    - stg_s2p_user_profiles: User profiles for industry role
    """
    def body() -> pl.DataFrame:
        partition_key = context.partition_key

        df = _transform_bt_lds_lead_spots(
            stg_s2p_leads=stg_s2p_leads,
            stg_s2p_projects=stg_s2p_projects,
            stg_s2p_prs_project_spots=stg_s2p_prs_project_spots,
            stg_s2p_appointments=stg_s2p_appointments,
            stg_s2p_apd_appointment_dates=stg_s2p_apd_appointment_dates,
            stg_s2p_alg_activity_log_projects=stg_s2p_alg_activity_log_projects,
            stg_s2p_user_profiles=stg_s2p_user_profiles,
        )

        context.log.info(
            f"bt_lds_lead_spots_bck: {df.height} rows for partition {partition_key}"
        )

        return df

    yield from iter_job_wrapped_compute(context, body)
