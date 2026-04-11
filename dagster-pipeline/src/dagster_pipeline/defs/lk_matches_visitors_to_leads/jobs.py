"""Jobs and schedules for the LK Visitors to Leads pipeline (lk_matches_visitors_to_leads only)."""
import dagster as dg


# Cleanup asset selection (runs at the end to free disk space)
cleanup_selection = dg.AssetSelection.assets("cleanup_storage")

# LK branch only: lk_vtl_lk_final_output and its upstreams, plus cleanup at the end
lk_vtl_selection = (
    dg.AssetSelection.assets("lk_vtl_lk_final_output").upstream()
    | dg.AssetSelection.assets("lk_vtl_lk_final_output")
    | cleanup_selection
)


lk_vtl_job = dg.define_asset_job(
    name="lk_vtl_job",
    description=(
        "LK Visitors to Leads: pipeline that writes LK_MAT_MATCHES_VISITORS_TO_LEADS "
        "(mat_/vis_/lead_ schema, PK lead_id + lead_fecha_cohort_dt)."
    ),
    selection=lk_vtl_selection,
)


lk_vtl_schedule = dg.ScheduleDefinition(
    name="lk_vtl_schedule",
    job=lk_vtl_job,
    cron_schedule="0 9 * * *",
    execution_timezone="America/Mexico_City",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
