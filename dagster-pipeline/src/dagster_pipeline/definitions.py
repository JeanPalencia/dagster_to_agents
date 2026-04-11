from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder

from dagster_pipeline.defs.lk_matches_visitors_to_leads import (
    lk_vtl_clients,
    lk_vtl_conversations,
    lk_vtl_lk_final_output,
    lk_vtl_lk_lead_events,
    lk_vtl_identity,
    lk_vtl_lk_matched,
    lk_vtl_lk_processed_lead_events,
    lk_vtl_job,
    lk_vtl_schedule,
)
from dagster_pipeline.defs.observability import (
    traffic_alerts_job,
    traffic_alerts_schedule,
    traffic_anomaly_alerts,
)

# Assets y jobs del pipeline LK (prefijo lk_ para evitar colisiones con visitors_to_leads)
LK_ASSETS = [
    lk_vtl_clients,
    lk_vtl_conversations,
    lk_vtl_lk_lead_events,
    lk_vtl_identity,
    lk_vtl_lk_processed_lead_events,
    lk_vtl_lk_matched,
    lk_vtl_lk_final_output,
    traffic_anomaly_alerts,
]

from dagster_pipeline.defs.data_lakehouse.s3_bronze_io_manager import s3_bronze_io_manager
from dagster_pipeline.defs.data_lakehouse.s3_silver_io_manager import s3_silver_io_manager
from dagster_pipeline.defs.data_lakehouse.s3_gold_io_manager import s3_gold_io_manager


@definitions
def defs():
    folder_defs = load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)
    lk_defs = Definitions(
        assets=LK_ASSETS,
        jobs=[lk_vtl_job, traffic_alerts_job],
        schedules=[lk_vtl_schedule, traffic_alerts_schedule],
    )
    merged = Definitions.merge(folder_defs, lk_defs)
    return merged.with_resources({
        "s3_bronze": s3_bronze_io_manager,
        "s3_silver": s3_silver_io_manager,
        "s3_gold": s3_gold_io_manager,
    })
