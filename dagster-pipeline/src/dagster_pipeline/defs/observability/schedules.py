"""Jobs y schedules del módulo de observabilidad (alertas de tráfico)."""

from dagster import DefaultScheduleStatus, ScheduleDefinition, define_asset_job

from dagster_pipeline.defs.observability.traffic_alerts import traffic_anomaly_alerts

traffic_alerts_job = define_asset_job(
    "traffic_alerts_job",
    selection=[traffic_anomaly_alerts],
)

traffic_alerts_schedule = ScheduleDefinition(
    name="traffic_alerts_schedule",
    job=traffic_alerts_job,
    cron_schedule="45 8 * * *",
    execution_timezone="America/Mexico_City",
    default_status=DefaultScheduleStatus.RUNNING,
)
