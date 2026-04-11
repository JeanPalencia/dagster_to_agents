import dagster as dg
from pathlib import Path
from typing import Any


# Configuration
STORAGE_DIR = Path("/home/ubuntu/dagster/dagster-pipeline/storage")


def _cleanup_all_storage_files(context: dg.AssetExecutionContext) -> dict:
    """Remove ALL pickled DataFrame files from storage directory."""
    total_freed = 0
    deleted_count = 0

    if not STORAGE_DIR.exists():
        context.log.warning(f"Storage directory does not exist: {STORAGE_DIR}")
        return {"freed_bytes": 0, "deleted_files": 0}

    for asset_dir in STORAGE_DIR.iterdir():
        if asset_dir.is_dir():
            for partition_file in asset_dir.iterdir():
                if partition_file.is_file():
                    try:
                        size = partition_file.stat().st_size
                        partition_file.unlink()
                        total_freed += size
                        deleted_count += 1
                        context.log.info(
                            f"Deleted: {asset_dir.name}/{partition_file.name} ({size / 1024:.1f} KB)"
                        )
                    except OSError as e:
                        context.log.warning(f"Failed to delete {partition_file}: {e}")

    return {"freed_bytes": total_freed, "deleted_files": deleted_count}


# ---------------------------------------------------------------------------
# Run storage cleanup (deshabilitado: conserva historial en Postgres).
# Para reactivar: descomentar imports, la función y el bloque STEP 2 en
# cleanup_old_partitions (y el campo runs_deleted en el return).
# ---------------------------------------------------------------------------
# from dagster import RunsFilter
# from dagster._core.storage.dagster_run import DagsterRunStatus
#
#
# def _cleanup_all_runs(context: dg.AssetExecutionContext) -> dict:
#     """Delete ALL completed runs from Dagster's run storage."""
#     deleted_count = 0
#
#     try:
#         instance = context.instance
#
#         for status in [DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED]:
#             runs = instance.get_runs(filters=RunsFilter(statuses=[status]))
#             context.log.info(f"Found {len(runs)} {status.value} runs to delete")
#
#             for run in runs:
#                 try:
#                     instance.delete_run(run.run_id)
#                     deleted_count += 1
#                     context.log.info(
#                         f"Deleted run: {run.run_id[:8]}... (job: {run.job_name})"
#                     )
#                 except Exception as e:
#                     context.log.warning(f"Failed to delete run {run.run_id}: {e}")
#
#     except Exception as e:
#         context.log.error(f"Error accessing Dagster instance: {e}")
#         return {"deleted_runs": 0, "error": str(e)}
#
#     return {"deleted_runs": deleted_count}


@dg.asset(
    group_name="maintenance",
    description="Daily cleanup: deletes local storage/ files only; run history stays in Postgres.",
)
def cleanup_old_partitions(context: dg.AssetExecutionContext) -> dict:
    """
    Borra archivos bajo storage/ en disco (pickles locales) para liberar espacio en la EC2.

    El borrado masivo de runs vía instance.delete_run está deshabilitado (código comentado
    arriba) para conservar el historial en Postgres.
    """
    context.log.info("=" * 60)
    context.log.info("MAINTENANCE: Starting daily cleanup (storage/ only, runs preserved)")
    context.log.info("=" * 60)

    context.log.info("")
    context.log.info("--- Deleting ALL storage files under storage/ ---")
    storage_result = _cleanup_all_storage_files(context)
    freed_gb = storage_result["freed_bytes"] / (1024**3)
    context.log.info(
        f"Storage cleanup complete: {storage_result['deleted_files']} files deleted, "
        f"{freed_gb:.2f} GB freed"
    )

    # # --- STEP 2 (opcional): borrar runs completados del run storage ---
    # context.log.info("")
    # context.log.info("--- STEP 2: Deleting ALL completed runs ---")
    # run_result = _cleanup_all_runs(context)
    # context.log.info(f"Run cleanup complete: {run_result['deleted_runs']} runs deleted")

    context.log.info("")
    context.log.info("=" * 60)
    context.log.info("MAINTENANCE: Cleanup complete")
    context.log.info(f"  - Storage files deleted: {storage_result['deleted_files']}")
    context.log.info(f"  - Space freed: {freed_gb:.2f} GB")
    # context.log.info(f"  - Runs deleted: {run_result['deleted_runs']}")
    context.log.info("=" * 60)

    return {
        "storage_files_deleted": storage_result["deleted_files"],
        "storage_freed_gb": round(freed_gb, 2),
        # "runs_deleted": run_result["deleted_runs"],
    }


# ============ Standalone Storage Cleanup Asset ============
# This asset can be added as a final step in any job to clean up storage files
# after the pipeline completes. Add it to job selections to run after other assets.

@dg.asset(
    group_name="maintenance",
    description="Cleans ALL pickled DataFrame files from storage. Add as final step in any pipeline.",
    deps=[],  # No dependencies by default - add to job selection with other assets
)
def cleanup_storage(context: dg.AssetExecutionContext):
    """
    Standalone storage cleanup asset.
    
    Deletes ALL pickled DataFrame files from the storage/ directory.
    Use this asset as the final step in any pipeline to free disk space
    after all data has been processed and written to S3.
    
    To add to a job, include it in the selection:
        selection = your_assets_selection | dg.AssetSelection.assets("cleanup_storage")
    """
    from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute

    def body():
        return _cleanup_storage_body(context)

    yield from iter_job_wrapped_compute(context, body)


def _cleanup_storage_body(context: dg.AssetExecutionContext) -> dict:
    context.log.info("=" * 60)
    context.log.info("CLEANUP_STORAGE: Deleting all storage files")
    context.log.info("=" * 60)

    result = _cleanup_all_storage_files(context)
    freed_gb = result["freed_bytes"] / (1024**3)

    context.log.info("")
    context.log.info(f"Storage cleanup complete:")
    context.log.info(f"  - Files deleted: {result['deleted_files']}")
    context.log.info(f"  - Space freed: {freed_gb:.2f} GB")
    context.log.info("=" * 60)

    return {
        "files_deleted": result["deleted_files"],
        "freed_gb": round(freed_gb, 2),
    }
