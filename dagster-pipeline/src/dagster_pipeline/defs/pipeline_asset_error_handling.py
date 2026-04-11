# defs/pipeline_asset_error_handling.py
"""
Error metadata (try/except + yield) for assets that participate in jobs resolved by
_job_names_for_error_metadata() (conv_job, lakehouse_daily_job, and lakehouse _new jobs from
jobs.LAKEHOUSE_NEW_PIPELINE_JOB_NAMES). Job names are imported lazily to avoid circular imports.

lk_vtl_job is handled separately in its module.
"""
from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import dagster as dg


def _job_names_for_error_metadata() -> tuple[str, ...]:
    """Lazy import: jobs module must not be loaded while this package initializes."""
    from dagster_pipeline.defs.data_lakehouse.jobs import LAKEHOUSE_NEW_PIPELINE_JOB_NAMES

    return ("conv_job", "lakehouse_daily_job", *LAKEHOUSE_NEW_PIPELINE_JOB_NAMES)

_wrapped_asset_keys: frozenset[str] | None = None


def _load_wrapped_asset_keys() -> frozenset[str]:
    global _wrapped_asset_keys
    if _wrapped_asset_keys is not None:
        return _wrapped_asset_keys
    from dagster_pipeline.definitions import defs

    repo = defs.get_repository_def()
    keys: set[str] = set()
    for job_name in _job_names_for_error_metadata():
        job = repo.get_job(job_name)
        keys.update(n.name for n in job.nodes_in_topological_order)
    _wrapped_asset_keys = frozenset(keys)
    return _wrapped_asset_keys


def asset_key_should_emit_error_metadata(asset_key: str) -> bool:
    return asset_key in _load_wrapped_asset_keys()


def emit_asset_error_metadata(
    context: dg.AssetExecutionContext, exc: BaseException
) -> Iterator[dg.MaterializeResult]:
    context.log.exception(
        "Asset failed; error metadata was yielded and the exception is re-raised so the step is FAILED"
    )
    meta: dict[str, Any] = {
        "handled": dg.MetadataValue.bool(True),
        "error_type": dg.MetadataValue.text(type(exc).__name__),
        "error_message": dg.MetadataValue.text(str(exc)),
        "asset_key": dg.MetadataValue.text(context.asset_key.to_user_string()),
    }
    pk = getattr(context, "partition_key", None)
    if pk is not None:
        meta["partition_key"] = dg.MetadataValue.text(str(pk))
    yield dg.MaterializeResult(metadata=meta)


def iter_job_wrapped_compute(
    context: dg.AssetExecutionContext,
    compute_fn: Callable[[], Any],
) -> Iterator[Any]:
    """
    Runs compute_fn(); if the asset belongs to a listed job, on error emits metadata and re-raises.
    On success always yields dg.Output(result) (compute must be a zero-arg callable).
    """
    key = context.asset_key.to_user_string()
    if not asset_key_should_emit_error_metadata(key):
        yield dg.Output(compute_fn())
        return
    try:
        yield dg.Output(compute_fn())
    except Exception as exc:
        yield from emit_asset_error_metadata(context, exc)
        raise


def reset_wrapped_asset_keys_cache_for_tests() -> None:
    """For tests / reloading definitions only."""
    global _wrapped_asset_keys
    _wrapped_asset_keys = None
