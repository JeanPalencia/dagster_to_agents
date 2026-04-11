"""
Context utilities for testing Dagster assets
"""
import sys
import os
from pathlib import Path
from datetime import date, timedelta
from typing import Optional
from dagster import build_asset_context


def _find_dagster_pipeline_path():
    """
    Finds the path to dagster-pipeline in a robust way.
    
    Tries multiple strategies:
    1. Relative path from this file
    2. Absolute hardcoded path
    3. DAGSTER_PIPELINE_PATH environment variable
    """
    # Option 1: Relative path from the file
    relative_path = Path(__file__).parent.parent.parent.parent / "dagster-pipeline" / "src"
    if relative_path.exists():
        return relative_path.resolve()
    
    # Option 2: Absolute hardcoded path (fallback)
    absolute_path = Path("/home/luis/Spot2/dagster/dagster-pipeline/src")
    if absolute_path.exists():
        return absolute_path.resolve()
    
    # Option 3: Environment variable
    env_path = os.getenv("DAGSTER_PIPELINE_PATH")
    if env_path and Path(env_path).exists():
        return Path(env_path).resolve()
    
    raise FileNotFoundError(
        "❌ Could not find dagster-pipeline.\n"
        "Options:\n"
        "  1) Run from lakehouse-sdk/ directory\n"
        "  2) Set DAGSTER_PIPELINE_PATH environment variable\n"
        f"  3) Verify that it exists: {absolute_path}"
    )


# Path to the production dagster-pipeline code
DAGSTER_PIPELINE_PATH = _find_dagster_pipeline_path()


def setup_import_paths():
    """
    Configures import paths to access dagster-pipeline.
    
    Adds the production code path to sys.path if not already present.
    """
    dagster_path_str = str(DAGSTER_PIPELINE_PATH)
    if dagster_path_str not in sys.path:
        sys.path.insert(0, dagster_path_str)


def create_test_context(partition_key: Optional[str] = None, **kwargs):
    """
    Creates a test context for Dagster assets.
    
    Args:
        partition_key: Partition date in YYYY-MM-DD format.
                      If None, uses yesterday.
        **kwargs: Additional arguments for build_asset_context
    
    Returns:
        AssetExecutionContext configured for testing
    
    Example:
        >>> context = create_test_context("2024-12-01")
        >>> df = raw_s2p_clients(context)
    """
    if partition_key is None:
        partition_key = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    return build_asset_context(partition_key=partition_key, **kwargs)

