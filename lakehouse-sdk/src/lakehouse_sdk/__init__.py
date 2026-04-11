"""
Lakehouse SDK - Testing and Development Toolkit for Spot2 Data Lakehouse

This SDK provides tools for:
- Auto-discover and load data lakehouse assets
- Execute assets with automatic dependency resolution
- Validate and profile data
- Test composite transformations before promoting them to production

Example:
    >>> from lakehouse_sdk import load_all_assets, execute_asset, quick_profile
    >>> 
    >>> # Load all assets
    >>> assets = load_all_assets()
    >>> print(f"Available assets: {list(assets.keys())}")
    >>> 
    >>> # Execute an asset
    >>> df = execute_asset("raw_s2p_clients", "2024-12-01")
    >>> quick_profile(df)
"""

__version__ = "0.1.0"

# Core loader functionality
from .loader import (
    load_all_assets,
    load_asset,
    execute_asset,
    list_assets,
    clear_cache,
)

# Context utilities
from .context import (
    create_test_context,
    setup_import_paths,
    DAGSTER_PIPELINE_PATH,
)

# Profiling utilities
from .profiling import (
    quick_profile,
    compare_schemas,
    advanced_profile,
)

# Validation utilities
from .validators import (
    ValidationRule,
    validate_asset,
    print_validation_results,
    CommonRules,
)

__all__ = [
    # Loader
    "load_all_assets",
    "load_asset",
    "execute_asset",
    "list_assets",
    "clear_cache",
    # Context
    "create_test_context",
    "setup_import_paths",
    "DAGSTER_PIPELINE_PATH",
    # Profiling
    "quick_profile",
    "compare_schemas",
    "advanced_profile",
    # Validation
    "ValidationRule",
    "validate_asset",
    "print_validation_results",
    "CommonRules",
]

