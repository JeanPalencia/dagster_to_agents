"""
Flow registry for testing.

Maps each flow to its module, gold asset, and schema contracts.
This registry is used by the test harness to dynamically import and materialize flows.
"""

FLOW_REGISTRY = {
    "amenity_description_consistency": {
        "module": "dagster_pipeline.defs.amenity_description_consistency",
        "gold_asset": "gold_amenity_desc_consistency",
        "schema_constant_module": "dagster_pipeline.defs.amenity_description_consistency.gold.gold_amenity_desc_consistency",
        "schema_constant_name": "_PUBLISH_COLUMNS",
        "description": "Checks amenity tags vs spot descriptions for consistency",
    },
    "spot_state_transitions": {
        "module": "dagster_pipeline.defs.spot_state_transitions",
        "gold_asset": "gold_lk_spot_status_history",
        "schema_constant_module": "dagster_pipeline.defs.spot_state_transitions.processing",
        "schema_constant_name": "FINAL_COLUMNS",
        "description": "Incremental spot status history with prev/next transitions and sstd IDs",
    },
    # Additional flows can be added here as they are onboarded to the test suite
}


def get_flow_config(flow_name: str) -> dict:
    """
    Get configuration for a specific flow.

    Args:
        flow_name: Name of the flow (key in FLOW_REGISTRY)

    Returns:
        Flow configuration dict

    Raises:
        KeyError: If flow_name not found in registry
    """
    if flow_name not in FLOW_REGISTRY:
        available = ", ".join(FLOW_REGISTRY.keys())
        raise KeyError(
            f"Flow '{flow_name}' not found in registry. Available flows: {available}"
        )
    return FLOW_REGISTRY[flow_name]
