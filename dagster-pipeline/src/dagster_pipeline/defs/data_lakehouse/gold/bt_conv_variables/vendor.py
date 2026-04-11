"""
Single module that imports from `conversation_analysis`.

If that pipeline is retired, swap implementations here (or copy modules into the lakehouse)
without touching other gold/silver assets: import only from
`gold.bt_conv_variables.config`, `gold.bt_conv_variables.phone_formats`, or this `vendor`.
"""
from dagster_pipeline.defs.conversation_analysis.assets import (
    _compute_incremental_mask,
    _enrich_messages_with_events,
    _get_events_in_conversation_window,
    _merge_conv_variables_for_upsert,
    _normalize_jsonb_column,
)
from dagster_pipeline.defs.conversation_analysis.utils.processors import create_conv_variables
from dagster_pipeline.defs.conversation_analysis.utils.s3_upload import sanitize_json_string
from dagster_pipeline.defs.conversation_analysis.utils.variables.experiments import (
    get_bot_graph_variant,
    get_experiment_variables,
    get_unification_mgs_variant,
)
from dagster_pipeline.defs.conversation_analysis.utils.variables.funnel_events import (
    get_funnel_events_variables,
)
from dagster_pipeline.defs.conversation_analysis.utils.variables.segment import (
    build_conv_seniority_map,
    get_seniority_variable,
)
from dagster_pipeline.defs.conversation_analysis.utils.variables.tags import get_follow_up_success

__all__ = [
    "_compute_incremental_mask",
    "_enrich_messages_with_events",
    "_get_events_in_conversation_window",
    "_merge_conv_variables_for_upsert",
    "_normalize_jsonb_column",
    "create_conv_variables",
    "sanitize_json_string",
    "get_bot_graph_variant",
    "get_experiment_variables",
    "get_unification_mgs_variant",
    "get_funnel_events_variables",
    "build_conv_seniority_map",
    "get_seniority_variable",
    "get_follow_up_success",
]
