"""Variable creation functions for conversation analysis."""

from dagster_pipeline.defs.conversation_analysis.utils.variables.metrics import (
    get_messages_count,
    get_user_messages_count,
    get_total_time_conversation,
    get_user_average_response_time,
    get_funnel_type,
    get_funnel_section,
    get_user_spot2_links_count,
    get_reco_interact,
)
from dagster_pipeline.defs.conversation_analysis.utils.variables.tags import (
    get_active_user,
    get_engaged_user,
    get_visit_intention,
    get_scheduled_visit,
    get_created_project,
    get_follow_up_success,
)
from dagster_pipeline.defs.conversation_analysis.utils.variables.ai_tags import (
    get_ai_tags,
    process_ai_tags_batch,
)

from dagster_pipeline.defs.conversation_analysis.utils.variables.dspy_evaluator import (
    get_dspy_evaluator_variables,
    process_dspy_evaluator_batch,
)
from dagster_pipeline.defs.conversation_analysis.utils.variables.experiments import (
    get_bot_graph_variant,
    get_experiment_variables,
    get_unification_mgs_variant,
)
from dagster_pipeline.defs.conversation_analysis.utils.variables.funnel_events import (
    get_funnel_events_variables,
)

from dagster_pipeline.defs.conversation_analysis.utils.variables.dspy_evaluator import (
    get_dspy_evaluator_variables,
    process_dspy_evaluator_batch,
)
from dagster_pipeline.defs.conversation_analysis.utils.variables.experiments import (
    get_experiment_variables,
)

__all__ = [
    # Metrics
    "get_messages_count",
    "get_user_messages_count",
    "get_total_time_conversation",
    "get_user_average_response_time",
    "get_user_spot2_links_count",
    "get_reco_interact",
    # Funnel Metrics
    "get_funnel_type",
    "get_funnel_section",
    # Tags
    "get_active_user",
    "get_engaged_user",
    "get_visit_intention",
    "get_scheduled_visit",
    "get_created_project",
    "get_follow_up_success",
    # AI Tags
    "get_ai_tags",
    "process_ai_tags_batch",
    # TODO: Uncomment when files are added to main
    # DSPy Evaluator
    "get_dspy_evaluator_variables",
    "process_dspy_evaluator_batch",
    # Experiments
    "get_bot_graph_variant",
    "get_experiment_variables",
    "get_unification_mgs_variant",
    # Funnel events
    "get_funnel_events_variables",
]

