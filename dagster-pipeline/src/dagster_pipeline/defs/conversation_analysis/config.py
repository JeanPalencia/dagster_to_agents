"""Configuration for the Conversation Analysis pipeline."""
from datetime import datetime, timedelta
from typing import Optional, List

from dagster import Config


# AI tags executed when ai_tags_filter is empty (only these 4)
ALL_AI_TAGS = [
    "is_broker",
    "info_requested",
    "missed_messages",
    "scaling_agent",
]


def get_dynamic_start_date() -> str:
    """Get start date dynamically: 3 days before today."""
    return (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")


def get_dynamic_end_date() -> str:
    """Get end date dynamically: current date (today)."""
    return datetime.now().strftime("%Y-%m-%d")


class ConversationConfig(Config):
    """Configuration for conversation data extraction.
    
    By default (empty start_date/end_date): start = 3 days ago, end = today.
    Example: If today is Jan 10, processes Jan 7 through Jan 10.
    
    Leave start_date and end_date empty ("") for automatic date calculation at runtime.
    """
    start_date: str = ""  # Empty = calculate dynamically at runtime
    end_date: str = ""    # Empty = calculate dynamically at runtime


class ConvVariablesConfig(Config):
    """Configuration for conversation variables processing.
    
    AI Tags Filter:
        By default (empty list), runs: is_broker, info_requested, missed_messages, scaling_agent.
        To run only specific tags, set ai_tags_filter to a list of those names (must exist in TAG_DEFINITIONS).
        
        Example: ai_tags_filter: ["is_broker", "info_requested"]
    """
    test_mode: bool = False
    test_sample_size: int = 50
    # OpenAI Analysis options
    enable_openai_analysis: bool = True  # Enable OpenAI analysis (generates costs)
    openai_start_date: Optional[str] = None  # Date from which to analyze with OpenAI (YYYY-MM-DD)
    ai_tags_filter: List[str] = []  # Empty = all tags, or specify which tags to run
    # DSPy Evaluator options (False = no se generan variables score; para actualizar en Geospot pasa True al lanzar el run)
    enable_dspy_evaluator: bool = True  # Enable DSPy conversation evaluator (generates costs)
    # Incremental: True = solo procesar conv_id nuevos o con len(conv_messages) distinto; False = procesar todos
    use_incremental_mask: bool = True


class ConvUpsertConfig(Config):
    """Configuration for Geospot upsert.
    x
    preserve_existing_conv_variables:
        True (default): al actualizar un registro, fusiona conv_variables con lo que ya estaba en Geospot;
        no pisa con vacíos las variables de IA (score, ai_tag) si el nuevo viene vacío.
        False: al actualizar, reemplaza conv_variables por el nuevo tal cual (puede quedar vacío/nulos).
    """
    preserve_existing_conv_variables: bool = False


class FinalOutputConfig(Config):
    """Configuration for final output export."""
    test_mode: bool = False
    dev_mode: bool = False
    test_sample_size: int = 50

