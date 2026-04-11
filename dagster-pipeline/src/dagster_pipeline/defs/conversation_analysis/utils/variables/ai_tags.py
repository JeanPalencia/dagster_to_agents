"""AI-generated tags for conversation analysis using OpenAI - Batch processing version with filtering."""

import os
import re
import pandas as pd
from typing import List, Dict, Optional

try:
    from langsmith import traceable
except ImportError:
    def traceable(name=None, run_type=None):
        def _noop(f):
            def _wrapped(*args, **kwargs):
                return f(*args, **kwargs)
            return _wrapped
        return _noop if not callable(name) else _noop(name)

from dagster_pipeline.defs.conversation_analysis.utils.openai_client import (
    analyze_conversations_sync,
    TAG_DEFINITIONS,
    ALL_TAG_NAMES,
    get_tag_openai_key,
)


def _sanitize_explanation(text: str) -> str:
    """Sanitize explanation text to ensure it's safe for JSONB."""
    if not text or pd.isna(text):
        return ""
    
    text = str(text)
    
    # Replace newlines and tabs with spaces
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    # Remove control characters
    text = re.sub(r'[\x00-\x1f\x7f]', '', text)
    
    # Normalize multiple spaces
    text = re.sub(r' +', ' ', text)
    
    # Limit length to avoid very long explanations
    if len(text) > 200:
        text = text[:197] + "..."
    
    return text.strip()


def _format_ai_tags(analysis: dict, tags_filter: Optional[List[str]] = None) -> List[dict]:
    """
    Convert OpenAI analysis result to variable format.
    
    Args:
        analysis: Dictionary with OpenAI analysis results
        tags_filter: List of tag names to include. None or empty = all tags.
    
    Returns:
        List of variable dictionaries
    """
    # Determine which tags to format
    if not tags_filter:
        tags_to_use = ALL_TAG_NAMES
    else:
        tags_to_use = [t for t in tags_filter if t in TAG_DEFINITIONS]
        if not tags_to_use:
            tags_to_use = ALL_TAG_NAMES
    
    tags = []
    
    for tag_name in tags_to_use:
        openai_key = get_tag_openai_key(tag_name)
        if not openai_key:
            continue
        tag_data = analysis.get(openai_key, {"value": 0, "explanation": ""})
        value = tag_data.get("value", 0)
        explanation = tag_data.get("explanation", "")
        
        # Store value as integer (consistent with other tags)
        # Store explanation in a separate field, sanitized
        tags.append({
            'conv_variable_category': 'ai_tag',
            'conv_variable_name': tag_name,
            'conv_variable_value': int(value),
            'conv_variable_explanation': _sanitize_explanation(explanation)
        })
    
    return tags


def _get_default_ai_tags(reason: str, tags_filter: Optional[List[str]] = None) -> List[dict]:
    """Return default AI tags when analysis cannot be performed."""
    # Determine which tags to include
    if not tags_filter:
        tags_to_use = ALL_TAG_NAMES
    else:
        tags_to_use = [t for t in tags_filter if t in TAG_DEFINITIONS]
        if not tags_to_use:
            tags_to_use = ALL_TAG_NAMES
    
    tags = []
    for tag_name in tags_to_use:
        tags.append({
            'conv_variable_category': 'ai_tag',
            'conv_variable_name': tag_name,
            'conv_variable_value': 0,
            'conv_variable_explanation': reason
        })
    
    return tags


# LangSmith project for AI tags (separate from DSPy's dspy-evaluator)
LANGSMITH_PROJECT_AI_TAGS = "conversation_analysis_ai_tags"


@traceable(name="process_ai_tags_batch", run_type="chain")
def process_ai_tags_batch(
    df: pd.DataFrame,
    tags_filter: Optional[List[str]] = None,
    progress_callback: Optional[callable] = None
) -> Dict[int, List[dict]]:
    """
    Process AI tags for an entire DataFrame in batch using async concurrency.
    Traces are sent to LangSmith project conversation_analysis_ai_tags when enabled.
    
    This function:
    1. Extracts conversation texts from all rows
    2. Sends them to OpenAI concurrently (max 20 at a time)
    3. Returns formatted AI tags for each row index
    
    Args:
        df: DataFrame with 'conversation_text' column
        tags_filter: List of tag names to analyze. None or empty = all tags.
                    Example: ["is_broker", "refinement"] -> only these 2 tags
        progress_callback: Optional callback(completed, total) for progress updates
    
    Returns:
        Dictionary mapping DataFrame index -> list of AI tag dictionaries
    """
    # Send AI tags traces to a different LangSmith project than DSPy
    prev_project = os.environ.get("LANGCHAIN_PROJECT")
    try:
        os.environ["LANGCHAIN_PROJECT"] = LANGSMITH_PROJECT_AI_TAGS
        return _process_ai_tags_batch_impl(df, tags_filter, progress_callback)
    finally:
        if prev_project is not None:
            os.environ["LANGCHAIN_PROJECT"] = prev_project
        elif os.environ.get("LANGCHAIN_PROJECT") == LANGSMITH_PROJECT_AI_TAGS:
            os.environ.pop("LANGCHAIN_PROJECT", None)


def _process_ai_tags_batch_impl(
    df: pd.DataFrame,
    tags_filter: Optional[List[str]] = None,
    progress_callback: Optional[callable] = None
) -> Dict[int, List[dict]]:
    """Implementation of process_ai_tags_batch (after setting LangSmith project)."""
    # Validate and log filter
    if tags_filter:
        valid_tags = [t for t in tags_filter if t in TAG_DEFINITIONS]
        invalid_tags = [t for t in tags_filter if t not in TAG_DEFINITIONS]
        if invalid_tags:
            print(f"⚠️ Warning: Invalid tag names ignored: {invalid_tags}")
        if valid_tags:
            print(f"🏷️ Running filtered AI tags: {valid_tags}")
        else:
            print(f"⚠️ No valid tags in filter, running all tags")
            tags_filter = None
    
    # Prepare conversations list: (index, conversation_text)
    conversations = []
    skip_indices = set()
    
    for idx, row in df.iterrows():
        conversation_text = str(row.get('conversation_text', ''))
        
        if not conversation_text or conversation_text == 'nan' or len(conversation_text.strip()) == 0:
            # Skip empty conversations, will use defaults
            skip_indices.add(idx)
        else:
            conversations.append((idx, conversation_text))
    
    # Process all non-empty conversations concurrently
    if conversations:
        analysis_results = analyze_conversations_sync(
            conversations, 
            tags_filter=tags_filter,
            progress_callback=progress_callback
        )
    else:
        analysis_results = {}
    
    # Build final results
    results = {}
    
    for idx in df.index:
        if idx in skip_indices:
            results[idx] = _get_default_ai_tags("No conversation text available", tags_filter)
        elif idx in analysis_results:
            results[idx] = _format_ai_tags(analysis_results[idx], tags_filter)
        else:
            results[idx] = _get_default_ai_tags("Analysis failed", tags_filter)
    
    return results


def get_ai_tags(row: pd.Series, tags_filter: Optional[List[str]] = None) -> List[dict]:
    """
    Generate AI-based tags for a single conversation row (legacy sync version).
    
    NOTE: For batch processing, use process_ai_tags_batch() instead for 
    much better performance.
    
    Args:
        row: DataFrame row with 'conversation_text' column
        tags_filter: List of tag names to analyze. None or empty = all tags.
    
    Returns:
        List of variable dictionaries with AI-generated tags
    """
    import asyncio
    from dagster_pipeline.defs.conversation_analysis.utils.openai_client import (
        analyze_conversation_async,
    )
    
    conversation_text = str(row.get('conversation_text', ''))
    
    if not conversation_text or conversation_text == 'nan':
        return _get_default_ai_tags("No conversation text available", tags_filter)
    
    # Run async analysis synchronously
    try:
        result = asyncio.run(analyze_conversation_async(conversation_text, tags_filter=tags_filter))
        return _format_ai_tags(result, tags_filter)
    except Exception as e:
        return _get_default_ai_tags(f"Analysis error: {str(e)[:50]}", tags_filter)
