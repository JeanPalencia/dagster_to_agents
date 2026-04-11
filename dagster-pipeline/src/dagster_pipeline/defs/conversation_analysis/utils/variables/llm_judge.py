"""LLM-as-a-Judge evaluation for conversation funnel analysis using DSPy."""

import json
import os
from typing import Dict, List, Optional, Callable

import dspy
import pandas as pd

# Optional langsmith import
try:
    from langsmith import traceable
except ImportError:
    # Fallback decorator if langsmith not available
    def traceable(name=None, run_type=None):
        # Support both @traceable and @traceable(...)
        if callable(name):
            # Used as @traceable (without parentheses)
            return name
        # Used as @traceable(...) (with arguments)
        def decorator(func):
            return func
        return decorator

from dagster_pipeline.defs.conversation_analysis.utils.database import get_openai_api_key


# =============================================================================
# DSPy Configuration
# =============================================================================

def _configure_dspy():
    """Configure DSPy with OpenAI."""
    if not dspy.settings.lm:
        api_key = get_openai_api_key()
        lm = dspy.OpenAI(model="gpt-4o-mini", api_key=api_key, max_tokens=2000)
        dspy.configure(lm=lm)


# =============================================================================
# Helper Functions
# =============================================================================

def parse_conv_messages(conv_messages) -> list:
    """Parse conv_messages to list of dicts."""
    if conv_messages is None:
        return []
    if isinstance(conv_messages, list):
        return conv_messages
    try:
        return json.loads(conv_messages)
    except:
        return []


def format_conversation_with_markers(conv_messages) -> str:
    """Format conversation with inline markers (DSPy format).
    
    Format:
    [event_section=profiling; event_type=conversation_start][user]: Hola, me interesa...
    [event_section=profiling; event_type=profiling_form_completed][assistant]: Perfecto, te envío...
    """
    messages = parse_conv_messages(conv_messages)
    
    if not messages:
        return ""
    
    formatted_lines = []
    
    for msg in messages:
        section = msg.get('event_section', 'unknown') or 'unknown'
        event_type = msg.get('event_type', 'unknown') or 'unknown'
        role = msg.get('type', msg.get('role', 'unknown'))
        content = msg.get('message_body', msg.get('content', ''))
        
        if isinstance(content, dict):
            content = "[Formulario/Carrusel enviado]"
        elif not content:
            continue
        
        # Format: [event_section=X; event_type=Y][role]: contenido
        prefix = f"[event_section={section}; event_type={event_type}]"
        formatted_lines.append(f"{prefix}[{role}]: {content}")
    
    return "\n".join(formatted_lines)[:5000]


# =============================================================================
# DSPy Signature and Module
# =============================================================================

class ConversationEvaluationSignature(dspy.Signature):
    """Eres un analista de funnel de un chatbot inmobiliario.
    
    Tu trabajo es identificar PUNTOS DE FUGA y FRICCIÓN entre event_types.
    
    === FLUJO IDEAL DEL FUNNEL ===
    
    conversation_start → spot_confirmation → profiling_form_sent → profiling_form_completed
                                                      ↓
    recommendations_sent → search_refinement → recommendation_scheduling
                                                      ↓
    scheduling_form_sent → scheduling_form_completed → explicit_visit_request → visit_scheduled ✓
    
    === EVENT_TYPES Y SU SIGNIFICADO ===
    
    | Event Type | Quién lo dispara | Qué significa |
    |------------|------------------|---------------|
    | conversation_start | Usuario | Inicia conversación |
    | spot_confirmation | Bot | Confirma inmueble de interés |
    | profiling_form_sent | Bot | Envía formulario de preferencias |
    | profiling_form_completed | Usuario | Completa formulario |
    | recommendations_sent | Bot | Muestra propiedades |
    | search_refinement | Usuario | Ajusta búsqueda |
    | recommendation_scheduling | Usuario | Pide agendar desde recomendación |
    | scheduling_form_sent | Bot | Envía formulario de agenda |
    | scheduling_form_completed | Usuario | Completa formulario de agenda |
    | explicit_visit_request | Usuario | Pide visita explícitamente |
    | visit_scheduled | Bot | Visita confirmada ✓ ÉXITO |
    
    === QUÉ ANALIZAR ===
    
    1. PUNTO DE FUGA: ¿En qué event_type se quedó el usuario? (último event_type antes de abandonar)
    2. FRICCIÓN: ¿Hubo problemas en alguna transición? (bot no entendió, usuario confundido)
    3. CAUSA: user_abandonment | bot_error | user_intent_change
    
    === FORMATO DE ENTRADA ===
    
    Cada mensaje tiene: [event_section=X; event_type=Y][role]: mensaje
    Analiza la SECUENCIA de event_types para identificar dónde se rompió el flujo.
    """
    
    conv_id: str = dspy.InputField(
        desc="ID de la conversación"
    )
    conversation_text: str = dspy.InputField(
        desc="Conversación con marcadores inline: [event_section=X; event_type=Y][role]: mensaje"
    )
    event_types_sequence: str = dspy.InputField(
        desc="Secuencia de event_types en orden (ej: conversation_start, profiling_form_completed, scheduling_form_sent)"
    )
    
    last_event_type: str = dspy.OutputField(
        desc="Último event_type alcanzado antes de que terminara la conversación"
    )
    reached_goal: bool = dspy.OutputField(
        desc="True si llegó a visit_scheduled, False si no"
    )
    drop_off_point: str = dspy.OutputField(
        desc="Event_type donde se produjo la fuga (donde el usuario dejó de avanzar). Vacío si reached_goal=True"
    )
    friction_points: str = dspy.OutputField(
        desc='JSON lista de fricciones detectadas. Ejemplo: [{"from": "profiling_form_sent", "to": "none", "cause": "user_abandonment", "reason": "Usuario no respondió al formulario"}]'
    )
    funnel_score: float = dspy.OutputField(
        desc="Score de avance en funnel: 0.0 (solo conversation_start) a 1.0 (visit_scheduled)"
    )
    summary: str = dspy.OutputField(
        desc="Resumen: qué pasó y dónde se perdió el usuario"
    )


class FunnelAnalyzer(dspy.Module):
    """DSPy module for funnel analysis."""
    
    def __init__(self):
        super().__init__()
        self.analyze = dspy.ChainOfThought(ConversationEvaluationSignature)
    
    def forward(self, conv_id: str, conversation_text: str, event_types_sequence: str):
        return self.analyze(
            conv_id=conv_id,
            conversation_text=conversation_text,
            event_types_sequence=event_types_sequence
        )


# Singleton for FunnelAnalyzer
_funnel_analyzer = None

def _get_funnel_analyzer():
    """Get or create FunnelAnalyzer singleton."""
    global _funnel_analyzer
    if _funnel_analyzer is None:
        _configure_dspy()
        _funnel_analyzer = FunnelAnalyzer()
    return _funnel_analyzer


# =============================================================================
# Evaluation Functions
# =============================================================================

@traceable(name="analyze_funnel", run_type="chain")
def evaluate_conversation_with_markers(conv_id: str, conv_messages) -> dict:
    """Analyze funnel drop-off points and friction using inline markers format.
    
    Args:
        conv_id: Conversation ID
        conv_messages: List of message dicts or JSON string
    
    Returns:
        Dict with:
        - last_event_type: str
        - reached_goal: bool
        - drop_off_point: str
        - friction_points: list
        - funnel_score: float
        - summary: str
    """
    # Format conversation
    conversation_text = format_conversation_with_markers(conv_messages)
    
    # Extract event_types sequence
    messages = parse_conv_messages(conv_messages)
    event_types_sequence = list(dict.fromkeys([
        m.get('event_type', 'unknown') 
        for m in messages 
        if m.get('event_type')
    ]))
    
    # Get analyzer
    analyzer = _get_funnel_analyzer()
    
    # Analyze
    result = analyzer(
        conv_id=conv_id,
        conversation_text=conversation_text,
        event_types_sequence=", ".join(event_types_sequence)
    )
    
    # Parse friction_points
    try:
        friction_json = json.loads(result.friction_points)
    except:
        friction_json = []
    
    # Parse reached_goal
    reached_goal = False
    if hasattr(result, 'reached_goal'):
        if isinstance(result.reached_goal, bool):
            reached_goal = result.reached_goal
        elif isinstance(result.reached_goal, str):
            reached_goal = result.reached_goal.lower() in ['true', '1', 'yes', 'sí']
    
    return {
        'conv_id': conv_id,
        'event_types_sequence': event_types_sequence,
        'last_event_type': str(result.last_event_type) if hasattr(result, 'last_event_type') else 'unknown',
        'reached_goal': reached_goal,
        'drop_off_point': str(result.drop_off_point) if hasattr(result, 'drop_off_point') else '',
        'friction_points': friction_json,
        'funnel_score': float(result.funnel_score) if hasattr(result, 'funnel_score') else 0.0,
        'summary': str(result.summary) if hasattr(result, 'summary') else ''
    }


def process_llm_judge_batch(
    df: pd.DataFrame,
    demos: Optional[List] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    use_markers: bool = True
) -> Dict[int, dict]:
    """
    Process multiple conversations in batch for LLM Judge evaluation.
    
    Args:
        df: DataFrame with 'conversation_id' (or 'conv_id') and 'messages' (or 'conv_messages') columns
        demos: Optional few-shot examples (not used in V3)
        progress_callback: Optional callback(completed, total) for progress updates
        use_markers: If True, use inline markers format (V3). If False, use section-based format (V1).
    
    Returns:
        Dictionary mapping DataFrame index -> evaluation result dict
    """
    results = {}
    total = len(df)
    
    for idx, row in df.iterrows():
        # Get conversation ID
        conv_id = row.get('conversation_id') or row.get('conv_id', f'conv_{idx}')
        
        # Get messages
        conv_messages = row.get('messages') or row.get('conv_messages')
        
        if not conv_messages:
            results[idx] = {
                'last_event_type': 'unknown',
                'reached_goal': False,
                'drop_off_point': '',
                'friction_points': [],
                'funnel_score': 0.0,
                'summary': 'No messages found'
            }
            if progress_callback:
                progress_callback(len(results), total)
            continue
        
        try:
            if use_markers:
                # V3: Use inline markers format
                result = evaluate_conversation_with_markers(conv_id, conv_messages)
            else:
                # V1: Section-based (not implemented in V3)
                result = {
                    'last_event_type': 'unknown',
                    'reached_goal': False,
                    'drop_off_point': '',
                    'friction_points': [],
                    'funnel_score': 0.0,
                    'summary': 'V1 format not supported in V3'
                }
            
            results[idx] = result
        except Exception as e:
            # Error handling
            results[idx] = {
                'last_event_type': 'error',
                'reached_goal': False,
                'drop_off_point': '',
                'friction_points': [],
                'funnel_score': 0.0,
                'summary': f'Error: {str(e)}'
            }
        
        if progress_callback:
            progress_callback(len(results), total)
    
    return results


def get_llm_judge_variables(evaluation_result: Dict) -> List[dict]:
    """
    Convert LLM Judge evaluation result to conv_variables format.
    
    Args:
        evaluation_result: Dict from evaluate_conversation_with_markers or process_llm_judge_batch
    
    Returns:
        List of variable dictionaries for conv_variables
    """
    # Check if V3 format (has 'funnel_score' and 'drop_off_point')
    if 'funnel_score' in evaluation_result:
        # V3 format
        friction_points = evaluation_result.get('friction_points', [])
        friction_str = json.dumps(friction_points, ensure_ascii=False) if isinstance(friction_points, list) else str(friction_points)
        
        return [
            {
                'conv_variable_category': 'funnel',
                'conv_variable_name': 'funnel_score',
                'conv_variable_value': evaluation_result.get('funnel_score', 0.0)
            },
            {
                'conv_variable_category': 'funnel',
                'conv_variable_name': 'last_event_type',
                'conv_variable_value': evaluation_result.get('last_event_type', '')
            },
            {
                'conv_variable_category': 'funnel',
                'conv_variable_name': 'reached_goal',
                'conv_variable_value': evaluation_result.get('reached_goal', False)
            },
            {
                'conv_variable_category': 'funnel',
                'conv_variable_name': 'drop_off_point',
                'conv_variable_value': evaluation_result.get('drop_off_point', '')
            },
            {
                'conv_variable_category': 'funnel',
                'conv_variable_name': 'friction_points',
                'conv_variable_value': friction_str
            },
            {
                'conv_variable_category': 'funnel',
                'conv_variable_name': 'funnel_summary',
                'conv_variable_value': evaluation_result.get('summary', '')
            }
        ]
    else:
        # V1 format (fallback - not used in V3)
        return []
