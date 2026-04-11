"""OpenAI client utilities for conversation analysis - Async version with tag filtering."""

import asyncio
import json
import random
from typing import Optional, List, Dict, Tuple
from openai import AsyncOpenAI

from dagster_pipeline.defs.conversation_analysis.utils.database import get_openai_api_key

try:
    from langsmith import traceable
    _LANGSMITH_AVAILABLE = True
except ImportError:
    def traceable(name=None, run_type=None):
        def _id(f):
            return f
        return _id if callable(name) else lambda f: f
    _LANGSMITH_AVAILABLE = False


# Cache the async client to avoid creating multiple instances
_async_openai_client: Optional[AsyncOpenAI] = None

# Concurrency control - max 20 simultaneous requests
MAX_CONCURRENT_REQUESTS = 20
_semaphore: Optional[asyncio.Semaphore] = None


def get_async_openai_client() -> AsyncOpenAI:
    """Get or create AsyncOpenAI client with API key from SSM."""
    global _async_openai_client
    if _async_openai_client is None:
        api_key = get_openai_api_key()
        _async_openai_client = AsyncOpenAI(api_key=api_key)
    return _async_openai_client


def _get_semaphore() -> asyncio.Semaphore:
    """Get or create the semaphore for concurrency control."""
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    return _semaphore


# =============================================================================
# TAG DEFINITIONS - Each tag with its OpenAI key, db name, and prompt definition
# =============================================================================

TAG_DEFINITIONS = {
    "claim": {
        "openai_key": "Claim",
        "definition": """- **Claim**: Marca 1 si el usuario expresa una queja, frustración, insatisfacción o menciona un problema. Marca 0 si no lo hace."""
    },
    "out_of_context": {
        "openai_key": "OutOfContext",
        "definition": """- **OutOfContext**: Marca 1 si la conversación NO está relacionada con bienes raíces (por ejemplo, propiedades, casas, departamentos, ventas, alquileres, visitas, etc.). Marca 0 si sí está relacionada.  
   - Importante: La conversación siempre empieza con interés en bienes raíces, pero si el usuario empieza a hablar de cosas que no tienen nada que ver con real estate, en esos casos márcalos como 1.  
   - Sé estricto con el out of context, si el usuario empieza a hablar de cosas que no tienen nada que ver con real estate en esos casos márcalos como 1.  
   - Si el usuario no responde apropiadamente a la pregunta del asistente, se considera out of context."""
    },
    "churn": {
        "openai_key": "Churn",
        "definition": """- **Churn**: Marca 1 SOLO si el BOT realiza una pregunta específica o puntual (por ejemplo: "¿Qué tipo de propiedad buscas?", "¿Cuál es tu presupuesto?", "¿En qué zona estás interesado?", "¿Podrías decirme tu nombre?", opciones con emojis 1️⃣2️⃣) y han pasado MÁS DE 24 HORAS sin respuesta del usuario.  
   - NO se considera churn si:  
     - El usuario responde (aunque sea con una despedida o cierre cordial), o  
     - Han pasado MENOS de 24 horas desde la última pregunta, o  
     - El Asistente NO hizo una pregunta específica (solo saludó, dio información, o cerró cordialmente)  
   - SI se considera churn SOLO si:  
     - El BOT hace una pregunta específica Y el usuario no responde en más de 24 horas  
   - IMPORTANTE: Si el BOT NO hace una pregunta específica, NO es churn aunque hayan pasado más de 24 horas."""
    },
    "scaling_agent": {
        "openai_key": "ScalingAgent",
        "definition": """- **ScalingAgent**: Marca 1 cuando el asistente responde que un asesor revisará su solicitud o que será escalado a un agente humano. Marca 0 si no ocurre esto."""
    },
    "assistant_error": {
        "openai_key": "AssistantError",
        "definition": """- **AssistantError**: Marca 1 cuando el asistente responde "Disculpa, aún no puedo responder a ese tipo de mensajes" o mensajes similares de error. Marca 0 si no ocurre esto."""
    },
    "conversation_loop": {
        "openai_key": "ConversationLoop",
        "definition": """- **ConversationLoop**: Marca 1 cuando el asistente repite la misma pregunta múltiples veces o hay un patrón repetitivo en la conversación. Marca 0 si no ocurre esto."""
    },
    "is_broker": {
        "openai_key": "IsBroker",
        "definition": """- **IsBroker**: Marca 1 si el usuario ES un BROKER o AGENTE INMOBILIARIO profesional (no un cliente final buscando propiedad para sí mismo). 
   - INDICADORES POSITIVOS (marca 1):
     - Pregunta sobre comisiones, porcentajes de comisión, o si se comparte comisión con otros agentes
     - Menciona que tiene un cliente interesado ("tengo un cliente", "mi cliente busca", "traigo un comprador", "tengo un prospecto")
     - Dice explícitamente que es broker, corredor, o que trabaja en bienes raíces/inmobiliaria
     - Pregunta por esquemas de colaboración entre agentes o corredores
     - Pregunta si la propiedad está en exclusiva o si trabajan con otros agentes
     - Menciona su inmobiliaria, su portafolio, o que representa a compradores
   - INDICADORES NEGATIVOS (NO es broker, marca 0):
     - El usuario PIDE hablar con un asesor/agente humano ("quiero hablar con un asesor", "pásame con un agente") - esto es un CLIENTE pidiendo ayuda, NO un broker
     - El usuario busca propiedad para sí mismo o su familia ("busco casa para mí", "queremos rentar")
     - El usuario solo pregunta información básica sobre propiedades sin mencionar clientes propios
   - IMPORTANTE: Distinguir entre "soy asesor" (ES broker) vs "quiero un asesor" (PIDE ayuda)."""
    },
    "info_requested": {
        "openai_key": "InfoRequested",
        "definition": """- **InfoRequested**: Marca 1 si el usuario solicita EXPLÍCITAMENTE información adicional sobre la propiedad o sobre el proceso/funcionamiento.
   - INDICADORES POSITIVOS (marca 1):
     - El usuario hace preguntas directas sobre la propiedad: "¿Cuántos metros tiene?", "¿Tiene estacionamiento?", "¿Cuál es el precio?", "¿Acepta mascotas?"
     - Solicita información sobre el proceso: "¿Cómo funciona?", "¿Cuáles son los requisitos?", "¿Qué documentos necesito?"
     - Pide detalles específicos: "¿Me puedes dar más información?", "Cuéntame más", "¿Qué incluye?"
     - Hace preguntas sobre ubicación, amenidades, servicios, costos adicionales (mantenimiento, HOA)
     - Pregunta sobre disponibilidad, fechas, condiciones del contrato
     - Preguntas sobre financiamiento, créditos, formas de pago
   - INDICADORES NEGATIVOS (marca 0):
     - El usuario solo responde a las preguntas del bot sin pedir información adicional
     - Solo proporciona datos (nombre, teléfono, presupuesto) sin hacer preguntas
     - Respuestas simples como "sí", "no", "ok", "gracias"
     - El usuario solo sigue el flujo de la conversación sin hacer preguntas propias
   - NOTA: Debe ser una solicitud EXPLÍCITA de información, no inferida. El usuario debe formular una pregunta o pedir información claramente."""
    },
    "missed_messages": {
        "openai_key": "MissedMessages",
        "definition": """- **MissedMessages**: CUENTA el número de veces que el asistente EXPLÍCITAMENTE indica que no puede responder.
   - El value debe ser un NÚMERO ENTERO (0, 1, 2, 3, etc.) indicando cuántas veces ocurrió.
   - CONTAR COMO MISSED MESSAGE SOLO cuando el asistente dice EXPLÍCITAMENTE:
     - "Disculpa, aún no puedo responder a ese tipo de mensajes"
     - "No entiendo tu mensaje" o "No comprendo lo que me dices"
     - "Lo siento, no puedo ayudarte con eso"
     - "No tengo información sobre eso"
     - Cualquier mensaje donde el asistente ADMITE que no puede o no sabe responder
   - NO CONTAR cuando:
     - El asistente da una respuesta (aunque sea genérica o no perfecta)
     - El asistente responde con información (aunque no sea exacta)
     - El asistente hace preguntas de seguimiento
     - El asistente escala a un agente humano
     - El usuario repite un mensaje (eso NO es un missed message del asistente)
   - IMPORTANTE: Solo cuenta cuando el ASISTENTE dice explícitamente que NO PUEDE responder. No asumas, debe ser explícito.
   - EJEMPLO: Si el asistente dice 2 veces "no puedo responder a eso", value = 2."""
    },
    "bug_detected": {
        "openai_key": "BugDetected",
        "definition": """- **BugDetected**: Marca 1 si se detecta un ERROR TÉCNICO o FALLA en el funcionamiento del asistente.
   - INDICADORES POSITIVOS (marca 1):
     - El asistente dice que tuvo un error al realizar una operación ("ocurrió un error", "hubo un problema técnico")
     - El asistente indica que algo no está disponible temporalmente ("en este momento no puedo", "el servicio no está disponible")
     - El asistente promete hacer algo y NO lo hace (dice "te envío la información" pero no la envía)
     - El asistente menciona fallas del sistema ("el sistema está fallando", "no pude completar la acción")
     - El asistente da una respuesta incompleta por un error técnico
     - El asistente se disculpa por un mal funcionamiento ("disculpa el inconveniente técnico")
     - Mensajes de error visibles como códigos de error, timeouts, o fallos de conexión
   - INDICADORES NEGATIVOS (marca 0):
     - El asistente simplemente no tiene la información (eso es missed_messages, no bug)
     - El asistente escala a un agente humano (es comportamiento normal)
     - El asistente pide más información al usuario (es flujo normal)
     - El asistente responde correctamente aunque sea genérico
   - NOTA: Busca evidencia de FALLOS TÉCNICOS o PROMESAS INCUMPLIDAS, no falta de conocimiento."""
    },
}

# All available tag names
ALL_TAG_NAMES = list(TAG_DEFINITIONS.keys())


def _build_prompt(conversation_text: str, tags_filter: Optional[List[str]] = None) -> Tuple[str, List[str]]:
    """
    Build the analysis prompt dynamically based on requested tags.
    
    Args:
        conversation_text: The conversation to analyze
        tags_filter: List of tag names to include. None or empty = all tags.
    
    Returns:
        Tuple of (prompt_string, list_of_openai_keys)
    """
    # Determine which tags to include
    if not tags_filter:
        tags_to_use = ALL_TAG_NAMES
    else:
        # Validate and filter
        tags_to_use = [t for t in tags_filter if t in TAG_DEFINITIONS]
        if not tags_to_use:
            tags_to_use = ALL_TAG_NAMES  # Fallback to all if none valid
    
    # Build JSON structure for prompt
    json_structure_parts = []
    for tag_name in tags_to_use:
        openai_key = TAG_DEFINITIONS[tag_name]["openai_key"]
        json_structure_parts.append(f'  "{openai_key}": {{"value": 0, "explanation": ""}}')
    
    json_structure = "{\n" + ",\n".join(json_structure_parts) + "\n}"
    # Escape braces for .format()
    json_structure_escaped = json_structure.replace("{", "{{").replace("}", "}}")
    
    # Build definitions section
    definitions = "\n".join([TAG_DEFINITIONS[t]["definition"] for t in tags_to_use])
    
    # Build full prompt
    prompt = f"""Responde SOLO con un objeto JSON válido que siga esta estructura EXACTA, sin texto adicional ni markdown:

{json_structure_escaped}

Definiciones:  
{definitions}

For each field, "value" must be 0 or 1 EXCEPT for MissedMessages where value is a COUNT (0, 1, 2, 3, etc.). "explanation" must be a brief reason IN ENGLISH (max 50 words) justifying the mark.

Conversación a analizar:  
{{conversation_text}}"""
    
    # Get OpenAI keys for validation
    openai_keys = [TAG_DEFINITIONS[t]["openai_key"] for t in tags_to_use]
    
    return prompt.format(conversation_text=conversation_text), openai_keys


def _get_default_result(error_msg: str, tags_filter: Optional[List[str]] = None) -> dict:
    """Return default result structure when analysis fails."""
    if not tags_filter:
        tags_to_use = ALL_TAG_NAMES
    else:
        tags_to_use = [t for t in tags_filter if t in TAG_DEFINITIONS]
        if not tags_to_use:
            tags_to_use = ALL_TAG_NAMES
    
    result = {}
    for tag_name in tags_to_use:
        openai_key = TAG_DEFINITIONS[tag_name]["openai_key"]
        result[openai_key] = {"value": 0, "explanation": error_msg}
    
    return result


@traceable(name="ai_tags_analyze_conversation", run_type="chain")
async def analyze_conversation_async(
    conversation_text: str,
    tags_filter: Optional[List[str]] = None,
    max_retries: int = 5,
    base_delay: float = 1.0
) -> dict:
    """
    Analyze a conversation using OpenAI GPT-4o-mini (async version).
    
    Features:
    - Async/await for non-blocking calls
    - Semaphore for concurrency control (max 20 concurrent)
    - Exponential backoff with jitter for retries
    - Handles rate limits (429) and temporary errors
    - Dynamic tag filtering
    
    Args:
        conversation_text: The conversation text to analyze
        tags_filter: List of tag names to analyze. None or empty = all tags.
        max_retries: Maximum number of retries on failure
        base_delay: Base delay in seconds for exponential backoff
    
    Returns:
        Dictionary with analysis results for each requested tag
    """
    client = get_async_openai_client()
    semaphore = _get_semaphore()
    
    # Truncate very long conversations to avoid token limits
    max_chars = 8000  # ~2000 tokens approximately
    if len(conversation_text) > max_chars:
        conversation_text = conversation_text[:max_chars] + "\n... [conversación truncada]"
    
    # Build prompt dynamically based on requested tags
    prompt, expected_keys = _build_prompt(conversation_text, tags_filter)
    
    for attempt in range(max_retries):
        try:
            # Use semaphore to control concurrency
            async with semaphore:
                response = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {
                            "role": "system",
                            "content": "You are an expert analyst of real estate chatbot conversations. You respond ONLY in valid JSON. All explanations must be in English."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    temperature=0.1,  # Low temperature for consistent results
                    max_tokens=500,   # Limit response size
                    response_format={"type": "json_object"}  # Force JSON output
                )
            
            # Parse the response
            content = response.choices[0].message.content
            result = json.loads(content)
            
            # Validate structure
            for key in expected_keys:
                if key not in result:
                    result[key] = {"value": 0, "explanation": "Not detected"}
                elif "value" not in result[key]:
                    result[key]["value"] = 0
                elif "explanation" not in result[key]:
                    result[key]["explanation"] = ""
            
            return result
            
        except json.JSONDecodeError as e:
            if attempt < max_retries - 1:
                delay = _calculate_backoff_delay(attempt, base_delay)
                await asyncio.sleep(delay)
                continue
            return _get_default_result(f"JSON parse error: {str(e)}", tags_filter)
        
        except Exception as e:
            error_str = str(e).lower()
            
            # Check if it's a rate limit error (429)
            is_rate_limit = "rate_limit" in error_str or "429" in error_str or "too many requests" in error_str
            
            if attempt < max_retries - 1:
                # Longer delay for rate limits
                if is_rate_limit:
                    delay = _calculate_backoff_delay(attempt, base_delay * 2, max_delay=60)
                else:
                    delay = _calculate_backoff_delay(attempt, base_delay)
                
                await asyncio.sleep(delay)
                continue
            
            return _get_default_result(f"API error: {str(e)[:100]}", tags_filter)
    
    return _get_default_result("Max retries exceeded", tags_filter)


def _calculate_backoff_delay(attempt: int, base_delay: float, max_delay: float = 30.0) -> float:
    """
    Calculate exponential backoff delay with jitter.
    
    Formula: min(base_delay * 2^attempt + random_jitter, max_delay)
    """
    exponential_delay = base_delay * (2 ** attempt)
    jitter = random.uniform(0, base_delay)
    return min(exponential_delay + jitter, max_delay)


async def process_conversations_batch(
    conversations: List[Tuple[int, str]],
    tags_filter: Optional[List[str]] = None,
    progress_callback: Optional[callable] = None
) -> Dict[int, dict]:
    """
    Process multiple conversations concurrently with controlled concurrency.
    
    Args:
        conversations: List of (index, conversation_text) tuples
        tags_filter: List of tag names to analyze. None or empty = all tags.
        progress_callback: Optional callback(completed, total) for progress updates
    
    Returns:
        Dictionary mapping index -> analysis result
    """
    total = len(conversations)
    completed = 0
    results = {}
    
    async def process_single(idx: int, text: str) -> Tuple[int, dict]:
        nonlocal completed
        try:
            result = await analyze_conversation_async(text, tags_filter=tags_filter)
        except Exception as e:
            result = _get_default_result(f"Unexpected error: {str(e)[:50]}", tags_filter)
        
        completed += 1
        if progress_callback and completed % 10 == 0:
            progress_callback(completed, total)
        
        return idx, result
    
    # Create all tasks
    tasks = [process_single(idx, text) for idx, text in conversations]
    
    # Execute all tasks concurrently (semaphore controls actual concurrency)
    task_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results
    for item in task_results:
        if isinstance(item, Exception):
            # This shouldn't happen as we catch exceptions in process_single
            continue
        idx, result = item
        results[idx] = result
    
    return results


def analyze_conversations_sync(
    conversations: List[Tuple[int, str]],
    tags_filter: Optional[List[str]] = None,
    progress_callback: Optional[callable] = None
) -> Dict[int, dict]:
    """
    Synchronous wrapper for batch conversation analysis.
    
    This function runs the async batch processor using asyncio.run(),
    making it compatible with Dagster assets.
    
    Args:
        conversations: List of (index, conversation_text) tuples
        tags_filter: List of tag names to analyze. None or empty = all tags.
        progress_callback: Optional callback(completed, total) for progress updates
    
    Returns:
        Dictionary mapping index -> analysis result
    """
    # Reset the semaphore for a fresh run
    global _semaphore
    _semaphore = None
    
    return asyncio.run(
        process_conversations_batch(conversations, tags_filter, progress_callback)
    )


# Export TAG_DEFINITIONS for use in ai_tags.py
def get_tag_openai_key(tag_name: str) -> Optional[str]:
    """Get the OpenAI key for a given tag name."""
    if tag_name in TAG_DEFINITIONS:
        return TAG_DEFINITIONS[tag_name]["openai_key"]
    return None
