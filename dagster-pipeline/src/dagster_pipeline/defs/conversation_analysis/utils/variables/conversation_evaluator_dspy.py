"""
Conversation Evaluator DSPy - Evaluador de conversaciones por secciones.

Este módulo implementa un evaluador que usa DSPy para evaluar las 3 secciones
(profiling, information, scheduling) de una conversación en una sola llamada.

Usado por el pipeline Dagster (conv_with_dspy_scores). La API key se obtiene
de SSM en prod o de env/local en desarrollo.
"""

import os
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Any

import dspy

# Optional langsmith import
try:
    from langsmith import traceable, Client
    LANGSMITH_AVAILABLE = True
except ImportError:
    def traceable(name=None, run_type=None):
        if callable(name):
            return name
        def decorator(func):
            return func
        return decorator
    Client = None
    LANGSMITH_AVAILABLE = False

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

MODEL_NAME = "gpt-4o-mini"
MAX_TOKENS = 4000
TEMPERATURE = 0.1

# Pesos por sección. Scheduling es obligatorio para poder llegar a 1; information es opcional.
SECTION_WEIGHTS = {"profiling": 0.2, "information": 0.3, "scheduling": 0.5}
CANONICAL_SECTION_ORDER = ["profiling", "information", "scheduling"]
# Denominador: siempre profiling + scheduling (0.7); si hay information se suma 0.3 → 1.0
WEIGHT_REQUIRED = SECTION_WEIGHTS["profiling"] + SECTION_WEIGHTS["scheduling"]  # 0.7


# =============================================================================
# DSPy SIGNATURE
# =============================================================================

class ConversationEvaluationSignature(dspy.Signature):
    """
    Evalúa una conversación completa identificando el éxito/fallo de cada sección.

    Analiza la conversación y evalúa SOLO las secciones que aparecen en ella:
    - profiling: Si aparece [event_section=profiling] en la conversación
    - information: Si aparece [event_section=information] en la conversación  
    - scheduling: Si aparece [event_section=scheduling] en la conversación

    CATEGORÍAS DE FALLO (failure_cause) - SOLO UNA DE ESTAS:
    - "user_abandonment": Usuario dejó de responder después de una acción del bot
    - "user_intent_change": Usuario cambió de intención (ej: abandonó agendar para ver más spots, canceló la cita para otra cosa). NO usar cuando el usuario pide "otra fecha", "escoger otra fecha", "cambiar fecha/hora" dentro de scheduling: eso es parte del flujo normal de agendar.
    - "bot_error": Bot no procesó correctamente, dio respuesta confusa/incorrecta, o no respondió adecuadamente
    - "conversation_loop": Conversación en loop con más de 3 interacciones repetidas (mismos mensajes/acciones)

    REGLAS DE EVALUACIÓN POR SECCIÓN:

    PROFILING (event_section=profiling):
    - ÉXITO (success=1): Usuario confirmó interés en un spot (event_type=spot_confirmation con respuesta positiva de parte del usuario)
    - FALLO (success=0):
      * user_abandonment: Usuario no respondió al sobre la confirmacion del spot ni tampoco pregunto o contonuo algun otro flujo, abandono la conversacion sin agendar ni despedirse.
      * user_intent_change: Usuario cambió de intención (ej: pidió agendar visita pero no completó el formulario o envio otro spot sin nunca agendar)
      * bot_error: Bot no procesó correctamente la solicitud del usuario por un error o entendio algo diferente.

    INFORMATION (event_section=information):
    - ÉXITO (success=1): Usuario interactuó con recomendaciones (click, pregunta sobre spots, quiso ver reafinar busqueda, envio otro link, etc.)
    - FALLO (success=0):
      * user_abandonment: Usuario no respondió después de recommendations_sent, abandono la conversacion sin agendar ni despedirse. Si agendo visito previamente no es user_abandonment.
      * user_intent_change: Usuario cambió de intención (ej: ignoro las reocmendaciones y solicito un agente o no continuo con agendamiento)
      * bot_error: Bot no envió recomendaciones, tuvo un error o alcanzo limite.

    SCHEDULING (event_section=scheduling):
    - ÉXITO (success=1): Usuario completó el agendamiento (event_type=visit_scheduled o scheduling_form_completed). Pedir "otra fecha", "escoger otra fecha", "cambiar la cita" es parte del flujo: si al final agenda, success=1.
    - FALLO (success=0):
      * user_abandonment: Usuario no completó el formulario después de scheduling_form_sent, no selecciono ninguna de las opciones para agendar del bot y tampoco envio un mensaje con intencion de agendar.
      * user_intent_change: Usuario abandonó el agendamiento para otra cosa (ej: canceló para ver más spots). NO es user_intent_change si solo pide otra fecha/hora dentro del mismo flujo de agendar. si al final agenda, success=1.
      * bot_error: Bot no envió el formulario cuando debía, o no envio opciones de agendamiento o hubo error técnico.

    IMPORTANTE: 
    - Solo incluye en el JSON las secciones que realmente aparecen en la conversación
    - Si una sección no aparece, NO la incluyas
    - failure_cause debe ser EXACTAMENTE una de las 4 categorías o string vacío ""
    - failure_event_type debe ser el event_type específico donde ocurrió el fallo (ej: "spot_confirmation", "scheduling_form_sent")
    """
    conversation_text: str = dspy.InputField(
        desc="Conversación en formato inline: [event_section=X; event_type=Y][role]: mensaje"
    )
    
    evaluation_result: str = dspy.OutputField(
        desc="""JSON con las evaluaciones. Solo incluye secciones que aparecen en la conversación.
        
        Formato exacto:
        {
          "profiling": {
            "success": 1|0,
            "failure_cause": "user_abandonment"|"user_intent_change"|"bot_error"|"conversation_loop"|"",
            "failure_reason": "Descripción detallada del error",
            "failure_event_type": "spot_confirmation"|"conversation_start"|""  // El event_type donde ocurrió
          },
          "information": {...},
          "scheduling": {...}
        }
        
        REGLAS:
        - success: 1 = éxito, 0 = fallo
        - failure_cause: Solo si success=0. DEBE SER EXACTAMENTE: "user_abandonment", "user_intent_change", "bot_error", "conversation_loop", o "" si success=1. En scheduling: "escoger otra fecha" / "otra fecha" NO es user_intent_change; es flujo normal de agendar.
        - failure_reason: Solo si success=0. Descripción detallada explicando por qué falló
        - failure_event_type: Solo si success=0. El event_type específico donde detectaste el fallo (ej: "spot_confirmation", "scheduling_form_sent", "recommendations_sent")
        - Si success=1, failure_cause, failure_reason y failure_event_type deben ser strings vacíos
        - user_abandonment no aplica para conversaciones donde el usuario ya agendo visito previamente.
        """
    )


# =============================================================================
# DSPy MODULE
# =============================================================================

class ConversationEvaluatorModule(dspy.Module):
    """Módulo DSPy para evaluar conversaciones completas."""
    
    def __init__(self):
        super().__init__()
        self.evaluator = dspy.ChainOfThought(ConversationEvaluationSignature)
    
    def forward(self, conversation_text: str) -> str:
        """Evalúa la conversación sin few-shot examples."""
        result = self.evaluator(conversation_text=conversation_text)
        if not hasattr(result, 'evaluation_result'):
            if hasattr(result, 'result'):
                return str(result.result)
            elif isinstance(result, str):
                return result
            else:
                return str(result)
        return result.evaluation_result


# =============================================================================
# CLASE PRINCIPAL: ConversationEvaluator
# =============================================================================

class ConversationEvaluator:
    """
    Evaluador de conversaciones basado en DSPy SIN golden dataset.
    
    Características:
    - Una sola llamada API por conversación
    - Firma mejorada con categorías fijas de fallo
    - Evalúa las 3 secciones: profiling, information, scheduling
    - Output en JSON con todas las evaluaciones
    - Calcula score como promedio de secciones presentes
    - Detecta conversation_loop (más de 3 interacciones repetidas)
    """
    
    VALID_FAILURE_CAUSES = ["user_abandonment", "user_intent_change", "bot_error", "conversation_loop"]
    
    def __init__(
        self, 
        api_key: str = None
    ):
        """
        Inicializa el evaluador. En pipeline Dagster la API key se obtiene de SSM (get_openai_api_key).
        """
        if api_key is None:
            from dagster_pipeline.defs.conversation_analysis.utils.database import get_openai_api_key
            api_key = get_openai_api_key()
        
        if not api_key:
            raise ValueError("❌ OPENAI_API_KEY no encontrada (SSM o env)")
        
        # Configurar LangSmith si está disponible
        self.langsmith_enabled = False
        if LANGSMITH_AVAILABLE:
            langchain_tracing = os.getenv("LANGCHAIN_TRACING_V2", "false").lower() == "true"
            langchain_project = os.getenv("LANGCHAIN_PROJECT", "")
            langchain_api_key = os.getenv("LANGCHAIN_API_KEY", "")
            
            if langchain_tracing and langchain_api_key:
                self.langsmith_enabled = True
                self.langsmith_project = langchain_project or "dspy-conversation-evaluator"
                print(f"   LangSmith tracing: ✅ Habilitado (proyecto: {self.langsmith_project})")
            else:
                print(f"   LangSmith tracing: ⚠️ Variables de entorno no configuradas")
        
        # Configurar DSPy
        self.lm = dspy.LM(
            model=f"openai/{MODEL_NAME}",
            api_key=api_key,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS
        )
        dspy.configure(lm=self.lm)
        
        if not dspy.settings.lm:
            raise RuntimeError("❌ DSPy no se configuró correctamente. Verifica la API key.")
        
        self.evaluator_module = ConversationEvaluatorModule()
        
        print(f"✅ ConversationEvaluator inicializado (modelo: {MODEL_NAME})")
        print(f"   Sin golden dataset - usando firma mejorada")
        print(f"   Categorías de fallo: {', '.join(self.VALID_FAILURE_CAUSES)}")
        
        if self.langsmith_enabled:
            print(f"\n🔗 Ver resultados en LangSmith:")
            print(f"   https://smith.langchain.com/")
            print(f"   Proyecto: {self.langsmith_project}")
        elif LANGSMITH_AVAILABLE:
            print(f"\n💡 Para habilitar LangSmith tracing, configura en .env o SSM:")
            print(f"   LANGCHAIN_TRACING_V2=true")
            print(f"   LANGCHAIN_PROJECT=dspy-conversation-evaluator")
            print(f"   LANGCHAIN_API_KEY=tu_api_key")
    
    def _detect_conversation_loop(self, conversation_text: str, section: str) -> bool:
        """Detecta si hay un loop de conversación en una sección (más de 3 interacciones repetidas)."""
        import re
        from collections import Counter
        
        section_pattern = rf'\[event_section={re.escape(section)}[^\]]*\](\[[^\]]+\]:[^\n]+)'
        section_messages = re.findall(section_pattern, conversation_text)
        
        if len(section_messages) < 4:
            return False
        
        normalized_messages = [msg.strip().lower()[:100] for msg in section_messages]
        message_counts = Counter(normalized_messages)
        
        for msg, count in message_counts.items():
            if count >= 4:
                return True
        
        if len(normalized_messages) >= 6:
            for i in range(len(normalized_messages) - 5):
                seq = tuple(normalized_messages[i:i+2])
                occurrences = 0
                for j in range(len(normalized_messages) - 1):
                    if tuple(normalized_messages[j:j+2]) == seq:
                        occurrences += 1
                if occurrences >= 3:
                    return True
        
        return False
    
    def _validate_failure_cause(self, failure_cause: str) -> str:
        """Valida y normaliza failure_cause a una de las categorías válidas."""
        if not failure_cause or not isinstance(failure_cause, str):
            return ""
        
        failure_cause = failure_cause.strip().lower()
        
        cause_mapping = {
            "user_abandonment": "user_abandonment",
            "abandonment": "user_abandonment",
            "user abandoned": "user_abandonment",
            "usuario abandonó": "user_abandonment",
            "user_intent_change": "user_intent_change",
            "intent_change": "user_intent_change",
            "cambio de intención": "user_intent_change",
            "changed intent": "user_intent_change",
            "bot_error": "bot_error",
            "error": "bot_error",
            "bot error": "bot_error",
            "error del bot": "bot_error",
            "conversation_loop": "conversation_loop",
            "loop": "conversation_loop",
            "conversation loop": "conversation_loop",
            "loop de conversación": "conversation_loop",
        }
        
        if failure_cause in self.VALID_FAILURE_CAUSES:
            return failure_cause
        
        for key, value in cause_mapping.items():
            if key in failure_cause or failure_cause in key:
                return value
        
        return ""
    
    def _detect_present_sections(self, conversation_text: str) -> List[str]:
        """Detecta qué secciones están presentes en la conversación."""
        if not conversation_text or not isinstance(conversation_text, str):
            return []
        
        import re
        present_sections = set()
        section_pattern = r'\[event_section=([^\]]+)\]'
        matches = re.findall(section_pattern, conversation_text)
        
        for match in matches:
            section = match.split(';')[0].strip().lower()
            if section in ['profiling', 'information', 'scheduling']:
                present_sections.add(section)
        
        return sorted(list(present_sections))
    
    @traceable(name="evaluate_conversation", run_type="chain")
    def evaluate(self, conversation_text: str, conv_id: str = None) -> Dict[str, Any]:
        """
        Evalúa una conversación completa, solo las secciones presentes.
        
        Returns:
            Dict con evaluation_json, score, parsed_result, present_sections, conv_id (opcional).
        """
        try:
            present_sections = self._detect_present_sections(conversation_text)
            
            if conv_id:
                print(f"🔍 [{conv_id}] Secciones detectadas: {present_sections}")
            
            if not present_sections:
                if conv_id:
                    print(f"⚠️ [{conv_id}] No se detectaron secciones en la conversación")
                result = {
                    "evaluation_json": "{}",
                    "score": 0.0,
                    "parsed_result": {},
                    "present_sections": [],
                    "section_path": "",
                    "reached_scheduling": 0,
                    "progressed_to_scheduling_after_info": 0,
                    "last_successful_stage": "",
                }
                if conv_id:
                    result["conv_id"] = conv_id
                return result
            
            try:
                if conv_id:
                    print(f"🔄 [{conv_id}] Llamando a DSPy para evaluar {len(present_sections)} secciones...")
                
                result_json = self.evaluator_module(
                    conversation_text=conversation_text
                )
                
                if not result_json:
                    raise ValueError("El modelo retornó None")
                if isinstance(result_json, str) and result_json.strip() == '':
                    raise ValueError("El modelo retornó un string vacío")
                
                if conv_id:
                    print(f"✅ [{conv_id}] DSPy retornó resultado (longitud: {len(str(result_json))})")
                
            except Exception as e:
                error_msg = f"❌ Error en llamada DSPy: {e}"
                if conv_id:
                    error_msg = f"❌ [{conv_id}] Error en llamada DSPy: {e}"
                print(error_msg)
                print(f"   Secciones presentes: {present_sections}")
                print(f"   Tipo de resultado: {type(result_json) if 'result_json' in locals() else 'N/A'}")
                raise
            
            try:
                parsed_result = json.loads(result_json)
                if conv_id:
                    print(f"✅ [{conv_id}] JSON parseado correctamente")
            except json.JSONDecodeError:
                import re
                json_match = re.search(r'\{.*\}', result_json, re.DOTALL)
                if json_match:
                    try:
                        parsed_result = json.loads(json_match.group())
                        if conv_id:
                            print(f"✅ [{conv_id}] JSON extraído y parseado")
                    except Exception as parse_error:
                        print(f"⚠️ Error parseando JSON extraído: {parse_error}")
                        raise ValueError(f"No se pudo parsear el JSON extraído: {str(parse_error)}")
                else:
                    print(f"⚠️ No se encontró JSON en la respuesta")
                    raise ValueError(f"No se encontró JSON válido en la respuesta. Primeros 200 chars: {result_json[:200]}")
            
            normalized_result = {}
            for section in ['profiling', 'information', 'scheduling']:
                if section in parsed_result:
                    section_data = parsed_result[section]
                    if isinstance(section_data, dict):
                        success_val = section_data.get('success', 0)
                        failure_cause_raw = section_data.get('failure_cause', '') or ''
                        failure_reason = section_data.get('failure_reason', '') or ''
                        failure_event_type = section_data.get('failure_event_type', '') or ''
                        
                        failure_cause = self._validate_failure_cause(failure_cause_raw)
                        
                        if success_val == 0 and not failure_cause:
                            if self._detect_conversation_loop(conversation_text, section):
                                failure_cause = "conversation_loop"
                                if not failure_reason:
                                    failure_reason = f"Loop detectado en sección {section} con más de 3 interacciones repetidas"
                        
                        if success_val == 1:
                            failure_cause = ""
                            failure_reason = ""
                            failure_event_type = ""
                        
                        normalized_result[section] = {
                            'success': success_val,
                            'failure_cause': failure_cause,
                            'failure_reason': failure_reason,
                            'failure_event_type': failure_event_type
                        }
                    else:
                        normalized_result[section] = {
                            'success': 0,
                            'failure_cause': '',
                            'failure_reason': '',
                            'failure_event_type': ''
                        }
            
            def _success_int(section_data: dict) -> int:
                # Si el modelo detectó fallo (failure_cause o failure_reason), esa sección es 0
                fc = (section_data.get("failure_cause") or "").strip()
                fr = (section_data.get("failure_reason") or "").strip()
                if fc or fr:
                    return 0
                success_val = section_data.get("success", 0)
                try:
                    if isinstance(success_val, bool):
                        return 1 if success_val else 0
                    if isinstance(success_val, (int, float)):
                        return 1 if int(success_val) == 1 else 0
                    if isinstance(success_val, str):
                        return 1 if success_val.strip().lower() in ("1", "true", "yes") else 0
                except Exception:
                    pass
                return 0

            # Score: numerador = suma(success * peso) de secciones presentes.
            # Denominador fijo: profiling + scheduling siempre (0.7); si hay information +0.3 → 1.0.
            # Así profiling+scheduling sin errores = 1.0; solo profiling no puede ser 1.
            weighted_sum = 0.0
            for section in present_sections:
                w = SECTION_WEIGHTS.get(section, 0.0)
                if w <= 0:
                    continue
                section_data = normalized_result.get(section, {})
                si = _success_int(section_data)
                weighted_sum += si * w
            total_weight = WEIGHT_REQUIRED + (
                SECTION_WEIGHTS["information"] if "information" in present_sections else 0.0
            )
            score = min(1.0, (weighted_sum / total_weight) if total_weight > 0 else 0.0)

            # Path en orden canónico (solo secciones presentes)
            section_path = ",".join(
                s for s in CANONICAL_SECTION_ORDER if s in present_sections
            )

            # Llegó y pasó scheduling
            scheduling_ok = _success_int(normalized_result.get("scheduling", {}))
            reached_scheduling = 1 if ("scheduling" in present_sections and scheduling_ok == 1) else 0

            # Estuvo en information y pasó scheduling
            progressed_to_scheduling_after_info = (
                1
                if (
                    "information" in present_sections
                    and "scheduling" in present_sections
                    and scheduling_ok == 1
                )
                else 0
            )

            # Última etapa con success=1 (orden canónico)
            last_successful_stage = ""
            for s in reversed(CANONICAL_SECTION_ORDER):
                if s in present_sections and _success_int(normalized_result.get(s, {})) == 1:
                    last_successful_stage = s
                    break

            final_result = {}
            for section in present_sections:
                if section in normalized_result:
                    final_result[section] = normalized_result[section]
            
            final_json = json.dumps(final_result, ensure_ascii=False)
            
            result = {
                "evaluation_json": final_json,
                "score": score,
                "parsed_result": final_result,
                "present_sections": present_sections,
                "section_path": section_path,
                "reached_scheduling": reached_scheduling,
                "progressed_to_scheduling_after_info": progressed_to_scheduling_after_info,
                "last_successful_stage": last_successful_stage,
            }
            if conv_id:
                result["conv_id"] = conv_id
            return result
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"❌ Error evaluando conversación: {e}")
            if conv_id:
                print(f"   Traceback: {error_details}")
            
            result = {
                "evaluation_json": "{}",
                "score": 0.0,
                "parsed_result": {},
                "present_sections": present_sections if "present_sections" in locals() else [],
                "section_path": "",
                "reached_scheduling": 0,
                "progressed_to_scheduling_after_info": 0,
                "last_successful_stage": "",
                "error": str(e),
            }
            if conv_id:
                result["conv_id"] = conv_id
            return result


def evaluate_conversations_to_dataframe(
    evaluator: ConversationEvaluator,
    conversations_df: pd.DataFrame,
    conversation_text_col: str = 'conversation_text',
    conv_id_col: str = 'conv_id'
) -> pd.DataFrame:
    """Evalúa múltiples conversaciones y retorna un DataFrame con los resultados."""
    results = []
    
    for idx, row in conversations_df.iterrows():
        conv_id = row.get(conv_id_col, f'conv_{idx}')
        conversation_text = row.get(conversation_text_col, '')
        
        if not conversation_text:
            continue
        
        evaluation = evaluator.evaluate(conversation_text, conv_id=conv_id)
        parsed = evaluation.get('parsed_result', {})
        
        result_row = {
            'conv_id': conv_id,
            'score': evaluation.get('score', 0.0),
            'evaluation_json': evaluation.get('evaluation_json', '{}')
        }
        
        present_sections = evaluation.get('present_sections', [])
        
        for section in ['profiling', 'information', 'scheduling']:
            if section in parsed and section in present_sections:
                section_data = parsed[section]
                result_row[f'{section}_success'] = section_data.get('success', 0)
                result_row[f'{section}_failure_cause'] = section_data.get('failure_cause', '') or ''
                result_row[f'{section}_failure_reason'] = section_data.get('failure_reason', '') or ''
            else:
                result_row[f'{section}_success'] = None
                result_row[f'{section}_failure_cause'] = ''
                result_row[f'{section}_failure_reason'] = ''
        
        results.append(result_row)
    
    return pd.DataFrame(results)
