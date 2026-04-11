"""
DSPy Evaluator Variables - Convierte resultados del evaluador DSPy a formato conv_variables.

Este módulo convierte los resultados del evaluador DSPy (score y evaluation_json)
al formato estándar de conv_variables para almacenamiento en la base de datos.
"""

import json
import pandas as pd
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def get_dspy_evaluator_variables(evaluation_result: Dict) -> List[dict]:
    """
    Convierte el resultado del evaluador DSPy a formato conv_variables.
    
    Args:
        evaluation_result: Dict con:
            - score: float (0.0 a 1.0, ponderado por sección)
            - evaluation_json: str (JSON con evaluaciones por sección)
            - parsed_result: dict (opcional)
            - present_sections: list (opcional)
            - section_path: str (path en orden canónico, ej. "profiling,information,scheduling")
            - reached_scheduling: 0|1
            - progressed_to_scheduling_after_info: 0|1
            - last_successful_stage: str (última etapa con success=1)
    
    Returns:
        Lista de diccionarios en formato conv_variables (dspy_score, dspy_section_path, etc.).
    """
    variables = []
    
    # Variable 1: Score general
    score = evaluation_result.get('score', 0.0)
    variables.append({
        'conv_variable_category': 'score',
        'conv_variable_name': 'dspy_score',
        'conv_variable_value': float(score) if score is not None else 0.0
    })
    
    # Variable 2: JSON completo de evaluación
    evaluation_json = evaluation_result.get('evaluation_json', '{}')
    # Asegurar que es un string JSON válido
    if isinstance(evaluation_json, dict):
        evaluation_json = json.dumps(evaluation_json, ensure_ascii=False)
    elif not isinstance(evaluation_json, str):
        evaluation_json = '{}'
    
    variables.append({
        'conv_variable_category': 'score',
        'conv_variable_name': 'dspy_evaluation_json',
        'conv_variable_value': evaluation_json
    })
    
    # Variables 3-N: Evaluaciones por sección (solo si están presentes)
    parsed_result = evaluation_result.get('parsed_result', {})
    present_sections = evaluation_result.get('present_sections', [])
    
    for section in ['profiling', 'information', 'scheduling']:
        if section in parsed_result and section in present_sections:
            section_data = parsed_result[section]
            
            # Success
            success_val = section_data.get('success', 0)
            try:
                if isinstance(success_val, bool):
                    success_int = 1 if success_val else 0
                elif isinstance(success_val, (int, float)):
                    success_int = 1 if int(success_val) == 1 else 0
                else:
                    success_int = 0
            except:
                success_int = 0
            
            variables.append({
                'conv_variable_category': 'score',
                'conv_variable_name': f'dspy_{section}_success',
                'conv_variable_value': success_int
            })
            
            # Failure cause (solo si hay fallo)
            if success_int == 0:
                failure_cause = section_data.get('failure_cause', '') or ''
                variables.append({
                    'conv_variable_category': 'score',
                    'conv_variable_name': f'dspy_{section}_failure_cause',
                    'conv_variable_value': failure_cause
                })
                
                # Failure reason
                failure_reason = section_data.get('failure_reason', '') or ''
                variables.append({
                    'conv_variable_category': 'score',
                    'conv_variable_name': f'dspy_{section}_failure_reason',
                    'conv_variable_value': failure_reason
                })
                
                # Failure event type (nuevo campo)
                failure_event_type = section_data.get('failure_event_type', '') or ''
                variables.append({
                    'conv_variable_category': 'score',
                    'conv_variable_name': f'dspy_{section}_failure_event_type',
                    'conv_variable_value': failure_event_type
                })

    # Progression variables (from evaluator: section_path, reached_scheduling, etc.)
    section_path = evaluation_result.get('section_path', '')
    variables.append({
        'conv_variable_category': 'score',
        'conv_variable_name': 'dspy_section_path',
        'conv_variable_value': section_path if isinstance(section_path, str) else ''
    })
    reached = evaluation_result.get('reached_scheduling', 0)
    variables.append({
        'conv_variable_category': 'score',
        'conv_variable_name': 'dspy_reached_scheduling',
        'conv_variable_value': 1 if reached == 1 else 0
    })
    progressed = evaluation_result.get('progressed_to_scheduling_after_info', 0)
    variables.append({
        'conv_variable_category': 'score',
        'conv_variable_name': 'dspy_progressed_to_scheduling_after_info',
        'conv_variable_value': 1 if progressed == 1 else 0
    })
    last_stage = evaluation_result.get('last_successful_stage', '')
    variables.append({
        'conv_variable_category': 'score',
        'conv_variable_name': 'dspy_last_successful_stage',
        'conv_variable_value': last_stage if isinstance(last_stage, str) else ''
    })

    return variables


def process_dspy_evaluator_batch(
    df: pd.DataFrame,
    golden_dataset_path: str = None,  # Ya no se usa, mantener para compatibilidad
    max_workers: int = 5,
    progress_callback: Optional[callable] = None
) -> Dict[int, List[dict]]:
    """
    Procesa evaluaciones DSPy para un DataFrame completo en batch con concurrencia limitada.
    
    Esta función:
    1. Extrae textos de conversación de todas las filas
    2. Los evalúa con DSPy concurrentemente (max_workers a la vez)
    3. Retorna variables formateadas para cada índice de fila
    
    Args:
        df: DataFrame con 'conversation_text' o 'messages_inline' column
        golden_dataset_path: DEPRECATED - Ya no se usa (Opción B: sin golden dataset)
        max_workers: Número máximo de workers concurrentes (default: 5 para evitar rate limits)
        progress_callback: Callback opcional(completed, total) para actualizaciones de progreso
    
    Returns:
        Diccionario mapeando índice del DataFrame -> lista de diccionarios de variables
    """
    from dagster_pipeline.defs.conversation_analysis.utils.variables.conversation_evaluator_dspy import (
        ConversationEvaluator
    )
    
    # Inicializar evaluador una sola vez (compartido entre threads)
    # Ya no usa golden_dataset_path (Opción B: sin golden dataset)
    try:
        evaluator = ConversationEvaluator()
    except Exception as e:
        print(f"❌ Error inicializando evaluador DSPy: {e}")
        # Retornar variables vacías para todas las filas
        return {idx: [] for idx in df.index}
    
    # Preparar lista de conversaciones: (index, conversation_text, conv_id)
    conversations = []
    skip_indices = set()
    
    for idx, row in df.iterrows():
        # Intentar obtener texto de conversación de diferentes columnas
        conversation_text = (
            row.get('messages_inline') or 
            row.get('conversation_text') or 
            ''
        )
        
        # Si es string, usar directamente; si es lista/dict, intentar extraer
        if isinstance(conversation_text, (list, dict)):
            # Si es una lista de mensajes, necesitamos formatearla
            # Por ahora, saltar si no es string
            skip_indices.add(idx)
            continue
        
        conversation_text = str(conversation_text) if conversation_text else ''
        
        if not conversation_text or conversation_text == 'nan' or len(conversation_text.strip()) == 0:
            skip_indices.add(idx)
        else:
            # Obtener conv_id si está disponible
            conv_id = row.get('conversation_id') or row.get('conv_id') or f'conv_{idx}'
            conversations.append((idx, conversation_text, conv_id))
    
    # Procesar conversaciones con concurrencia limitada
    results = {}
    
    def evaluate_single(conv_data):
        """Evalúa una sola conversación."""
        idx, text, conv_id = conv_data
        try:
            evaluation = evaluator.evaluate(text, conv_id=str(conv_id))
            return idx, evaluation
        except Exception as e:
            print(f"⚠️ Error evaluando conversación {conv_id}: {e}")
            return idx, {
                'evaluation_json': '{}',
                'score': 0.0,
                'parsed_result': {},
                'present_sections': [],
                'error': str(e)
            }
    
    # Procesar en batch con ThreadPoolExecutor
    if conversations:
        completed = 0
        total = len(conversations)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Enviar todas las tareas
            future_to_idx = {
                executor.submit(evaluate_single, conv_data): conv_data[0]
                for conv_data in conversations
            }
            
            # Procesar resultados conforme se completan
            for future in as_completed(future_to_idx):
                try:
                    idx, evaluation = future.result()
                    # Convertir evaluación a variables
                    variables = get_dspy_evaluator_variables(evaluation)
                    results[idx] = variables
                    
                    completed += 1
                    if progress_callback:
                        progress_callback(completed, total)
                except Exception as e:
                    idx = future_to_idx[future]
                    print(f"⚠️ Error procesando resultado para índice {idx}: {e}")
                    results[idx] = []
    
    # Agregar resultados vacíos para índices saltados
    for idx in skip_indices:
        results[idx] = []
    
    # Asegurar que todos los índices tengan resultado
    for idx in df.index:
        if idx not in results:
            results[idx] = []
    
    return results
