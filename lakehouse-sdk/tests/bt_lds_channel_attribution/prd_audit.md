# PRD v2 vs Implementation Audit

Revision cruzada sistematica del PRD `PRD-Atribucion-Canal-Spots-v2.md` contra el codigo implementado.

## Seccion 3 -- Canales

| Requisito | PRD | Implementacion | Estado |
|-----------|-----|----------------|--------|
| Canal 1 ID = 1, nombre = "Algorithm" | 1 = Recomendacion Algoritmo | `lds_channel_attribution_id=1`, `lds_channel_attribution='Algorithm'` | OK |
| Canal 2 ID = 2, nombre = "Chatbot" | 2 = Recomendacion Chatbot | `lds_channel_attribution_id=2`, `lds_channel_attribution='Chatbot'` | OK |
| Canal 3 ID = 3, nombre = "Manual" | 3 = Adicion Manual (fallback) | `lds_channel_attribution_id=3`, `lds_channel_attribution='Manual'` | OK |
| Mutuamente excluyentes | Si | Si (test 6d.2 confirma suma = total) | OK |

## Seccion 4 -- Fuentes de datos

| Fuente | PRD | Implementacion | Estado |
|--------|-----|----------------|--------|
| Algorithm: `recommendation_projects` | `geospot_postgres` | `raw_gs_recommendation_projects_new.py` usa query con JSONB::text | OK |
| Chatbot: `conversation_events` | `staging_postgres`, filtro `event_type='spot_confirmation'` | `raw_cb_conversation_events_new.py` usa `staging_postgres`, SQL filtrado | OK |
| Chatbot flags: `recommendation_chatbot` | `geospot_postgres` | `raw_gs_recommendation_chatbot_new.py` usa query con JSONB::text | OK |
| Manual: fallback sin fuente | No necesita fuente | Implementado como `otherwise(3)` en la logica | OK |

## Seccion 5 -- Indicadores de recomendacion

| Indicador | PRD: Fuente y condicion | Implementacion | Estado |
|-----------|------------------------|----------------|--------|
| `is_recommended_algorithm` | `recommendation_projects`: spot en ANY de spots_suggested, white_list, black_list | JOIN con exploded (todos los list_type) | OK |
| `is_recommended_chatbot` | `recommendation_chatbot`: spot en ANY de spots_suggested, white_list, black_list | JOIN con exploded chatbot (todos los list_type) | OK |
| `is_recommended_visits` | Descartado | No implementado (correcto) | OK |
| Pares _id / texto | `_id` = 0/1 (SMALLINT), texto = 'Yes'/'No' | Implementado (test 6f.2, 6f.3) | OK |

## Seccion 6 -- Algoritmo de atribucion

| Paso | PRD | Implementacion | Estado |
|------|-----|----------------|--------|
| Evidencia Algorithm: `(project_id, spot_id)` en white_list | Paso 1 | LEFT JOIN con exploded filtrado `list_type='white_list'` por `(project_id, spot_id)` | OK |
| Evidencia Chatbot: `(lead_id, spot_id)` en conversation_events | Paso 1, sin project_id | LEFT JOIN con stg_conv_events por `(lead_id, spot_id)` | OK |
| Solo un canal: atribuir al que tenga evidencia | Paso 2.1 | `pl.when(has_algo).then(1).when(has_chat).then(2)` | OK |
| Ambos canales: desempate por fecha mas antigua | Paso 2.2 | `pl.when(algo_first).then(1).otherwise(2)` donde `algo_first = algo_ts <= chat_ts` | OK |
| Sin evidencia: Manual | Paso 2.3 | `.otherwise(3)` | OK |

## Seccion 7 -- Nuevos campos

| Campo | Tipo PRD | Tipo implementado | Posicion PRD | Posicion real | Estado |
|-------|----------|-------------------|-------------|---------------|--------|
| `lds_channel_attribution_id` | SMALLINT | Int16 | Despues de lds_cohort_at | Col 23 (despues lds_cohort_at) | OK |
| `lds_channel_attribution` | VARCHAR(30) | Utf8 | Col 24 | Col 24 | OK |
| `lds_algorithm_evidence_at` | TIMESTAMP | Datetime | Col 25 | Col 25 | OK |
| `lds_chatbot_evidence_at` | TIMESTAMP | Datetime | Col 26 | Col 26 | OK |
| `is_recommended_algorithm_id` | SMALLINT | Int16 | Col 27 | Col 27 | OK |
| `is_recommended_algorithm` | VARCHAR(3) | Utf8 | Col 28 | Col 28 | OK |
| `is_recommended_chatbot_id` | SMALLINT | Int16 | Col 29 | Col 29 | OK |
| `is_recommended_chatbot` | VARCHAR(3) | Utf8 | Col 30 | Col 30 | OK |
| Total columnas: 35 (27+8) | | 35 | | | OK |

## Seccion 8 -- Nuevos assets de Dagster

| Asset PRD | Archivo implementado | Patron | Estado |
|-----------|---------------------|--------|--------|
| `raw_gs_recommendation_projects_new` | `bronze/raw_gs_recommendation_projects_new.py` | `make_bronze_asset` con query custom (JSONB::text) | OK |
| `raw_gs_recommendation_chatbot_new` | `bronze/raw_gs_recommendation_chatbot_new.py` | `make_bronze_asset` con query custom (JSONB::text) | OK |
| `raw_cb_conversation_events_new` | `bronze/raw_cb_conversation_events_new.py` | `@dg.asset` + `_run_bronze_extract` + `staging_postgres` | OK |
| `stg_gs_recommendation_projects_exploded_new` | `silver/stg/stg_gs_recommendation_projects_exploded_new.py` | `make_silver_stg_asset` con transform | OK |
| `stg_gs_recommendation_chatbot_exploded_new` | `silver/stg/stg_gs_recommendation_chatbot_exploded_new.py` | `make_silver_stg_asset` con transform | OK |
| `stg_cb_conversation_events_new` | `silver/stg/stg_cb_conversation_events_new.py` | `make_silver_stg_asset` con transform | OK |

## Seccion 8.4 -- Fork de la funcion de transformacion

| Requisito | Implementacion | Estado |
|-----------|----------------|--------|
| Fork independiente de legacy | `_transform_bt_lds_lead_spots_new` en `gold_bt_lds_lead_spots_new.py` | OK |
| Eliminar import de legacy | Import eliminado; funcion completa en archivo _new | OK |
| Leer nuevos silvers desde S3 | `read_silver_safe()` con fallback a DataFrame vacio | OK |
| Enriquecer Block 3 | `_enrich_spot_added_with_channel_attribution()` | OK |
| Null para bloques 1,2,4-10 | `_null_channel_cols()` en cada bloque | OK |
| Actualizar BT_LDS_LEAD_SPOTS_COLUMN_ORDER | 35 columnas, 8 nuevas despues de lds_cohort_at | OK |

## Seccion 9.8 -- Error handling

| Requisito | Implementacion | Estado |
|-----------|----------------|--------|
| Bronze make_bronze_asset hereda wrapper | Si (via `_build_asset`) | OK |
| Bronze custom usa wrapper | `raw_cb_conversation_events_new` usa `iter_job_wrapped_compute` | OK |
| Gold usa wrapper | `gold_bt_lds_lead_spots_new` ya lo tenia | OK |
| Jobs registrados en LAKEHOUSE_NEW_PIPELINE_JOBS | Nuevos assets agregados a `GOLD_BT_LDS_LEAD_SPOTS_BRONZE/SILVER` | OK |

## ARCHITECTURE.md -- Naming y trazabilidad

| Regla | Cumplimiento | Estado |
|-------|-------------|--------|
| `raw_{source}_{table}` | `raw_gs_recommendation_projects_new`, `raw_gs_recommendation_chatbot_new`, `raw_cb_conversation_events_new` | OK |
| `stg_` = 1:1 con raw | 3 stg para 3 raw | OK |
| STG responsabilidad unica (solo datos de su raw) | Cada STG procesa unicamente su raw | OK |
| Gold no hace transformaciones pesadas (nota: excepcion documentada) | Logica de atribucion en Gold (excepcion pre-existente documentada en PRD) | OK |

## DDL

| Campo | DDL (bt_lds_lead_spots.sql) | Estado |
|-------|---------------------------|--------|
| 8 nuevos campos | Agregados entre lds_cohort_at y aud_inserted_at | OK |
| Index `idx_bt_lds_lead_spots_channel` | `CREATE INDEX ... ON bt_lds_lead_spots(lds_channel_attribution_id)` | OK |
| COMMENTs | 8 nuevos COMMENT ON COLUMN | OK |

## Resultado final

**30/30 checks de regresion pasados. 0 discrepancias PRD vs implementacion.**
