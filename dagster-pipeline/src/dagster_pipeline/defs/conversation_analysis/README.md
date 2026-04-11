# Conversation Analysis Pipeline — Documentación para agentes

Este README describe el pipeline de **conversation_analysis** con el detalle necesario para que un agente (o un desarrollador) pueda extenderlo, modificar lógica o replicar patrones en otro pipeline (por ejemplo `tickets_service_desk`).

---

## 1. Propósito del pipeline

- **Origen**: mensajes de chat (PostgreSQL Chatbot), clientes (MySQL prod), eventos (Staging/Spot Chatbot), project_requirements (MySQL).
- **Lecturas Geospot**: tabla canónica **`bt_conv_conversations`** (incremental mask, upsert merge, SQL de análisis).
- **Escritura Geospot** (S3 + API replace): solo **`bt_conv_conversations_bck`** — el lakehouse (`gold_bt_conv_conversations_new`) sigue siendo quien carga **`bt_conv_conversations`**.
- **Flujo**: extraer → agrupar → enriquecer con eventos/funnel/proyectos → calcular variables (métricas, tags, OpenAI, DSPy) → upsert en Geospot → subir CSV a S3 y disparar reemplazo de tabla.

---

## 2. Estructura de carpetas y módulos

```
defs/conversation_analysis/
├── __init__.py          # Exporta assets, job y schedules para load_from_defs_folder
├── config.py            # Clases de configuración por asset/op
├── jobs.py              # conv_job, conv_schedule_hourly, conv_schedule_4am, selección de assets
├── assets.py            # Todos los @asset del pipeline (ver sección 4)
├── queries/             # SQL parametrizados (placeholders {start_date}, etc.)
│   ├── messages.sql
│   ├── clients.sql
│   ├── project_requirements.sql
│   └── chat_events.sql
├── utils/
│   ├── __init__.py      # Reexporta database, processors, s3_upload, etc.
│   ├── database.py      # Conexiones SSM, engines (chatbot, prod, geospot, spot_chatbot)
│   ├── phone_formats.py # Normalización de teléfonos para merge
│   ├── processors.py    # group_conversations, create_conv_variables
│   ├── s3_upload.py     # sanitize_*, prepare_dataframe_for_csv, upload, trigger_table_replacement
│   ├── openai_client.py # OpenAI + TAG_DEFINITIONS para AI tags
│   └── variables/       # Creación de conv_variables
│       ├── __init__.py
│       ├── metrics.py   # messages_count, user_messages_count, total_time, etc.
│       ├── tags.py      # active_user, engaged_user, visit_intention, scheduled_visit, created_project, follow_up_success
│       ├── ai_tags.py   # process_ai_tags_batch (OpenAI), TAG_DEFINITIONS
│       ├── llm_judge.py # format_conversation_with_markers (formato inline para DSPy)
│       ├── dspy_evaluator.py # process_dspy_evaluator_batch, get_dspy_evaluator_variables
│       ├── experiments.py    # get_experiment_variables, get_unification_mgs_variant, get_bot_graph_variant
│       └── conversation_evaluator_dspy.py  # Lógica interna del evaluador DSPy
└── notebooks/           # Exploración y pruebas (fuera del DAG); trigger_replace_geospot.py (solo disparar replace si el CSV ya está en S3), upload_to_geospot.py (subir CSV y replace)
```

---

## 3. Configuración (`config.py`)

Todas las configs heredan de `dagster.Config`. Se pasan por op config en la UI (por ejemplo `conv_raw_conversations`, `conv_with_variables`, etc.).

### 3.1 Fechas dinámicas

- `get_dynamic_start_date()` → 3 días antes de hoy (`%Y-%m-%d`).
- `get_dynamic_end_date()` → hoy (`%Y-%m-%d`).
- Se usan cuando `start_date` / `end_date` vienen vacíos (`""`) en `ConversationConfig`.

### 3.2 `ConversationConfig` (asset: conv_raw_conversations)

| Campo       | Tipo | Default | Descripción |
|------------|------|---------|-------------|
| `start_date` | str | `""` | Inicio del rango de mensajes. Vacío = dinámico (3 días atrás). |
| `end_date`   | str | `""` | Fin del rango. Vacío = hoy. |

### 3.3 `ConvVariablesConfig` (assets: conv_with_variables, conv_with_dspy_scores)

| Campo | Tipo | Default | Descripción |
|-------|------|---------|-------------|
| `test_mode` | bool | False | Si True, se muestrea con `test_sample_size` registros. |
| `test_sample_size` | int | 50 | Tamaño de muestra en test_mode. |
| `enable_openai_analysis` | bool | True | Ejecutar AI tags (OpenAI). |
| `openai_start_date` | Optional[str] | None | Fecha desde la cual analizar con OpenAI (YYYY-MM-DD). |
| `ai_tags_filter` | List[str] | [] | Lista de tags a ejecutar; vacío = todos. Ver `ALL_AI_TAGS` en config. |
| `enable_dspy_evaluator` | bool | True | Ejecutar evaluador DSPy (scores en conv_variables). |
| `use_incremental_mask` | bool | True | True = solo procesar conv_id nuevos o con len(conv_messages) distinto; False = procesar todos. |

**AI tags por defecto (ALL_AI_TAGS):** is_broker, info_requested, missed_messages, scaling_agent. Con `ai_tags_filter` se puede restringir o ampliar (nombres deben existir en TAG_DEFINITIONS).

### 3.4 `ConvUpsertConfig` (asset: conv_upsert)

| Campo | Tipo | Default | Descripción |
|-------|------|---------|-------------|
| `preserve_existing_conv_variables` | bool | False | True = en updates fusionar conv_variables (no pisar score/ai_tag con vacíos); False = reemplazo directo. |

### 3.5 `FinalOutputConfig` (asset: conv_final_output)

| Campo | Tipo | Default | Descripción |
|-------|------|---------|-------------|
| `test_mode` | bool | False | No disparar table replacement en Geospot. |
| `dev_mode` | bool | False | Subir a S3 pero no disparar table replacement. |
| `test_sample_size` | int | 50 | Usado si se aplica sample en ese asset. |

---

## 4. Assets (orden y dependencias)

Cada asset es un `@asset` en `assets.py`. El grafo es lineal salvo ramas explícitas.

| Asset | Config | Dependencias | Descripción breve |
|------|--------|--------------|-------------------|
| **conv_raw_conversations** | ConversationConfig | — | Lee mensajes de PostgreSQL (Chatbot) con `queries/messages.sql`; fechas por config o dinámicas. |
| **conv_raw_clients** | — | — | Lee clientes de MySQL con `queries/clients.sql`. |
| **conv_raw_project_requirements** | — | — | Lee project_requirements + calendar (visit_created_at, visit_updated_at, project_created_at) con `queries/project_requirements.sql`. |
| **conv_grouped** | — | conv_raw_conversations | Agrupa mensajes en conversaciones por gap de 72h (`group_conversations` en processors); genera `conversation_id` = `{phone}_{YYYYMMDD}`. |
| **conv_raw_events** | — | conv_raw_conversations | Lee eventos de Staging (chat_events.sql); rango de fechas derivado de los mensajes cargados. |
| **conv_with_events** | — | conv_grouped, conv_raw_events | Enriquece cada mensaje con `event_type` y `event_section` según eventos por conversation_id y ventana temporal. |
| **conv_merged** | — | conv_with_events, conv_raw_clients | Merge con clientes por teléfono (phone_formats); añade lead_id, fecha_creacion_cliente. |
| **conv_with_funnels** | — | conv_merged, conv_raw_events | Calcula funnel_type y funnel_section desde eventos en ventana [conv_start±2h, conv_end±2h], sin duplicados consecutivos. |
| **conv_with_projects** | — | conv_with_funnels, conv_raw_project_requirements | scheduled_visit_count: (1) visit_created_at en [conv_start, conv_end+5h], (2) si 0, visit_updated_at en ese rango, (3) si sigue 0, cuenta event_type=visit_scheduled en mensajes. created_project_count: proyectos con project_created_at en [conv_start-3h, conv_end]. |
| **conv_raw_lk_projects** | — | — | Lee lk_projects desde Geospot (columnas para seniority). Si lk_projects está en otro sistema, sustituir por asset que use MCP o query_mysql. |
| **conv_seniority** | — | conv_with_projects, conv_raw_lk_projects | Asigna seniority_level por conv_id: join por lead_id a lk_projects. Por lead se toma **el proyecto más avanzado** según funnel (project_created_at → visit_created_date → visit_confirmed_at → visit_realized_at → loi_date); si empate, el **más reciente** (project_updated_at DESC). Salida: conversation_id, seniority_level. |
| **conv_with_variables** | ConvVariablesConfig | conv_with_projects, conv_raw_events, conv_seniority | Máscara incremental (opcional), variables base, experiment variables (unification_mgs, bot_graph), follow_up_success, funnel_events, segment.seniority_level, AI tags en batch (OpenAI). Añade columna `_to_process`. |
| **conv_with_dspy_scores** | ConvVariablesConfig | conv_with_variables | Para filas _to_process y con texto inline válido, ejecuta DSPy y añade variables de score a conv_variables (dspy_score, dspy_evaluation_json, etc.). |
| **conv_upsert** | ConvUpsertConfig | conv_with_dspy_scores | Lee Geospot, combina inserts/updates; en updates puede fusionar conv_variables (preserve=True) o reemplazar (preserve=False). Renombra columnas a nombres finales (conv_id, conv_messages, etc.). |
| **conv_final_output** | FinalOutputConfig | conv_upsert | Añade columnas de auditoría, sube CSV a S3 (prepare_dataframe_for_csv + upload_dataframe_to_s3), y si no test/dev dispara trigger_table_replacement. |

**Nota:** El job en `jobs.py` puede incluir además `cleanup_storage` (asset que no está definido en este módulo; puede venir de otro componente del repo). La selección efectiva es `conv_final_output.upstream() | cleanup_selection`.

---

## 5. Lógica crítica por componente

### 5.1 Máscara incremental (`use_incremental_mask`)

- **Función:** `_compute_incremental_mask` en `assets.py`.
- **Entrada:** DataFrame de conv_with_projects, `query_geospot`, nombre de tabla, log.
- **Firma de igualdad:** `conv_id` existe en Geospot **y** `len(sanitize_json_string(messages)) == len(conv_messages)` en Geospot → no procesar (reutilizar conv_variables de Geospot).
- **Importante:** La longitud en pipeline se calcula con `sanitize_json_string(row["messages"])` (mismo criterio que al escribir en Geospot en s3_upload) para evitar falsos len_mismatch.
- **Ventana de lectura Geospot:** rango de fechas de las conversaciones ±3 días.
- **Salida:** columna `_to_process` (bool) y opcionalmente el DataFrame de Geospot para rellenar conv_variables en filas no procesadas.

### 5.2 conv_variables: base, experiment, AI tags, DSPy

- **Base:** `create_conv_variables(row, enable_ai_tags=False)` en `processors.py` llama a metrics (messages_count, user_messages_count, user_spot2_links_count, total_time_conversation en minutos, user_average_response_time, funnel_type, funnel_section) y tags (active_user, engaged_user, visit_intention, scheduled_visit). AI tags no se hacen aquí.
- **Experimentos:** `get_experiment_variables`, `get_follow_up_success`, `get_unification_mgs_variant`, `get_bot_graph_variant` usando eventos en ventana de la conversación (variables: test_schedule, test_schedule_result, follow_up_success, unification_mgs, bot_graph). bot_graph toma valor de event_data["ab_variants"]["monolith_graph"] (monolith/control) con prioridad de evento: conversation_start > spot_link_received > spot_confirmation > otro.
- **AI tags:** `process_ai_tags_batch` (utils/variables/ai_tags.py) con OpenAI; opcionalmente `tags_filter`; 20 peticiones concurrentes; resultado se fusiona a la lista de conv_variables por fila.
- **DSPy:** `process_dspy_evaluator_batch` (dspy_evaluator.py); conv_variables se parsean, se añaden variables de categoría `score` (dspy_score, dspy_evaluation_json, dspy_*_success, etc.) y se vuelven a serializar a JSON.

### 5.3 Upsert y merge de conv_variables

- **Columnas enviadas a Geospot:** conv_id, lead_phone_number, conv_start_date, conv_end_date, conv_text, conv_messages, conv_last_message_date, lead_id, lead_created_at, conv_variables (y las de auditoría en final_output).
- **conv_messages:** siempre se reemplazan con el payload del pipeline (no se hace merge de mensajes).
- **conv_variables en updates:** si `preserve_existing_conv_variables` es True, se llama `_merge_conv_variables_for_upsert(existing_vars, new_vars)`. Categorías tratadas como “IA” para preservar: `AI_VARIABLE_CATEGORIES = ("score", "ai_tag")`. Si el valor nuevo está “vacío” (None o string vacío) y el existente tiene valor, se conserva el existente; si no, se usa el nuevo. Variables existentes de categoría score/ai_tag que no vengan en el nuevo payload se añaden al final del merge.
- **Normalización de preserve:** en conv_upsert se lee como bool/string ("true"/"false") y se loguea el valor efectivo.

### 5.4 scheduled_visit (conv_with_projects)

- **Paso 1:** contar visitas con `visit_created_at` en [conv_start, conv_end + 5h].
- **Paso 2:** si ese conteo es 0, contar con `visit_updated_at` en ese mismo rango.
- **Paso 3:** si sigue 0, contar en los mensajes de la conversación cuántos tienen `event_type == 'visit_scheduled'` y usar ese número como scheduled_visit_count.

### 5.5 Enriquecimiento de mensajes con eventos (conv_with_events)

- Por cada mensaje se busca el evento más adecuado por conversation_id (msg.id) y timestamp: preferencia por evento antes o en el momento del mensaje (ventana de lookback 12h), o evento hasta 2 minutos después si no hay “antes”. Cada mensaje gana `event_type` y `event_section`.

### 5.6 Funnel (conv_with_funnels)

- Por cada conversación se recogen todos los conversation_ids de sus mensajes; se agregan eventos de esos IDs en ventana [conv_start - 2h, conv_end + 2h], se ordenan por tiempo y se construyen cadenas sin duplicados consecutivos para `event_type` (funnel_type) y `event_section` (funnel_section).

### 5.7 Listado completo de conv_variables (detalle)

Todas las variables se guardan en `conv_variables` (lista de dicts con `conv_variable_category`, `conv_variable_name`, `conv_variable_value`). Origen: **metrics** (processors), **tags** (processors), **experiments** (assets), **funnel_events** (assets), **segment** (assets, lk_projects), **ai_tag** (OpenAI batch), **score** (DSPy). Las de **funnel** (llm_judge) no se usan en el flujo principal actual.

#### Métricas (`metric`, `funnel_metric`)

| conv_variable_name | Categoría | Detalle |
|--------------------|-----------|---------|
| **messages_count** | metric | Número total de mensajes en la conversación. |
| **user_messages_count** | metric | Número de mensajes del usuario (type=user). |
| **user_spot2_links_count** | metric | Cantidad de URLs distintas que el usuario envió con https://spot2.mx (cualquier path). |
| **total_time_conversation** | metric | Duración de la conversación en **minutos** (conversation_end - conversation_start). |
| **user_average_time_respond_minutes** | metric | Promedio de minutos entre mensaje del asistente y la siguiente respuesta del usuario. |
| **funnel_type** | funnel_metric | Secuencia de `event_type` por orden cronológico (sin duplicados consecutivos), separada por " > ". Calculada en conv_with_funnels desde eventos en [conv_start-2h, conv_end+2h]. |
| **funnel_section** | funnel_metric | Secuencia de `event_section` por orden cronológico (sin duplicados consecutivos), separada por " > ". Misma ventana que funnel_type. |

#### Tags (`tag`)

| conv_variable_name | Detalle |
|--------------------|---------|
| **active_user** | 1 si el usuario envió más de 1 mensaje; 0 si no. |
| **engaged_user** | 1 si el usuario envió más de 2 mensajes; 0 si no. |
| **visit_intention** | 1 si hay intención de visita: frases del asistente sobre agendar visita o el usuario dice "agendar visita"/"agendar cita"; 0 si no. |
| **scheduled_visit** | Conteo de visitas agendadas en el rango de la conversación (visit_created_at en [conv_start, conv_end+5h], o visit_updated_at, o event_type=visit_scheduled en mensajes). Pre-calculado en conv_with_projects. |

#### Experimentos (`experiments`)

| conv_variable_name | Detalle |
|--------------------|---------|
| **test_schedule** | Variante AB del experimento: se toma de `event_data["ab_test_variant"]` del primer evento `explicit_visit_request`, o si no hay, del primer `visit_scheduled`. Solo conversaciones con conversation_start >= 2026-01-28. |
| **test_schedule_result** | Resultado del experimento (0–3 si la variante viene de explicit_visit_request: 0=sin form ni visit, 1=visit sin form, 2=form y visit, 3=form sin visit; 1 o 2 si la variante viene solo de visit_scheduled). |
| **follow_up_success** | 1 si después del primer evento `scheduling_reminder` hay un `visit_scheduled`; 0 si hay reminder pero no visit. No se añade si no hay scheduling_reminder. |
| **unification_mgs** | Variante del primer evento `spot_link_received`: valor de `event_data["first_interaction_variant"]`; solo "control" o "combined". No se añade si no hay evento o valor inválido. |
| **bot_graph** | Variante de `event_data["ab_variants"]["monolith_graph"]`: "monolith" o "control". Se elige un evento por prioridad (conversation_start > spot_link_received > spot_confirmation > otro) y se itera hasta encontrar uno con ab_variants.monolith_graph válido. No se añade si no hay ninguno. |

#### Funnel events (`funnel_events`)

Variables binarias (1/0) según secuencia de eventos en la ventana de la conversación. Cuando no aplica el funnel, la variable no se añade (None).

| conv_variable_name | Detalle |
|--------------------|---------|
| **spot_link_to_spot_conf** | 1 si hay `spot_link_received` y luego `spot_confirmation`, o si unification_mgs = 'combined'; 0 si hay spot_link pero no spot_conf y no combined; no se añade si no hay spot_link y no combined. |
| **spot_conf_to_request_visit** | 1 si (hay `spot_confirmation` o unification_mgs = 'combined') y luego `explicit_visit_request`; 0 si (spot_conf o combined) pero no explicit_visit_request; no se añade si no hay spot_conf y no combined. |
| **visit_request_to_schedule** | 1 si hay `explicit_visit_request` y luego `visit_scheduled` o scheduled_visit_count > 0; 0 si hay explicit_visit_request pero no visit; no se añade si no hay explicit_visit_request. |
| **reco_index** | 1 si hay `recommendations_sent` y luego `recommendation_scheduling`; 0 si hay recommendations_sent pero no recommendation_scheduling; no se añade si no hay recommendations_sent. |
| **schedule_form_index** | 1 si hay `scheduling_form_sent` y luego `scheduling_form_completed`; 0 si hay scheduling_form_sent pero no completed; no se añade si no hay scheduling_form_sent. |
| **profiling_form_index** | 1 si hay `profiling_form_sent` y luego `profiling_form_completed`; 0 si hay profiling_form_sent pero no completed; no se añade si no hay profiling_form_sent. |

#### Segment (`segment`)

Derivadas de **lk_projects** (asset `conv_raw_lk_projects`). Se usa el primer proyecto de la conversación (project_created_at en [conv_start-3h, conv_end]) para calcular seniority por sector, modalidad, área y precios.

| conv_variable_name | Detalle |
|--------------------|---------|
| **seniority_level** | Nivel de seniority (spot_1): SS, Top SS, Jr, Mid, Sr. Lógica por sector: Sale (precio venta), Industrial/Office (m²), Retail/Land (precio renta). Modalidad: rent si hay precios de renta > 0, si no sale. No se añade si no hay proyecto en ventana o no se puede calcular. |

#### AI tags (`ai_tag`)

Evaluados por OpenAI en batch. Por defecto solo se ejecutan estos 4 (config `ALL_AI_TAGS`). Valor 0/1 salvo **missed_messages**, que es entero (conteo).

| conv_variable_name | Detalle breve |
|--------------------|----------------|
| **is_broker** | 1 = el usuario es broker/agente inmobiliario (comisiones, "tengo un cliente", etc.); 0 = cliente final. |
| **info_requested** | 1 = el usuario pide explícitamente más información (propiedad, proceso, requisitos, etc.). |
| **missed_messages** | Entero: veces que el asistente dice explícitamente que no puede responder. |
| **scaling_agent** | 1 = se escaló a agente humano. |

#### Scores DSPy (`score`)

Generados por `process_dspy_evaluator_batch` (evaluador multi-etapa). Las secciones típicas son: **profiling**, **information**, **scheduling**.

| conv_variable_name | Detalle |
|--------------------|---------|
| **dspy_score** | Puntuación global (float) del evaluador. |
| **dspy_evaluation_json** | JSON con el resultado completo por sección. |
| **dspy_section_path** | Ruta de secciones (section_path del evaluador). |
| **dspy_reached_scheduling** | 1 si se llegó a la etapa de scheduling; 0 si no. |
| **dspy_progressed_to_scheduling_after_info** | 1 si hubo avance a scheduling tras información; 0 si no. |
| **dspy_last_successful_stage** | Última etapa marcada como exitosa. |
| **dspy_{section}_success** | 1/0 por sección (profiling, information, scheduling). |
| **dspy_{section}_failure_cause** | Causa de fallo (si success=0). |
| **dspy_{section}_failure_reason** | Razón de fallo (si success=0). |
| **dspy_{section}_failure_event_type** | event_type donde ocurrió el fallo (si success=0). |

#### Variables de funnel (llm_judge, no usadas en el pipeline actual)

Definidas en `utils/variables/llm_judge.py`; no se añaden en conv_with_variables ni conv_with_dspy_scores: funnel_score, last_event_type, reached_goal, drop_off_point, friction_points, funnel_summary.

---

## 6. Base de datos y credenciales (`utils/database.py`)

- **SSM (us-east-1):** credenciales para Chatbot, Prod (MySQL), Geospot, Spot Chatbot, OpenAI, LangSmith.
- **Funciones:** `get_engine("chatbot"|"prod"|"geospot"|"spot_chatbot")`, `query_postgres`, `query_mysql`, `query_geospot`, `query_spot_chatbot` (alias `query_staging`), `get_ssm_parameter`, `get_openai_api_key`, `ensure_langsmith_env_from_ssm()`.
- El `__init__.py` del pipeline llama a `ensure_langsmith_env_from_ssm()` al cargar para que LangSmith esté disponible para DSPy/AI tags.

---

## 7. S3 y Geospot (`utils/s3_upload.py`)

- **Constantes:** `GEOSPOT_API_URL`, `GEOSPOT_API_KEY_PARAM`, `S3_BUCKET_NAME`. Lecturas SQL: `bt_conv_conversations`; carga API (replace): `bt_conv_conversations_bck`.
- **Sanitización:** `sanitize_text`, `sanitize_json_string` (recursivo en strings de listas/dicts); `prepare_dataframe_for_csv` aplica sanitize a columnas JSONB (`conv_messages`, `conv_variables`) y texto (`conv_text`).
- **Flujo de salida:** `upload_dataframe_to_s3(df, test_mode)` → CSV sanitizado a S3; `trigger_table_replacement(s3_key, table_name)` → POST a Geospot para reemplazar tabla.

---

## 8. Jobs y schedules (`jobs.py`)

- **conv_selection:** `AssetSelection.assets("conv_final_output").upstream() | cleanup_selection` (cleanup_storage puede ser otro componente).
- **conv_job:** `define_asset_job(name="conv_job", selection=conv_selection)`.
- **conv_schedule_hourly:** cron `0 9-18 * * *` (cada hora de 9:00 a 18:00), timezone America/Mexico_City, default RUNNING.
- **conv_schedule_4am:** cron `0 4 * * *` (diario a las 4:00), timezone America/Mexico_City, default RUNNING.
- **CONV_JOB_CONFIG_WITH_AI:** ejemplo de config para habilitar OpenAI y DSPy en conv_with_variables (útil para Launch run en UI).

---

## 9. Queries SQL (`queries/`)

- **messages.sql:** mensajes del Chatbot; placeholders `{start_date}`, `{end_date}`, `{previous_day_start}`. Devuelve id (conversation_id), phone_number, message_body, type, created_at_utc, etc.
- **clients.sql:** clientes (lead_id, phone_number, fecha_creacion).
- **project_requirements.sql:** project_requirements + calendar_appointments; columnas visit_created_at, visit_updated_at, project_created_at, lead_id (client_id).
- **chat_events.sql:** eventos de conversación en Staging; placeholders `{previous_day_start}`, `{end_date}`; columnas conversation_id, event_type, event_section, created_at_local, etc.

---

## 10. Cómo extender o modificar

- **Añadir un asset:** definir `@asset` en `assets.py`, declarar dependencias, y si hace falta una nueva config añadirla en `config.py` y pasarla al asset. Si debe estar en el job, la selección `conv_final_output.upstream()` lo incluirá si el nuevo asset es upstream de algún asset existente; si no, ajustar `jobs.py` (p. ej. añadir otro AssetSelection).
- **Añadir una variable a conv_variables:** implementar una función que devuelva un dict con `conv_variable_category`, `conv_variable_name`, `conv_variable_value` (y opcionalmente `conv_variable_explanation`). Llamarla desde `create_conv_variables` en processors (variables base) o desde el asset correspondiente (experimentos, AI, DSPy).
- **Nueva fuente de datos:** añadir credenciales/engine en `database.py` y, si aplica, un nuevo SQL en `queries/` y un asset que lo use.
- **Cambiar comportamiento incremental o de upsert:** ajustar `use_incremental_mask` / `preserve_existing_conv_variables` en config y la lógica en `conv_with_variables` y `conv_upsert` según esta doc.

Con esto un agente puede ubicar cada proceso, config, variable y paso de datos para construir algo nuevo sobre el pipeline o modificar una parte concreta sin perder el contexto global.
