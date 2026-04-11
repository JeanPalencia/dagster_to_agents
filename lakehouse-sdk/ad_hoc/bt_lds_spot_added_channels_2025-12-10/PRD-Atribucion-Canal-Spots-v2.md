# PRD: Atribucion de Canal de Origen para Spots Agregados a Proyectos

## 1. Contexto y Problema

Actualmente, cuando un spot se agrega a un proyecto (tabla `project_requirement_spot` en core-service), **no se registra por cual canal fue agregado**. No existe una columna `origin`, `source` ni `channel` que indique si el spot llego al proyecto porque lo recomendo el algoritmo de GeoSpot, porque el cliente lo confirmo en una conversacion de chatbot, o porque un agente lo agrego manualmente desde el panel.

El objetivo de este documento es definir los campos de atribucion de canal e indicadores de recomendacion que se agregaran a la tabla existente **`bt_lds_lead_spots`** en el data lake, enriqueciendo los registros del evento **"Spot Added" (`lds_event_id = 3`)** con informacion cruzada de multiples bases de datos.

---

## 2. Tabla Destino: `bt_lds_lead_spots`

Los nuevos campos se integraran en la tabla existente **`bt_lds_lead_spots`**, que es una tabla de eventos (event log) del data lake gestionada por Dagster. Esta tabla ya rastrea el ciclo de vida completo de leads, proyectos y spots en 10 etapas:

| `lds_event_id` | Evento | Descripcion |
|---|---|---|
| 1 | Lead Created | Se crea un lead |
| 2 | Project Created | Se crea un proyecto vinculado a un lead |
| **3** | **Spot Added** | **Se agrega un spot a un proyecto** |
| 4 | Visit Created | Se crea una visita/cita para un spot |
| 5 | Visit Confirmed | La visita se confirma |
| 6 | Visit Realized | La visita se realiza |
| 7 | LOI | Carta de intencion |
| 8 | Contract | Contrato |
| 9 | Spot Won | Spot ganado |
| 10 | Transaction | Transaccion cerrada (nivel proyecto) |

**Los campos de atribucion de canal e indicadores de recomendacion aplican exclusivamente al evento 3 ("Spot Added").** Para los demas eventos, estos campos seran `null`.

### Pipeline actual (Bronze -> Silver -> Gold)

El ETL de `bt_lds_lead_spots` ya existe en el repositorio de Dagster (`Spot2HQ/dagster`) con la siguiente estructura:

**Bronze:** Extraccion cruda de tablas fuente
- `raw_s2p_project_requirement_spots_new` <- MySQL `project_requirement_spot`
- `raw_s2p_clients_new` <- MySQL `clients` (renombrada como leads en silver; `client_id` pasa a llamarse `lead_id`)
- `raw_s2p_project_requirements_new` <- MySQL `project_requirements` (renombrada como projects)
- `raw_s2p_calendar_appointments_new`, `raw_s2p_calendar_appointment_dates_new`, `raw_s2p_activity_log_projects_new`, `raw_s2p_profiles_new`

**Silver:** Transformacion y limpieza
- `stg_s2p_prs_project_spots_new` -- renombra `project_requirement_id` -> `project_id`, filtra nulls, deduplica por `(project_id, spot_id)`
- Otras silver tables de leads, projects, appointments, etc.

**Gold:** Construccion del event log
- `gold_bt_lds_lead_spots_new` -- concatena los 10 bloques de eventos, agrega segmentacion (sector, lead_type, industry_role) y cohort fields
- **Block 3 (Spot Added)** construye una fila por cada `(lead_id, project_id, spot_id)` con `lds_event_at = project_spot_created_at`

**Publish (embebido en Gold):** `gold_bt_lds_lead_spots_new` escribe directamente a S3 (parquet) y carga a GeoSpot PostgreSQL (tabla `bt_lds_lead_spots`, modo replace) dentro del propio asset. No existe un asset de publish separado para el flujo _new. **Nota:** este patron no sigue la separacion de capas del medallion pero se mantiene asi por ahora.

### Archivos clave del pipeline

| Capa | Archivo | Descripcion |
|------|---------|-------------|
| Bronze | `defs/data_lakehouse/bronze/raw_s2p_project_requirement_spots_new.py` | Extrae `project_requirement_spot` de MySQL |
| Silver | `defs/data_lakehouse/silver/stg/stg_s2p_prs_project_spots_new.py` | Transforma y deduplica project-spots |
| Gold + Publish | `defs/data_lakehouse/gold/gold_bt_lds_lead_spots_new.py` | Pipeline activo: construye el event log (10 bloques), escribe a S3 parquet y carga a GeoSpot (publish embebido) |
| DDL | `lakehouse-sdk/sql/bt_lds_lead_spots.sql` | Definicion SQL de la tabla (27 columnas actuales) |

> **Nota sobre el pipeline legacy:** Existe un pipeline legacy compuesto por `gold_bt_lds_lead_spots.py` (funcion `_transform_bt_lds_lead_spots`) y `publish/bt_lds_lead_spots.py` (publica a tabla `bt_lds_lead_spots_bck`). Este pipeline sera desactivado en un futuro cercano. **El pipeline _new no debe tener dependencias hacia el.**

---

## 3. Canales de Atribucion

Se definen **tres canales mutuamente excluyentes**. Cada spot agregado a un proyecto se atribuye a exactamente un canal:

| Canal | Nombre Propuesto | Descripcion |
|-------|-----------------|-------------|
| 1 | **Recomendacion Algoritmo** | El spot fue sugerido por el motor de recomendaciones de GeoSpot (basado en los requirements del proyecto) y un agente lo aprobo desde el panel de administracion. |
| 2 | **Recomendacion Chatbot** | El spot fue sugerido durante una conversacion de chatbot y el cliente confirmo su interes, lo que disparo la adicion automatica al proyecto. |
| 3 | **Adicion Manual** | No hay evidencia de que el spot haya sido agregado como resultado de una recomendacion algoritmica ni del chatbot. Incluye spots agregados manualmente por agentes, por el propio cliente desde el marketplace, por interacciones de WhatsApp, formularios de contacto, o cualquier otro camino sin evidencia de recomendacion previa. Es el canal por defecto (fallback). |

---

## 4. Fuentes de Datos para Atribucion de Canal

Cada canal se determina consultando una fuente de datos distinta. Las fuentes provienen de bases de datos diferentes.

### 4.1 Fuente para "Recomendacion Algoritmo"

**Base de datos:** GeoSpot (PostgreSQL)
**Tabla:** `recommendation_projects`
**Tipo de fuente en Dagster:** `geospot_postgres`
**Raw asset necesario (nuevo):** `raw_gs_recommendation_projects_new`

Esta tabla almacena las recomendaciones generadas por el motor de GeoSpot para cada proyecto. Cuando un agente aprueba un spot sugerido desde el panel de core-service, el sistema llama a GeoSpot para mover el `spot_id` del campo `spots_suggested` al campo `white_list`. Por lo tanto, **un spot_id presente en `white_list` para un `project_id` dado es evidencia directa de que ese spot fue aprobado como resultado de una recomendacion algoritmica**.

**Campos relevantes para atribucion:**

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `project_id` | Integer | ID del proyecto en core-service (clave de cruce) |
| `white_list` | JSON array | spot_ids aprobados desde las sugerencias del algoritmo |
| `updated_at` | DateTime | Ultima modificacion del registro |

**Timestamp disponible para desempate:** `updated_at` a nivel de registro. **Limitacion:** este campo refleja la ultima modificacion del registro completo, no el momento exacto en que un spot individual fue aprobado. No hay timestamp por spot dentro del JSON de `white_list`.

**Confiabilidad:** Alta. El flujo de aprobacion (`ApproveSuggestedRequirementSpot` en core-service -> evento `SpotSuggestedApproved` -> listener `ApproveSpotSuggested` -> POST a GeoSpot) esta verificado en el codigo y actualiza la `white_list` de forma consistente.

### 4.2 Fuente para "Recomendacion Chatbot"

**Base de datos:** Chat2 Staging (PostgreSQL — replica de lectura)
**Tabla:** `conversation_events`
**Tipo de fuente en Dagster:** `staging_postgres`
**Raw asset necesario (nuevo):** `raw_cb_conversation_events_new` (query custom con filtro `WHERE event_type = 'spot_confirmation'`, historico completo sin ventana temporal)

> **Nota:** Se usa `staging_postgres` (credenciales SSM `spot_chatbot_*`) en lugar de `chatbot_postgres` (credenciales `db_chatbot_*`). Ambos apuntan al mismo servidor de Chat2, pero con usuarios distintos. El usuario de `chatbot_postgres` no tiene permisos de lectura sobre `conversation_events`, mientras que `staging_postgres` si los tiene y es el que ya se usa en el bronze existente `raw_staging_conversation_events_new`. Ademas, usar la replica staging evita impactar el rendimiento del chat productivo.

Cuando un cliente interactua con el chatbot y confirma interes en un spot, el sistema registra un evento de tipo `spot_confirmation` en esta tabla **antes** de llamar a core-service para agregar el spot al proyecto. Este evento contiene el `spot_id`, el `client_id` y un timestamp exacto.

**Nota sobre `client_id` vs `lead_id`:** En la capa Bronze, el campo se extrae como `client_id` (nombre original en chat2). En la capa Silver del pipeline de `bt_lds_lead_spots`, la tabla `clients` se renombra como `leads` y `client_id` se transforma a `lead_id`. Por lo tanto, **todos los JOINs en la capa Gold se realizan usando `lead_id`**, no `client_id`.

**Nota importante:** La tabla `recommendation_chatbot` de GeoSpot **no sirve** como fuente para la atribucion de este canal. Aunque tiene un campo `white_list`, este nunca se actualiza en el flujo actual -- el chatbot no llama al endpoint de aprobacion de GeoSpot. Por eso se utiliza `conversation_events` de chat2 como fuente alternativa y confiable.

**Campos relevantes para atribucion:**

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `client_id` | Integer | ID del cliente en core-service (se transforma a `lead_id` en silver) |
| `spot_id` | Integer | ID del spot confirmado |
| `event_type` | String | Debe ser `'spot_confirmation'` |
| `created_at` | DateTime | Timestamp exacto del momento de confirmacion |

**Timestamp disponible para desempate:** `created_at` por cada evento individual. **Ventaja:** es un timestamp exacto por spot, no a nivel de registro agrupado.

**Confiabilidad:** Alta, con una salvedad menor. El evento se registra en el mismo momento que se llama a core-service para agregar el spot. Si la llamada a core-service fallara permanentemente (despues de 3 reintentos de Celery), el evento existiria en chat2 pero el spot no estaria en el proyecto. La probabilidad de este escenario es baja, y al cruzar con los spots efectivamente agregados, estos casos se filtran naturalmente.

**Acceso:** Dagster ya tiene credenciales configuradas en AWS SSM (`/dagster/lk_conversations_metrics/spot_chatbot_*`). Ya existe un bronze `raw_staging_conversation_events_new` que usa `staging_postgres`, pero aplica un filtro `WHERE` con ventana de 3 dias. Para la atribucion de canal se necesita un nuevo raw asset dedicado sin restriccion temporal.

**Calidad de datos (validada):** De 5,249 registros `spot_confirmation`, solo 2,842 (54%) son usables (con `spot_id` y `client_id` no nulos). Los 2,407 restantes son registros de backfill/migracion (contienen `{"backfill": true, "migration": ...}` en `event_data`) sin `spot_id`. El STG debe filtrar estos registros (`WHERE spot_id IS NOT NULL AND client_id IS NOT NULL`).

### 4.3 Fuente para "Adicion Manual" (Fallback)

No se necesita una fuente externa adicional. La tabla `project_requirement_spot` ya se extrae en el bronze existente (`raw_s2p_project_requirement_spots_new`). Todo spot que aparezca en `bt_lds_lead_spots` con `lds_event_id = 3` y que **no tenga evidencia** en ninguna de las dos fuentes anteriores, se clasifica automaticamente como "Adicion Manual".

---

## 5. Indicadores de Recomendacion (`is_recommended_*`)

Ademas del canal ganador (mutuamente excluyente), cada spot debe llevar **indicadores independientes** que senalen si el spot aparece como recomendado en las tablas de recomendacion. Esto permite saber que un spot, aunque fue atribuido a un canal especifico, tambien fue recomendado por otros canales.

> **Diferencia fundamental entre la metodologia de canal y la metodologia de indicadores:**
>
> - La **atribucion de canal** (Seccion 6) usa criterios estrictos para determinar *por cual canal se agrego* el spot al proyecto. Para el canal "Algorithm", solo cuenta la `white_list` (evidencia de aprobacion efectiva). Para el canal "Chatbot", solo cuenta el evento `spot_confirmation` en chat2 (evidencia de confirmacion por el usuario).
>
> - Los **indicadores de recomendacion** (esta seccion) son senales mas amplias: solo buscan responder *"fue este spot recomendado en alguna ocasion por este canal?"*, sin importar si fue aprobado, rechazado, o ignorado. Por eso **no se usan las `white_list`**, sino los campos generales de recomendacion que incluyen todos los spots que el motor sugirio, independientemente de su destino posterior.

| Indicador (numerico) | Indicador (texto) | Fuente | Condicion para activarse |
|------|-----------------|--------|--------------------------|
| `is_recommended_algorithm_id` | `is_recommended_algorithm` | `recommendation_projects` (GeoSpot) | El `spot_id` aparece en **cualquiera** de los campos `spots_suggested`, `white_list` o `black_list` para el `project_id` correspondiente. Es decir, basta con que el motor de recomendaciones lo haya sugerido en algun momento, sin importar si luego fue aprobado, rechazado o sigue pendiente. |
| `is_recommended_chatbot_id` | `is_recommended_chatbot` | `recommendation_chatbot` (GeoSpot) | El `spot_id` aparece en **cualquiera** de los campos `spots_suggested`, `white_list` o `black_list` del registro de chatbot vinculado al `lead_id` (via `client_id`). No se usa `project_id`. Indica que el motor de recomendaciones genero ese spot como sugerencia dentro de una conversacion de chatbot. |

> **Indicador descartado: `is_recommended_visits`** — La tabla `recommendation_visitspotsrecommendations` fue evaluada como fuente para un tercer indicador. Sin embargo, tras la validacion de datos se determino que **no es viable actualmente**:
> - El campo `spots` (unico con datos, 100% poblado) contiene los spots **visitados** por el cliente (INPUT para el motor de recomendaciones), NO los spots recomendados (OUTPUT).
> - Los campos `suggested_spots`, `white_list` y `black_list` (que deberian contener el OUTPUT) estan vacios al 100%.
> - Esto indica un bug o feature incompleta en el flujo de GeoSpot: el motor genera las recomendaciones pero no las persiste en esta tabla.
>
> **Implementacion futura:** Si el equipo de GeoSpot corrige este flujo y comienza a poblar `suggested_spots` con los spots recomendados, se podra agregar `is_recommended_visits_id` / `is_recommended_visits` como tercer indicador siguiendo el mismo patron de los dos existentes.

Cada indicador tiene dos campos:
- **`*_id`** (SMALLINT): valor numerico `1` = recomendado, `0` = no recomendado.
- **`*`** (VARCHAR): valor texto `'Yes'` o `'No'`.

---

## 6. Algoritmo de Atribucion

El algoritmo se ejecuta dentro de la funcion de transformacion de `gold_bt_lds_lead_spots_new`, especificamente al construir el **Block 3 (Spot Added)**. Para cada fila de evento "Spot Added" (cada combinacion `lead_id, project_id, spot_id`), el proceso es:

> **Nota arquitectonica:** Segun `ARCHITECTURE.md`, la logica de negocio (joins, reglas de atribucion) pertenece a la capa Core, y Gold solo agrega campos de auditoria. Sin embargo, `gold_bt_lds_lead_spots_new` es un asset pre-existente cuyo ownership no es nuestro, y ya contiene toda la logica de transformacion (10 bloques de eventos) dentro del archivo gold. Mantenemos este patron tal cual para no interferir con el ownership del equipo responsable. En los flujos bajo nuestro control seguimos la separacion estricta de capas.

### Paso 1 -- Recopilar evidencia de canal

Para cada fila de evento "Spot Added" `(lead_id, project_id, spot_id)`, buscar evidencia en las dos fuentes de atribucion. **Cada fuente usa claves de cruce distintas:**

- **Evidencia de Algoritmo — clave: `(project_id, spot_id)`:** Verificar si el `spot_id` aparece en la `white_list` de `recommendation_projects` para el `project_id` dado. **Solo la `white_list` cuenta como evidencia de atribucion** (no `spots_suggested` ni `black_list`), porque la presencia en `white_list` confirma que el spot fue efectivamente aprobado desde la sugerencia del algoritmo.

- **Evidencia de Chatbot — clave: `(lead_id, spot_id)`:** Verificar si existe un registro en `conversation_events` de chat2 con `event_type = 'spot_confirmation'` donde el `spot_id` y el `lead_id` coincidan (nota: `client_id` de chat2 se transforma a `lead_id` en silver). **No se usa `project_id`** porque `conversation_events` no lo tiene — el chatbot opera a nivel de cliente, no de proyecto. La existencia de este evento confirma que el cliente confirmo interes en ese spot a traves del chatbot, lo que disparo la adicion al proyecto.

### Paso 2 -- Resolver atribucion

Con la evidencia recopilada, aplicar las siguientes reglas en orden:

1. **Si solo hay evidencia de un canal** (Algorithm o Chatbot, pero no ambos): atribuir al canal con evidencia.

2. **Si hay evidencia de ambos canales** (el mismo `spot_id` aparece en la `white_list` de `recommendation_projects` para el `project_id` Y tiene un evento `spot_confirmation` en chat2 para el `lead_id`): **desempatar por fecha**. Comparar el `updated_at` de `recommendation_projects` con el `created_at` del evento `spot_confirmation` en chat2. El canal con la fecha **mas antigua** gana la atribucion, bajo la logica de que la primera recomendacion que llego fue la que motivo la adicion.

3. **Si no hay evidencia de ningun canal**: atribuir como "Manual" (fallback).

### Paso 3 -- Calcular indicadores de recomendacion

**Importante:** Esta etapa usa una metodologia completamente distinta a la atribucion de canal. Mientras que la atribucion (Pasos 1 y 2) busca evidencia de que un spot fue *agregado al proyecto por* un canal especifico (usando `white_list` y `spot_confirmation`), los indicadores simplemente buscan si el spot *aparecio alguna vez como recomendacion* en cada tabla, sin importar su estado.

Para cada par `(project_id, spot_id)`, consultar las dos tablas de recomendacion disponibles:

- **`is_recommended_algorithm`**: Buscar si el `spot_id` aparece en **cualquiera** de `spots_suggested`, `white_list` o `black_list` de `recommendation_projects` para el `project_id`. No importa si esta en la white_list (aprobado), black_list (rechazado) o spots_suggested (pendiente) -- lo que importa es que el motor de recomendaciones lo genero como sugerencia.

- **`is_recommended_chatbot`**: Buscar si el `spot_id` aparece en **cualquiera** de `spots_suggested`, `white_list` o `black_list` de `recommendation_chatbot` para el registro vinculado al `lead_id` (via `client_id`). Misma logica: basta con que haya sido sugerido. **No se usa `project_id`** (la tabla `recommendation_chatbot` no lo tiene).

> **Descartado:** `is_recommended_visits` (ver Seccion 5 para justificacion). Sera implementado cuando `recommendation_visitspotsrecommendations` comience a poblar el campo `suggested_spots`.

Un spot puede tener su canal atribuido como "Manual" y aun asi tener uno o mas indicadores activos (`'Yes'`) -- esto significa que el spot fue recomendado en algun momento por uno o mas motores, pero no hay evidencia suficiente de que la adicion al proyecto fue consecuencia directa de esa recomendacion.

---

## 7. Nuevos Campos en `bt_lds_lead_spots`

Los siguientes campos se agregan a la tabla. Solo se llenan para filas con `lds_event_id = 3` (Spot Added); para los demas eventos son `null`.

| Campo | Tipo SQL | Nullable | Descripcion |
|-------|----------|----------|-------------|
| `lds_channel_attribution_id` | SMALLINT | Si | ID del canal: 1 = Algorithm, 2 = Chatbot, 3 = Manual. Null para eventos != 3. |
| `lds_channel_attribution` | VARCHAR(30) | Si | Canal ganador: `'Algorithm'`, `'Chatbot'` o `'Manual'`. Null para eventos != 3. |
| `lds_algorithm_evidence_at` | TIMESTAMP | Si | `updated_at` de `recommendation_projects` si hay evidencia de Algorithm. |
| `lds_chatbot_evidence_at` | TIMESTAMP | Si | `created_at` del evento `spot_confirmation` si hay evidencia de Chatbot. |
| `is_recommended_algorithm_id` | SMALLINT | Si | `1` si el spot aparece en cualquier campo de recomendacion de `recommendation_projects`, `0` si no. Null para eventos != 3. |
| `is_recommended_algorithm` | VARCHAR(3) | Si | `'Yes'` si el spot aparece en cualquier campo de recomendacion de `recommendation_projects`, `'No'` si no. Null para eventos != 3. |
| `is_recommended_chatbot_id` | SMALLINT | Si | `1` si el spot aparece en cualquier campo de recomendacion de `recommendation_chatbot`, `0` si no. Null para eventos != 3. |
| `is_recommended_chatbot` | VARCHAR(3) | Si | `'Yes'` si el spot aparece en cualquier campo de recomendacion de `recommendation_chatbot`, `'No'` si no. Null para eventos != 3. |

Esto lleva la tabla de 27 a 35 columnas. Los 8 nuevos campos se ubican **despues de los campos de cohort** (`lds_cohort_at`) y **antes de los campos de auditoria** (`aud_inserted_at`). El orden en `BT_LDS_LEAD_SPOTS_COLUMN_ORDER` queda:

```
... lds_cohort_type_id, lds_cohort_type, lds_cohort_at,
lds_channel_attribution_id, lds_channel_attribution,
lds_algorithm_evidence_at, lds_chatbot_evidence_at,
is_recommended_algorithm_id, is_recommended_algorithm,
is_recommended_chatbot_id, is_recommended_chatbot,
aud_inserted_at, aud_inserted_date, aud_updated_at, aud_updated_date, aud_job
```

El `BT_LDS_LEAD_SPOTS_COLUMN_ORDER` en `gold_bt_lds_lead_spots_new.py` y el DDL en `lakehouse-sdk/sql/bt_lds_lead_spots.sql` deben actualizarse con este orden.

---

## 8. Nuevos Assets de Dagster Necesarios

Para alimentar el algoritmo de atribucion y los flags, se necesitan los siguientes assets nuevos:

### 8.1 Bronze: Desde GeoSpot (`geospot_postgres`)

| Asset | Tabla fuente | Campos a extraer | Descripcion |
|-------|-------------|-------------------|-------------|
| `raw_gs_recommendation_projects_new` | `recommendation_projects` | `project_id`, `spots_suggested`, `white_list`, `black_list`, `created_at`, `updated_at` | Recomendaciones del algoritmo por proyecto |
| `raw_gs_recommendation_chatbot_new` | `recommendation_chatbot` | `id`, `spots_suggested`, `white_list`, `black_list`, `created_at`, `updated_at` | Recomendaciones del chatbot por conversacion |

> **Descartado:** `raw_gs_recommendation_visits_new` (`recommendation_visitspotsrecommendations`). El campo `spots` contiene spots visitados (INPUT), no recomendados (OUTPUT). Los campos de OUTPUT (`suggested_spots`) estan vacios al 100%. Se podra agregar cuando GeoSpot corrija este flujo (ver Seccion 5).

Estos assets usan `make_bronze_asset("geospot_postgres", table_name=...)` siguiendo la convencion existente (`raw_gs_*_new`). Referencia de implementacion: archivos existentes en `defs/data_lakehouse/bronze/`. El generador `_build_asset` en `bronze/base.py` ya incorpora el wrapper `iter_job_wrapped_compute` (ver Seccion 9.8), por lo que los assets creados con `make_bronze_asset` lo heredan automaticamente.

### 8.2 Bronze: Desde Chat2 (`chatbot_postgres`)

| Asset | Tabla fuente | Descripcion |
|-------|-------------|-------------|
| `raw_cb_conversation_events_new` | `conversation_events` (filtrado) | Query custom: extrae solo eventos con `event_type = 'spot_confirmation'`. Campos: `conversation_id`, `client_id`, `spot_id`, `event_data`, `created_at`. Todo el historico (sin ventana temporal). |

Este asset define `@dg.asset` directamente y llama a `_run_bronze_extract` con SQL custom y `source_type="staging_postgres"`, siguiendo el patron establecido por `raw_staging_conversation_events_new.py`. Convencion de nombres: `raw_cb_*_new`. **Debe usar el wrapper `iter_job_wrapped_compute`** (ver Seccion 9.8).

**Nota sobre trazabilidad:** El nombre sigue la regla `raw_{source}_{table}` de `ARCHITECTURE.md` — `cb` es el prefijo de Chatbot y `conversation_events` es la tabla origen. Ya existe `raw_staging_conversation_events_new` que lee la misma tabla pero con `staging_` como parte del nombre descriptivo, una ventana de 3 dias y sin filtro de `event_type`. Nuestro asset se diferencia en: (1) usa el prefijo `cb_` correcto, (2) filtra solo `event_type = 'spot_confirmation'`, y (3) extrae el historico completo sin ventana temporal.

### 8.3 Silver: Transformacion intermedia

Se recomienda crear silver assets intermedios que:
- **Exploten los JSON arrays** de las tablas de recomendacion (convertir `white_list: [1,2,3]` en filas individuales `(project_id, spot_id)`) para facilitar los JOINs en Gold.
- **Limpien y normalicen** los datos de `conversation_events` (extraer `spot_id` de `event_data` si no es columna directa, normalizar tipos).

Assets silver sugeridos:

| Asset | Descripcion |
|-------|-------------|
| `stg_gs_recommendation_projects_exploded_new` | Explota `white_list`, `spots_suggested` y `black_list` en filas `(project_id, spot_id, list_type)` + `updated_at` |
| `stg_gs_recommendation_chatbot_exploded_new` | Explota los JSON arrays de `recommendation_chatbot` en filas individuales |
| `stg_cb_conversation_events_new` | Normaliza `raw_cb_conversation_events_new`: castea tipos, deduplica si es necesario. **Filtro critico:** descartar registros de backfill/migracion (`WHERE spot_id IS NOT NULL AND client_id IS NOT NULL`); ~46% de los registros carecen de `spot_id` por ser backfills. |

> **Descartado:** `stg_gs_recommendation_visits_exploded_new`. Depende del bronze de visits que fue descartado (ver Seccion 5).

### 8.4 Gold: Nueva funcion de transformacion independiente

El asset `gold_bt_lds_lead_spots_new` actualmente importa `_transform_bt_lds_lead_spots` del archivo legacy `gold_bt_lds_lead_spots.py`. **Esta dependencia debe eliminarse.** Se creara una nueva funcion de transformacion independiente directamente dentro de `gold_bt_lds_lead_spots_new.py` (fork), que incluya la logica de atribucion de canal integrada en el Block 3.

> **Razon del fork:** El pipeline legacy (`gold_bt_lds_lead_spots.py` + `publish/bt_lds_lead_spots.py`) sera desactivado en un futuro cercano. El pipeline _new no debe depender de ninguna definicion legacy.

La nueva funcion de transformacion debe:

1. **Leer los nuevos silver assets desde S3** usando `read_silver_from_s3()`, siguiendo el mismo patron que los silvers existentes (`stg_s2p_clients_new`, `stg_s2p_projects_new`, etc.). No se pasan como parametros del asset Dagster.
2. **En el Block 3 (Spot Added)**, despues de construir `df_spot_events`, enriquecerlo con:
   - LEFT JOIN con silver de `recommendation_projects` exploded (filtrado a `list_type = 'white_list'`) para obtener evidencia de Algoritmo y su timestamp.
   - LEFT JOIN con silver de `conversation_events` (via `lead_id` + `spot_id`, sin `project_id`) para obtener evidencia de Chatbot y su timestamp.
   - LEFT JOIN con los dos silver exploded (algorithm y chatbot, sin filtro de list_type) para calcular los flags `is_recommended_*`.
   - Calculo de `lds_channel_attribution_id`, `lds_channel_attribution`, y los pares `is_recommended_*_id` / `is_recommended_*` segun el algoritmo de la Seccion 6.
3. **Para los demas bloques** (eventos 1, 2, 4-10), agregar las 8 columnas como `null`.
4. **Actualizar `BT_LDS_LEAD_SPOTS_COLUMN_ORDER`** para incluir los 8 nuevos campos.
5. **Actualizar el DDL** en `lakehouse-sdk/sql/bt_lds_lead_spots.sql` con las nuevas columnas.

**Patron de asset actualizado (ver Seccion 9.8):** La logica de la nueva funcion debe vivir en `_gold_bt_lds_lead_spots_new_impl()` (no en el asset directamente). El asset `gold_bt_lds_lead_spots_new` ya delega a esta funcion via `iter_job_wrapped_compute`.

---

## 9. Consideraciones y Limitaciones

### 9.1 Precision del timestamp de "Recomendacion Algoritmo"

El campo `updated_at` de `recommendation_projects` se actualiza a nivel del registro completo, no por spot individual. Si multiples spots se aprueban en momentos distintos, el `updated_at` solo refleja la ultima aprobacion. Esto significa que para desempates, la fecha del canal Algoritmo es **aproximada** mientras que la del canal Chatbot es **exacta**. En caso de empate muy cerrado (diferencia de minutos), la precision favorece al canal Chatbot.

### 9.2 `recommendation_visitspotsrecommendations` — Descartada (implementacion futura)

La tabla `recommendation_visitspotsrecommendations` fue evaluada como fuente para un indicador `is_recommended_visits` y descartada por las siguientes razones:

1. **Semantica incorrecta del campo `spots`:** El unico campo JSONB poblado (`spots`, 100% de registros) contiene los spots **visitados** por el cliente, que son el INPUT del motor de recomendaciones. No contiene spots recomendados.
2. **Campos de OUTPUT vacios:** Los campos que deberian contener los spots recomendados (`suggested_spots`, `white_list`, `black_list`) estan vacios al 100% en todos los registros.
3. **No hay canal de atribucion viable:** No existe un mecanismo para registrar si el cliente abrio el email con las recomendaciones y agrego un spot desde ahi. No hay endpoint dedicado en GeoSpot para aprobar spots de este canal.

**Accion futura:** Si el equipo de GeoSpot corrige el flujo y comienza a persistir los spots recomendados en `suggested_spots`, se podra:
- Agregar `raw_gs_recommendation_visits_new` (bronze) y `stg_gs_recommendation_visits_exploded_new` (silver).
- Agregar los campos `is_recommended_visits_id` / `is_recommended_visits` al DDL y al column order.
- Evaluar si "Visits" podria funcionar como tercer canal de atribucion (requiere ademas un mecanismo de tracking de clics/acciones desde el email).

### 9.3 Cobertura del canal Chatbot

La fuente `conversation_events` de chat2 registra confirmaciones de interes, no la adicion exitosa al proyecto. En un escenario improbable donde la llamada de chat2 a core-service falle permanentemente (despues de 3 reintentos de Celery), el evento existiria en chat2 pero el spot no estaria en el proyecto. Al cruzar con `bt_lds_lead_spots` (que solo incluye spots efectivamente agregados), estos casos se filtran naturalmente.

### 9.4 Historico de datos

- **GeoSpot (`recommendation_projects`):** Los datos existen desde que se implemento el motor de recomendaciones V3. Registros anteriores no tendran datos.
- **Chat2 (`conversation_events`):** Los datos existen desde que se implemento el logging de eventos en el chatbot.
- **Core-service (`project_requirement_spot`):** Tabla del sistema actual con registros desde la migracion al sistema Kanban/Requirements.
- Para spots agregados antes de que existieran las fuentes de recomendacion, el canal sera `'Manual'` y todos los indicadores seran `0` / `'No'`. Esto es correcto y esperado.

### 9.5 Cruce entre bases de datos

El enriquecimiento requiere cruzar datos de tres bases de datos distintas, todas ya accesibles desde Dagster:

| Base de datos | Tablas | Tipo de fuente Dagster | Credenciales SSM |
|---|---|---|---|
| Core-service (MySQL) | `project_requirement_spot`, `project_requirements`, `clients` | `mysql_prod` | Ya configurado |
| GeoSpot (PostgreSQL) | `recommendation_projects`, `recommendation_chatbot` | `geospot_postgres` | Ya configurado |
| Chat2 Staging (PostgreSQL) | `conversation_events` | `staging_postgres` | `/dagster/lk_conversations_metrics/spot_chatbot_*` |

### 9.6 Cruce de `recommendation_chatbot` con proyectos

La tabla `recommendation_chatbot` usa `conversation_id` como clave primaria (no `project_id`). Para vincular una recomendacion de chatbot con un proyecto especifico, se necesita cruzar `conversation_events.client_id` -> `clients.id` (= `lead_id` en silver) -> `project_requirements.client_id` (= `lead_id` en silver) -> `project_id`. Este cruce ya esta disponible en los silver assets existentes del pipeline `bt_lds_lead_spots`, donde `client_id` se renombra como `lead_id`.

### 9.7 Impacto en consumidores downstream

La tabla `bt_lds_lead_spots` es consumida por:
- **Metabase** (dashboards de funnel y lead-spot interactions)
- **Pipeline `effective_supply`**: lee `bt_lds_lead_spots WHERE lds_event_id = 3` como fuente bronze (`raw_gs_bt_lds_spot_added`)

Los nuevos campos son aditivos (nullable), por lo que no rompen consumidores existentes. Sin embargo, se recomienda:
- Actualizar la documentacion en `docs/golden_tables/bt_lds_lead_spots.md`
- Notificar a los usuarios de Metabase que hay nuevos campos disponibles para analisis

### 9.8 Patron de error handling (`iter_job_wrapped_compute`)

Desde los commits recientes en `main` (PRs #143-#146), todos los assets del lakehouse usan un wrapper estandarizado para manejo de errores definido en `defs/pipeline_asset_error_handling.py`:

```python
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute

@dg.asset(name="mi_asset", ...)
def mi_asset(context):
    def body():
        # logica real aqui
        return resultado

    yield from iter_job_wrapped_compute(context, body)
```

**Que hace:** Si el asset pertenece a un job monitoreado (listado en `LAKEHOUSE_NEW_PIPELINE_JOB_NAMES`), en caso de error emite metadata de error (`error_type`, `error_message`, `asset_key`, `partition_key`) como `MaterializeResult` y luego re-lanza la excepcion. Esto alimenta al sensor `lakehouse_new_pipeline_failure_chat` que notifica fallos al canal Google Chat `data-lake-errors`.

**Impacto en la implementacion:**

1. **Gold:** `gold_bt_lds_lead_spots_new` ya fue refactorizado: la logica vive en `_gold_bt_lds_lead_spots_new_impl(context)` y el asset llama `yield from iter_job_wrapped_compute(context, body)`. Nuestro fork de la funcion de transformacion debe modificar `_gold_bt_lds_lead_spots_new_impl()`, no el asset.
2. **Bronze con `make_bronze_asset`:** `bronze/base.py._build_asset` ya incorpora el wrapper automaticamente. Los assets creados con `make_bronze_asset` (como `raw_gs_recommendation_projects_new`) lo heredan sin cambios.
3. **Bronze custom (`@dg.asset` + `_run_bronze_extract`):** Assets como `raw_cb_conversation_events_new` deben implementar el wrapper manualmente, siguiendo el patron de `raw_staging_conversation_events_new.py`.
4. **Registro en jobs (Regla 12 de `ARCHITECTURE.md`):** Todo job que publique a GeoSpot **debe** registrarse en `LAKEHOUSE_NEW_PIPELINE_JOBS` (en `data_lakehouse/jobs.py`) para que el sensor `lakehouse_new_pipeline_failure_chat` envie alertas al canal `data-lake-errors` en caso de fallo. Si se crean jobs nuevos para los bronze/silver de atribucion, agregarlos a esta tupla.
