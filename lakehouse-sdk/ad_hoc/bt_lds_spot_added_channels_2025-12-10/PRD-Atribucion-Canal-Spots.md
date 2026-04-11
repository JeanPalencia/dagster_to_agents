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

**Publish:** Carga a destinos
- S3 (Parquet) + GeoSpot PostgreSQL (tabla `bt_lds_lead_spots`, modo replace)

### Archivos clave del pipeline

| Capa | Archivo | Descripcion |
|------|---------|-------------|
| Bronze | `defs/data_lakehouse/bronze/raw_s2p_project_requirement_spots_new.py` | Extrae `project_requirement_spot` de MySQL |
| Silver | `defs/data_lakehouse/silver/stg/stg_s2p_prs_project_spots_new.py` | Transforma y deduplica project-spots |
| Gold | `defs/data_lakehouse/gold/gold_bt_lds_lead_spots.py` | Funcion `_transform_bt_lds_lead_spots()` con los 10 bloques |
| Gold (new) | `defs/data_lakehouse/gold/gold_bt_lds_lead_spots_new.py` | Pipeline activo que normaliza silvers _new y llama a `_transform_bt_lds_lead_spots` |
| Publish | `defs/data_lakehouse/publish/bt_lds_lead_spots.py` | Carga legacy a S3 CSV + GeoSpot PostgreSQL |
| DDL | `lakehouse-sdk/sql/bt_lds_lead_spots.sql` | Definicion SQL de la tabla (27 columnas actuales) |

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

**Base de datos:** Chat2 (PostgreSQL)
**Tabla:** `conversation_events`
**Tipo de fuente en Dagster:** `chatbot_postgres`
**Raw asset necesario (nuevo):** `raw_cb_spot_confirmations_new` (query custom con filtro `WHERE event_type = 'spot_confirmation'`)

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

**Acceso:** Dagster ya tiene credenciales para conectarse a esta base de datos (almacenadas en AWS SSM bajo `/dagster/lk_conversations_metrics/spot_chatbot_*`). Ya existe un bronze `raw_staging_conversation_events_new` pero con ventana temporal de 3 dias y usando `staging_postgres`. Para el historico completo se necesita un nuevo raw asset dedicado usando `chatbot_postgres`.

### 4.3 Fuente para "Adicion Manual" (Fallback)

No se necesita una fuente externa adicional. La tabla `project_requirement_spot` ya se extrae en el bronze existente (`raw_s2p_project_requirement_spots_new`). Todo spot que aparezca en `bt_lds_lead_spots` con `lds_event_id = 3` y que **no tenga evidencia** en ninguna de las dos fuentes anteriores, se clasifica automaticamente como "Adicion Manual".

---

## 5. Indicadores de Recomendacion (`is_recommended_*`)

Ademas del canal ganador (mutuamente excluyente), cada spot debe llevar **indicadores independientes** que senalen si el spot aparece como recomendado en cada una de las tres tablas de recomendacion. Esto permite saber que un spot, aunque fue atribuido a un canal especifico, tambien fue recomendado por otros canales.

> **Diferencia fundamental entre la metodologia de canal y la metodologia de indicadores:**
>
> - La **atribucion de canal** (Seccion 6) usa criterios estrictos para determinar *por cual canal se agrego* el spot al proyecto. Para el canal "Algorithm", solo cuenta la `white_list` (evidencia de aprobacion efectiva). Para el canal "Chatbot", solo cuenta el evento `spot_confirmation` en chat2 (evidencia de confirmacion por el usuario).
>
> - Los **indicadores de recomendacion** (esta seccion) son senales mas amplias: solo buscan responder *"fue este spot recomendado en alguna ocasion por este canal?"*, sin importar si fue aprobado, rechazado, o ignorado. Por eso **no se usan las `white_list`**, sino los campos generales de recomendacion que incluyen todos los spots que el motor sugirio, independientemente de su destino posterior.

| Indicador (numerico) | Indicador (texto) | Fuente | Condicion para activarse |
|------|-----------------|--------|--------------------------|
| `is_recommended_algorithm_id` | `is_recommended_algorithm` | `recommendation_projects` (GeoSpot) | El `spot_id` aparece en **cualquiera** de los campos `spots_suggested`, `white_list` o `black_list` para el `project_id` correspondiente. Es decir, basta con que el motor de recomendaciones lo haya sugerido en algun momento, sin importar si luego fue aprobado, rechazado o sigue pendiente. |
| `is_recommended_chatbot_id` | `is_recommended_chatbot` | `recommendation_chatbot` (GeoSpot) | El `spot_id` aparece en **cualquiera** de los campos `spots_suggested`, `white_list` o `black_list` del registro de chatbot vinculado al lead/proyecto. Indica que el motor de recomendaciones genero ese spot como sugerencia dentro de una conversacion de chatbot. |
| `is_recommended_visits_id` | `is_recommended_visits` | `recommendation_visitspotsrecommendations` (GeoSpot) | El `spot_id` aparece en **cualquiera** de los campos `suggested_spots`, `white_list` o `black_list` para el `project_id` y `lead_id` correspondientes. Indica que el spot fue recomendado en base al historial de visitas del cliente. |

Cada indicador tiene dos campos:
- **`*_id`** (SMALLINT): valor numerico `1` = recomendado, `0` = no recomendado.
- **`*`** (VARCHAR): valor texto `'Yes'` o `'No'`.

**Nota sobre el indicador de visitas:** "Visits" no es un canal de atribucion (porque no hay mecanismo para confirmar que un spot se agrego *por* esa recomendacion), pero el indicador aporta contexto analitico valioso: indica que el spot fue recomendado al cliente basandose en su historial de visitas, aunque no se pueda confirmar que esa recomendacion fue la causa de la adicion.

---

## 6. Algoritmo de Atribucion

El algoritmo se ejecuta dentro de la capa **Gold** del pipeline de Dagster, especificamente al construir el **Block 3 (Spot Added)** de `gold_bt_lds_lead_spots_new`. Para cada fila de evento "Spot Added" (cada combinacion `lead_id, project_id, spot_id`), el proceso es:

### Paso 1 -- Recopilar evidencia de canal

Para cada par `(project_id, spot_id)`, buscar evidencia en las dos fuentes de atribucion:

- **Evidencia de Algoritmo:** Verificar si el `spot_id` aparece en la `white_list` de `recommendation_projects` para el `project_id` dado. **Solo la `white_list` cuenta como evidencia de atribucion** (no `spots_suggested` ni `black_list`), porque la presencia en `white_list` confirma que el spot fue efectivamente aprobado desde la sugerencia del algoritmo.

- **Evidencia de Chatbot:** Verificar si existe un registro en `conversation_events` de chat2 con `event_type = 'spot_confirmation'` donde el `spot_id` y el `lead_id` coincidan con los del proyecto (nota: `client_id` de chat2 se transforma a `lead_id` en silver). La existencia de este evento confirma que el cliente confirmo interes en ese spot a traves del chatbot, lo que disparo la adicion al proyecto.

### Paso 2 -- Resolver atribucion

Con la evidencia recopilada, aplicar las siguientes reglas en orden:

1. **Si solo hay evidencia de un canal** (Algorithm o Chatbot, pero no ambos): atribuir al canal con evidencia.

2. **Si hay evidencia de ambos canales** (el mismo spot aparece en la `white_list` de `recommendation_projects` Y tiene un evento `spot_confirmation` en chat2 para el mismo proyecto/lead): **desempatar por fecha**. Comparar el `updated_at` de `recommendation_projects` con el `created_at` del evento `spot_confirmation` en chat2. El canal con la fecha **mas antigua** gana la atribucion, bajo la logica de que la primera recomendacion que llego fue la que motivo la adicion.

3. **Si no hay evidencia de ningun canal**: atribuir como "Manual" (fallback).

### Paso 3 -- Calcular indicadores de recomendacion

**Importante:** Esta etapa usa una metodologia completamente distinta a la atribucion de canal. Mientras que la atribucion (Pasos 1 y 2) busca evidencia de que un spot fue *agregado al proyecto por* un canal especifico (usando `white_list` y `spot_confirmation`), los indicadores simplemente buscan si el spot *aparecio alguna vez como recomendacion* en cada tabla, sin importar su estado.

Para cada par `(project_id, spot_id)`, consultar las tres tablas de recomendacion de GeoSpot:

- **`is_recommended_algorithm`**: Buscar si el `spot_id` aparece en **cualquiera** de `spots_suggested`, `white_list` o `black_list` de `recommendation_projects` para el `project_id`. No importa si esta en la white_list (aprobado), black_list (rechazado) o spots_suggested (pendiente) -- lo que importa es que el motor de recomendaciones lo genero como sugerencia.

- **`is_recommended_chatbot`**: Buscar si el `spot_id` aparece en **cualquiera** de `spots_suggested`, `white_list` o `black_list` de `recommendation_chatbot` para el registro vinculado al lead/proyecto. Misma logica: basta con que haya sido sugerido.

- **`is_recommended_visits`**: Buscar si el `spot_id` aparece en **cualquiera** de `suggested_spots`, `white_list` o `black_list` de `recommendation_visitspotsrecommendations` para el `project_id` y `lead_id`. Misma logica.

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
| `is_recommended_visits_id` | SMALLINT | Si | `1` si el spot aparece en cualquier campo de recomendacion de `recommendation_visitspotsrecommendations`, `0` si no. Null para eventos != 3. |
| `is_recommended_visits` | VARCHAR(3) | Si | `'Yes'` si el spot aparece en cualquier campo de recomendacion de `recommendation_visitspotsrecommendations`, `'No'` si no. Null para eventos != 3. |

Esto lleva la tabla de 27 a 37 columnas. El `BT_LDS_LEAD_SPOTS_COLUMN_ORDER` en `gold_bt_lds_lead_spots_new.py` debe actualizarse para incluir estos 10 campos. Igualmente, el DDL en `lakehouse-sdk/sql/bt_lds_lead_spots.sql` debe extenderse.

---

## 8. Nuevos Assets de Dagster Necesarios

Para alimentar el algoritmo de atribucion y los flags, se necesitan los siguientes assets nuevos:

### 8.1 Bronze: Desde GeoSpot (`geospot_postgres`)

| Asset | Tabla fuente | Campos a extraer | Descripcion |
|-------|-------------|-------------------|-------------|
| `raw_gs_recommendation_projects_new` | `recommendation_projects` | `project_id`, `spots_suggested`, `white_list`, `black_list`, `created_at`, `updated_at` | Recomendaciones del algoritmo por proyecto |
| `raw_gs_recommendation_chatbot_new` | `recommendation_chatbot` | `id`, `spots_suggested`, `white_list`, `black_list`, `created_at`, `updated_at` | Recomendaciones del chatbot por conversacion |
| `raw_gs_recommendation_visits_new` | `recommendation_visitspotsrecommendations` | `project_id`, `client_id`, `suggested_spots`, `white_list`, `black_list`, `visits_ids`, `created_at`, `updated_at` | Recomendaciones basadas en visitas |

Estos assets usan `make_bronze_asset("geospot_postgres", table_name=...)` siguiendo la convencion existente (`raw_gs_*_new`). Referencia de implementacion: archivos existentes en `defs/data_lakehouse/bronze/`.

### 8.2 Bronze: Desde Chat2 (`chatbot_postgres`)

| Asset | Tabla fuente | Descripcion |
|-------|-------------|-------------|
| `raw_cb_spot_confirmations_new` | `conversation_events` (filtrado) | Query custom: extrae solo eventos con `event_type = 'spot_confirmation'`. Campos: `conversation_id`, `client_id`, `spot_id`, `event_data`, `created_at`. Todo el historico (sin ventana temporal). |

Este asset usa `make_bronze_asset("chatbot_postgres", query=..., asset_name=..., description=...)` siguiendo la convencion `raw_cb_*_new`.

**Nota:** Ya existe `raw_staging_conversation_events_new` que extrae conversation_events, pero usa `staging_postgres` con una ventana de 3 dias. Para la atribucion de canal se necesita el historico completo y especificamente filtrado por `spot_confirmation`.

### 8.3 Silver: Transformacion intermedia

Se recomienda crear silver assets intermedios que:
- **Exploten los JSON arrays** de las tablas de recomendacion (convertir `white_list: [1,2,3]` en filas individuales `(project_id, spot_id)`) para facilitar los JOINs en Gold.
- **Limpien y normalicen** los datos de `conversation_events` (extraer `spot_id` de `event_data` si no es columna directa, normalizar tipos).

Assets silver sugeridos:

| Asset | Descripcion |
|-------|-------------|
| `stg_gs_recommendation_projects_exploded_new` | Explota `white_list`, `spots_suggested` y `black_list` en filas `(project_id, spot_id, list_type)` + `updated_at` |
| `stg_gs_recommendation_chatbot_exploded_new` | Explota los JSON arrays de `recommendation_chatbot` en filas individuales |
| `stg_gs_recommendation_visits_exploded_new` | Explota los JSON arrays de `recommendation_visitspotsrecommendations` en filas individuales |
| `stg_cb_spot_confirmations_new` | Normaliza `raw_cb_spot_confirmations_new`: castea tipos, deduplica si es necesario |

### 8.4 Gold: Modificacion del asset existente

El asset `gold_bt_lds_lead_spots_new` (y la funcion compartida `_transform_bt_lds_lead_spots`) debe modificarse para:

1. **Recibir como inputs adicionales** los nuevos silver assets de recomendaciones y confirmaciones de chatbot.
2. **En el Block 3 (Spot Added)**, despues de construir `df_spot_events`, enriquecerlo con:
   - LEFT JOIN con silver de `recommendation_projects` exploded (filtrado a `list_type = 'white_list'`) para obtener evidencia de Algoritmo y su timestamp.
   - LEFT JOIN con silver de `spot_confirmations` (via `lead_id` + `spot_id`) para obtener evidencia de Chatbot y su timestamp.
   - LEFT JOIN con los tres silver exploded (sin filtro de list_type) para calcular los flags.
   - Calculo de `lds_channel_attribution_id`, `lds_channel_attribution`, y los pares `is_recommended_*_id` / `is_recommended_*` segun el algoritmo de la Seccion 6.
3. **Para los demas bloques** (eventos 1, 2, 4-10), agregar las 10 columnas como `null`.
4. **Actualizar `BT_LDS_LEAD_SPOTS_COLUMN_ORDER`** para incluir los 10 nuevos campos.
5. **Actualizar el DDL** en `lakehouse-sdk/sql/bt_lds_lead_spots.sql` con las nuevas columnas.

---

## 9. Consideraciones y Limitaciones

### 9.1 Precision del timestamp de "Recomendacion Algoritmo"

El campo `updated_at` de `recommendation_projects` se actualiza a nivel del registro completo, no por spot individual. Si multiples spots se aprueban en momentos distintos, el `updated_at` solo refleja la ultima aprobacion. Esto significa que para desempates, la fecha del canal Algoritmo es **aproximada** mientras que la del canal Chatbot es **exacta**. En caso de empate muy cerrado (diferencia de minutos), la precision favorece al canal Chatbot.

### 9.2 El canal "Visitas" no es un canal de atribucion

Las recomendaciones basadas en visitas (`recommendation_visitspotsrecommendations`) generan sugerencias que se envian al cliente por email. Sin embargo:
- No existe un mecanismo para registrar si el cliente abrio el email y agrego un spot desde ahi.
- La `white_list` de esta tabla nunca se actualiza en el flujo actual.
- No hay endpoint dedicado en GeoSpot para aprobar spots de este canal.

Por estas razones, "Visitas" no puede funcionar como canal de atribucion. Solo participa como flag de recomendacion.

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
| GeoSpot (PostgreSQL) | `recommendation_projects`, `recommendation_chatbot`, `recommendation_visitspotsrecommendations` | `geospot_postgres` | Ya configurado |
| Chat2 (PostgreSQL) | `conversation_events` | `chatbot_postgres` | `/dagster/lk_conversations_metrics/spot_chatbot_*` |

### 9.6 Cruce de `recommendation_chatbot` con proyectos

La tabla `recommendation_chatbot` usa `conversation_id` como clave primaria (no `project_id`). Para vincular una recomendacion de chatbot con un proyecto especifico, se necesita cruzar `conversation_events.client_id` -> `clients.id` (= `lead_id` en silver) -> `project_requirements.client_id` (= `lead_id` en silver) -> `project_id`. Este cruce ya esta disponible en los silver assets existentes del pipeline `bt_lds_lead_spots`, donde `client_id` se renombra como `lead_id`.

### 9.7 Impacto en consumidores downstream

La tabla `bt_lds_lead_spots` es consumida por:
- **Metabase** (dashboards de funnel y lead-spot interactions)
- **Pipeline `effective_supply`**: lee `bt_lds_lead_spots WHERE lds_event_id = 3` como fuente bronze (`raw_gs_bt_lds_spot_added`)

Los nuevos campos son aditivos (nullable), por lo que no rompen consumidores existentes. Sin embargo, se recomienda:
- Actualizar la documentacion en `docs/golden_tables/bt_lds_lead_spots.md`
- Notificar a los usuarios de Metabase que hay nuevos campos disponibles para analisis
