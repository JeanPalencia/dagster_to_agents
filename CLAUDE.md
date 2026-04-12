# dagster_to_agents — CLAUDE.md

## Propósito

Entorno de prueba para desarrollar y testear agentes que ejecutan flujos de Dagster.
Desplegado en Railway. La producción corre en AWS EC2 (CodeDeploy) con código separado.

## Arquitectura

```
dagster-pipeline/          # Paquete Python principal (Dagster)
chat-agent/                # Agente Google Chat → Dagster (FastAPI + AWS Bedrock)
lakehouse-sdk/             # SDK de utilidades (no es dependencia de dagster-pipeline)
Dockerfile                 # Build para Railway — servicio dagster_to_agents
start.sh                   # Arranque: dagster-daemon (bg) + dagster-webserver
railway.toml               # Config Railway: builder=DOCKERFILE, healthcheck=/healthz
```

## Railway

- **URL pública**: https://dagstertoagents-production.up.railway.app
- **Proyecto**: `dagster-to-agents` (workspace: `dantes4ur's Projects`, plan Pro)
- **Project ID**: `585835c9-4bc1-4a69-9a49-1f1ee86683ac`
- **Servicio**: `dagster_to_agents` (ID: `7f4245b9-df84-427d-acf3-f69213e40695`)
- **Postgres interno**: `postgres.railway.internal:5432` (solo accesible dentro de Railway)
- **Vincular CLI**: `railway link` → seleccionar `dantes4ur's Projects` → `dagster-to-agents` → `production`

## Convención de nombres para pruebas

Todo output de prueba lleva el prefijo `dagster_agent_`:

| Tipo | Producción | Prueba |
|---|---|---|
| S3 key prefix | `amenity_description_consistency/gold/...` | `dagster_agent_amenity_description_consistency/gold/...` |
| Tabla destino | `rpt_amenity_description_consistency` | `dagster_agent_rpt_amenity_description_consistency` |

El bucket S3 (`dagster-assets-production`) es el mismo para producción y prueba.

## Credenciales: env vars en lugar de SSM

En Railway no hay IAM role, así que los secrets van como variables de entorno.
El código en `shared.py` ya tiene fallback: env var → SSM.

| Variable Railway | Propósito |
|---|---|
| `GEOSPOT_DB_HOST` | Host de GeoSpot PostgreSQL staging |
| `GEOSPOT_DB_USER` | Usuario GeoSpot PG |
| `GEOSPOT_DB_PASSWORD` | Password GeoSpot PG |
| `GEOSPOT_API_KEY` | API key de GeoSpot Lakehouse |
| `AWS_ACCESS_KEY_ID` | boto3: S3 writes |
| `AWS_SECRET_ACCESS_KEY` | boto3: S3 writes |
| `AWS_SESSION_TOKEN` | Si las creds son STS (temporales, expiran) |
| `AWS_DEFAULT_REGION` | `us-east-1` |
| `DAGSTER_PG_*` | Postgres interno de Railway (auto-seteado desde `${{Postgres.*}}`) |

> **Atención**: Si `AWS_ACCESS_KEY_ID` empieza con `ASIA`, son credenciales STS temporales
> y van a expirar. Actualizarlas con `aws configure get` cuando fallen las escrituras a S3.

## Cómo agregar un nuevo flujo

1. En `dagster-pipeline/src/dagster_pipeline/definitions.py`, importar los assets/jobs/sensors del flujo
2. Agregarlos a la instancia `Definitions`
3. Si el flujo escribe a tablas/S3, usar el prefijo `dagster_agent_` en su publish file
4. Si el flujo usa secrets de SSM, agregar el fallback de env var en `shared.py` (patrón ya establecido)
5. Sensors y schedules del nuevo flujo deben tener `default_status=STOPPED` (ver convención abajo)
6. Commit + push → Railway redeploy automático

**No usar `load_from_defs_folder`** — carga todos los módulos incluyendo dependencias pesadas
(catboost, scikit-learn, bigquery) y provoca OOM kill en el container de Railway.

**Cada job nuevo debe usar `executor_def=dg.in_process_executor`** — el executor multiprocess
lanza un subprocess por step en paralelo, multiplicando el uso de memoria y causando OOM kill.
El in_process_executor corre los steps secuencialmente en el mismo proceso.

## Flujos activos

| Flujo | Job | Estado |
|---|---|---|
| `amenity_description_consistency` | `amenity_desc_consistency_job` | ✅ Activo |

## Sensores y schedules: siempre STOPPED en Railway

Todos los sensores y schedules deben declararse con `default_status=STOPPED` en este entorno:

```python
@dg.sensor(
    job=my_job,
    default_status=dg.DefaultSensorStatus.STOPPED,   # ← siempre STOPPED
)
```

Los jobs se lanzan **manualmente** desde el UI o vía GraphQL. Esto evita ejecuciones
automáticas no deseadas mientras se desarrollan y prueban flujos de agentes uno a uno.

## `cleanup_storage`

El asset `cleanup_storage` (de `defs/maintenance/assets.py`) **siempre debe incluirse** en
`definitions.py` y en la selección de cada job. Limpia los pickles temporales que Dagster
genera en disco durante la ejecución de assets, evitando que el container llene el storage.

## Patrón de `definitions.py`: instancia directa, no factory

Usar `defs = Definitions(...)` (instancia), **nunca** `def defs(): return Definitions(...)` (factory callable).

El código legacy de producción (EC2) usa el patrón factory (`@repository` o callable), por eso
`pipeline_asset_error_handling.py` llamaba `defs()` con paréntesis. En este repo usamos el
patrón moderno (Dagster 1.x): `defs` es una instancia directa y no es callable.

Cualquier módulo que importe `defs` debe tratarlo como objeto: `defs.get_repository_def()`,
**no** `defs().get_repository_def()`.

## chat-agent — Agente Google Chat

Servicio FastAPI independiente desplegado en Railway como segundo servicio del proyecto.
Recibe mensajes de Google Chat, los procesa con **Claude Agent SDK** via AWS Bedrock, y llama al GraphQL de Dagster.

```
chat-agent/
    Dockerfile              # python:3.12-slim + uv, non-root (appuser), expone puerto 8000
    railway.toml            # builder=DOCKERFILE, healthcheck=/health
    pyproject.toml          # fastapi, uvicorn, claude-agent-sdk, httpx, google-auth
    src/chat_agent/
        main.py             # POST /chat/webhook  +  GET /health (formato Google Workspace Add-on)
        agent.py            # Claude Agent SDK: query() con MCP tools, session tracking por space
        tools.py            # GraphQL: launch_job, get_run_status, get_recent_runs, list_jobs
        config.py           # Env vars + JOB_REGISTRY + DAGSTER_SYSTEM_PROMPT
        google_auth.py      # Verifica JWT de Google Chat con audience = webhook URL
    src/dagster_mcp/
        server.py           # Tools de Dagster con @tool decorator (McpSdkServerConfig, in-process)
```

### Arquitectura del agente

El agente usa `claude_agent_sdk.query()`. Los tools de Dagster corren in-process (`McpSdkServerConfig`). Engram corre como subprocess stdio (`McpStdioServerConfig`). El SDK usa el CLI de Claude Code bundled internamente — las credenciales de Bedrock se pasan via `ClaudeAgentOptions(env={...})` porque el subprocess NO hereda las env vars del proceso padre.

Las sesiones se trackean en memoria por `space.name` de Google Chat (se pierden en redeploy — aceptable).

### Agregar una nueva capability

Agregar una entrada a `_MCP_REGISTRY` en `chat-agent/src/chat_agent/agent.py`:

```python
"my_server": {
    "server": McpStdioServerConfig(command="...", args=["..."]),
    "tools": ["mcp__my_server__tool_a", "mcp__my_server__tool_b"],
    "internal": False,  # True = el agente la usa pero no la expone al usuario
},
```

`internal=False` → aparece en el system prompt como capacidad del agente (el usuario puede preguntar por ella).
`internal=True` → el agente la usa internamente (ej: engram para memoria), no se expone al usuario.

`allowed_tools`, `mcp_servers` y el system prompt se derivan automáticamente del registry — no hay que editar nada más.

### Deploy en Railway

1. Dashboard → proyecto `dagster-to-agents` → **"+ New"** → **"GitHub Repo"**
2. Root directory: `chat-agent/`
3. Variables requeridas:

| Variable | Valor |
|---|---|
| `CLAUDE_CODE_USE_BEDROCK` | `1` |
| `AWS_ACCESS_KEY_ID` | Credenciales AWS |
| `AWS_SECRET_ACCESS_KEY` | Credenciales AWS |
| `AWS_SESSION_TOKEN` | Token STS temporal (si las creds son ASIA*) |
| `AWS_DEFAULT_REGION` | `us-east-1` |
| `DAGSTER_GRAPHQL_URL` | `https://dagstertoagents-production.up.railway.app/graphql` |
| `GOOGLE_CHAT_AUDIENCE` | URL completa del webhook: `https://<domain>/chat/webhook` |

4. Generar dominio público → usar como webhook URL en Google Chat API config

> **⚠️ Credenciales STS (ASIA\*)**: Si `AWS_ACCESS_KEY_ID` empieza con `ASIA`, son temporales y expiran.
> Síntoma: el agente no responde (el SDK hace retries silenciosos con 403 authentication_failed).
> Renovar con: `railway variables --set "AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)" --set "AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)" --set "AWS_SESSION_TOKEN=$(aws configure get aws_session_token)"`

**Nota sobre Google Chat JWT**: El app usa formato Google Workspace Add-ons, donde:
- `aud` en el JWT = URL del webhook (con espacio al final que se stripea)
- `iss` = `https://accounts.google.com` (no `chat@system.gserviceaccount.com`)
- El body del evento viene en `body["chat"]["messagePayload"]["message"]["text"]`

**Nota sobre formato de respuestas**: Google Chat usa `*negrita*` (un asterisco), no `**doble**`. El system prompt en `config.py` ya instruye a Claude con las reglas de formato correctas.

**Nota sobre non-root**: El Dockerfile crea `appuser (uid=1000)` para el runtime. El SDK requiere usuario no-root para poder usar `bypassPermissions`.

### Agregar un nuevo job al agente

Editar `chat-agent/src/chat_agent/config.py` → `JOB_REGISTRY` (también actualiza las respuestas del agente sobre jobs disponibles):
```python
"new_job_name": {
    "description": "What this job does",
    "target_table": "dagster_agent_rpt_...",
    "s3_prefix": "dagster_agent_.../gold",
},
```

## Archivos clave

| Archivo | Qué hace |
|---|---|
| `dagster-pipeline/src/dagster_pipeline/definitions.py` | Registro de flujos activos — editar para agregar flujos |
| `dagster-pipeline/src/dagster_pipeline/defs/data_lakehouse/shared.py` | Utilidades compartidas: S3, GeoSpot PG, GeoSpot API, SSM fallbacks (boto3 lazy) |
| `dagster-pipeline/src/dagster_pipeline/defs/pipeline_asset_error_handling.py` | Error metadata wrapper — usa `defs.get_repository_def()` (instancia, no callable) |
| `dagster-pipeline/dagster.yaml` | Config de Dagster: storage en Railway Postgres |
| `start.sh` | Entrypoint Railway: mapea `DATABASE_URL` → `DAGSTER_PG_*`, arranca daemon + webserver |
| `chat-agent/src/chat_agent/config.py` | JOB_REGISTRY — agregar nuevos jobs aquí para que el agente los conozca |

## Auto-activación de agentes

Cuando se detecta cualquiera de estos contextos, el agente correspondiente se delega AUTOMÁTICAMENTE:

| Contexto | Agente |
|----------|--------|
| Modificar lógica de columna en silver/gold, cambiar cálculos en tablas lakehouse | logic_modifier |

**Nota**: Los agentes se invocan con `Agent(subagent_type="...", ...)`, NO con `Skill(...)`.

## Agentes disponibles

Este proyecto tiene agentes especializados que se activan automáticamente según el contexto:

| Contexto | Agente | Propósito |
|----------|--------|-----------|
| Modificar lógica de columna en silver/gold, cambiar cálculos en tablas lakehouse | `logic_modifier` | Modifica la lógica de cálculo de columnas existentes preservando estructura y patrones arquitecturales |

### logic_modifier

**Cuándo se activa**: Cuando solicitas modificar cómo se calcula una columna existente en una tabla lakehouse (ej: "cambiar la forma en que se calcula X", "agregar prefijo a Y columna").

**Qué hace**:
- Modifica lógica de columnas existentes en assets silver (STG o Core)
- Preserva estructura de tabla (mismas columnas, mismo schema)
- Sigue 100% `dagster-pipeline/ARCHITECTURE.md`
- Ejecuta en worktree aislado para testing
- Valida cambios con backfills antes de merge

**Ubicación**: `.claude/agents/logic_modifier.md`

**Uso**: Se invoca automáticamente - no es necesario llamarlo explícitamente.
