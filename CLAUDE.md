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
Dockerfile.chat-agent       # (raíz del repo) — clona repo completo a /app/repo/ durante build
chat-agent/
    Dockerfile              # python:3.12-slim + uv + engram binary, non-root (appuser), puerto 8000
    railway.toml            # builder=DOCKERFILE, healthcheck=/health, watchPaths=[chat-agent/**,...]
    pyproject.toml          # fastapi, uvicorn, claude-agent-sdk, httpx, google-auth
    src/chat_agent/
        main.py             # POST /chat/webhook + GET /health; respuesta async con service account
        agent.py            # _REGISTRY (kind=mcp|subagent) → allowed_tools + mcp_servers + system prompt
        tools.py            # GraphQL: launch_job, get_run_status, get_recent_runs, list_jobs
        config.py           # Env vars + JOB_REGISTRY + build_system_prompt()
        google_auth.py      # Verifica JWT de Google Chat con audience = webhook URL
    src/dagster_mcp/
        server.py           # Tools de Dagster con @tool decorator (McpSdkServerConfig, in-process)
                            # Exporta DAGSTER_TOOLS (lista de tool objects) usado por agent.py
```

### Arquitectura del agente

El agente usa `claude_agent_sdk.query()` con `cwd="/app/repo"` — el repo completo clonado durante el build. Esto permite que el sub-agente `logic_modifier` lea, modifique y haga push de cambios al código de `dagster-pipeline/`.

- **Dagster tools**: in-process (`McpSdkServerConfig`)
- **Engram**: subprocess stdio (`McpStdioServerConfig`), datos en Railway Volume `/data/.engram`
- **Bedrock**: credenciales pasadas via `ClaudeAgentOptions(env={...})` — el subprocess NO hereda env vars del proceso padre

Las sesiones se trackean en memoria por `space.name` de Google Chat (se pierden en redeploy — aceptable).

**Respuesta async**: mensajes que tardan más de 5s reciben inmediatamente un "⏳ Analizando..." y el resultado llega vía Google Chat REST API (`POST /v1/{space}/messages`) usando la service account. Esto evita el placeholder "no responde" de Google Chat (timeout ~30s).

**watchPaths**: el servicio `dagster-chat2` solo redesploy cuando cambian `chat-agent/**`, `Dockerfile.chat-agent`, o `.claude/**`. Commits del `logic_modifier` en `dagster-pipeline/` no triggean redeploy del chat-agent.

### Agregar una nueva capability

Agregar una entrada a `_REGISTRY` en `chat-agent/src/chat_agent/agent.py`:

```python
# MCP server con tools
"my_server": {
    "kind": "mcp",
    "server": McpStdioServerConfig(command="...", args=["..."]),
    "tools": ["mcp__my_server__tool_a", "mcp__my_server__tool_b"],
    "internal": False,
},

# Sub-agente de Claude Code (definido en .claude/agents/<name>.md)
"my_agent": {
    "kind": "subagent",
    "description": "Qué hace este sub-agente (se muestra al usuario)",
    "internal": False,
},
```

`internal=False` → aparece en el system prompt (el usuario puede preguntar por ello).
`internal=True` → el agente lo usa silenciosamente (ej: engram para memoria).

`allowed_tools`, `mcp_servers` y el system prompt se derivan automáticamente — no hay que editar nada más.

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
| `ENGRAM_DATA_DIR` | `/data/.engram` (Railway Volume montado en `/data`) |
| `GOOGLE_CHAT_SA_EMAIL` | Email de la service account para mensajes proactivos |
| `GOOGLE_CHAT_SA_PRIVATE_KEY` | Private key de la service account (con `\n` literales) |
| `GITHUB_TOKEN` | PAT con `repo` scope — para clonar el repo privado durante el build |
| `SPOT2_API_KEY` | API key del MCP de spot2 (usado por `logic_modifier` para queries a DB) |

4. Crear Railway Volume `dagster-chat2-volume` con mount path `/data` (para persistir Engram DB)
5. Generar dominio público → usar como webhook URL en Google Chat API config
6. Para la service account de Google Chat (mensajes proactivos): crear en Google Cloud Console → IAM → Service Accounts → sin rol de IAM → Keys → JSON. El `private_key` se sube con `\n` literales (Railway lo preserva correctamente).

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
| `chat-agent/src/chat_agent/config.py` | `JOB_REGISTRY` + `build_system_prompt()` — el prompt se genera dinámicamente desde el registry |
| `chat-agent/src/chat_agent/agent.py` | `_MCP_REGISTRY` — agregar nuevas capabilities aquí |

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

**Cuándo se activa**: Cuando se pide modificar cómo se calcula una columna existente en una tabla lakehouse, tanto desde Claude Code local como desde el chat-agent de Google Chat.

**Qué hace**:
- Modifica lógica de columnas existentes en assets silver (STG o Core)
- Preserva estructura de tabla (mismas columnas, mismo schema)
- Sigue 100% `dagster-pipeline/ARCHITECTURE.md`
- Ejecuta en worktree aislado, hace commit y push a `main` cuando el usuario confirma

**Acceso a datos**: Usa el MCP de spot2 (`https://mcp.ai.spot2.mx/mcp`) para queries a MySQL y PostgreSQL cuando necesita evaluar disponibilidad de datos (Phase 2).

**Ubicación**: `.claude/agents/logic_modifier.md`

**Uso via Google Chat**: enviar un mensaje describiendo el cambio + "Tienes mi confirmación" para que proceda al commit. El agente corre async y notifica via proactive message cuando termina.

**Nota**: el sub-agente opera en `/app/repo/` dentro del container, con git configurado como `Dagster Agent <dagster-agent@railway.app>` para los commits.
