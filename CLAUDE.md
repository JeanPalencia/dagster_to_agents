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
Recibe mensajes de Google Chat, los procesa con Claude via AWS Bedrock (tool_use), y llama al GraphQL de Dagster.

```
chat-agent/
    Dockerfile              # python:3.12-slim + uv, expone puerto 8000
    railway.toml            # builder=DOCKERFILE, healthcheck=/health
    pyproject.toml          # fastapi, uvicorn, boto3, httpx, google-auth
    src/chat_agent/
        main.py             # POST /chat/webhook  +  GET /health (formato Google Workspace Add-on)
        agent.py            # Loop Claude tool_use via Bedrock (claude-sonnet-4-6, max 10 iteraciones)
        tools.py            # GraphQL: launch_job, get_run_status, get_recent_runs, list_jobs
        config.py           # Env vars + JOB_REGISTRY
        google_auth.py      # Verifica JWT de Google Chat con audience = webhook URL
```

### Deploy en Railway

1. Dashboard → proyecto `dagster-to-agents` → **"+ New"** → **"GitHub Repo"**
2. Root directory: `chat-agent/`
3. Variables requeridas:

| Variable | Valor |
|---|---|
| `AWS_ACCESS_KEY_ID` | Credenciales AWS (mismas que para S3 en dagster_to_agents) |
| `AWS_SECRET_ACCESS_KEY` | Credenciales AWS (mismas que para S3) |
| `AWS_SESSION_TOKEN` | Token STS temporal (si las creds son ASIA*) |
| `AWS_DEFAULT_REGION` | `us-east-1` |
| `DAGSTER_GRAPHQL_URL` | `https://dagstertoagents-production.up.railway.app/graphql` |
| `GOOGLE_CHAT_AUDIENCE` | URL completa del webhook: `https://<domain>/chat/webhook` |

4. Generar dominio público → usar como webhook URL en Google Chat API config

**Nota sobre Google Chat JWT**: El app usa formato Google Workspace Add-ons, donde:
- `aud` en el JWT = URL del webhook (con espacio al final que se stripea)
- `iss` = `https://accounts.google.com` (no `chat@system.gserviceaccount.com`)
- El body del evento viene en `body["chat"]["messagePayload"]["message"]["text"]`

**Nota sobre AWS Bedrock**: El modelo se invoca via `bedrock-runtime.invoke_model()` con `us.anthropic.claude-sonnet-4-6`. Todos los content blocks requieren `{"type": "text", "text": "..."}` explícito (system, messages, tool_result). Los campos de respuesta son snake_case (`stop_reason`, no `stopReason`).

### Agregar un nuevo job al agente

Editar `chat-agent/src/chat_agent/config.py` → `JOB_REGISTRY`:
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
