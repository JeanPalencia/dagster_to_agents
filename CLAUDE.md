# dagster_to_agents — CLAUDE.md

## Propósito

Entorno de prueba para desarrollar y testear agentes que ejecutan flujos de Dagster.
Desplegado en Railway. La producción corre en AWS EC2 (CodeDeploy) con código separado.

## Arquitectura

```
dagster-pipeline/          # Paquete Python principal (Dagster)
lakehouse-sdk/             # SDK de utilidades (no es dependencia de dagster-pipeline)
Dockerfile                 # Build para Railway (python:3.12-slim + uv)
start.sh                   # Arranque: dagster-daemon (bg) + dagster-webserver
railway.toml               # Config Railway: builder=DOCKERFILE, healthcheck=/healthz
```

## Railway

- **URL pública**: https://dagster-to-agents-production.up.railway.app
- **Proyecto**: `innovative-kindness`
- **Servicio**: `dagster-to-agents`
- **Postgres interno**: `postgres.railway.internal:5432` (solo accesible dentro de Railway)
- **Vincular CLI**: `railway link` → seleccionar `innovative-kindness` → `production`

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

## Archivos clave

| Archivo | Qué hace |
|---|---|
| `dagster-pipeline/src/dagster_pipeline/definitions.py` | Registro de flujos activos — editar para agregar flujos |
| `dagster-pipeline/src/dagster_pipeline/defs/data_lakehouse/shared.py` | Utilidades compartidas: S3, GeoSpot PG, GeoSpot API, SSM fallbacks |
| `dagster-pipeline/dagster.yaml` | Config de Dagster: storage en Railway Postgres |
| `start.sh` | Entrypoint Railway: mapea `DATABASE_URL` → `DAGSTER_PG_*`, arranca daemon + webserver |
