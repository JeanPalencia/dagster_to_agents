# Tests & Validation Scripts

Scripts de validación para los flujos de datos de Dagster.

## Entorno de ejecución

Todos los scripts dependen de paquetes instalados en el entorno virtual del
pipeline (`dagster-pipeline/.venv`), que se gestiona con **uv** a partir del
`dagster-pipeline/pyproject.toml`.

Para ejecutar cualquier script se debe usar `uv run` desde el directorio
`dagster-pipeline/`:

```bash
cd dagster-pipeline
uv run python ../lakehouse-sdk/tests/<ruta_al_script>.py
```

> **No usar** `python` directamente ni activar el venv con `source .venv/bin/activate`,
> ya que el shell de Cursor puede interceptar el intérprete. `uv run` garantiza
> que se use el Python y los paquetes correctos.

## Scripts disponibles

### effective_supply

| Script | Descripción |
|--------|-------------|
| `effective_supply/validate_refactor.py` | Compara la salida del pipeline refactorizado contra las tablas actuales en GeoSpot. |
| `effective_supply/recreate_all_tables.sql` | DROP + CREATE de las 6 tablas de effective_supply (ejecutar manualmente en GeoSpot). |

```bash
cd dagster-pipeline
uv run python ../lakehouse-sdk/tests/effective_supply/validate_refactor.py
```

### bt_lds_channel_attribution

| Script | Descripcion |
|--------|-------------|
| `bt_lds_channel_attribution/capture_baseline.py` | Captura el parquet de referencia desde S3 (gold actual sin cambios). |
| `bt_lds_channel_attribution/run_pipeline.py` | Ejecuta bronze, silver y gold del flujo de atribucion de canal localmente. |
| `bt_lds_channel_attribution/test_regression.py` | 30 checks de regresion e integridad (columnas originales + nuevas). |
| `bt_lds_channel_attribution/prd_audit.md` | Revision cruzada PRD v2 vs implementacion. |

```bash
cd dagster-pipeline
uv run python ../lakehouse-sdk/tests/bt_lds_channel_attribution/test_regression.py
```

### spot_state_transitions

| Script | Descripción |
|--------|-------------|
| `spot_state_transitions/validate_sst_dagster.py` | Compara el pipeline Pandas original vs el pipeline Dagster/Polars fila a fila. |
| `spot_state_transitions/update_spot_status_history_v3_dagster.py` | Pipeline Pandas original adaptado para usar credenciales de AWS SSM. |
| `spot_state_transitions/update_spot_status_history_v3.py` | Pipeline Pandas original (usa archivos `.env` locales). |

```bash
cd dagster-pipeline
uv run python ../lakehouse-sdk/tests/spot_state_transitions/validate_sst_dagster.py
```

## Credenciales AWS

Los scripts que consultan MySQL y GeoSpot obtienen las credenciales desde
**AWS SSM Parameter Store** mediante `boto3`. Antes de ejecutar, es necesario
autenticarse con AWS desde la **máquina local** (no desde Cursor):

```bash
aws_login
```

Este comando es una función de zsh definida en `~/.zshrc` que realiza el login
SSO y descarga las credenciales necesarias para acceder a Parameter Store.

Si al ejecutar un script ves un error `ExpiredTokenException`, vuelve a correr
`aws_login` en tu terminal local y reintenta.
