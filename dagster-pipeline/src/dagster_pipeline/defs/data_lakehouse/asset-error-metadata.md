# Metadata de error en assets (try/except + yield)

Guía para quien agregue **assets nuevos** en este paquete (`data_lakehouse/`): alinear el comportamiento con el resto del framework para que, en **fallo**, Dagster reciba metadata estructurada (`handled`, `error_type`, `error_message`, etc.) antes de que el step quede en **FAILED**.

## Qué hace el mecanismo

- El helper vive en `../pipeline_asset_error_handling.py` (`iter_job_wrapped_compute`, `emit_asset_error_metadata`).
- Dagster sigue marcando el materialization como **fallido**; no “traga” el error. Solo se **enriquece** el evento con metadata y logs.
- En **éxito** no se añaden esas claves de error; la metadata de éxito depende de cada asset (`add_output_metadata`, IO manager, etc.).

## Jobs que activan la metadata de error

Los nombres de job se resuelven en `_job_names_for_error_metadata()`:

- Siempre: `conv_job`, `lakehouse_daily_job`.
- Además: todos los de `LAKEHOUSE_NEW_PIPELINE_JOB_NAMES` en `jobs.py` (este directorio).

A partir de esos jobs se arma la **unión** de ops (`nodes_in_topological_order`). Un asset solo recibe el camino “con metadata en error” si **su nombre de op** aparece en esa unión.

### Alta de un job nuevo (framework `_new`)

1. Definí el `define_asset_job(...)` en `jobs.py` como ya hacés.
2. Agregá el **nombre del job** (el `name="..."` del job) a la tupla **`LAKEHOUSE_NEW_PIPELINE_JOB_NAMES`** en el mismo archivo, junto al resto de jobs `_new`.

No hace falta tocar `pipeline_asset_error_handling.py` para listar el job: ahí se **importa en lazy** `LAKEHOUSE_NEW_PIPELINE_JOB_NAMES` para evitar imports circulares.

### Jobs fuera del lakehouse `_new`

- **`conv_job`** / **`lakehouse_daily_job`**: ya están en `_job_names_for_error_metadata()`. Si un asset **solo** corre en otro job (p. ej. mantenimiento), no entrará en el conjunto salvo que ese job también se agregue explícitamente a la tupla devuelta por `_job_names_for_error_metadata()` (hoy no está automatizado por prefijo).

- **`lk_vtl_job`**: se maneja **aparte** en `../lk_matches_visitors_to_leads/` (no uses esta guía como copia literal ahí sin revisar ese módulo).

## Según cómo esté definido el asset

### 1. Asset vía factory (casi no tenés que hacer nada)

Si el asset sale de:

- **`make_bronze_asset`** → `bronze/base.py` (`_build_asset` ya usa `iter_job_wrapped_compute`).
- **`make_silver_stg_asset`** → `silver/stg/base.py` (idem).

En ese caso **no editás el archivo del asset** (p. ej. `bronze/raw_s2p_listings_new.py` con una sola línea al `make_bronze_asset`). El wrapper ya está en la base.

**Condición:** el asset tiene que ser op de algún job incluido en `_job_names_for_error_metadata()` (ver sección anterior).

### 2. Asset “a mano” (`@dg.asset` con lógica propia)

Tenés que envolver el compute en el patrón **`body` + `yield from`**:

```python
from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute

@dg.asset(...)
def mi_asset_nuevo(context):  # sin anotar tipo en context si la función hace yield (ver nota abajo)
    def body():
        # Toda la lógica que antes hacía return resultado
        return resultado

    yield from iter_job_wrapped_compute(context, body)
```

- Si el compute es **muy largo**, es válido extraer **`_mi_asset_impl(context, ...)`** y que `body()` sea `return _mi_asset_impl(...)`, como en `gold/gold_lk_users_new.py` o `gold/qa_gold.py`.

### Nota sobre tipos y `yield` (Dagster)

En esta versión de Dagster, si el asset **usa `yield`** (generador), la función decorada con `@dg.asset` **no debe** anotar el primer parámetro como `AssetExecutionContext` / `dg.AssetExecutionContext` (falla la validación de definiciones). Dejá `context` sin anotación; la implementación interna puede seguir usando el mismo objeto.

### 3. Publish / legacy con patrón existente

Varios publish ya tienen `def body():` y `yield from iter_job_wrapped_compute` dentro del asset. Para uno nuevo en la misma capa, **replicá ese patrón** y registrá el job correspondiente si aplica.

## Metadata emitida solo en error

Cuando corre `emit_asset_error_metadata`:

| Clave            | Contenido                          |
|------------------|------------------------------------|
| `handled`        | `true`                             |
| `error_type`     | Nombre de la clase de la excepción |
| `error_message`  | `str(exc)`                         |
| `asset_key`      | `context.asset_key.to_user_string()` |
| `partition_key`  | Solo si existe en el contexto      |

Además se hace `context.log.exception(...)` para el traceback en logs.

## Checklist rápido

1. ¿El asset entra por **`make_bronze_asset` / `make_silver_stg_asset`?** → No tocar el wrapper; sí verificar que el **job** esté en `LAKEHOUSE_NEW_PIPELINE_JOB_NAMES` (o en `conv_job` / `lakehouse_daily_job`).
2. ¿Es un **`@dg.asset` custom?** → `iter_job_wrapped_compute` + `body()`; `context` sin tipo si hay `yield`.
3. ¿**Job nuevo** `_new`?** → Agregar su `name` a **`LAKEHOUSE_NEW_PIPELINE_JOB_NAMES`** en `jobs.py`.
4. Tras cambiar la lista de jobs en tests: usar **`reset_wrapped_asset_keys_cache_for_tests()`** si recargás definiciones en el mismo proceso.

## Archivos de referencia

- `../pipeline_asset_error_handling.py` — lógica y metadata.
- `jobs.py` — `LAKEHOUSE_NEW_PIPELINE_JOB_NAMES`.
- Ejemplos custom: `bronze/raw_cb_messages_new.py`, `silver/stg/stg_bt_conv_intermediate.py`, `gold/gold_lk_projects_new.py`, `gold/qa_gold.py`.
