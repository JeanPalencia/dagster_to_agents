# Optimización del job LK (lead_events_from_funnel_lk)

## Por qué no se puede usar solo funnel_with_channel

- **funnel_with_channel** está a nivel **(user, event_date)**: una fila por usuario por día, con atribución (Channel, entry_point, conversion_point, etc.).
- Para el pipeline necesitamos **event_datetime** y **event_name** reales (p. ej. `clientRequestedWhatsappForm` y la hora exacta) para el matching con chats y para la tabla MAT.
- Esos campos solo existen a nivel **evento** en GA4, no en el funnel agregado. Por eso la query usa **raw_events** (eventos de conversión) y hace **JOIN** a funnel para traer la atribución.

## Qué hace la query actual

1. **raw_events**: lee solo los eventos de conversión en `events_*` (lista de `event_name`), con un **solo UNNEST** de `event_params` para sacar email, phone, page_location en una pasada (más eficiente que varias subconsultas).
2. **funnel_base**: lee de `funnel_with_channel` **solo las columnas que usamos** (menos bytes).
3. **JOIN** por `(user_pseudo_id, event_date)` y se devuelve evento + atribución.

## Cómo bajar los ~25 min de materialización

### 1. Materializar funnel_with_channel (recomendado)

Si `funnel_with_channel` es una **vista**, BigQuery la ejecuta entera cada vez que corre el job. Es la parte más pesada.

- Crear una **tabla** (p. ej. `analitics_spot2.funnel_with_channel_materialized`) con una **scheduled query diaria** que haga:
  ```sql
  TRUNCATE TABLE analitics_spot2.funnel_with_channel_materialized;
  INSERT analitics_spot2.funnel_with_channel_materialized
  SELECT * FROM analitics_spot2.funnel_with_channel;
  ```
- En `lead_events_from_funnel_lk.sql`, en el CTE `funnel_base`, cambiar a:
  ```sql
  FROM `analitics_spot2.funnel_with_channel_materialized`
  ```
- Así el job de Dagster solo **lee** una tabla ya calculada; el coste de la vista se paga una vez al día en el scheduled query.

### 2. Acotar rango de fechas (si el negocio lo permite)

En **raw_events**, el filtro es `event_date >= '20250201'`. Si la tabla MAT solo necesita p. ej. últimos 12 meses:

- Cambiar a algo como:  
  `event_date >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))`  
- Reduce bytes escaneados en `events_*`.

### 3. Particiones y clustering en events_*

Si el dataset `analytics_276054961` usa partición por `event_date`, el filtro por `event_date` ya limita las particiones. Si además tienes clustering por `event_name`, puede ayudar; no es algo que se cambie en esta query.

---

Resumen: **sí hace falta raw_events** para tener `event_datetime` y `event_name`. La ganancia grande de tiempo suele venir de **materializar funnel_with_channel** y leer esa tabla en esta query.
