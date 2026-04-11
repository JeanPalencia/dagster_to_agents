# Reporte: Fuentes de Datos y Patrones de URLs de Browse Pages — spot2.mx

## 1. Estructura de URLs de Browse Pages

Basado en el archivo `src/Seo/Core/Application/Actions/StructureSlug.php`, las browse pages siguen esta estructura jerárquica:

```
/{sector_slug}/{operation_slug}/{state_slug}/{corridor_slug}/{municipality_slug}/{settlement_slug}
```

Cada segmento es **opcional** y se agrega solo si el parámetro correspondiente existe.

---

### 1.1 Sector Slugs (6 valores posibles)

| Slug | Tipo | `ty` value | Nota |
|------|------|------------|------|
| `locales-comerciales` | Retail | 13 | Sector canónico |
| `naves-industriales` | Industrial | 9 | Sector canónico (>= 3000 m²) |
| `oficinas` | Oficinas | 11 | Sector canónico |
| `terrenos` | Terrenos | 15 | Sector canónico |
| `bodegas` | Bodegas | 91 | Subsector virtual (mapea a Industrial en OpenSearch) |
| `coworking` | Coworking | 92 | Subsector virtual (mapea a Oficinas en OpenSearch) |

**Fuente:** `app/Traits/Spot/SpotHelper.php` líneas 466-482, `src/Seo/Core/Domain/SeoBrowseSectorTy.php`

### 1.2 Operation Slugs (2 valores válidos para SEO)

| Slug | Tipo de precio | `pt` value |
|------|---------------|------------|
| `renta` | Renta | 1 |
| `venta` | Venta | 2 |

> `renta-y-venta` (pt=3) existe en el código pero **no se usa en browse pages SEO** (solo pt=1 y pt=2 son válidos para indexabilidad).

**Fuente:** `app/Traits/Spot/SpotHelper.php` líneas 547-555

### 1.3 State Slugs (9 valores posibles)

| Slug | Estado | `StateEnum` value |
|------|--------|-------------------|
| `ciudad-de-mexico` | CDMX | 7 |
| `mexico` | Estado de México | 15 |
| `jalisco` | Jalisco | 14 |
| `nuevo-leon` | Nuevo León | 19 |
| `chihuahua` | Chihuahua | 6 |
| `guerrero` | Guerrero | 12 |
| `queretaro` | Querétaro | 22 |
| `quintana-roo` | Quintana Roo | 23 |
| `yucatan` | Yucatán | 31 |

**Fuente:** `app/Enums/Seo/StateEnum.php`

### 1.4 Corridor, Municipality y Settlement Slugs

- **Corridor (`co`):** Slug de zona/corredor de la tabla `zones`. Filtrado en OpenSearch via `zones.slug` + `zones.state.slug` (nested).
- **Municipality (`mu`):** Slug del municipio, almacenado en OpenSearch como `address.slugs.municipality`.
- **Settlement (`sm`):** Slug de la colonia, almacenado en OpenSearch como `address.slugs.settlement`.

Estos son **slugs dinámicos** generados a partir de los datos geográficos reales. Sus valores están almacenados en la tabla `seo_browse_urls` y `seo_indexable_urls`.

---

## 2. Todas las Combinaciones Posibles de URLs

Basado en `StructureSlug.php`, las combinaciones válidas son:

### Con sector:
1. `/{sector}` — ej: `/oficinas`
2. `/{sector}/{operation}` — ej: `/oficinas/renta`
3. `/{sector}/{operation}/{state}` — ej: `/oficinas/renta/ciudad-de-mexico`
4. `/{sector}/{operation}/{state}/{corridor}` — ej: `/oficinas/renta/jalisco/santa-fe`
5. `/{sector}/{operation}/{state}/{municipality}` — ej: `/oficinas/renta/ciudad-de-mexico/miguel-hidalgo`
6. `/{sector}/{operation}/{state}/{corridor}/{municipality}` — ej: con corredor y municipio
7. `/{sector}/{operation}/{state}/{municipality}/{settlement}` — ej: `/oficinas/renta/ciudad-de-mexico/miguel-hidalgo/polanco`
8. `/{sector}/{operation}/{state}/{corridor}/{municipality}/{settlement}` — máxima granularidad

### Sin sector (solo operación):
9. `/{operation}` — ej: `/renta`
10. `/{operation}/{state}` — ej: `/renta/ciudad-de-mexico`

> **Nota:** La jerarquía geográfica es: state → corridor (opcional) → municipality → settlement

---

## 3. Propuesta de Expresión Regular

### 3.1 Regex para capturar TODAS las browse pages de spot2.mx

```regex
^/(locales-comerciales|naves-industriales|oficinas|terrenos|bodegas|coworking|renta|venta)(/(?:renta|venta))?(/[a-z0-9-]+){0,4}$
```

### 3.2 Regex más precisa con validación de estructura

```regex
^/(?:(?:locales-comerciales|naves-industriales|oficinas|terrenos|bodegas|coworking)(?:/(?:renta|venta)(?:/[a-z0-9\u00e0-\u00ff-]+){0,4})?|(?:renta|venta)(?:/[a-z0-9\u00e0-\u00ff-]+){0,4})$
```

### 3.3 Regex para BigQuery (sintaxis RE2)

```sql
-- Filtrar browse pages en BigQuery
WHERE REGEXP_CONTAINS(page_path,
  r'^/(locales-comerciales|naves-industriales|oficinas|terrenos|bodegas|coworking|renta|venta)(/[a-z0-9-]+)*$'
)
```

### 3.4 Regex más estricta (recomendada para producción)

```sql
-- Regex estricta que valida la estructura completa
WHERE REGEXP_CONTAINS(page_path,
  r'^/(?:'
  -- Patrón 1: Comienza con sector
  r'(?:locales-comerciales|naves-industriales|oficinas|terrenos|bodegas|coworking)'
  r'(?:/(?:renta|venta)'
    r'(?:/(?:ciudad-de-mexico|mexico|jalisco|nuevo-leon|chihuahua|guerrero|queretaro|quintana-roo|yucatan)'
      r'(?:/[a-z0-9][-a-z0-9]*){0,3}'  -- corridor/municipality/settlement
    r')?'
  r')?'
  -- Patrón 2: Comienza con operación (sin sector)
  r'|(?:renta|venta)'
    r'(?:/(?:ciudad-de-mexico|mexico|jalisco|nuevo-leon|chihuahua|guerrero|queretaro|quintana-roo|yucatan)'
      r'(?:/[a-z0-9][-a-z0-9]*){0,3}'
    r')?'
  r')$'
)
```

---

## 4. Fuentes de Datos Identificadas

### 4.1 MySQL — Tablas clave

| Tabla | Descripción | Campos relevantes |
|-------|-------------|-------------------|
| **`seo_browse_urls`** | Catálogo maestro acumulativo de todas las browse URLs registradas. Se actualiza vía **upsert** (nunca borra registros). Solo refleja el estado actual, no snapshots históricos. | `url`, `sector_slug`, `operation_slug`, `state_slug`, `corridor_slug`, `municipality_slug`, `settlement_slug`, `*_name` |
| **`seo_indexable_urls`** | URLs determinadas como indexables por ejecución. **Guarda historia:** cada ejecución diaria inserta registros nuevos con un `execution_id` distinto sin borrar los anteriores. Para obtener las más recientes se filtra por el último `execution_id`. | `url_path`, `execution_id`, `sector_slug`, `operation_slug`, `state_slug`, `corridor_slug`, `municipality_slug`, `settlement_slug`, `spots_count_at_evaluation`, `median_price_by_square_space_mxn` |
| **`seo_indexability_executions`** | Historial de ejecuciones del proceso de indexabilidad. **1 registro por ejecución diaria** (a las 5:00 AM). Funciona como log/audit trail de todas las generaciones. | `execution_id`, `job_run_id`, `started_at`, `completed_at`, `total_combinations_evaluated`, `indexable_urls_count`, `execution_time_seconds`, `created_at` |
| **`seo_ai_contents`** | Contenido AI generado para browse pages | `seo_browse_url_id`, `seo_copy_block`, `faqs`, `process_block`, `is_current` |
| **`analytics`** | Eventos de analítica interna (views de spots) | `event`, `origin` (1=admin, 2=tenant), `user_id`, `spot_id`, `spot_type`, `created_at` |
| **`clients`** | Datos de leads/prospectos con UTMs | `hs_utm_campaign`, `hs_utm_source`, `hs_utm_medium`, `origin`, `lead0_at`...`lead4_at`, `hs_first_conversion`, `hs_recent_conversion`, `hs_analytics_source`, `hs_analytics_source_data_1`, `hs_analytics_source_data_2`, `spot_type_id`, `client_state` |

### 4.2 OpenSearch — Índices clave

| Índice | Descripción | Campos relevantes para browse |
|--------|-------------|------------------------------|
| **`spots`** | Índice principal de propiedades | `address.slugs.state`, `address.slugs.municipality`, `address.slugs.settlement`, `zones[].slug`, `zones[].state.slug`, `type.id`, `prices.type`, `spot_state.id`, `score` |
| **`seo_interlinking`** | Datos de interlinking SEO | `url_path`, `source_data.sector_slug`, `source_data.operation_slug`, `source_data.state_slug`, `source_data.municipality_slug`, `source_data.settlement_slug`, `interlinking_links[]`, `geo_interlinking_links[]` |

### 4.3 Frontend — UTM Tracker

- **Archivo:** `resources/js/utm-tracker/src/utm-tracker.ts`
- **Cookie:** `utm_data` en dominio `.spot2.mx` (TTL: 1 día)
- **Parámetros capturados:** `utm_source`, `utm_medium`, `utm_campaign`, `utm_term`, `utm_content`
- **Intercepción fetch:** Añade automáticamente campos `hs_utm_source`, `hs_utm_medium`, `hs_utm_campaign`, `hs_utm_term`, `hs_utm_content` a requests POST/PUT/PATCH hacia `services.spot2.mx`
- **Flujo:** URL params → Cookie → Interceptor fetch → Backend → Tabla `clients` (campos `hs_utm_*`)

### 4.4 API Endpoints SEO relevantes

| Endpoint | Descripción |
|----------|-------------|
| `GET /m/v2/seo/check-indexability` | Verifica si una URL es indexable, retorna interlinking y AI content |
| `GET /m/v2/seo/spots-search` | Búsqueda de spots para browse pages SEO |
| `GET /m/v2/quadtree/resolve-slug/{slug}` | Resuelve un slug geográfico |

---

## 5. Contenido y Refresco de las Tablas SEO

### 5.1 ¿Qué almacenan estas tablas?

Las tablas `seo_indexable_urls` y `seo_browse_urls` guardan **URLs reales generadas** (ej. `/oficinas/renta/jalisco/guadalajara`), **no patrones ni templates**. Los patrones/reglas de indexabilidad se almacenan por separado en `seo_indexability_rules`.

El proceso de generación:
1. `GenerateSeoIndexableUrlsAction` consulta OpenSearch con agregaciones por sector/operación/estado/corredor/municipio/colonia
2. Para cada combinación que cumple las reglas de indexabilidad (min spots threshold), genera el URL path concatenando los slugs reales
3. Guarda la URL completa junto con los componentes descompuestos (`sector_slug`, `operation_slug`, `state_slug`, etc.)

### 5.2 Frecuencia de refresco y retención de historia

| Tabla | Frecuencia | Método | ¿Guarda historia? | Detalle |
|-------|-----------|--------|-------------------|----------|
| `seo_indexable_urls` | **Diario** (5:00 AM) | Insert con nuevo `execution_id` | **Sí** — cada ejecución queda como snapshot | No borra registros anteriores. Para las más recientes: filtrar por último `execution_id` |
| `seo_indexability_executions` | **Diario** (5:00 AM) | 1 registro por ejecución | **Sí** — log de todas las ejecuciones | Contiene métricas: `indexable_urls_count`, `execution_time_seconds` |
| `seo_browse_urls` | Bajo demanda (AI content) | Upsert por `url` (unique key) | **No snapshots** — solo estado actual | Nunca borra registros, solo actualiza slugs si cambian. Crece conforme aparecen nuevas URLs indexables |

### 5.3 Implicaciones para el análisis

- **Para obtener todas las URLs actuales de browse pages:** consultar `seo_browse_urls` directamente (catálogo acumulativo).
- **Para analizar evolución temporal del inventario de URLs:** usar `seo_indexable_urls` filtrando por `execution_id` (cada uno es un snapshot diario).
- **La regex de BigQuery sigue siendo útil** como fallback para capturar visitas a URLs que no estén en estas tablas (ej. combinaciones no indexables que aún así reciben tráfico).

---

## 6. Flujo de Datos: Visitante → Lead

```
1. Visitante llega a spot2.mx (con o sin UTMs)
   └─ UTM Tracker captura params → cookie utm_data

2. Visitante navega browse pages
   └─ URL sigue patrón: /{sector}/{operation}/{state}/...
   └─ Frontend consulta API: /m/v2/seo/spots-search?ty=X&pt=Y&ds=Z...
   └─ Se registran page views en BigQuery (GA4)

3. Visitante interactúa (contacto, WhatsApp, formulario)
   └─ UTM Tracker inyecta hs_utm_* en request body
   └─ Backend crea Client con UTMs
   └─ Lead progresa: L0 → L1 → L2 → L3 → L4

4. Datos disponibles para análisis:
   └─ BigQuery: page views con URL path (usar regex para filtrar browse)
   └─ MySQL clients: leads con UTMs y timestamps de cada nivel
   └─ MySQL seo_browse_urls: catálogo de URLs válidas
   └─ MySQL analytics: eventos internos por spot
   └─ OpenSearch spots: inventario de propiedades por ubicación
```

---

## 7. Recomendaciones para el Modelo de Conversión

### Para extraer patrones de browse pages desde BigQuery:

1. **Usar la tabla `seo_browse_urls`** de MySQL como fuente de verdad para todas las URLs válidas de browse pages. Esta tabla ya tiene las URLs decomponidas en sus componentes (sector, operación, estado, corredor, municipio, colonia).

2. **Aplicar la regex propuesta** (sección 3.3/3.4) sobre los page_path de BigQuery para filtrar solo tráfico de browse pages.

3. **Cruzar con datos de leads** de la tabla `clients` usando:
   - `hs_utm_source` / `hs_utm_medium` / `hs_utm_campaign` para atribución
   - `hs_first_conversion` y `hs_recent_conversion` para identificar la landing page de conversión
   - `hs_analytics_source_data_1` y `hs_analytics_source_data_2` para datos de fuente analítica
   - `spot_type_id` para saber el sector de interés del lead

4. **Variables sugeridas para el modelo:**
   - Sector visitado (extraer de URL)
   - Operación (renta/venta, extraer de URL)
   - Granularidad geográfica (nivel de profundidad en la URL)
   - Fuente de tráfico (UTMs)
   - Cantidad de spots disponibles en esa browse page (de `seo_indexable_urls.spots_count_at_evaluation`)
   - Precio mediano de la zona (de `seo_indexable_urls.median_price_by_square_space_mxn`)
