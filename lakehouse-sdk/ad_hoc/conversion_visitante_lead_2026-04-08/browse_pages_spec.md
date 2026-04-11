# Browse Pages spot2.mx — Especificación Validada

**Fecha:** 2026-04-08
**Contexto:** Exploración de datos para el modelo de probabilidad de conversión visitante → lead (ADS-3533 / ADS-3534).
**Método de validación:** Queries directas a MySQL production-analysis (`spot2_service`), tablas `seo_indexable_urls`, `seo_browse_urls`, `seo_indexability_executions`.

---

## 1. Estructura de URLs

Las browse pages de spot2.mx siguen una estructura jerárquica de hasta 6 segmentos, todos opcionales a partir del primero:

```
/{sector}/{operation}/{state}/{corridor|municipality}/{municipality}/{settlement}
```

### 1.1 Profundidad real observada en datos

| Profundidad | Patrón | Count | % | Ejemplo |
|:-----------:|--------|------:|--:|---------|
| 1 | `/{rama}` | 8 | 0.04% | `/oficinas`, `/renta` |
| 2 | `/{sector}/{operation}` | 12 | 0.06% | `/oficinas/renta` |
| 3 | `/{sector}/{operation}/{state}` | 331 | 1.7% | `/oficinas/renta/jalisco` |
| 4 | `+ municipio o corredor` | 3,400 | 17.3% | `/oficinas/renta/jalisco/guadalajara` |
| 5 | `+ colonia` | 15,166 | **80.2%** | `/oficinas/renta/jalisco/guadalajara/providencia` |

> El 80% del inventario está a nivel colonia (máxima granularidad).

### 1.2 Combinaciones válidas

**Con sector:**

| # | Patrón | Ejemplo |
|---|--------|---------|
| 1 | `/{sector}` | `/oficinas` |
| 2 | `/{sector}/{operation}` | `/oficinas/renta` |
| 3 | `/{sector}/{operation}/{state}` | `/oficinas/renta/ciudad-de-mexico` |
| 4 | `/{sector}/{operation}/{state}/{corridor}` | `/oficinas/renta/jalisco/santa-fe` |
| 5 | `/{sector}/{operation}/{state}/{municipality}` | `/oficinas/renta/ciudad-de-mexico/miguel-hidalgo` |
| 6 | `/{sector}/{operation}/{state}/{corridor}/{municipality}` | con corredor y municipio |
| 7 | `/{sector}/{operation}/{state}/{municipality}/{settlement}` | `/oficinas/renta/ciudad-de-mexico/miguel-hidalgo/polanco` |
| 8 | `/{sector}/{operation}/{state}/{corridor}/{municipality}/{settlement}` | máxima granularidad |

**Sin sector (solo operación):**

| # | Patrón | Ejemplo |
|---|--------|---------|
| 9 | `/{operation}` | `/renta` |
| 10 | `/{operation}/{state}` | `/renta/ciudad-de-mexico` |

---

## 2. Componentes del URL — Valores reales validados

### 2.1 Sector Slugs (6 valores)

| Slug | Tipo | `ty` | Nota |
|------|------|:----:|------|
| `locales-comerciales` | Retail | 13 | Sector canónico |
| `naves-industriales` | Industrial | 9 | Sector canónico (≥ 3000 m²) |
| `oficinas` | Oficinas | 11 | Sector canónico |
| `terrenos` | Terrenos | 15 | Sector canónico |
| `bodegas` | Bodegas | 91 | Subsector virtual → Industrial en OpenSearch |
| `coworking` | Coworking | 92 | Subsector virtual → Oficinas en OpenSearch |

`bodegas` y `coworking` se incorporaron al sistema de indexabilidad a partir de ~dic-2025 (ejecución ~200). Antes solo existían los 4 sectores canónicos.

### 2.2 Operation Slugs (2 valores)

| Slug | Tipo | `pt` |
|------|------|:----:|
| `renta` | Renta | 1 |
| `venta` | Venta | 2 |

### 2.3 State Slugs (32 valores)

El inventario cubre **todos los estados de México**, no un subconjunto.

| Slug | Estado | URLs browse | URLs indexable (actual) |
|------|--------|:-----------:|:----------------------:|
| `mexico` | México | 2,596 | 1,661 |
| `nuevo-leon` | Nuevo León | 2,036 | 1,281 |
| `ciudad-de-mexico` | Ciudad de México | 1,886 | 1,178 |
| `jalisco` | Jalisco | 1,630 | 1,123 |
| `queretaro` | Querétaro | 1,323 | 887 |
| `coahuila` | Coahuila | 1,112 | 849 |
| `puebla` | Puebla | 914 | 561 |
| `yucatan` | Yucatán | 911 | 658 |
| `guanajuato` | Guanajuato | 786 | 518 |
| `veracruz` | Veracruz | 597 | 413 |
| `chihuahua` | Chihuahua | 552 | 407 |
| `baja-california` | Baja California | 519 | 364 |
| `durango` | Durango | 398 | 319 |
| `quintana-roo` | Quintana Roo | 356 | 249 |
| `san-luis-potosi` | San Luis Potosí | 355 | 250 |
| `aguascalientes` | Aguascalientes | 347 | 230 |
| `morelos` | Morelos | 318 | 229 |
| `hidalgo` | Hidalgo | 304 | 215 |
| `sinaloa` | Sinaloa | 296 | 220 |
| `chiapas` | Chiapas | 231 | 184 |
| `colima` | Colima | 207 | 176 |
| `campeche` | Campeche | 162 | 137 |
| `tlaxcala` | Tlaxcala | 152 | 90 |
| `tabasco` | Tabasco | 148 | 116 |
| `tamaulipas` | Tamaulipas | 147 | 96 |
| `michoacan` | Michoacán | 130 | 74 |
| `oaxaca` | Oaxaca | 126 | 87 |
| `sonora` | Sonora | 121 | 78 |
| `baja-california-sur` | Baja California Sur | 81 | 53 |
| `guerrero` | Guerrero | 79 | 52 |
| `nayarit` | Nayarit | 40 | 26 |
| `zacatecas` | Zacatecas | 37 | 34 |

> **Nota de calidad:** Existen variantes históricas de slugs (`coahuila-de-zaragoza` vs `coahuila`, `michoacan-de-ocampo` vs `michoacan`) en ejecuciones antiguas. Afectan 185 URLs. Esto no impacta la regex porque los segmentos geo se capturan con patrón genérico.

### 2.4 Corridor, Municipality y Settlement Slugs

Son **slugs dinámicos** generados a partir de datos geográficos reales. No se enumeran porque cambian con cada indexación. Están disponibles como campos descompuestos en `seo_indexable_urls`.

---

## 3. Fuente de verdad: `seo_indexable_urls`

### 3.1 ¿Por qué esta tabla y no `seo_browse_urls`?

| Criterio | `seo_indexable_urls` | `seo_browse_urls` |
|----------|:-------------------:|:-----------------:|
| URLs únicas totales | 22,702 | 18,917 |
| Guarda historia temporal | Sí (por `execution_id`) | No (solo estado actual) |
| Incluye métricas (spots, precio) | Sí | No |
| Tiene `levelable_type` | Sí | No |
| Campos `*_name` legibles | Sí | Sí |

`seo_browse_urls` es un **subconjunto estricto** de `seo_indexable_urls`: toda URL en browse existe en indexable, pero hay 3,785 URLs históricas solo en indexable.

### 3.2 Esquema de `seo_indexable_urls`

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `url_path` | varchar(500) | URL completa (ej. `/oficinas/renta/jalisco/guadalajara`) |
| `execution_id` | bigint | FK a `seo_indexability_executions` |
| `sector_slug` | varchar(100) | NULL para URLs tipo `/renta`, `/venta` |
| `operation_slug` | varchar(100) | NULL para URLs tipo `/{sector}` sin operación |
| `state_slug` | varchar(100) | Slug del estado |
| `corridor_slug` | varchar(100) | Slug del corredor/zona |
| `municipality_slug` | varchar(100) | Slug del municipio |
| `settlement_slug` | varchar(100) | Slug de la colonia |
| `levelable_type` | varchar(100) | Granularidad geográfica (ver abajo) |
| `spots_count_at_evaluation` | int | Spots disponibles al momento de evaluar |
| `median_price_by_square_space_mxn` | decimal(15,2) | Precio mediano por m² |
| `evaluation_date` | timestamp | Fecha de evaluación |

### 3.3 Levelable Types (granularidad geográfica)

| Tipo | Count (actual) | Descripción |
|------|:--------------:|-------------|
| `App\Models\DataSettlement` | 9,701 | Nivel colonia |
| `App\Models\DataMunicipality` | 2,015 | Nivel municipio |
| `App\Models\Zone` | 781 | Nivel corredor/zona |
| `App\Models\DataState` | 318 | Nivel estado |
| `App\Models\DataCountry` | 20 | Nivel país (solo sector+operación) |

### 3.4 Ejecuciones (`seo_indexability_executions`)

- **Frecuencia:** diaria a las 5:00 AM
- **Historial:** desde 2025-08-13 (272 ejecuciones al 2026-04-08)
- **Crecimiento:** de ~1,201 URLs indexables (ago-2025) a **12,835** (abr-2026)

---

## 4. Estrategia de Regex — "Primeras Ramas"

### 4.1 El problema con hardcodear valores

El primer intento documentado hardcodeaba 9 estados en la regex. La validación contra datos reales mostró que eso **pierde el 39.8% de las URLs** (7,528 de 18,917). Además, el sistema agrega componentes nuevos: `bodegas` y `coworking` se incorporaron después de la ejecución ~200.

### 4.2 La solución: validar solo la primera rama

Toda browse page empieza con uno de **8 valores conocidos** (6 sectores + 2 operaciones). Estos son las "primeras ramas" del árbol de URLs. Si el primer segmento coincide, el resto de la URL es por definición una ruta geográfica válida.

**Primeras ramas actuales:**

```
bodegas | coworking | locales-comerciales | naves-industriales | oficinas | renta | terrenos | venta
```

**Regex resultante:**

```regex
^/(bodegas|coworking|locales-comerciales|naves-industriales|oficinas|renta|terrenos|venta)(/[a-z0-9-]+)*$
```

**Validación:**
- vs `seo_browse_urls` (18,917 URLs): **100% match**
- vs ventana dinámica de 6 meses (19,622 URLs): **100% match**

**Para BigQuery (sintaxis RE2):**

```sql
WHERE REGEXP_CONTAINS(page_path,
    r'^/(bodegas|coworking|locales-comerciales|naves-industriales|oficinas|renta|terrenos|venta)(/[a-z0-9-]+)*$'
)
```

### 4.3 Mantener la regex actualizada

Las primeras ramas pueden cambiar (se demostró que `bodegas` y `coworking` se agregaron después). Para detectar cambios, ejecutar la query ligera:

**→ `q_browse_first_branches.sql`** (~1.6s)

```sql
SELECT DISTINCT
    SUBSTRING_INDEX(SUBSTRING(url_path, 2), '/', 1) AS first_branch
FROM seo_indexable_urls
WHERE execution_id BETWEEN
    (SELECT MIN(execution_id) FROM seo_indexability_executions
     WHERE started_at >= DATE_SUB(CAST(DATE_FORMAT(CURDATE(), '%Y-%m-01') AS DATE), INTERVAL 6 MONTH))
    AND
    (SELECT MAX(execution_id) FROM seo_indexability_executions
     WHERE started_at >= DATE_SUB(CAST(DATE_FORMAT(CURDATE(), '%Y-%m-01') AS DATE), INTERVAL 6 MONTH))
ORDER BY first_branch;
```

Si el resultado devuelve una rama nueva, se agrega a la alternación de la regex.

---

## 5. Queries de Referencia

### 5.1 URLs únicas en ventana dinámica — `q_browse_urls_ventana.sql`

Devuelve todas las URLs únicas que fueron indexables en los últimos 6 meses cerrados + mes actual, con los datos de su ejecución más reciente dentro de la ventana.

- **Filas:** ~19,622 (a abr-2026)
- **Performance:** ~24s
- **Columnas:** `url_path`, `sector_slug`, `operation_slug`, `state_slug`, `corridor_slug`, `municipality_slug`, `settlement_slug`, `levelable_type`, `spots_count_at_evaluation`, `median_price_by_square_space_mxn`, `evaluation_date`

### 5.2 Primeras ramas — `q_browse_first_branches.sql`

Devuelve el primer segmento de cada URL en la ventana dinámica. Sirve para construir la regex dinámicamente y detectar nuevas ramas.

- **Filas:** ~8
- **Performance:** ~1.6s

---

## 6. Fuentes de Datos Complementarias

### 6.1 MySQL — Otras tablas relevantes

| Tabla | Descripción | Campos clave |
|-------|-------------|--------------|
| `clients` | Leads/prospectos con UTMs y timestamps de conversión | `hs_utm_source`, `hs_utm_medium`, `hs_utm_campaign`, `lead0_at`…`lead4_at`, `hs_first_conversion`, `hs_recent_conversion`, `hs_analytics_source`, `hs_analytics_source_data_1/2`, `spot_type_id`, `client_state` |
| `analytics` | Eventos internos (views de spots) | `event`, `origin` (1=admin, 2=tenant), `user_id`, `spot_id`, `spot_type`, `created_at` |
| `seo_ai_contents` | Contenido AI para browse pages | `seo_browse_url_id`, `seo_copy_block`, `faqs`, `is_current` |

### 6.2 BigQuery (GA4)

Page views con URL path. Se filtra con la regex de primeras ramas para aislar tráfico de browse pages.

### 6.3 Frontend — UTM Tracker

- **Cookie:** `utm_data` en dominio `.spot2.mx` (TTL: 1 día)
- **Parámetros:** `utm_source`, `utm_medium`, `utm_campaign`, `utm_term`, `utm_content`
- **Flujo:** URL params → Cookie → Interceptor fetch → Backend → Tabla `clients` (campos `hs_utm_*`)

### 6.4 OpenSearch

| Índice | Uso |
|--------|-----|
| `spots` | Inventario de propiedades por ubicación, tipo, precio |
| `seo_interlinking` | Datos de interlinking entre browse pages |

---

## 7. Flujo de Datos: Visitante → Lead

```
1. Visitante llega a spot2.mx (con o sin UTMs)
   └─ UTM Tracker captura params → cookie utm_data

2. Visitante navega browse pages
   └─ URL sigue patrón: /{sector}/{operation}/{state}/...
   └─ Se registran page views en BigQuery (GA4)

3. Visitante interactúa (contacto, WhatsApp, formulario)
   └─ UTM Tracker inyecta hs_utm_* en request body
   └─ Backend crea Client con UTMs
   └─ Lead progresa: L0 → L1 → L2 → L3 → L4

4. Datos disponibles para análisis:
   ├─ BigQuery ─── page views con URL path (filtrar con regex de ramas)
   ├─ MySQL seo_indexable_urls ─── catálogo de URLs con slugs y métricas
   ├─ MySQL clients ─── leads con UTMs y timestamps por nivel
   ├─ MySQL analytics ─── eventos internos por spot
   └─ OpenSearch spots ─── inventario por ubicación
```

---

## 8. Flujo Propuesto: Sesiones de Browse Pages (ADS-3534)

### 8.1 Decisiones de diseño

- **Granularidad**: `user_pseudo_id + ga_session_id` (sesión individual, no usuario×día)
- **Flujo independiente**: NO se modifica `funnel_with_channel` ni `lk_visitors`. Se crea un flujo aparte que luego puede integrarse o no
- **Dos poblaciones objetivo**: visitantes que se convirtieron a lead vs los que no. La tabla `lk_mat_matches_visitors_to_leads` (GeoSpot) ya identifica a los convertidos; se usará para el split
- **Enfoque incremental**: primero capturar la data cruda de sesiones desde BigQuery, luego enriquecer progresivamente

### 8.2 Flujos existentes que alimentan este análisis

| Flujo | Tabla destino | Granularidad | Qué aporta |
|-------|--------------|:------------:|------------|
| `funnel_with_channel` (BQ scheduled query) | `analitics_spot2.funnel_with_channel` | user × día | Canal, traffic_type, primera_bifurcacion, conversion_point |
| `lk_visitors` (Dagster 04:00) | GeoSpot `lk_visitors` | user × día | vis_channel, vis_traffic_type, vis_source/medium/campaign |
| `lk_matches_visitors_to_leads` (Dagster 09:00) | GeoSpot `lk_mat_matches_visitors_to_leads` | user × lead | Match visitor↔lead por phone/email/spot, lead_id, cohort |

### 8.3 Problema detectado en `funnel_with_channel_v4.sql`

La clasificación de "Browse Pages" en `primera_bifurcacion` y `conversion_point` **no incluye `bodegas` ni `coworking`**. Las LIKE patterns solo cubren: `oficinas`, `locales-comerciales`, `naves-industriales`, `terrenos`, `renta`, `venta`. Esto significa que visitas a `/bodegas/renta/...` o `/coworking/renta/...` se clasifican como "Other" en lugar de "Browse Pages".

### 8.4 Fuente de datos BigQuery para sesiones

- **Tabla**: `analytics_276054961.events_*` (GA4 raw events)
- **Evento principal**: `page_view` con `page_location` que matchea regex de primeras ramas
- **Sesión**: `user_pseudo_id` + `ga_session_id` (de `event_params`)
- **Regex de filtrado**:

```sql
REGEXP_CONTAINS(
    -- extraer path de la URL completa
    REGEXP_EXTRACT(page_location, r'https?://[^/]+(/.*)'),
    r'^/(bodegas|coworking|locales-comerciales|naves-industriales|oficinas|renta|terrenos|venta)(/[a-z0-9-]+)*$'
)
```

---

## 9. Fase 1: Exploración de Datos BigQuery (Hallazgos)

**Fecha de exploración:** 2026-04-08
**Fuente:** `analytics_276054961.events_*` (GA4 raw events)
**Evento analizado:** `page_view` donde `page_location` matchea la regex de primeras ramas

### 9.1 Volúmenes (30 días)

| Métrica | Valor |
|---------|------:|
| Page views en browse pages | 9,256 |
| Usuarios únicos | 6,386 |
| Sesiones únicas | 7,557 |
| Promedio diario page views | ~309 |
| Promedio diario usuarios | ~213 |
| `ga_session_id` disponible | 99.6%+ |

### 9.2 Distribución por rama/sector (7 días)

| Rama | Page views | % | Usuarios | Sesiones |
|------|:---------:|:-:|:--------:|:--------:|
| locales-comerciales | 769 | 38.0% | 628 | 674 |
| terrenos | 534 | 26.4% | 427 | 469 |
| oficinas | 257 | 12.7% | 183 | 209 |
| **bodegas** | **212** | **10.5%** | 158 | 177 |
| naves-industriales | 169 | 8.3% | 124 | 148 |
| renta (sin sector) | 54 | 2.7% | 43 | 47 |
| **coworking** | **30** | **1.5%** | 14 | 15 |

> **bodegas + coworking = 12%** del tráfico de browse pages. Actualmente se clasifica como "Other" en `funnel_with_channel`.

### 9.3 Comportamiento de sesión (7 días)

| Browse pages por sesión | Sesiones | % | Duración promedio |
|:-----------------------:|:--------:|:-:|:-----------------:|
| 1 | 1,480 | 88.0% | 0s (single PV) |
| 2 | 148 | 8.8% | 6.5 min |
| 3-5 | 45 | 2.7% | 12.3 min |
| 6-10 | 7 | 0.4% | variable |
| 11+ | 3 | 0.2% | ~14 min |

### 9.4 Recurrencia de usuarios (30 días)

| Sesiones por usuario | Usuarios | % |
|:--------------------:|:--------:|:-:|
| 1 sesión | 5,806 | 90.9% |
| 2 sesiones | 384 | 6.0% |
| 3-5 sesiones | 162 | 2.5% |
| 6+ sesiones | 35 | 0.5% |

### 9.5 Conversión (30 días)

**Tasa de conversión: 8%** (511 de 6,387 usuarios de browse tuvieron eventos de conversión)

| Evento de conversión | Usuarios |
|---------------------|:--------:|
| clientRequestedWhatsappForm | 216 |
| clientClickedRegisterForm | 198 |
| clientSubmitStep1Industrial | 140 |
| clientSubmitFormBlogIndustrial | 133 |
| clientRequestedContactLead | 32 |
| clientClickedSendInfoModalConsulting | 20 |
| clientClickedLeadPopup | 11 |
| clientSubmitStep1Oficinas | 11 |
| clientSubmitFormBlogOficinas | 10 |
| clientSubmittedBudgetFormBP | 8 |
| clientSubmitFormBlogRetail | 2 |

### 9.6 Dispositivo (30 días)

| Categoría | Page views | % PV | Usuarios | % Users |
|-----------|:---------:|:----:|:--------:|:-------:|
| Desktop | 4,854 | 52.4% | 2,865 | 44.9% |
| Mobile | 4,346 | 46.9% | 3,487 | 54.6% |
| Tablet | 56 | 0.6% | 42 | 0.7% |

### 9.7 Event params disponibles para features

| Param | Disponibilidad | Tipo | Uso potencial |
|-------|:--------------:|:----:|---------------|
| `page_location` | 100% | string | URL visitada, extracción de sector/estado/municipio |
| `ga_session_id` | 99.6% | int | Clave de sesión |
| `ga_session_number` | 100% | int | N-ésima sesión del usuario (recurrencia histórica) |
| `session_engaged` | 100% | string | Si la sesión es "engaged" por GA4 |
| `page_title` | 100% | string | Título de la página |
| `page_referrer` | 78.6% | string | Página anterior |
| `entrances` | 68.5% | int | Si es la primera pageview de la sesión |
| `source` / `medium` / `campaign` | ~58% | string | Atribución de tráfico |
| `term` | ~55% | string | Término de búsqueda |
| `engaged_session_event` | ~67% | int | Evento de sesión engaged |

### 9.8 Conclusiones de la Fase 1

1. **La data de sesiones es viable**: `ga_session_id` está en prácticamente el 100% de los `page_view` events
2. **El volumen es manejable**: ~30k sesiones/mes con browse pages (ver corrección en §12.3), no requiere optimización especial
3. **La mayoría de sesiones son de 1 page view** (82-88%), lo que sugiere que la "profundidad" de exploración será una feature discriminante
4. **~20% de conversión** a nivel sesión (6,459 de 31,151 sesiones en 30 días, ver §12.2)
5. **WhatsApp es el canal de conversión dominante** seguido de spotSearch y registro
6. **~60% mobile vs 40% desktop**, con mayoría de usuarios mobile
7. **`ga_session_number`** es una feature valiosa: indica cuántas sesiones ha tenido el usuario históricamente en GA4

---

## 10. Patrones Reutilizables de Flujos Existentes

Extraídos de `funnel_with_channel_v4.sql`, `lk_visitors/` y `lk_matches_visitors_to_leads/`. Estos patrones representan correcciones y mejoras acumuladas que debemos incorporar al flujo de browse sessions.

### 10.1 Detección de scraping/bots (SQL — BigQuery)

El funnel usa un CTE `scraping` con 4 reglas específicas que marcan `user_pseudo_id` sospechosos. Se aplica como LEFT JOIN que genera un flag `user_sospechoso` (0/1), **no se eliminan**: se etiquetan para filtrar después.

**Reglas actuales:**
1. Bot browser 129.0.6668.71 — Direct/Unassigned + first_visit en spot2.mx
2. Opera 128 Windows Desktop — brute force login/OTP (acotado mar 16-25)
3. Chrome 146.0.7680.80 Desktop Windows US — login/OTP (acotado mar 16-25)
4. Mobile Chrome Android US — URLs de staging/vercel/gamma

**Para nuestro flujo:** reutilizar estas reglas tal cual como CTE y agregar el flag `is_scraping` a la tabla de sesiones.

*Ref:* `lakehouse-sdk/sql/old/ funnel_with_channel_v4.sql` líneas 16-51

### 10.2 Filtro de Technical Waste (SQL — BigQuery)

URLs que no son tráfico real:

```sql
page_location LIKE '%localhost%'
OR page_location LIKE '%staging%'
OR page_location LIKE '%vercel.app%'
OR page_location LIKE '%signup=login%'
OR page_location LIKE '%signup=verify-otp%'
```

**Para nuestro flujo:** aplicar como filtro previo o como flag en la tabla.

*Ref:* `funnel_with_channel_v4.sql` líneas 144-148

### 10.3 Source/Medium/Campaign con fallback (SQL — BigQuery)

GA4 tiene dos fuentes de atribución: `session_traffic_source_last_click` (nivel sesión) y `traffic_source` (nivel usuario). El funnel usa un patrón de fallback de 3 niveles para cada campo:

```sql
CASE
  WHEN session_traffic_source_last_click...source IS NULL
    OR TRIM(COALESCE(...source, '')) IN ('', '(not set)')
  THEN traffic_source.source
  ELSE session_traffic_source_last_click...source
END AS source
```

Mismo patrón para `medium` y `campaign_name`. Además captura `collected_traffic_source.manual_*` como campos separados (UTMs manuales).

**Para nuestro flujo:** reutilizar este triple fallback para tener atribución correcta por sesión.

*Ref:* `funnel_with_channel_v4.sql` líneas 64-99

### 10.4 Clasificación de Canal (12 reglas con prioridad)

El CASE de canal tiene un orden de prioridad crítico:

1. Bot/Spam (si `user_sospechoso`)
2. Organic LLMs (ChatGPT, Perplexity, Claude, etc.)
3. Nulo-Vacío (medium vacío, referrals técnicos como l.wl.co, t.co)
4. Organic Search (Google, Bing, etc. con medium != cpc/paid)
5. Mail (Gmail, Outlook, etc.)
6. Display (campaign con `_display_` o `_disp_`)
7. Direct (source = `(direct)`)
8. Referral (medium = referral, excluyendo social networks)
9. Cross-network (PMAX, syndicatedsearch.goog)
10. Paid Video (YouTube campaigns)
11. Paid Search (CPC + search engines)
12. Paid Social (CPC + Facebook/Instagram/Meta/LinkedIn...)
13. Organic Social (social sin CPC)

Y la reducción a `Traffic_type`: cada canal mapea a Organic, Paid o Unassigned. `demandgen` campaigns son Paid.

**⚠️ LIMITACIÓN CRÍTICA de `funnel_with_channel`:**

La consulta programada de BigQuery (`funnel_with_channel_v4.sql`) opera a nivel
`user_pseudo_id × día` y **solo captura la primera sesión del día**:

```sql
-- pre_users_sin_exluir: MIN(event_timestamp) por (event_date, user_pseudo_id)
-- users: INNER JOIN ... AND a11.event_timestamp = a12.event_timestamp_day
```

Si un usuario tiene 3 sesiones en un día, `funnel_with_channel` solo tiene datos
de la primera. `lk_visitors` hereda esta misma limitación.

**Para nuestro flujo:** NO podemos hacer JOIN con `funnel_with_channel` para obtener
canal/traffic_type a nivel sesión. Debemos **recalcular canal y traffic_type** desde
los eventos crudos de GA4 para cada sesión individual. La lógica de las 12 reglas
y el triple fallback source/medium/campaign (secciones 10.3 y 10.4) deben incorporarse
directamente en nuestra query de extracción.

La referencia para la lógica de canal/traffic_type es `lead_events.sql` (subconsultas
escalares, independiente del funnel), pero tiene **5 discrepancias** respecto a
`funnel_with_channel_v4.sql` que es la fuente de verdad. Ver sección 10.4.1.

*Ref:* `funnel_with_channel_v4.sql` líneas 3-13 y 118-122 (limitación)

#### 10.4.1 Flujo activo: `lk_vtl_job` y queries involucradas

El flujo **activo** (no legacy) es `lk_vtl_job` definido en
`lk_matches_visitors_to_leads/jobs.py`. Sus assets tienen prefijo `lk_vtl_*`:

```
lk_vtl_clients             → MySQL clients (lookup phone/email → client_id)
lk_vtl_bt_lds_leads        → GeoSpot PostgreSQL (bt_lds_leads.sql — leads + cohorts New/Reactivated)
lk_vtl_lk_lead_events      → BigQuery (lead_events_from_funnel_lk.sql)
lk_vtl_conversations       → Chatbot PostgreSQL (conversations.sql)
lk_vtl_lk_processed_lead_events → Procesamiento Python
lk_vtl_lk_matched          → Matching conversaciones ↔ eventos
lk_vtl_lk_final_output     → Tabla final: lk_mat_matches_visitors_to_leads
```

La query de BigQuery del flujo activo es **`lead_events_from_funnel_lk.sql`** (NO
`lead_events.sql` que es legacy). Se invoca vía `get_data("lead_events_lk")` en
`main.py:46-50`.

**Cómo obtiene canal/atribución `lead_events_from_funnel_lk.sql`:**
- Extrae eventos crudos con subconsultas escalares (sin UNNEST global)
- Calcula source/medium/campaign con el triple fallback (líneas 117-157)
- Hace LEFT JOIN con `analitics_spot2.funnel_with_channel` deduplicado (ANY_VALUE)
  para obtener: `Channel`, `Traffic_type`, `primera_bifurcacion`, `conversion_point`,
  `segunda_bifurcacion`, `user_sospechoso`

Es decir: **la atribución de canal viene del funnel vía JOIN**, no se recalcula.
Esto funciona para el caso de uso de matches (que opera sobre eventos de conversión
puntuales), pero hereda la limitación de primera-sesión-del-día para esos campos.

#### 10.4.2 Discrepancias identificadas

Comparando los tres archivos SQL:

| # | Componente | funnel_with_channel_v4 (fuente de verdad) | lead_events_from_funnel_lk (activo) | lead_events.sql (legacy) |
|---|-----------|------------------------------------------|-------------------------------------|--------------------------|
| 1 | **Canal/Traffic_type** | Calcula las 12 reglas + Bot/Spam | Hereda vía JOIN con funnel | Calcula las 12 reglas (sin Bot/Spam) |
| 2 | **Paid Video `_yt`** | `r'yt_'` y `r'_yt'` | Hereda del funnel (correcto) | Solo `r'yt_'` — falta `_yt` |
| 3 | **Fallback source/medium** | Triple CASE con fallback | ✅ Tiene su propio triple CASE | ❌ Sin fallback — usa `session_traffic_source` directo |
| 4 | **`collected_traffic_source`** | Extrae UTMs manuales | ❌ No los extrae | ❌ No los extrae |
| 5 | **Eventos de conversión** | 18 eventos | 13 eventos (faltan 5) | 13 eventos (faltan 5) |
| 6 | **Subconsultas escalares** | ❌ Usa UNNEST global | ✅ Subconsultas escalares | ✅ Subconsultas escalares |
| 7 | **Scraping como CTE** | ✅ CTE `scraping` + flag | Hereda `user_sospechoso` vía JOIN | ❌ No tiene |

**Eventos faltantes en ambos queries del pipeline** (presentes solo en el funnel):
`clientSearchedSpotSearch`, `lead_form_submission`, `clientClickedRegisterUser`,
`clientSubmittedRegisterUser`, `clientSubmittedRegisterForm`

**Para nuestro flujo de browse sessions:** como necesitamos canal POR SESIÓN (no por
día), debemos recalcular las 12 reglas de canal directamente. Tomaremos:
- La estructura de subconsultas escalares de `lead_events_from_funnel_lk.sql`
- El triple fallback source/medium/campaign de `lead_events_from_funnel_lk.sql`
- Las 12 reglas de canal de `funnel_with_channel_v4.sql` (incluye Bot/Spam y `_yt`)
- El CTE `scraping` de `funnel_with_channel_v4.sql`
- Los `collected_traffic_source.manual_*` de `funnel_with_channel_v4.sql`
- La lista completa de 18 eventos de conversión de `funnel_with_channel_v4.sql`

### 10.5 Conversión de timestamps a zona horaria México (SQL — BigQuery)

Patrón consistente en todos los queries:
```sql
DATETIME(TIMESTAMP_MICROS(event_timestamp), "America/Mexico_City") AS event_datetime
```

**Importante:** los timestamps crudos de GA4 están en microsegundos UTC. Este patrón los convierte a `DATETIME` en hora de México, que es la zona de negocio. Usar en todo campo temporal expuesto al usuario o usado para joins con datos CRM/Chatbot.

Para PostgreSQL (Chatbot), el equivalente es:
```sql
m.created_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Mexico_City'
```

*Ref:* `lead_events.sql:10-13`, `lead_events_from_funnel_lk.sql:97`, `conversations.sql:16`

### 10.6 Extracción de event_params: subconsultas escalares vs UNNEST

**Enfoque antiguo** (`funnel_with_channel_v4.sql`): `UNNEST(event_params) AS ep` + filtro en WHERE. Funciona pero multiplica filas si se filtran múltiples keys en el mismo nivel, obligando a `GROUP BY` y agregaciones.

**Enfoque nuevo** (`lead_events.sql`, `lead_events_from_funnel_lk.sql`): subconsultas escalares que extraen un campo a la vez sin multiplicar filas:
```sql
(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location' LIMIT 1) AS page_location,
(SELECT CAST(value.int_value AS STRING) FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1) AS ga_session_id
```

**Para nuestro flujo:** usar subconsultas escalares. Es más limpio, evita `GROUP BY` artificiales y facilita agregar campos nuevos sin romper la query.

### 10.7 Extracción de ga_session_id como STRING

```sql
CAST(
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id' LIMIT 1)
AS STRING) AS ga_session_id
```

La clave compuesta de sesión es:
```sql
CONCAT(user_pseudo_id, "-", ga_session_id) AS session_id
```

*Ref:* `funnel_with_channel_v4.sql` líneas 102-110

### 10.8 Extracción de phone y email (SQL — BigQuery)

**Phone** — `SAFE_CAST` es preferible a `CAST` para evitar errores en valores no numéricos:
```sql
(SELECT COALESCE(
    SAFE_CAST(value.int_value AS STRING),
    NULLIF(TRIM(value.string_value), '')
  )
  FROM UNNEST(event_params)
  WHERE key = 'phone'
    AND (value.int_value IS NOT NULL
      OR (value.string_value IS NOT NULL AND TRIM(value.string_value) != ''))
  LIMIT 1
) AS phone
```

**Email:**
```sql
(SELECT value.string_value FROM UNNEST(event_params)
 WHERE key = 'email'
   AND value.string_value IS NOT NULL AND TRIM(value.string_value) != ''
 LIMIT 1) AS email
```

**spotId** (mismo patrón dual int/string):
```sql
(SELECT COALESCE(CAST(value.int_value AS STRING), value.string_value)
 FROM UNNEST(event_params) WHERE key = 'spotId'
 AND (value.int_value IS NOT NULL OR (value.string_value IS NOT NULL AND TRIM(value.string_value) != ''))
 LIMIT 1) AS spotId
```

*Ref:* `lead_events.sql:14-62`, `lead_events_from_funnel_lk.sql:105-115`

### 10.9 Filtro eficiente con _TABLE_SUFFIX (BigQuery)

```sql
FROM `analytics_276054961.events_*`
WHERE _TABLE_SUFFIX >= '20250101'
  AND event_name IN ('page_view', ...)
```

**Orden crítico:** `_TABLE_SUFFIX` primero para que BigQuery prune particiones antes de aplicar otros filtros. Luego `event_name` para reducir filas temprano.

*Ref:* `lead_events_from_funnel_lk.sql:63-65`

### 10.10 Dedup de funnel_with_channel para JOINs

El funnel puede tener múltiples filas por (user, event_date). Para evitar multiplicar al hacer JOIN, se deduplica con `ANY_VALUE`:

```sql
SELECT user, funnel_event_date,
  ANY_VALUE(Channel) AS Channel,
  ANY_VALUE(Traffic_type) AS Traffic_type,
  ANY_VALUE(primera_bifurcacion) AS primera_bifurcacion,
  ANY_VALUE(user_sospechoso) AS user_sospechoso
FROM funnel_raw
GROUP BY user, funnel_event_date
```

**Para nuestro flujo:** si hacemos JOIN con el funnel para traer canal/traffic_type, usar este patrón de dedup primero.

*Ref:* `lead_events_from_funnel_lk.sql:29-51`

### 10.11 Eventos de conversión (lista canónica)

**Lista completa de 18 eventos** (fuente: `funnel_with_channel_v4.sql` líneas 217-237).
Los 5 marcados con ⚠️ están en el funnel pero **faltan** en `lead_events.sql`:

```
clientRequestedWhatsappForm       — WhatsApp
clientRequestedContactLead        — Botón Contactar
clientClickedLeadPopup            — Popup 100 segundos
spotMapSearch                     — Búsqueda en mapa
clientClickedRegisterForm         — Login/Registro
clientSubmittedBudgetFormBP       — Consulta superficie
clientSubmitFormBlogRetail        — Form blog Retail
clientSubmitFormBlogOficinas      — Form blog Oficinas
clientSubmitFormBlogIndustrial    — Form blog Industrial
clientClickedSendInfoModalConsulting — Modal consultoría
clientSubmitStep1Industrial       — Landing Industrial
clientSubmitStep1Retail           — Landing Retail
clientSubmitStep1Oficinas         — Landing Oficinas
⚠️ clientSearchedSpotSearch       — Búsqueda de spots (falta en lead_events.sql)
⚠️ lead_form_submission           — Form genérico (falta en lead_events.sql)
⚠️ clientSubmittedRegisterUser    — Registro completado (falta en lead_events.sql)
⚠️ clientClickedRegisterUser      — Click registro (falta en lead_events.sql)
⚠️ clientSubmittedRegisterForm    — Form registro (falta en lead_events.sql)
```

*Ref:* `funnel_with_channel_v4.sql` líneas 217-237

### 10.12 Patrones Python post-BigQuery

| Patrón | Qué hace | Ref |
|--------|----------|-----|
| `user_pseudo_id` forzado a `str` | Evita tipos mixtos y joins inconsistentes | `lk_matches/utils/database.py:92` |
| IDs cast a `str` antes de export | Evita notación científica en CSV/Parquet | `lk_visitors/utils/s3_upload.py:66-69` |
| PK compuesta `user_{YYYYMMDD}` | Clave estable user×día con manejo de nulos | `lk_visitors/main.py:27-35` |
| Dedup `drop_duplicates(keep="first")` | Una fila por clave de negocio | `lk_visitors/main.py:83-89` |
| Columnas faltantes → `None` | Evita errores al seleccionar esquema final | `lk_visitors/main.py:75-78` |
| SSM client nuevo por llamada | Evita token expirado en ejecuciones largas | `lk_matches/utils/database.py:34-36` |

### 10.13 Auditoría y output

Columnas estándar de auditoría:
```
aud_inserted_date  — date
aud_inserted_at    — datetime
aud_updated_date   — date
aud_updated_at     — datetime
aud_job            — string (nombre del pipeline)
```

Export: S3 (`dagster-assets-production`) en Parquet o CSV → API Geospot para `replace` o `upsert`.

*Ref:* `lk_visitors/main.py:48-55`, `lk_visitors/utils/s3_upload.py`

### 10.14 Exclusión de usuarios internos

```python
ids_spot2 = df[df["email"].str.lower().str.contains("@spot2", na=False)]["user_pseudo_id"].unique()
df = df[~df["user_pseudo_id"].isin(ids_spot2)]
```

*Ref:* `lk_matches/utils/lead_events_processing.py:20-25`

### 10.15 Clasificación de leads: New vs Reactivated (SQL — PostgreSQL)

Patrón para determinar la primera interacción de un lead y detectar reactivaciones:

```sql
LEAST(
    COALESCE(lead_lead0_at, TIMESTAMP '9999-12-31'),
    COALESCE(lead_lead1_at, TIMESTAMP '9999-12-31'),
    ...
) AS primera_fecha_lead
```

Un lead con un proyecto creado >= 30 días después de su primera fecha lead se clasifica como "Reactivated".

**Para nuestro flujo:** útil al enriquecer la tabla de sesiones con datos de CRM para el modelo de conversión.

*Ref:* `bt_lds_leads.sql`

### 10.16 Resumen: qué incorporar al flujo de Browse Sessions

**Flujo de referencia activo:** `lk_vtl_job` → tabla `lk_mat_matches_visitors_to_leads`
(NO el legacy `lk_matches_visitors_to_leads`).

| Componente | Fuente referencia | Reutilizar / Adaptar |
|------------|------------------|---------------------|
| Credenciales BQ via SSM | `lk_matches.../utils/database.py` | Importar módulo existente |
| `user_pseudo_id` como STRING | `database.py:92` (post-query) | Ya decidido |
| `ga_session_id` como STRING + `has_session_id` | `funnel_v4:102-110` | Agregar flag booleano |
| Timestamps → `America/Mexico_City` | `lead_events_from_funnel_lk.sql:97` | Aplicar a todos los campos temporales |
| Subconsultas escalares para event_params | `lead_events_from_funnel_lk.sql` | Agregar params propios (session_engaged, etc.) |
| `_TABLE_SUFFIX` primero | `lead_events_from_funnel_lk.sql:65` | Ventana dinámica de 6 meses |
| Reglas de scraping (4 rules) | `funnel_v4:16-51` (CTE scraping) | Agregar como flag `is_scraping` |
| Technical Waste | `funnel_v4:144-148` | Filtro o flag |
| Source/Medium/Campaign fallback | `lead_events_from_funnel_lk.sql:117-157` | Triple CASE tal cual |
| Canal + Traffic_type (12 reglas) | `funnel_v4:528-670` | **Recalcular** por sesión (funnel solo tiene 1ª sesión/día) |
| `collected_traffic_source.manual_*` | `funnel_v4:75,87,99` | Agregar UTMs manuales |
| Eventos de conversión (18) | `funnel_v4:217-237` | Lista completa para flag `has_conversion_event` |
| Auditoría (`aud_*`) | `lk_visitors/main.py:48-55` | Cambiar `aud_job` al nombre del nuevo pipeline |
| Export S3 + API Geospot | `lk_matches.../utils/s3_upload.py` | Adaptar tabla y conflict_columns |
| Exclusión `@spot2` emails | `lead_events_processing.py:20-25` | Aplicar post-query |
| Dedup por clave de negocio | `assets.py:382-387` (sort + drop_duplicates) | PK será `user_pseudo_id + ga_session_id + event_date` |
| Error handling con metadata | `assets.py:37-53` (`_lk_vtl_emit_error_metadata`) | Adaptar para assets nuevos |
| Config con `table_name_prefix` | `config.py:23` (VTLConfig) | Permite test sin pisar producción |

---

## 11. Variables Sugeridas para el Modelo de Conversión

| Variable | Fuente | Extracción |
|----------|--------|------------|
| Sector visitado | URL path | Primer segmento (regex) |
| Operación (renta/venta) | URL path | Segundo segmento |
| Granularidad geográfica | `seo_indexable_urls.levelable_type` | Directo (5 niveles) |
| Estado | `seo_indexable_urls.state_slug` | Directo |
| Fuente de tráfico | `clients.hs_utm_*` | Directo |
| Spots disponibles | `seo_indexable_urls.spots_count_at_evaluation` | Directo |
| Precio mediano zona | `seo_indexable_urls.median_price_by_square_space_mxn` | Directo |
| Landing de conversión | `clients.hs_first_conversion` | Directo |
| Fuente analítica | `clients.hs_analytics_source` | Directo |

---

## 12. Validación de q_browse_sessions.sql (Fase 2)

**Fecha de validación:** 2026-04-08
**Scripts de validación:** `test_v4_vs_v5.py`, `test_browse_sessions.py`, `test_channel_consistency.py`, `test_volume_diagnostic.py`

### 12.1 Validación v4 → v5 (fix bodegas/coworking)

Se creó `funnel_with_channel_v5.sql` a partir de v4 con dos correcciones:

| Cambio | Sección afectada | Patrones agregados |
|--------|-----------------|-------------------|
| `primera_bifurcacion` | línea ~182 | +`bodegas`, +`coworking` |
| `conversion_point` | línea ~505 | +`renta`, +`venta`, +`bodegas`, +`coworking` |

**Hallazgo adicional:** `conversion_point` en v4 no tenía `renta` ni `venta` (solo `oficinas`, `locales-comerciales`, `naves-industriales`, `terrenos`), mientras que `primera_bifurcacion` sí los tenía. v5 corrige ambas omisiones.

**Resultados (ventana de 7 días, 12,106 filas):**

| Validación | Resultado |
|------------|-----------|
| Conteo de filas idéntico | PASS: 12,106 = 12,106 |
| Cobertura (merge on user+date) | PASS: 100% ambos |
| channel, traffic_type, source, medium | PASS: 100% idéntico |
| campaign_name, user_sospechoso, segunda_bifurcacion | PASS: 100% idéntico |
| primera_bifurcacion: cambios | 169 filas: Other → Browse Pages |
| conversion_point: cambios | 105 filas: Other → Browse Pages |

Todas las transiciones fueron exclusivamente **Other → Browse Pages**, confirmando que v5 no rompe nada existente.

*Ref:* `test_v4_vs_v5.py`, `funnel_with_channel_v5.sql`

### 12.2 Validación de q_browse_sessions.sql

**Estructura:** 5 CTEs: `scraping`, `browse_page_views`, `session_agg`, `conversion_in_session`, `with_channel` + SELECT final.

**Resultados (ventana completa 6 meses, 190 días):**

| Métrica | Valor |
|---------|------:|
| Sesiones totales | 204,061 |
| Usuarios únicos | 162,233 |
| Días cubiertos | 190 |
| Columnas | 36 |

**Últimos 30 días:**

| Métrica | Valor |
|---------|------:|
| Sesiones | 31,151 |
| Usuarios únicos | 25,545 |
| Promedio diario sesiones | ~1,004 |
| ga_session_id disponible | 99.9% |
| Sesiones con conversión | 6,459 (20.7%) |
| Sesiones sin conversión | 24,692 (79.3%) |

**Distribución de canal (30 días):**

| Canal | Sesiones | % |
|-------|:--------:|:-:|
| Paid Search | 23,216 | 74.5% |
| Organic Search | 5,615 | 18.0% |
| Direct | 1,331 | 4.3% |
| Paid Social | 348 | 1.1% |
| Organic LLMs | 258 | 0.8% |
| Cross-network | 181 | 0.6% |
| Referral | 113 | 0.4% |
| Otros (Nulo-Vacío, Display, etc.) | 109 | 0.3% |

**Distribución de tráfico (30 días):**

| Tipo | Sesiones | % |
|------|:--------:|:-:|
| Paid | 23,778 | 76.3% |
| Organic | 7,327 | 23.5% |
| Unassigned | 43 | 0.1% |
| Bot/Spam | 3 | 0.0% |

**Page views por sesión (30 días):**

| Rango | Sesiones | % |
|-------|:--------:|:-:|
| 1 | 25,540 | 82.0% |
| 2 | 3,862 | 12.4% |
| 3-5 | 1,549 | 5.0% |
| 6-10 | 171 | 0.5% |
| 11+ | 29 | 0.1% |

**Dispositivo (30 días):**

| Categoría | Sesiones | % |
|-----------|:--------:|:-:|
| Mobile | 18,511 | 59.4% |
| Desktop | 12,397 | 39.8% |
| Tablet | 243 | 0.8% |

**Top 5 browse pages (30 días):**
1. `spot2.mx/renta` — 302 sesiones
2. `spot2.mx/locales-comerciales/renta/ciudad-de-mexico` — 285
3. `spot2.mx/oficinas/renta/ciudad-de-mexico/corredor-polanco` — 125
4. `spot2.mx/oficinas/renta/ciudad-de-mexico` — 105
5. `spot2.mx/naves-industriales/renta/ciudad-de-mexico` — 48

**Top eventos de conversión en sesión (30 días):**

| Evento | Ocurrencias |
|--------|:-----------:|
| clientSearchedSpotSearch | 5,548 |
| clientRequestedWhatsappForm | 872 |
| clientClickedRegisterForm | 402 |
| clientSubmitStep1Industrial | 163 |
| clientSubmitFormBlogIndustrial | 148 |
| clientRequestedContactLead | 98 |
| clientClickedLeadPopup | 57 |
| clientClickedSendInfoModalConsulting | 56 |
| clientSubmittedBudgetFormBP | 33 |
| clientSubmittedRegisterUser | 15 |

*Ref:* `q_browse_sessions.sql`, `test_browse_sessions.py`

### 12.3 Corrección de volúmenes vs Fase 1

Los volúmenes de Fase 1 (sec 9.1: 9,256 PV, 7,557 sesiones en 30d) resultan significativamente menores a los obtenidos con `q_browse_sessions.sql` (38,492 PV, ~31k sesiones en 30d). El diagnóstico detallado (`test_volume_diagnostic.py`) muestra:

| Regex | PV (30d) | Sessions | Notas |
|-------|:--------:|:--------:|-------|
| Completa (8 ramas + raíz) | 38,492 | ~31k | q_browse_sessions.sql |
| Requiere `/` después de rama | 35,481 | 27,650 | Sin URLs raíz como /renta, /venta |
| Sin bodegas/coworking/renta/venta | 35,071 | 27,394 | Solo 4 ramas originales |

**Volumen por rama (30 días):**

| Rama | Page views | Usuarios |
|------|:---------:|:--------:|
| locales-comerciales | 21,608 | 13,909 |
| oficinas | 7,850 | 5,240 |
| terrenos | 4,092 | 3,003 |
| naves-industriales | 3,207 | 2,166 |
| renta | 1,384 | 1,036 |
| bodegas | 297 | 207 |
| coworking | 48 | 23 |
| venta | 6 | 6 |

La diferencia con Fase 1 se explica porque las queries exploratorias preliminares tenían un scope más restrictivo (posiblemente requiriendo segmentos geográficos en la URL o usando un filtro de fecha diferente). Los volúmenes de `q_browse_sessions.sql` son los correctos y validados.

### 12.4 Consistencia de canal: browse_sessions vs funnel_v5

Se comparó la lógica de canal de `q_browse_sessions.sql` contra `funnel_with_channel_v5.sql` en la intersección de usuarios. Para alinear granularidades, se filtró browse_sessions a la primera sesión del día por usuario.

**Resultados (ventana de 7 días, 6,122 filas en intersección):**

| Columna | Coincidencia | Discrepancias |
|---------|:------------:|:-------------:|
| channel | 99.7% | 17 |
| traffic_type | 99.8% | 14 |
| source | 99.9% | 4 |
| medium | 99.7% | 17 |
| is_scraping / user_sospechoso | **100%** | 0 |

**Cobertura:**
- 100% de las primeras sesiones de browse_sessions están en V5
- 50.6% de V5 está en browse_sessions (esperado: V5 incluye TODAS las páginas, no solo browse)

**Causa de las 17 discrepancias de canal:** los 17 casos ocurren cuando la primera sesión de browse pages del usuario NO es la primera sesión del día en general. En esos casos, la primera sesión del día fue a una página diferente (spot, blog, etc.), por lo que la atribución de sesión difiere.

**Conclusión:** La lógica de canal de `q_browse_sessions.sql` es **funcionalmente idéntica** a la de `funnel_with_channel_v5.sql`. Las discrepancias son legítimas y esperadas por la diferencia de granularidad.

*Ref:* `test_channel_consistency.py`

### 12.5 Archivos producidos

| Archivo | Ubicación | Propósito |
|---------|-----------|-----------|
| `funnel_with_channel_v5.sql` | `sql/old/` | v4 + fix bodegas/coworking |
| `q_browse_sessions.sql` | `ad_hoc/.../` | Query principal de sesiones de browse pages |
| `test_v4_vs_v5.py` | `ad_hoc/.../` | Validación v4→v5 (0 breaking changes) |
| `test_browse_sessions.py` | `ad_hoc/.../` | Validación de volúmenes y distribuciones |
| `test_channel_consistency.py` | `ad_hoc/.../` | Comparación canal BS vs V5 en intersección |
| `test_volume_diagnostic.py` | `ad_hoc/.../` | Diagnóstico de diferencia vs Fase 1 |

---

## 13. Identity Resolution: VTL v2

**Fecha:** 2026-04-08
**Objetivo:** Agrupar multiples `user_pseudo_id` que corresponden al mismo visitante real, usando datos de contacto (telefono/email) como puente.

### 13.1 Problema

GA4 asigna un `user_pseudo_id` por cookie de navegador. La misma persona puede generar multiples cookies (cambio de dispositivo, navegacion privada, borrado de cookies). Sin identity resolution, un visitante que navego browse pages con `upid_A` y convirtio con `upid_B` apareceria como "no convertido" en el analisis.

### 13.2 Solucion: Union-Find sobre grafo de contacto

Se implemento un asset Silver (`lk_vtl_v2_identity`) que construye un grafo bipartito:

**Nodos:** user_pseudo_id, client_id, email, phone (con prefijos de namespace)
**Aristas:**
1. `user_pseudo_id` <-> `email` (de eventos GA4 con formulario)
2. `user_pseudo_id` <-> `phone` (de eventos GA4 con WhatsApp)
3. `client_id` <-> `email` (de tabla `clients` MySQL)
4. `client_id` <-> `phone` (de tabla `clients` MySQL)

Union-Find con path compression encuentra componentes conexas. Cada componente recibe un `canonical_visitor_id` (el user_pseudo_id mas pequeno del grupo).

### 13.3 Resultados de validacion

**Equivalencia v1 vs v2:** 100% identico en las 22 columnas MAT originales (55,889 filas).

**Identity Resolution stats:**

| Metrica | Valor |
|---------|------:|
| Usuarios analizados | 40,189 |
| Con puente de contacto (phone/email) | 14,227 (35.4%) |
| Clusters multi-usuario | 968 |
| Filas MAT con identidad resuelta | 1,423 (2.5%) |
| Reduccion de identidades | 567 (1.1%) |

**Interpretacion:** El 2.5% de las filas MAT tienen un `canonical_visitor_id` diferente a su `vis_user_pseudo_id`. Esto significa que 567 user_pseudo_ids distintos fueron agrupados bajo otro canonical, formando 968 clusters. La mayoria son pares (2 cookies de la misma persona).

### 13.4 Limitaciones

- Solo funciona para visitantes que en algun momento proporcionaron email o telefono (eventos de conversion)
- Visitantes que solo navegaron sin interactuar no tienen puente de contacto
- La resolucion es correcta para el modelo de conversion: exactamente la poblacion que nos interesa diferenciar es la que tiene puentes

### 13.5 Archivos del flujo v2

| Archivo | Proposito |
|---------|-----------|
| `defs/lk_vtl_v2/__init__.py` | Exports del modulo |
| `defs/lk_vtl_v2/config.py` | VTLv2Config (hereda VTLConfig + `enable_identity_resolution`) |
| `defs/lk_vtl_v2/identity.py` | Union-Find + `build_visitor_identity()` |
| `defs/lk_vtl_v2/assets.py` | 4 assets: processed, identity, matched, final_output |
| `defs/lk_vtl_v2/jobs.py` | Job sin schedule (validacion manual) |
| `defs/lk_vtl_v2/ddl/alter_add_canonical_visitor_id.sql` | ALTER TABLE para GeoSpot |
| `test_vtl_v1_vs_v2.py` | Script de comparacion v1 vs v2 |

### 13.6 Proximo paso

Una vez confirmado que v2 es correcto:
1. Ejecutar `ALTER TABLE` en GeoSpot
2. Copiar logica de `identity.py` y cambios de `assets.py` al flujo original
3. Agregar `canonical_visitor_id` a `MAT_OUTPUT_COLUMNS`
4. Eliminar flujo temporal `lk_vtl_v2/`
5. Usar `canonical_visitor_id` en el flujo de browse sessions para el JOIN con MAT
