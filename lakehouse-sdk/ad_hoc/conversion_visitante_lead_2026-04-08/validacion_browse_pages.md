# Validación del Reporte: Browse Pages spot2.mx

**Fecha:** 2026-04-08
**Fuente validada:** `reporte_browse_pages_spot2.md`
**Método:** Queries directas a MySQL production-analysis (spot2_service)

---

## Resumen Ejecutivo

El reporte contiene información estructuralmente correcta sobre la arquitectura de URLs, pero tiene un **error crítico en la cobertura de estados**: documenta solo 9 estados cuando en realidad hay **32 estados** activos. Esto haría que la regex "estricta" (sección 3.4 del reporte) pierda el **39.8% de las URLs**.

---

## 1. Validación de Conteos

| Tabla | Filas totales | URLs únicas |
|-------|--------------|-------------|
| `seo_browse_urls` | 18,917 | 18,917 (1:1, todas únicas) |
| `seo_indexable_urls` | 2,239,649 | 22,702 (acumulado histórico) |
| `seo_indexability_executions` | 272 | — |

- La primera ejecución fue el **2025-08-13**, la más reciente el **2026-04-08**.
- Las URLs indexables crecieron de ~1,201 (ago-2025) a **12,835** (abr-2026).

### Relación entre tablas

- **UNION total** (browse + indexable histórico): **22,702 URLs únicas**
- Todas las URLs de `seo_browse_urls` existen en `seo_indexable_urls` (0 huérfanas)
- **3,785 URLs** en `seo_indexable_urls` NO existen en `seo_browse_urls`

---

## 2. Error Crítico: Estados (State Slugs)

El reporte documenta **9 estados** (sección 1.3), pero la data real muestra **32 estados**:

### Los 9 documentados (correctos):
`ciudad-de-mexico`, `mexico`, `jalisco`, `nuevo-leon`, `chihuahua`, `guerrero`, `queretaro`, `quintana-roo`, `yucatan`

### Los 23 NO documentados (faltantes en el reporte):
| Estado | URLs en browse | URLs en indexable (última ejecución) |
|--------|---------------|--------------------------------------|
| coahuila | 1,112 | 849 |
| puebla | 914 | 561 |
| guanajuato | 786 | 518 |
| veracruz | 597 | 413 |
| baja-california | 519 | 364 |
| durango | 398 | 319 |
| san-luis-potosi | 355 | 250 |
| aguascalientes | 347 | 230 |
| morelos | 318 | 229 |
| hidalgo | 304 | 215 |
| sinaloa | 296 | 220 |
| chiapas | 231 | 184 |
| colima | 207 | 176 |
| campeche | 162 | 137 |
| tlaxcala | 152 | 90 |
| tabasco | 148 | 116 |
| tamaulipas | 147 | 96 |
| michoacan | 130 | 74 |
| oaxaca | 126 | 87 |
| sonora | 121 | 78 |
| baja-california-sur | 81 | 53 |
| nayarit | 40 | 26 |
| zacatecas | 37 | 34 |

**Impacto de usar la regex con solo 9 estados (sección 3.4):** se pierden **7,528 URLs (39.8%)**.

---

## 3. Validación de Sector y Operation Slugs

### Sectores — CORRECTO
El reporte documenta 6 sectores, la data confirma exactamente los mismos:

| Sector | URLs en browse | URLs en indexable (exec 272) |
|--------|---------------|------------------------------|
| terrenos | 5,884 | 4,413 |
| naves-industriales | 4,153 | 1,178 |
| locales-comerciales | 3,748 | 2,667 |
| bodegas | 2,829 | 2,808 |
| oficinas | 2,067 | 1,533 |
| coworking | 234 | 234 |
| NULL (sin sector) | 2 | 2 |

### Operaciones — CORRECTO
| Operación | URLs en browse | URLs en indexable (exec 272) |
|-----------|---------------|------------------------------|
| renta | 9,735 | 6,459 |
| venta | 9,176 | 6,370 |
| NULL (sin operación) | 6 | 6 |

Las 2 URLs con sector NULL son: `/renta`, `/venta`
Las 6 URLs con operación NULL son: `/locales-comerciales`, `/naves-industriales`, `/oficinas`, `/terrenos`, `/bodegas`, `/coworking`

---

## 4. Brecha de URLs: browse vs indexable

### 3,785 URLs en indexable que NO están en browse

**Causas identificadas:**

1. **Variantes de state_slug (185 URLs):**
   - `coahuila-de-zaragoza` → en browse es `coahuila` (123 URLs)
   - `michoacan-de-ocampo` → en browse es `michoacan` (62 URLs)
   - Son la misma ubicación con slug diferente entre versiones del sistema.

2. **URLs históricamente efímeras (~3,600 URLs):**
   - URLs que fueron indexables en ejecuciones tempranas pero dejaron de serlo.
   - Ejemplo: `/locales-comerciales/renta/nuevo-leon/monterrey/mitras-norte` (existió en ejecuciones 11-86)
   - Distribuidas en 4 sectores: terrenos (1,289), naves-industriales (1,035), locales-comerciales (961), oficinas (500)
   - Nota: `bodegas` y `coworking` NO tienen URLs faltantes.

---

## 5. Validación de Regex

| Regex | Cobertura browse_urls (18,917) | Nota |
|-------|-------------------------------|------|
| Simple (3.1): genérica con `[a-z0-9-]+` | **18,917 (100%)** | Captura todo |
| Estricta (3.2): estructura con unicode | **18,917 (100%)** | Captura todo |
| Estricta con 9 estados (3.4) | **11,389 (60.2%)** | Pierde 7,528 URLs |

**Recomendación:** Usar la regex simple (3.1) o la estricta sin hardcodear estados. La regex 3.4 del reporte NO debe usarse en producción.

---

## 6. Profundidad de URLs

| Profundidad (segmentos) | Count | Ejemplo |
|------------------------|-------|---------|
| 1 (solo sector u operación) | 8 | `/oficinas`, `/renta` |
| 2 (sector + operación) | 12 | `/oficinas/renta` |
| 3 (+ estado) | 331 | `/oficinas/renta/jalisco` |
| 4 (+ municipio o corredor) | 3,400 | `/oficinas/renta/jalisco/guadalajara` |
| 5 (+ colonia) | 15,166 | `/oficinas/renta/jalisco/guadalajara/providencia` |

El 80.2% de las URLs browse son de nivel "colonia" (máxima granularidad).

---

## 7. Recomendación para el modelo de conversión

### Fuente de verdad: `seo_indexable_urls`

`seo_browse_urls` es un subconjunto estricto de `seo_indexable_urls` (0 URLs en browse que no estén en indexable). Por lo tanto, **`seo_indexable_urls` es suficiente** como fuente de todas las URLs, incluyendo históricas.

```sql
-- TODAS las URLs únicas históricas (22,702)
SELECT DISTINCT url_path FROM seo_indexable_urls;

-- URLs actualmente indexables con métricas (12,835)
SELECT url_path, spots_count_at_evaluation, median_price_by_square_space_mxn
FROM seo_indexable_urls
WHERE execution_id = (SELECT MAX(execution_id) FROM seo_indexability_executions);
```

Ambas tablas tienen los campos `*_name` (nombres legibles), así que no hay necesidad de cruzar con `seo_browse_urls`.

### Regex recomendada para BigQuery:
```sql
WHERE REGEXP_CONTAINS(page_path,
  r'^/(locales-comerciales|naves-industriales|oficinas|terrenos|bodegas|coworking|renta|venta)(/[a-z0-9-]+)*$'
)
```

---

## 8. Levelable Types (campo adicional no documentado)

`seo_indexable_urls` tiene un campo `levelable_type` no mencionado en el reporte:

| Tipo | Count (exec 272) | Descripción |
|------|-----------------|-------------|
| App\Models\DataSettlement | 9,701 | Nivel colonia |
| App\Models\DataMunicipality | 2,015 | Nivel municipio |
| App\Models\Zone | 781 | Nivel corredor/zona |
| App\Models\DataState | 318 | Nivel estado |
| App\Models\DataCountry | 20 | Nivel país (sector+operación) |

Este campo es útil para clasificar la granularidad geográfica de cada URL sin parsear el path.
