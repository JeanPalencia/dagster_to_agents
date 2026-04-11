# Diagnóstico: Diferencias GFG vs FinN — Leads por canal

**Fecha**: 2026-03-26
**Meses analizados**: Diciembre 2025 — Marzo 2026
**Cohorts**: New y Reactivated

## Resultado principal

| Métrica | GFG | FinN | Diferencia |
|---------|-----|------|------------|
| Total leads New (dic-2025) | 1,097 | 1,097 | 0 |
| Leads en 4 canales orgánicos | 125 | 119 | +6 |

La cantidad total de leads es idéntica. La diferencia de 6 leads aparece solo al filtrar por canal.

## Causa raíz

**FinN asigna un solo canal por lead (el evento más reciente), GFG asigna un canal por cohort.**

### Mecanismo exacto

1. **FinN** usa `DISTINCT ON (lead_id) ORDER BY mat_event_datetime DESC` en `mat_lead_attrs`. Esto toma el canal del evento más reciente del lead, sin importar a qué cohort pertenece.

2. **GFG** usa `ROW_NUMBER() OVER(PARTITION BY lead_id, lead_fecha_cohort_dt::date ORDER BY mat_event_datetime DESC)`. Esto preserva un canal distinto por cada cohort (New y Reactivated).

3. Cuando un lead tiene un cohort "New" (dic-2025) con canal real (ej: "Paid Search") y luego un cohort "Reactivated" posterior con entrada CRM_OFFLINE (canal "Offline"), ocurre lo siguiente:
   - **GFG**: El cohort New conserva su canal original ("Paid Search"). El Reactivated tiene "Offline". Al filtrar por New, se ve "Paid Search".
   - **FinN**: El evento más reciente es el CRM_OFFLINE Reactivated → canal global = "Offline". Al filtrar por New, se ve "Offline".

### Impacto cuantificado (19 leads con canal distinto)

| De (GFG) | A (FinN) | Leads | Causa |
|----------|----------|-------|-------|
| Paid Search → Offline | CRM_OFFLINE Reactivated más reciente | 11 |
| Direct → Offline | CRM_OFFLINE Reactivated más reciente | 4 |
| Organic Search → Offline | CRM_OFFLINE Reactivated más reciente | 3 |
| Paid Search → Organic Search | Reactivated con Organic Search más reciente | 1 |

Esto produce en los 4 canales orgánicos:
- **+7 leads en GFG** que FinN clasifica como Offline (GFG los ve con su canal New real)
- **-1 lead en FinN** que GFG clasifica como Paid Search (FinN lo ve con Organic Search del Reactivated)
- **Neto: +6 leads en GFG** (125 vs 119)

## Corrección aplicada

El canal correcto es el del cohort (como hace GFG), no el global más reciente.

### Cambio en `Query: Projects Funnel.sql`

**Antes** (original): `DISTINCT ON (lead_id) ORDER BY mat_event_datetime DESC` — un canal global por lead.

```sql
mat_lead_attrs AS (
    SELECT DISTINCT ON (lead_id)
        lead_id,
        COALESCE(mat_channel, 'Unassigned') AS channel,
        ...
    FROM lk_mat_matches_visitors_to_leads
    ORDER BY lead_id, mat_event_datetime DESC
)
-- JOIN en leads_calculados por lead_id
```

**Después** (FIXED): `ROW_NUMBER() OVER(PARTITION BY lead_id, lead_fecha_cohort_dt::date)` — un canal por cohort, join por `(lead_id, fecha_cohort)`.

```sql
mat_lead_attrs AS (
    SELECT lead_id, lead_fecha_cohort_dt::date AS cohort_date,
        COALESCE(mat_channel, 'Unassigned') AS channel,
        ...
    FROM (
        SELECT *, ROW_NUMBER() OVER(
            PARTITION BY lead_id, lead_fecha_cohort_dt::date
            ORDER BY mat_event_datetime DESC
        ) AS rn
        FROM lk_mat_matches_visitors_to_leads
    ) sub WHERE rn = 1
)
-- JOIN al final: ON m.lead_id = t.lead_id AND m.cohort_date = DATE(t.fecha_cohort)
```

El canal se mueve de `leads_calculados` al SELECT final, donde `fecha_cohort` ya está calculado.

### Validación

| Métrica | GFG vs FinN original | GFG vs FinN FIXED |
|---------|---------------------|-------------------|
| Leads con canal distinto | 19 | **0** |
| Diferencia en canales orgánicos | 6 (125 vs 119) | **0** (125 vs 125) |
| Distribución por canal | Diff en Paid Search (+11), Offline (-17), etc. | **Diff 0 en todos** |

La corrección elimina **todas** las diferencias de canal entre GFG y FinN.

## Validación general (Dic 2025 — Mar 2026, New + Reactivated)

Después de aplicar la corrección, se ejecutó un test general con ambos cohorts en 4 meses.

### Cohort New — Coincidencia perfecta

| Mes | GFG | FinN FIXED | Canal diff | Fecha diff |
|-----|-----|------------|------------|------------|
| 2025-12 | 1,097 | 1,097 | 0 | 0 |
| 2026-01 | 1,963 | 1,963 | 0 | 0 |
| 2026-02 | 1,570 | 1,570 | 0 | 0 |
| 2026-03 | 1,315 | 1,315 | 0 | 0 |

### Cohort Reactivated — Coincidencia perfecta a nivel `(lead_id, fecha_cohort)`

Al comparar por `lead_id` (un valor por lead), aparecían diferencias aparentes (17 canal, 59 fecha). Sin embargo, son un artefacto de la deduplicación: un lead puede tener múltiples proyectos reactivados en el mismo mes, y al forzar un solo valor por lead cada query elige uno distinto.

Al comparar a nivel `(lead_id, fecha_cohort_date)` — la unidad correcta — la coincidencia es total:

| Mes | Keys GFG | Keys FinN | Solo GFG | Solo FinN | Canal diff |
|-----|----------|-----------|----------|-----------|------------|
| 2025-12 | 185 | 185 | 0 | 0 | 0 |
| 2026-01 | 296 | 296 | 0 | 0 | 0 |
| 2026-02 | 254 | 254 | 0 | 0 | 0 |
| 2026-03 | 213 | 213 | 0 | 0 | 0 |

La única diferencia estructural es que FinN puede tener más filas que GFG cuando dos proyectos caen en la misma fecha (FinN: 1 fila por proyecto, GFG: 1 fila por fecha). Esto no afecta al contar DISTINCT lead_id.

## Evidencia

Los 19 leads con canal distinto en cohort New fueron verificados contra la tabla MAT. Todos siguen el mismo patrón: múltiples filas en MAT (New + Reactivated), donde el evento Reactivated tiene un canal distinto al New (generalmente CRM_OFFLINE → Offline).

### Scripts y archivos

- `test_gfg_vs_finn.py` — diagnóstico inicial (identifica los 19 leads con canal distinto en New)
- `test_gfg_vs_finn_fixed.py` — validación de la corrección (0 diferencias en New)
- `test_gfg_vs_finn_general.py` — test general 4 meses, New + Reactivated por lead_id
- `test_reactivated_deep.py` — investigación profunda Reactivated a nivel (lead_id, fecha_cohort)
- `test_pop_disagg_vs_fixed.py` — validación de equivalencia entre PoP DISAGG y FIXED
- `Query: Projects Funnel.sql` — query original (conservada sin cambios)
- `Query: Projects Funnel FIXED.sql` — query corregida
- `Query: PoP Cohorts FIXED.sql` — query PoP con corrección de canal por cohort (pre-agregada)
- `Query: PoP Cohorts DISAGG.sql` — query PoP desagregada (filas individuales con flags 1/0)
- `model_lead_matcheados.sql` — query GFG (referencia)

---

## PoP Cohorts: versión desagregada (DISAGG)

**Fecha**: 2026-03-27

### Problema

La query `PoP Cohorts FIXED.sql` pre-agrega los datos (conteos por grupo dimensional). Al usarla en Metabase, los filtros de canal/fuente/campaña se aplican **después** de la agregación, lo que impide:
- Filtrar dinámicamente y obtener conteos correctos
- Evitar doble conteo de leads reactivados con múltiples canales

### Solución

`Query: PoP Cohorts DISAGG.sql` produce filas individuales con flags 1/0 en lugar de conteos:

- **Mismos CTEs** base que la FIXED (mat_lead_attrs → universo_relevante)
- **Elimina** todos los CTEs de agregación (dims, curr_lead_first, agg_curr_lead, etc.)
- **Salida**: `UNION ALL` de dos bloques:
  - **By Lead**: todas las filas, `entity_id = lead_id`
  - **By Project**: solo filas con `project_id IS NOT NULL`, `entity_id = project_id`
- **Cada fila** expone `lead_id`, `project_id`, `entity_id`, `mode` y 12 flags 1/0

### Metabase: formulas de reconstrucción

| Tarjeta | Formula | Filtro mode |
|---------|---------|-------------|
| Leads current | `distinct(case([leads_current]=1, [lead_id]))` | Fijo: By Lead (desconectado del filtro) |
| Leads prev | `distinct(case([leads_prev]=1, [lead_id]))` | Fijo: By Lead |
| Projects current | `distinct(case([projects_current]=1, [entity_id]))` | Conectado al filtro |
| Visitas solicitadas | `distinct(case([scheduled_visits_current]=1, [entity_id]))` | Conectado al filtro |
| (resto de metricas) | `distinct(case([flag]=1, [entity_id]))` | Conectado al filtro |

### Validación

`test_pop_disagg_vs_fixed.py` reconstruye los totales de la DISAGG con la lógica COUNTD de Metabase y los compara con la FIXED:

| Métrica | Resultado |
|---------|-----------|
| leads_current (By Lead) | OK — 0 diferencias |
| leads_prev (By Lead) | OK — 0 diferencias |
| projects_current | OK — 0 diferencias |
| projects_prev | OK — 0 diferencias |
| scheduled_visits_current | OK — 0 diferencias |
| scheduled_visits_prev | OK — 0 diferencias |
| completed_visits_current | OK — 0 diferencias |
| completed_visits_prev | OK — 0 diferencias |
| lois_current | OK — 0 diferencias |
| lois_prev | OK — 0 diferencias |
| won_current | OK — 0 diferencias |
| won_prev | OK — 0 diferencias |

**27 grupos** de la FIXED aparecen solo en mode="By Project" sin actividad de proyecto (leads sin proyectos en esa combinación dimensional). Es esperado: en DISAGG, "By Project" solo contiene filas con `project_id IS NOT NULL`.
