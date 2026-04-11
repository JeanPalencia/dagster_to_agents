# lk_okrs - OKRs Mensuales (Targets Definidos por Negocio)

## Overview

La tabla **lk_okrs** almacena los Objetivos y Resultados Clave (OKRs) mensuales **definidos por el negocio**. Son los TARGETS/METAS que espera alcanzar cada mes en el funnel de conversión.

**Propósito Analítico:**
- Base de datos de targets mensuales del negocio
- Comparar actual vs OKR para evaluar performance
- Seguimiento de trends: ¿estamos acelerados o decelerados?
- Input para decisiones estratégicas
- Validar que nuestras queries den números correctos

**Estructura**: Simple y directa - solo targets, SIN campos de actual/achievement
**Frecuencia de Actualización:** Manual (el negocio define al inicio de cada mes)
**Grain**: Un registro por métrica por mes
**Atributos**: 7 fields (1 PK + 6 targets)

---

## Table Structure (REAL, basado en CSV)

```
okr_month_start_ts    → Fecha inicio del mes (timestamp)
okr_leads             → Target de leads nuevos ese mes
okr_projects          → Target de proyectos nuevos ese mes
okr_scheduled_visits  → Target de visitas agendadas
okr_confirmed_visits  → Target de visitas confirmadas
okr_completed_visits  → Target de visitas completadas
okr_lois              → Target de LOIs emitidas
```

### Definición de Campos

| Campo | Tipo | Descripción |
|-------|------|-------------|
| **okr_month_start_ts** | Timestamp | Fecha inicio mes. Ej: "February 1, 2026, 12:00 AM" |
| **okr_leads** | Integer | Target: Nuevos leads que espera crear ese mes |
| **okr_projects** | Integer | Target: Nuevos proyectos que espera crear ese mes |
| **okr_scheduled_visits** | Integer | Target: Visitas agendadas/solicitadas |
| **okr_confirmed_visits** | Integer | Target: Visitas confirmadas por broker |
| **okr_completed_visits** | Integer | Target: Visitas completadas/realizadas |
| **okr_lois** | Integer | Target: LOIs (Cartas de Intención) emitidas |

---

## Datos Reales 2026 (del CSV)

```
Mes            | Leads | Projects | Scheduled | Confirmed | Completed | LOIs
----           |-------|----------|-----------|-----------|-----------|------
January        | 1,898 | 1,632    | 286       | 180       | 167       | 54
February       | 2,192 | 1,929    | 351       | 232       | 217       | 74
March          | 2,772 | 2,495    | 474       | 322       | 303       | 106
April          | 3,355 | 3,019    | 574       | 390       | 367       | 128
May            | 4,227 | 3,804    | 723       | 491       | 462       | 162
June           | 4,882 | 4,394    | 835       | 568       | 534       | 187
July           | 5,382 | 4,844    | 920       | 626       | 588       | 206
August         | 6,499 | 5,849    | 1,111     | 756       | 710       | 249
September      | 8,189 | 7,370    | 1,400     | 952       | 895       | 313
October        | 10,318| 9,286    | 1,764     | 1,200     | 1,128     | 395
November       | 12,459| 11,213   | 2,131     | 1,449     | 1,362     | 477
December       | 5,980 | 5,382    | 1,023     | 695       | 654       | 229
```

**Insight**: Targets crecen mensualmente hasta noviembre, bajan en diciembre (holiday season).

---

## Estructura DDL

```sql
CREATE TABLE lk_okrs (
  okr_month_start_ts TIMESTAMP,
  okr_leads INTEGER,
  okr_projects INTEGER,
  okr_scheduled_visits INTEGER,
  okr_confirmed_visits INTEGER,
  okr_completed_visits INTEGER,
  okr_lois INTEGER,

  PRIMARY KEY (okr_month_start_ts)
);
```

---

## Cómo Usar: Validar tus Queries

**Paso 1**: Calcula métrica del mes desde golden tables
```sql
-- Leads en febrero
SELECT COUNT(DISTINCT lead_id) as actual_leads
FROM lk_leads
WHERE CAST(lead_created_date AS DATE) BETWEEN '2026-02-01' AND '2026-02-28'
  AND lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL;
-- Resultado: 2,100
```

**Paso 2**: Compara con OKR
```sql
SELECT okr_leads
FROM lk_okrs
WHERE CAST(okr_month_start_ts AS DATE) = '2026-02-01';
-- Resultado: 2,192
```

**Paso 3**: Evalúa
- Actual: 2,100
- Target: 2,192
- Achievement: 95.8%
- Status: 🟢 Green (>90%)

---

## Métricas Mapeadas

| OKR Field | Golden Table | Query |
|-----------|--------------|-------|
| **okr_leads** | lk_leads | `COUNT(DISTINCT lead_id)` |
| **okr_projects** | lk_projects | `COUNT(DISTINCT project_id)` |
| **okr_scheduled_visits** | lk_projects | `COUNT(DISTINCT CASE WHEN project_visit_requested_date IS NOT NULL THEN project_id END)` |
| **okr_confirmed_visits** | lk_projects | `COUNT(DISTINCT CASE WHEN project_visit_confirmed_at IS NOT NULL THEN project_id END)` |
| **okr_completed_visits** | lk_projects | `COUNT(DISTINCT CASE WHEN project_visit_completed_date IS NOT NULL THEN project_id END)` |
| **okr_lois** | lk_projects | `COUNT(DISTINCT CASE WHEN project_loi_date IS NOT NULL THEN project_id END)` |

---

## Query para Dashboard OKR Tracking

```sql
WITH actual_metrics AS (
  SELECT
    'leads' as metric,
    COUNT(DISTINCT lead_id) as actual_value
  FROM lk_leads
  WHERE CAST(lead_created_date AS DATE) BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE - INTERVAL '1 day'
    AND lead_domain NOT IN ('spot2.mx')
    AND lead_deleted_at IS NULL

  UNION ALL

  SELECT 'projects' as metric,
    COUNT(DISTINCT project_id) as actual_value
  FROM lk_projects
  WHERE CAST(project_created_at AS DATE) BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE - INTERVAL '1 day'
    AND project_deleted_at IS NULL

  -- ... más metrics
)
SELECT
  am.metric,
  am.actual_value,
  CASE am.metric
    WHEN 'leads' THEN o.okr_leads
    WHEN 'projects' THEN o.okr_projects
    -- ...
  END as target,
  ROUND(100.0 * am.actual_value / CASE am.metric
    WHEN 'leads' THEN o.okr_leads
    WHEN 'projects' THEN o.okr_projects
  END, 1) as achievement_pct
FROM actual_metrics am
CROSS JOIN lk_okrs o
WHERE CAST(o.okr_month_start_ts AS DATE) = DATE_TRUNC('month', CURRENT_DATE);
```

---

## Notas

- 📊 OKRs son **inputs del negocio**, no cálculos
- 🎯 Se definen al inicio de cada mes basados en:
  - Metas estratégicas
  - Historial de performance
  - Capacidad del equipo
- ⚠️ Estos son los números contra los que se miden el éxito
- 🔄 Tu job: Comparar actual vs OKR mensualmente

---

**Última actualización**: 2026-02-24
**Versión**: 2.0.0
**Cambios**: Refactor completo basado en estructura real del CSV (simple, sin calculated fields)
