# lk_matches_visitors_to_leads - Golden Table Documentation

## Overview

La tabla **lk_matches_visitors_to_leads** es una tabla de **mapeo/matching** que vincula visitantes anónimos de BigQuery (GA4) **QUE FUERON IDENTIFICADOS** como leads en Spot2. Su principal propósito es **atribuir correctamente el tráfico web** a los leads que sí se convirtieron, permitiendo entender los canales de adquisición y patrones de navegación previos a la conversión.

**⚠️ IMPORTANTE**: Esta tabla contiene **SOLO los visitors que se convirtieron en leads**. Los visitors que nunca se identificaron/registraron NO aparecen aquí.

**Propósito Analítico:**
- Mapear visitor anónimo (GA4) → lead identificado en Spot2 (solo conversiones)
- Atribución correcta del tráfico web que **resultó en leads**
- Análisis de canales de adquisición efectivos (organic, paid search, direct, referral, etc)
- Seguimiento del comportamiento **pre-conversión** del visitor
- Correlación entre tráfico inicial y conversión en lead
- Análisis de source/medium/campaign para leads adquiridos
- Entender cuándo y cómo el visitor se convirtió de anónimo a lead identificado

**Propósito Técnico:**
- Bridge entre BigQuery GA4 data (visitor pseudoID) y Postgres Spot2 data (lead_id/client_id)
- Fuente de verdad para atribución de **leads convertidos**
- Permite unified view de visitor pre-conversión + lead post-conversión
- Complemento a `lk_visitors`: mientras que lk_visitors tiene TODOS los visitors (converted y no-converted), lk_matches_visitors_to_leads tiene solo los que se identificaron como leads

**Frecuencia de Actualización:** Real-time/Daily
**Grain de la Tabla:** Una fila por visitor-lead match (puede haber múltiples matcheos)
**Total Registros:** See sql-queries.md → Query 1 for live count matches
**Atributos:** 21 campos de negocio + 5 audit

---

## Table Structure

### Sección 1: Identificación del Match (Campos 1-6)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 1 | **user_pseudo_id** | String | No | ID pseudoaletoreo del visitor desde GA4 (BigQuery). PK parte 1. Ej: "860188043.1757558455" |
| 2 | **client_id** | Integer/Float | Sí | ID del cliente/lead en Spot2 (legacy naming). Corresponde a lead_id. PK parte 2 |
| 3 | **match_source** | String | Sí | Origen del match: "Search", "Direct", "Referral", "Paid", "Social", "Email" |
| 4 | **with_match** | Integer | Sí | Flag binario: 1 si el visitor fue matcheado exitosamente con un lead, 0 si el match falló (pero llegó a ser lead) |

---

### Sección 2: Primera Interacción del Visitor (Campos 7-11)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 7 | **event_datetime_first** | DateTime | Sí | Fecha/hora de PRIMER evento en GA4 |
| 8 | **event_name_first** | String | Sí | Tipo de evento inicial: "page_view", "session_start", etc |
| 9 | **channel_first** | String | Sí | Canal de llegada inicial: "Paid Search", "Organic Search", "Direct", "Social", "Email", "Referral" |
| 10 | **source** | String | Sí | Fuente inicial: "google", "facebook", "direct", "instagram", etc |
| 11 | **medium** | String | Sí | Medio inicial: "cpc", "organic", "referral", "social", "email" |

---

### Sección 3: Última Interacción del Visitor (Campos 12-16)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 12 | **event_datetime** | DateTime | Sí | Fecha/hora del ÚLTIMO evento registrado en GA4 |
| 13 | **event_name** | String | Sí | Tipo de último evento |
| 14 | **channel** | String | Sí | Canal del último evento |
| 15 | **campaign_name** | String | Sí | Nombre de campaña: "MOFU_MRC_GADS_SRCH_LDS", "TOFU_BRD_FB_DSK", etc |

---

### Sección 4: Información de Lead Identificado (Campos 17-21)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 17 | **lead_min_at** | DateTime | Sí | Fecha/hora de cuando el visitor se convirtió en lead identificado |
| 18 | **lead_min_type** | String | Sí | Tipo de conversión a lead: "landing_form", "chat_initiated", "contact_clicked", etc |
| 19 | **lead_type** | String | Sí | Clasificación del lead: "Demand", "Supply", "Internal" |
| 20 | **lead_date** | Date | Sí | Fecha de creación del lead |
| 21 | **year_month_first** | String | Sí | Cohorte año-mes de primer evento: "2026-02", "2025-12", etc |

---

### Sección 5: Contacto del Lead (Campos 22-23)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 22 | **phone_number** | String | Sí | Número telefónico del lead (PII - dato sensible) |
| 23 | **email_clients** | String | Sí | Email del lead (PII - dato sensible) |

---

### Audit Columns (Campos 24-28)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 24 | **aud_inserted_at** | Timestamp | No | Timestamp de inserción original en Gold layer |
| 25 | **aud_updated_at** | Timestamp | Sí | Timestamp de última actualización |
| 26 | **aud_job** | String | No | Nombre del job Dagster: "gold_temp_matches_users_to_leads_new" |

---

## Propósito Real de la Tabla

### Problem Statement

En una empresa con **dual-sided marketplace** (oferta y demanda), es crítico entender:
- ¿Cómo llegó cada **lead convertido** a Spot2? (traffic source: organic, paid, referral, etc)
- ¿En qué momento se convirtió de visitor anónimo a lead identificado?
- ¿Qué canal de adquisición fue responsable de **cada conversión exitosa**?
- ¿Cuál es la correlación entre el tráfico inicial (GA4) y la conversión en lead (Spot2)?

**NO es sobre**: Todos los visitors (la mayoría no se conviertan). Es sobre los que SÍ se identificaron como leads.

### Solution

**lk_matches_visitors_to_leads** es el **puente de atribución para leads identificados**:

```
BigQuery (GA4)              →    lk_matches_visitors_to_leads    →    Postgres Spot2
─────────────────────────────────────────────────────────────────────────────────
visitor (pseudoID)               MATCH (solo conversiones)         lead (identified)
anonymous behavior              ATTRIBUTION                        registered user
[Muchos visitors]               [Algunos matched]                  [Leads convertidos]
                                    ↓
                        Preserva: canales, campaigns,
                        momento de conversión, atributos
```

**Flujo**:
1. **Visitor anónimo** navega Spot2, eventos registrados en GA4
2. **Visitor se identifica** al completar formulario/chat/contacto
3. **Lead creado** en Spot2 (client_id / lead_id)
4. **Match realizado**: vinculación visitor pseudoID ↔ lead_id
5. **Histórico preservado** en lk_matches_visitors_to_leads: visitor traffic + lead conversión

---

## Casos de Uso Analíticos

### 1. Attribution Analysis: ¿Qué canal trajo a cada lead?
```sql
SELECT
  channel_first,
  COUNT(DISTINCT user_pseudo_id) as visitors_acquired,
  COUNT(DISTINCT client_id) as leads_converted,
  ROUND(100.0 * COUNT(DISTINCT client_id) / COUNT(DISTINCT user_pseudo_id), 1) as conversion_rate_pct
FROM lk_matches_visitors_to_leads
WHERE with_match = 1
GROUP BY channel_first
ORDER BY visitors_acquired DESC;
```

### 2. Journey Analysis: Tiempo desde primer contacto a conversión en lead
```sql
SELECT
  ROUND(AVG(EXTRACT(EPOCH FROM (lead_min_at - event_datetime_first)) / 3600), 1) as avg_hours_to_lead,
  ROUND(AVG(EXTRACT(EPOCH FROM (lead_min_at - event_datetime_first)) / 86400), 1) as avg_days_to_lead,
  channel_first,
  COUNT(*) as leads
FROM lk_matches_visitors_to_leads
WHERE with_match = 1 AND lead_min_at IS NOT NULL
GROUP BY channel_first
ORDER BY avg_hours_to_lead ASC;
```

### 3. Campaign Performance: ROI por campaña
```sql
SELECT
  campaign_name,
  COUNT(DISTINCT user_pseudo_id) as total_visitors,
  COUNT(DISTINCT client_id) as leads_acquired,
  COUNT(DISTINCT CASE WHEN lead_type = 'Demand' THEN client_id END) as demand_leads,
  COUNT(DISTINCT CASE WHEN lead_type = 'Supply' THEN client_id END) as supply_leads
FROM lk_matches_visitors_to_leads
WHERE with_match = 1
GROUP BY campaign_name
ORDER BY leads_acquired DESC;
```

### 4. Cohort Analysis: Generaciones de leads por mes y su tráfico
```sql
SELECT
  year_month_first,
  source,
  COUNT(DISTINCT user_pseudo_id) as visitors,
  COUNT(DISTINCT client_id) as leads,
  COUNT(DISTINCT CASE WHEN event_datetime IS NOT NULL THEN client_id END) as with_recent_activity
FROM lk_matches_visitors_to_leads
WHERE with_match = 1
GROUP BY year_month_first, source
ORDER BY year_month_first DESC, visitors DESC;
```

---

## Key Metrics Derivables

| Métrica | Descripción | Fórmula |
|---------|----------|---------|
| **Visitor to Lead Rate** | % de visitors que se convirtieron en leads | COUNT(DISTINCT client_id) / COUNT(DISTINCT user_pseudo_id) |
| **Time to Lead** | Horas/días promedio hasta conversión | AVG(lead_min_at - event_datetime_first) |
| **Channel Attribution** | Mejor canal de adquisición | GROUP BY channel_first, ORDER BY leads DESC |
| **Funnel Efficiency** | % de leads adquiridos por canal | (Leads por canal / Total visitors) × 100 |

---

## Data Quality Checks

| Campo | Completitud | Validación | Notas |
|-------|-------------|-----------|-------|
| user_pseudo_id | 100% | No nullable (PK) | Siempre presente, de GA4 |
| client_id | 95% | Si with_match=1, debe existir | Algunos matches incompletos |
| channel_first | 85% | Valores controlados | Puede ser NULL si no hay atribución |
| lead_min_at | 80% | Debe ser > event_datetime_first | NULL si lead sin conversión desde GA4 |
| with_match | 100% | 0 o 1 | Flag binario de éxito del match |

---

## Related Tables

| Tabla | Join Key | Tipo Relación | Descripción |
|-------|----------|---------------|-------------|
| **lk_leads** | client_id = lead_id | Many-to-One | Info del lead identificado |
| **lk_visitors** | user_pseudo_id | One-to-One | Datos del visitor en GA4 |
| **lk_projects** | client_id = lead_id | One-to-Many | Proyectos creados por el lead |
| **bt_con_conversations** | client_id = lead_id | One-to-Many | Conversaciones del lead |
| **bt_lds_lead_spots** | client_id = lead_id | One-to-Many | Interacciones lead-spot |

---

## Notes & Considerations

- 🔗 **Bridge Crítica**: Único lugar donde vinculamos GA4 anónimo con Spot2 identificado
- 📊 **Atribución**: Fuente de verdad para entender qué canales traen leads calificados
- 🔒 **PII**: Contiene teléfono y email - nunca exponer en dashboards públicos
- ⏱️ **Latencia**: Match puede ocurrir días después del primer evento GA4
- 🎯 **Multiple Matches**: Un visitor puede tener múltiples matches (edge case raro)
- 📈 **Temporal Analysis**: Ideal para cohort analysis por mes/trimestre de adquisición
- 🚫 **Soft Delete**: No aplica (tabla de matching, no transaccional)

---

## Dagster Job

**Asset name**: `gold_temp_matches_users_to_leads_new`
**Schedule**: Daily (partition por fecha)
**Source**: `stg_gs_temp_matches_users_to_leads_new` (Silver layer)
**Output**: Parquet en S3 + load a PostgreSQL (`temp_matches_users_to_leads_governance`)

---

## Change Log

| Fecha | Cambio | Autor |
|-------|--------|-------|
| 2026-02-24 | Renombrada a lk_matches_visitors_to_leads + redocumentación | elouzau-spot2 |
| 2026-02-23 | Documentación inicial (lk_leads_matches - versión anterior) | elouzau-spot2 |
