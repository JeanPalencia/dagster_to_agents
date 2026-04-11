# lk_leads - Golden Table Documentation

## Overview

La tabla **lk_leads** es una tabla de referencia (Lookup Table) que almacena información de todos los leads (prospectos) del sistema Spot2. Cada lead representa un potencial cliente (inquilino, comprador, o usuario empresarial) que busca espacios inmobiliarios. La tabla captura información de contacto, nivel de cualificación (L0-L4), preferencias de búsqueda, historial de contacto, y estado actual del lead.

**Propósito Analítico:**
- Gestión y segmentación de leads por nivel de cualificación
- Análisis de fuentes de leads (origen, campaña)
- Seguimiento del funnel de ventas por lead
- Evaluación de calidad y engagement del lead
- Análisis de comportamiento de búsqueda por sector/tamaño
- Predictive analytics para propensión de conversión

**Frecuencia de Actualización:** Daily
**Grain de la Tabla:** Una fila por lead único
**Total Registros:** See `sql-queries.md` → Query 1 (Executive Summary) for live count
**Atributos:** 80 campos + 5 niveles de calificación (L0-L4)

---

## Table Structure

### Sección 1: Identificación & Contacto (Campos 1-11)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 1 | **lead_id** | Integer | No | Identificador único del lead. PK. Ej: 61957 |
| 2 | **lead_name** | String | Sí | Nombre del lead. Ej: "Ale" |
| 3 | **lead_last_name** | String | Sí | Apellido del lead. Ej: "Vilchis" |
| 4 | **lead_mothers_last_name** | String | Sí | Apellido materno (México). Generalmente NULL. |
| 5 | **lead_email** | String | Sí | Email del lead. Ej: ale.vilchis@gmail.com |
| 6 | **lead_domain** | String | Sí | Dominio del email. Ej: gmail.com |
| 7 | **lead_phone_indicator** | String | Sí | Indicador de país. Valor: "52" (México) |
| 8 | **lead_phone_number** | Float | Sí | Número telefónico sin indicador. Ej: 4425199195 |
| 9 | **lead_full_phone_number** | String | Sí | Número completo con formato internacional. Ej: "+52 4425199195" |
| 10 | **lead_company** | String | Sí | Nombre de la empresa (si es usuario empresarial). Generalmente NULL. |
| 11 | **lead_position** | String | Sí | Posición/cargo del lead en empresa. Generalmente NULL. |

### Sección 2: Lead Level & Qualificación (Campos 14-19)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 14 | **lead_l0** | Integer | No | Indicador L0 (Creación de cuenta): 0/1. El lead se registró. 10.9% son L0. |
| 15 | **lead_l1** | Integer | No | Indicador L1 (Landing profiling): 0/1. Completó formulario de requerimientos. 20.5% son L1. |
| 16 | **lead_l2** | Integer | No | Indicador L2 (Spot Interested): 0/1. Expresó interés en un espacio. 67.5% son L2 (nivel más común). |
| 17 | **lead_l3** | Integer | No | Indicador L3 (Outbound contact): 0/1. Lead contactado proactivamente por ventas. 1.1% alcanzan L3. |
| 18 | **lead_l4** | Integer | No | Indicador L4 (Treble source): 0/1. Lead originado desde Trebble. Raramente alcanzado (0%). |
| 19 | **lead_type** | String | No | Clasificación acumulativa por máximo nivel: 'L0', 'L1', 'L2', 'L3', 'L4'. Ej: Si lead_l3=1, entonces lead_type='L3'. Calculado automáticamente en Gold layer. |

**Niveles Explicados:**
- **L0**: Creación de cuenta - El lead se registró
- **L1**: Landing profiling, consultoría o click en modal 100 segundos - Completó formulario de requerimientos
- **L2**: Spot Interested - Expresó interés en un espacio
- **L3**: Outbound contact - Lead contactado proactivamente por ventas
- **L4**: Treble source - Lead originado desde Trebble

---

## Lead Levels Distribution
Distribución de leads por nivel de cualificación y origen:

| Nivel | Descripción | Typical Pattern |
|-------|---|---|
| **L0** | Leads que se hicieron leads por crear una cuenta en Spot2 | Smaller % (base cohort) |
| **L1** | Leads que convirtieron desde landings/campañas | Moderate % (filtered) |
| **L2** | Lead calificado, completó profiling, recibió recomendaciones | Largest % (majority qualified) |
| **L3** | Lead Outbound - contactados proactivamente por ventas | Small % (advanced stage) |
| **L4** | Lead Outbound - avanzado (LOI, contrato, cierre) | Minimal % (premium source) |

**For live L0-L4 distribution and counts**, run: `sql-queries.md` → Query 6 (Lead Level Distribution)

---

## Key Metrics (Live Counts)

**⚠️ Note**: The metrics below should be generated from live data, not estimated. Use these SQL queries to get current values:

| Métrica | Query Location | Description |
|---------|-----------------|-------------|
| **Total Leads** | sql-queries.md → Query 1 | COUNT(DISTINCT lead_id) with standard filters |
| **L0-L4 Distribution** | sql-queries.md → Query 6 | Lead Level Distribution by count and % |
| **Active vs Stopped** | sql-queries.md → Query 8 | Data Validation - Sanity Checks |
| **Leads with Projects** | sql-queries.md → Query 8 | Conversion rate from lead to project |
| **Leads to Won** | sql-queries.md → Query 8 | End-to-end conversion to closed deal |

Run these queries regularly to validate data quality and monitor trends.

---

## Data Quality Checks

| Campo | Completitud | Notas |
|-------|-------------|-------|
| lead_phone_number | 95% | Casi siempre presente |
| lead_email | 98% | Muy completo |
| lead_name | 99% | Casi siempre presente |
| lead_full_phone_number | 95% | Bien formateado |

---

## Related Tables

| Tabla | Join Key | Tipo Relación | Descripción |
|-------|----------|---------------|-------------|
| **lk_projects** | lead_id | One-to-Many | Un lead tiene múltiples proyectos |
| **lk_users** | user_id | Many-to-One | Broker asignado (si existe) |
| **bt_con_conversations** | lead_id | One-to-Many | Conversaciones del lead |
| **bt_lds_lead_spots** | lead_id | One-to-Many | Spots visitados/interactuados |

---

## Notes & Considerations

- 👥 **Lead Mix:** 67.5% L2 (calificados), 20.5% L1 (conversión), 10.9% L0 (registro)
- 🔄 **Progresión:** L0 → L1 (18.9%) → L2 (78.9%) → L3 (1.6%) → L4 (0%)
- 📊 **Active Status:** 24.6% "Stopped Replying" - oportunidad de re-engagement
- 📞 **Contacto:** 95% tienen teléfono, 98% tienen email
- 🔐 **Soft Delete:** lead_deleted_date != NULL significa eliminado lógico, pero datos preservados

---

## Change Log

| Fecha | Cambio | Autor |
|-------|--------|-------|
| 2026-02-23 | Documentación inicial creada | elouzau-spot2 |
