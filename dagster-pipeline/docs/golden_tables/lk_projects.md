# lk_projects - Golden Table Documentation

## Overview

La tabla **lk_projects** es una tabla de referencia (Lookup Table) que almacena todos los proyectos de búsqueda inmobiliaria registrados en Spot2. Cada proyecto representa una búsqueda activa de un lead con criterios específicos (ubicación, sector, precio, tamaño, etc.) y su progresión a través del funnel de ventas (proyecto → visita → LOI → contrato → transacción).

**Propósito Analítico:**
- Seguimiento de proyectos inmobiliarios por lead
- Análisis del funnel de conversión (proyecto → visita → LOI → contrato → ganado)
- Medición de engagement y tasa de cierre por proyecto
- Análisis de criterios de búsqueda (precio, tamaño, sector, estado)
- Segmentación de proyectos por estado (activo vs pausado vs ganado)
- Análisis de comportamiento de broker asignado

**Frecuencia de Actualización:** Daily
**Grain de la Tabla:** Una fila por proyecto único
**Atributos:** ~60 campos

---

## Table Structure

### Sección 1: Identificación (Campos principales)

| Campo | Tipo | Nullable | Descripción |
|-------|------|----------|-------------|
| **project_id** | Integer | No | Identificador único del proyecto. PK |
| **project_name** | String | Sí | Nombre del proyecto asignado por sistema o usuario |
| **lead_id** | Integer | No | FK a lk_leads. Lead propietario del proyecto |
| **user_id** | Integer | No | FK a lk_users. Broker/agente asignado |

### Sección 2: Criterios de Búsqueda (Parámetros del proyecto)

| Campo | Tipo | Nullable | Descripción |
|-------|------|----------|-------------|
| **spot_sector** | String | Sí | Sector buscado: "Retail", "Industrial", "Office", "Land" |
| **project_min_square_space** | Float | Sí | Tamaño mínimo requerido en m² |
| **project_max_square_space** | Float | Sí | Tamaño máximo requerido en m² |
| **project_min_rent_price** | Float | Sí | Precio de renta mínimo deseado |
| **project_max_rent_price** | Float | Sí | Precio de renta máximo deseado |
| **project_min_sale_price** | Float | Sí | Precio de compra mínimo deseado |
| **project_max_sale_price** | Float | Sí | Precio de compra máximo deseado |
| **project_state_ids** | Array | Sí | IDs de estados donde busca (puede ser múltiple) |
| **project_rent_months** | Integer | Sí | Plazo de arrendamiento deseado en meses |
| **lead_profiling_completed_at** | DateTime | Sí | Cuándo lead completó el perfil de requerimientos |

### Sección 3: Estado & Clasificación

| Campo | Tipo | Nullable | Descripción |
|-------|------|----------|-------------|
| **project_enable** | String | Sí | Estado activo: "Yes" o "No" |
| **project_disable_reason** | String | Sí | Razón de deshabilitación (si proyecto_enable = 'No') |
| **project_funnel_relevant** | String | Sí | ¿Es relevante para el funnel? "Yes" o "No" |
| **project_funnel_relevant_reason** | String | Sí | Razón de relevancia en el funnel |
| **lead_campaign_type** | String | Sí | Tipo de campaña: "Organic", "Paid", etc |
| **lead_max_type** | String | Sí | Tipo de lead máximo (L0-L4) |

### Sección 4: Funnel de Conversión (Etapas del proyecto)

| Campo | Tipo | Nullable | Descripción |
|-------|------|----------|-------------|
| **project_funnel_lead_date** | Date | Sí | Fecha cuando el lead creó el proyecto |
| **project_funnel_visit_created_date** | Date | Sí | Fecha cuando se solicitó una visita |
| **project_funnel_visit_confirmed_at** | Date | Sí | Fecha cuando visita fue confirmada |
| **project_funnel_visit_realized_at** | Date | Sí | Fecha cuando visita fue realizada |
| **project_funnel_visit_status** | String | Sí | Estado de la visita: "Visited", "NoClientAnswerRejected", "UnsuitableSpotRejected", etc |
| **project_funnel_loi_date** | Date | Sí | Fecha cuando LOI (Letter of Intent) fue emitida |
| **project_funnel_contract_date** | Date | Sí | Fecha cuando contrato fue firmado |
| **project_funnel_transaction_date** | Date | Sí | Fecha cuando transacción fue completada |
| **project_won_date** | Date | Sí | Fecha cuando proyecto fue ganado/cerrado |

### Sección 5: Últimas Etapas (Tracking)

| Campo | Tipo | Nullable | Descripción |
|-------|------|----------|-------------|
| **project_last_spot_stage** | String | Sí | Última etapa alcanzada: "Project", "Visit Completed", "Won", etc |
| **project_last_spot_stage_id** | Integer | Sí | ID de la última etapa |
| **project_funnel_flow** | String | Sí | Flujo de eventos en el funnel |
| **project_funnel_events** | Array | Sí | Array de eventos ocurridos (ej: ["whatsappInteraction", "accountCreation"]) |

### Sección 6: Financiero

| Campo | Tipo | Nullable | Descripción |
|-------|------|----------|-------------|
| **project_commission** | Float | Sí | Comisión estimada del proyecto |
| **spot_price_sqm_mxn_rent_min** | Float | Sí | Precio mínimo por m² para renta (MXN) |
| **spot_price_sqm_mxn_rent_max** | Float | Sí | Precio máximo por m² para renta (MXN) |
| **spot_price_sqm_mxn_sale_min** | Float | Sí | Precio mínimo por m² para venta (MXN) |
| **spot_price_sqm_mxn_sale_max** | Float | Sí | Precio máximo por m² para venta (MXN) |

### Sección 7: Administración & Auditoría

| Campo | Tipo | Nullable | Descripción |
|-------|------|----------|-------------|
| **project_created_at** | DateTime | No | Fecha de creación exacta del registro |
| **project_created_date** | Date | No | Fecha de creación (solo fecha) |
| **project_updated_at** | DateTime | No | Fecha de última actualización |
| **project_updated_date** | Date | No | Fecha de última actualización (solo fecha) |
| **aud_inserted_at** | DateTime | No | Timestamp de inserción en Gold layer |
| **aud_inserted_date** | Date | No | Fecha de inserción en Gold layer |
| **aud_updated_at** | DateTime | No | Timestamp de última actualización en Gold |
| **aud_updated_date** | Date | No | Fecha de última actualización en Gold |
| **aud_job** | String | No | Nombre del job Dagster que cargó el dato |

---

## Related Tables

| Tabla | Join Key | Tipo Relación | Descripción |
|-------|----------|---------------|-------------|
| **lk_leads** | lead_id | Many-to-One | Lead propietario del proyecto |
| **lk_users** | user_id | Many-to-One | Broker/agente asignado |
| **lk_spots** | (via spot_sector) | Many-to-Many | Spots considerados en el proyecto |
| **bt_lds_lead_spots** | lead_id | Many-to-One | Interacciones lead-spot en este proyecto |
| **bt_transactions** | project_id | One-to-One | Transacción resultante (si ganó) |

---

## Data Quality Notes

- **project_funnel_* fields**: Pueden ser NULL si el proyecto no ha avanzado a esa etapa
- **project_disable_reason**: Solo tiene valor si project_enable = 'No'
- **project_state_ids**: Array - puede contener múltiples estados si lead busca en varias regiones
- **Audit fields**: aud_inserted_at es INMUTABLE, aud_updated_at cambia en cada ETL

---

## Related Queries

- Query 1: Executive Summary - cuenta total de proyectos
- Query 3: Full Funnel - análisis de conversión por etapa
- Query 5: Sector Comparison - proyectos y conversión por sector
- Query 8: Data Validation - sanity checks de proyectos

---

## Change Log

| Fecha | Cambio | Autor |
|-------|--------|-------|
| 2026-02-24 | Actualización completa basada en muestra real lk_projects_sample.csv | elouzau-spot2 |
| 2026-02-23 | Documentación inicial creada | elouzau-spot2 |
