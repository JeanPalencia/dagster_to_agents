# Data Lineage - De Dónde Vienen los Datos

## Objetivo
Documento que traza cómo los datos fluyen desde sistemas fuente hasta las tablas Golden, transformaciones aplicadas usando Medallion Architecture, y orquestación mediante Dagster.

**Referencia**: Data Governance (TO 012) - Medallion Architecture (Bronze, Silver, Gold)

---

## 📊 ARQUITECTURA GENERAL DE DATOS

```
┌──────────────────────────────────────────────────────────────────────┐
│                         FUENTES DE DATOS (ORIGEN)                    │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────┐  ┌──────────────────┐  ┌─────────────────────┐ │
│  │  MySQL          │  │  Google BigQuery │  │  Fuentes Externas   │ │
│  │  spot2_service  │  │  (GA4/Analytics) │  │  (HubSpot, Chatbot) │ │
│  │  (Transactional)│  │                  │  │                     │ │
│  └────────┬────────┘  └────────┬─────────┘  └────────┬────────────┘ │
│           │                    │                     │               │
└───────────┼────────────────────┼─────────────────────┼───────────────┘
            │                    │                     │
            └────────────────────┼─────────────────────┘
                                 │
          ┌──────────────────────┴──────────────────────┐
          ↓                                             ↓
    ┌─────────────┐                          ┌──────────────────┐
    │   Dagster   │                          │  PostgreSQL      │
    │(Orchestration) │◄──────────────────────►│  (Warehouse)     │
    └─────────────┘   Manages pipelines      └──────────────────┘
          │                                             │
          └──────────────────────┬──────────────────────┘
                                 │
        ┌────────────────────────┴────────────────────────┐
        │                                                 │
        ↓                                                 ↓
   ┌──────────────┐                            ┌─────────────────┐
   │  Metabase    │                            │   SQL Client    │
   │  (BI/Viz)    │                            │   (Queries)     │
   └──────────────┘                            └─────────────────┘
```

---

## 📥 FUENTES DE DATOS (ORIGEN)

### 1. MySQL - spot2_service (Transaccional Principal)
**Tipo**: Base transaccional operada por core-service
**Propósito**: Almacena todas las tablas del negocio

**Tablas Fuente** (en PostgreSQL warehouse, mapeadas como `public.*`):
```
Entidades Principales:
├─ clients        → Clientes/Usuarios (origen)
├─ users          → Usuarios registrados
├─ projects       → Proyectos/búsquedas
├─ spots          → Propiedades/ubicaciones
├─ analytics      → Datos analíticos
├─ conversations  → Interacciones chatbot
└─ transactions   → Transacciones/deals cerrados
```

**Características**:
- OLTP (optimizado para lecturas/escrituras)
- Datos actualizados en tiempo real
- Contiene datos de prueba (debe filtrar)

---

### 2. Google BigQuery (GA4 / Analytics Web)
**Fuente**: Google Analytics 4 Property
**Tipo**: Eventos web + atribución
**Actualización**: Daily batch (export automático)

**Datos**:
- Visitor pseudo IDs
- Session data, page views
- Campaign attribution
- Device/browser info

---

### 3. Fuentes Externas
**Incluyen**:
- HubSpot (CRM data)
- Chatbot (conversaciones)
- Otros servicios integrados

---

## 🏛️ MEDALLION ARCHITECTURE - Tres Capas

Nuestro data lakehouse usa el modelo Medallion: **Bronze → Silver → Gold**

### 🟤 Capa Bronze (Raw): Ingesta Directa
**Ubicación**: PostgreSQL `public.*` schema
**Propósito**: Ingesta sin modificaciones desde fuentes

```
MySQL spot2_service
    ↓ (Sync real-time/batch)
PostgreSQL public.* (Raw data, completo)
    • public.users
    • public.projects
    • public.spots
    • public.clients
    • public.conversations
    • public.analytics
    • public.transactions
```

**Características**:
- Datos crudos, sin transformación
- Pueden incluir duplicados
- Incluye test data
- Sirve como backup/auditoría

---

### 🟡 Capa Silver (Cleansed): Validación y Estandarización
**Ubicación**: PostgreSQL (schema específico para silver, ej: `silver.*` o similar)
**Propósito**: Datos filtrados, validados, estandarizados

**Transformaciones Aplicadas**:
```
Bronze (public.*)
    ↓ (Dagster job - Orquestado)
    ├─→ Remove duplicates (deduplicate by PK)
    ├─→ Remove test data (filter by domain)
    ├─→ Handle soft deletes (lead_deleted_at IS NULL)
    ├─→ Type casting + validation
    ├─→ Standardize values (upper/lower case, formats)
    └─→ Add audit timestamps
    ↓
Silver (schema.*)
    • silver_users
    • silver_projects
    • silver_spots
    • silver_conversations
    • silver_transactions
```

**Pilares del Método Medallion (aplicados en Silver)**:
- ✅ Limpieza: Sin duplicados, tipos correctos
- ✅ Validación: Reglas de negocio aplicadas
- ✅ Estandarización: Formatos consistentes

---

### 🟢 Capa Gold (Business): Optimizada para Análisis
**Ubicación**: PostgreSQL `public.lk_*` y `public.bt_*` (Golden Tables)
**Propósito**: Tablas optimizadas para negocio, el usuario final NO conoce complejidad previa

**Tablas Golden** (11 entidades):
```
LK_* (Lookup - Tablas de Referencia):
├─ lk_leads              → Catálogo de leads
├─ lk_projects           → Catálogo de proyectos
├─ lk_spots              → Catálogo de propiedades
├─ lk_users              → Catálogo de usuarios
├─ lk_leads_matches      → Matching engine results
└─ lk_okrs               → OKRs mensuales

BT_* (Business/Fact Tables - Eventos):
├─ bt_con_conversations  → Conversaciones chatbot
├─ bt_lds_lead_spots     → Lead-spot interactions
├─ bt_transactions       → Deals cerrados

DM_* (Dimension Tables - Atributos):
├─ [Aplica para aperturar datos]
```

**Transformaciones en Gold**:
```
Silver (validated data)
    ↓ (Dagster job)
    ├─→ Add business logic flags (lead_l0-l4)
    ├─→ Calculate metrics (engagement_score, conversion_stage)
    ├─→ Join multiple sources
    ├─→ Create surrogate keys
    ├─→ Add audit columns (AUD_INSERT_AT, AUD_UPDATE_AT, AUD_JOB)
    └─→ Optimize para queries frecuentes
    ↓
Gold (lk_*, bt_*, dm_*)
    • Listo para consumo en Metabase
    • Documentado y validado
    • Trazabilidad completa
```

**Audit Columns (Obligatorios en Gold)**:
```
AUD_INSERT_AT   → Timestamp de creación original
AUD_UPDATE_AT   → Timestamp de última modificación
AUD_JOB         → Nombre del job Dagster responsable
```

---

## 🔄 ETL PIPELINE - DAGSTER ORQUESTACIÓN

**Tool**: Dagster (Self-hosted)
**Propósito**: Orquestar transformaciones, validar SLAs, alertar errores

### Job Schedule en Dagster
```
├─ Sync Real-time:     Data desde MySQL/BigQuery (continuous)
├─ Silver Transforms:  Cada hora (validación + limpieza)
├─ Gold Transforms:    Cada hora (business logic)
├─ Full Refresh:       Diario 01:00 UTC (rebuild completo)
└─ Validaciones:       Continuous (quality checks, FK integrity)
```

### Monitoreo por Dagster
- ✅ Data freshness (alerta si > 2 horas sin actualizar)
- ✅ Row count anomaly detection (alerta si ± 20% cambio)
- ✅ Data quality assertions (no duplicados, no NULLs críticos)
- ✅ FK Integrity checks

---

## 📋 NOMENCLATURA (Data Governance)

### Prefijos de Tabla
| Prefijo | Tipo | Ejemplo | Propósito |
|---------|------|---------|-----------|
| `LK_` | Lookup | lk_leads, lk_spots | Tablas de referencia/catálogos maestros |
| `BT_` | Business/Fact | bt_transactions | Eventos de negocio, métricas cuantitativas |
| `DM_` | Dimension | dm_sector | Atributos descriptivos para aperturar datos |

### Prefijos de Columna
**Regla**: Campos inician con abreviación de tabla origen
```
lk_spots:
├─ spot_id          (PK)
├─ spot_name
├─ spot_type
└─ spot_category

Cuando spot_id aparece en bt_transactions:
├─ transaction_id   (PK)
├─ spot_id          (FK, conserva nombre igual → coherencia)
└─ transaction_amount
```

---

## 📊 DATA FLOW EJEMPLO: Lead → Deal

```
1. ORIGIN (MySQL spot2_service)
   clients/users table → "Maria Johnson signed up"

2. BRONZE (PostgreSQL public)
   public.users → Raw row ingested

3. SILVER (PostgreSQL silver schema)
   silver.users → Cleaned, duplicates removed, test data filtered

4. GOLD (PostgreSQL lk_leads)
   lk_leads → Business logic applied, lead_l0-l4 flags calculated
              Audit columns added, ready for Metabase

5. CONSUMPTION (Metabase)
   Dashboard: "Lead Performance by Sector"
```

**Dagster orquesta TODO y valida en cada paso.**

---

## ⚠️ DATOS IMPORTANTES

### Test Data
**Ubicación**: Filtrado en Silver
**Filtro**: `lead_domain NOT IN ('spot2.mx')`
**Impacto**: Significant volume removed - always verify counts after filtering

### Data Freshness
| Capa | Latencia | Frecuencia |
|------|----------|-----------|
| Bronze | < 5 min | Real-time |
| Silver | 1 hour | Cada hora |
| Gold | 1 hour | Cada hora |
| Full Refresh | 24 hours | Nightly 01:00 UTC |

### Cutoff Rule
**Siempre usar**: `CURRENT_DATE - INTERVAL '1 day'`
**Razón**: Datos incompletos de "hoy" están en progreso

---

## 🔗 REFERENCIAS

- **Data Governance**: TO 012 - Data Governance (Medallion Architecture, Prefijos, Nomenclatura)
- **Golden Tables**: Ver `golden_tables/` para schema detallado de cada tabla
- **Métricas**: Ver `metrics-definition.md` para definiciones de KPIs

---

**Última actualización**: 2026-02-24
**Versión**: 2.0.0 (Refactored con Medallion Architecture real)
**Next Review**: 2026-04-01
