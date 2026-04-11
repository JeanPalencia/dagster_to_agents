# Métricas y Definiciones - Spot2

Documento comprehensivo que define todas las métricas del funnel de conversión, con visualizaciones del journey, conversion rates, y queries SQL listos para usar.

---

## 🎯 LEAD_TYPE: Clasificación por Nivel Máximo Alcanzado

### Definición

El campo **lead_type** se asigna automáticamente en la capa Gold (lk_leads) basándose en el **nivel máximo de cualificación alcanzado por el lead**. Es una clasificación jerárquica y acumulativa donde un lead progresa a través de los niveles L0 → L1 → L2 → L3 → L4.

**Principio**: Por definición, le asignamos el lead_type al máximo nivel alcanzado. Si un lead alcanzó L3, su lead_type es `L3` (no importa si también alcanzó L0, L1, L2).

### Regla de Asignación

```sql
CASE
  WHEN lead_l4 = 1 THEN 'L4'
  WHEN lead_l3 = 1 THEN 'L3'
  WHEN lead_l2 = 1 THEN 'L2'
  WHEN lead_l1 = 1 THEN 'L1'
  ELSE 'L0'
END AS lead_type
```

**Lógica**: Evaluamos en orden descendente (L4 → L3 → L2 → L1 → L0) y asignamos el primer nivel que sea igual a 1.

### Mapping de Niveles

| lead_type | Criterio | Descripción | Ejemplo de Lead |
|-----------|----------|-------------|-----------------|
| **L0** | lead_l4=0 AND lead_l3=0 AND lead_l2=0 AND lead_l1=0 AND lead_l0=1 | Creó cuenta pero sin profiling | Juan se registró, nunca completó formulario |
| **L1** | lead_l4=0 AND lead_l3=0 AND lead_l2=0 AND lead_l1=1 | Completó landing/profiling | María llenó formulario de requerimientos |
| **L2** | lead_l4=0 AND lead_l3=0 AND lead_l2=1 | Expresó interés en espacios | Pedro vio propiedades, hizo click en "Interesado" |
| **L3** | lead_l4=0 AND lead_l3=1 | Contacto outbound, propuesta | Ana recibió llamada de ventas, vio propuestas |
| **L4** | lead_l4=1 | Fuente Trebble, deal avanzado | Roberto llegó desde Trebble, close en progreso |

### Distribución Esperada

**Note**: Distribution percentages shown below are patterns. For live lead counts by level, see Query 6 (Lead Level Distribution) in sql-queries.md

| Nivel | Patrón Típico | Perfil |
|-------|---------------|--------|
| L0 | Small % of base cohort | Registrados sin acción |
| L1 | Moderate % after filtering | Completaron profiling |
| L2 | Majority qualified | Expresaron interés |
| L3 | Small % advanced stage | En etapa avanzada |
| L4 | Minimal % premium source | Fuente premium/cierre |

### SQL de Generación (Capa Gold - lk_leads)

```sql
SELECT
  lead_id,
  lead_name,
  lead_l0,
  lead_l1,
  lead_l2,
  lead_l3,
  lead_l4,
  -- Derivar lead_type basado en máximo nivel
  CASE
    WHEN lead_l4 = 1 THEN 'L4'
    WHEN lead_l3 = 1 THEN 'L3'
    WHEN lead_l2 = 1 THEN 'L2'
    WHEN lead_l1 = 1 THEN 'L1'
    ELSE 'L0'
  END AS lead_type,
  -- Audit columns
  AUD_INSERT_AT,
  AUD_UPDATE_AT,
  AUD_JOB
FROM silver_leads
WHERE lead_deleted_at IS NULL
  AND created_at >= CURRENT_DATE - INTERVAL '1 day'
```

### Casos de Uso en Análisis

#### 1. Segmentación Rápida por Tipo
```sql
-- Contar leads por tipo
SELECT
  lead_type,
  COUNT(*) as total,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
FROM lk_leads
WHERE lead_deleted_at IS NULL
GROUP BY lead_type
ORDER BY
  CASE lead_type
    WHEN 'L4' THEN 1
    WHEN 'L3' THEN 2
    WHEN 'L2' THEN 3
    WHEN 'L1' THEN 4
    ELSE 5
  END;
```

#### 2. Filtrar Solo "Calificados" (L2+)
```sql
-- Leads que alcanzaron mínimo L2
SELECT *
FROM lk_leads
WHERE lead_type IN ('L2', 'L3', 'L4')
  AND lead_deleted_at IS NULL;
```

#### 3. Progresión por Mes
```sql
-- ¿Cuántos leads por tipo, este mes?
SELECT
  lead_type,
  COUNT(*) as leads_count
FROM lk_leads
WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 day')
  AND lead_deleted_at IS NULL
GROUP BY lead_type;
```

### Notas Importantes

- ⚡ **Lead_type es READ-ONLY**: Se calcula automáticamente por Dagster en la capa Gold, no se modifica manualmente
- 🔄 **Acumulativo**: Un lead nunca "baja" de nivel. Si alcanzó L3 y luego no hay más actividad, sigue siendo L3
- 📊 **Métrica agregada**: Usar `lead_type` para reportes rápidos; usar campos individuales `lead_l0/l1/l2/l3/l4` para análisis detallado
- 🎯 **Preferencia analítica**: En dashboards, generalmente filtras por `lead_type IN ('L2','L3','L4')` para analizar leads "calificados"
- ✅ **Validación**: Dagster valida que el máximo nivel coincida con la suma acumulada (L4 ≥ L3 ≥ L2 ≥ L1 ≥ L0)

---

## 📊 METODOLOGÍA DE ANÁLISIS: Cohort vs Rolling

El dashboard se divide en **dos enfoques temporales distintos** para analizar la performance del funnel:

### 🔵 A. Dynamic Cohort (Cohorte Dinámica)

**Definición**: Agrupa a los Leads según su "momento de origen" o su "re-entrada" al ecosistema de Spot2.

**Regla de Pertenencia**: Un Lead se asigna a la cohorte del mes si cumple **CUALQUIERA** de estas dos condiciones:
- **Adquisición**: El Lead fue creado durante ese mes y año
- **Re-activación**: El Lead ya existía, pero volvió a tener actividad en ese mes tras un periodo de inactividad superior a 30 días

**Propósito**: Realizar un **análisis longitudinal** para observar cómo evoluciona un grupo específico de leads a lo largo del tiempo, independientemente de cuándo realicen sus actividades posteriores.

**Caso de uso**:
- "¿Cómo se comportan los leads creados en enero? ¿Cuántos eventualmente cierran deals?"
- Entender el lifetime value de un grupo de leads en el tiempo
- Identificar qué generaciones de leads son más productivas

**Ejemplo**:
- Lead creado el 2025-01-15 → Pertenece a cohorte de **Enero 2025**
- Lead creado el 2025-01-10 pero sin actividad hasta 2025-03-05 (>30 días) → También pertenece a cohorte de **Marzo 2025**

### 🟢 B. Rolling (Móvil/Dinámica Operativa)

**Definición**: Analiza la actividad ocurrida en un periodo de tiempo dinámico, **sin importar la fecha de creación del lead**.

**Propósito**: Evaluar el **volumen operativo actual** y la carga de trabajo reciente. Considera todos los eventos (creación, proyectos, visitas, etc.) que sucedieron dentro del rango seleccionado.

**Caso de uso**:
- "¿Cuántas visitas completadas tuvimos la semana pasada?"
- "¿Cuál es el volumen actual de deals en pipeline?"
- Monitorear operaciones en tiempo real
- Presupuestar recursos (ventas, brokers, etc.)

**Ejemplo**:
- Filtro: 2025-02-17 a 2025-02-23
- Cuento TODOS los leads que crearon proyectos en esa semana, sin importar cuándo se creó el lead (podría ser 2024-01-01)

### 📋 Matriz de Diferencias

| Aspecto | **Cohort Dinámico** | **Rolling** |
|---------|-------------------|-----------|
| **Punto de referencia** | Mes de origen del lead | Rango de fechas seleccionado |
| **Reactivación** | ✅ Sí (>30 días inactivo) | ❌ No, solo primer origen |
| **Horizonte** | Longitudinal (meses/años) | Temporal (semana/mes) |
| **Análisis típico** | Generaciones de leads, LTV | Operaciones, volumen actual |
| **Pregunta típica** | "¿Cómo evolucionó enero?" | "¿Cuántos deals hay ahora?" |

### 🎯 Comportamiento de Métricas por Metodología

Las métricas se calculan **diferente** según el enfoque:

#### **Métrica: Leads**
- **Cohort**: Conteo único de `lead_id` cuya fecha de creación O fecha de reactivación coincide con el periodo del Cohort
- **Rolling**: Conteo único de `lead_id` creados dentro del rango de fechas seleccionado

#### **Métrica: Projects**
- **Cohort**: Conteo único de `lead_id`/`project_id` (según visualización) del Cohort que generaron al menos un proyecto
- **Rolling**: Conteo único de `lead_id` que crearon proyectos dentro del rango de fechas seleccionado

#### **Métrica: Request Visits**
- **Cohort**: Conteo único de `lead_id`/`project_id` del Cohort que realizaron al menos una solicitud de visita
- **Rolling**: Conteo único de `lead_id` que solicitaron visitas dentro del rango de fechas seleccionado

#### **Métrica: Confirmed Visits**
- **Cohort**: Conteo único de `lead_id`/`project_id` del Cohort que obtuvieron al menos una confirmación de visita
- **Rolling**: Conteo único de `lead_id` con al menos una visita confirmada dentro del periodo seleccionado

#### **Métrica: Completed Visits**
- **Cohort**: Conteo único de `lead_id`/`project_id` del Cohort que realizaron al menos una visita
- **Rolling**: Conteo único de `lead_id` que realizaron al menos una visita dentro del rango de fechas seleccionado

#### **Métrica: LOIs**
- **Cohort**: Conteo único de `lead_id`/`project_id` del Cohort que emitieron al menos una Carta de Intención
- **Rolling**: Conteo único de `lead_id` que emitieron al menos una LOI dentro del periodo seleccionado

#### **Métrica: Contracts**
- **Cohort**: Conteo único de `lead_id`/`project_id` del Cohort que avanzaron a la etapa de contrato
- **Rolling**: Conteo único de `lead_id` que avanzaron a la etapa de contrato dentro del periodo seleccionado

#### **Métrica: Won**
- **Cohort**: Conteo único de `lead_id`/`project_id` del Cohort que cerraron una transacción exitosa
- **Rolling**: Conteo único de `lead_id` con transacciones cerradas dentro del periodo seleccionado

### 👁️ Vista por Leads vs. Vista por Proyectos

En la solapa **Cohort**, puedes alternar entre dos vistas:

**Vista por Leads** (Default):
- Las métricas se contabilizan por usuario
- En "Request Visits" → Total de leads que solicitaron **al menos una** visita

**Vista por Proyectos** (Al hacer click en Link):
- Las métricas se contabilizan por propiedad
- En "Request Visits" → Total de proyectos que recibieron **al menos una** solicitud de visita

---

## 🎯 FUNNEL PRINCIPAL: Lead → Deal Cerrado

```
┌─────────────────────────────────────────────────────────────┐
│ STAGE 0: VISITOR (Anónimo)                                  │
│ • Usuario llega a spot2.mx sin registrarse                 │
│ • Rastreado por GA4 (user_pseudo_id)                       │
│ • Puede navegar, abandonar o convertir                     │
└────────────────┬────────────────────────────────────────────┘
                 │
     ┌───────────┴──────────┬─────────────────┬──────────┐
     │                      │                 │          │
     ↓                      ↓                 ↓          ↓
[Abandona]    [Chat con Bot]  [Landing Click] [Directo]  [Email]
                    │                 │           │        │
                    └─────┬───────────┴───────────┴────────┘
                          │
        ┌─────────────────┴────────────────┐
        ↓                                  ↓
    [Lead L2]                        [Lead L1/L0]
    (chatbot)                        (sin chatbot)
        │                                 │
        └─────────────┬───────────────────┘
                      ↓
        ┌──────────────────────────────────┐
        │ STAGE 1: LEAD CREATED            │
        │ • Identificado (lead_id)         │
        │ • lead_l0 = 1 SIEMPRE            │
        │ • + otros flags (l1-l4)          │
        │ • Domain, sector, nivel captura  │
        └────────────────┬─────────────────┘
                         │
             ┌───────────┴───────────┐
             ↓                       ↓
        [Active]              [Inactivo/Deleted]
    (lead_deleted_at              (no incluir en
       IS NULL)                    análisis)
             │
             ↓
    ┌──────────────────────────────────┐
    │ STAGE 2: PROJECT CREATED         │
    │ • Lead abre búsqueda (project_id)│
    │ • Define criterios               │
    │ • project_created_at             │
    │                                  │
    │ Days to here: 0-30 (típico: 2-7) │
    └────────────────┬─────────────────┘
                     │
         ┌───────────┴────────────┐
         ↓                        ↓
    [Con spots]            [Sin spots]
    agregados              abandonado
         │
         ↓
    ┌──────────────────────────────────┐
    │ STAGE 3: VISIT REQUESTED         │
    │ • Lead solicita visita           │
    │ • project_visit_requested_date   │
    │                                  │
    │ Conversion: Variable by segment  │
    │ Days from project: 1-5           │
    └────────────────┬─────────────────┘
                     │
         ┌───────────┴────────────┐
         ↓                        ↓
    [Confirmada]            [Cancelada]
         │                    (no llega a visita)
         ↓
    ┌──────────────────────────────────┐
    │ STAGE 4: VISIT CONFIRMED         │
    │ • Lead confirma asistencia       │
    │ • project_visit_confirmed_at     │
    │                                  │
    │ Conversion: High % of requested  │
    └────────────────┬─────────────────┘
                     │
         ┌───────────┴────────────┐
         ↓                        ↓
    [Realizada]              [No show]
         │
         ↓
    ┌──────────────────────────────────┐
    │ STAGE 5: VISIT COMPLETED         │
    │ • Lead visitó la propiedad       │
    │ • project_visit_completed_date   │
    │                                  │
    │ Conversion: Most of confirmed    │
    │ Típicamente: Mismo día o +1      │
    └────────────────┬─────────────────┘
                     │
         ┌───────────┴────────────┐
         ↓                        ↓
    [Interesado]            [No interesado]
    negocia                 (funnel muere)
         │
         ↓
    ┌──────────────────────────────────┐
    │ STAGE 6: LOI (Letter of Intent)  │
    │ • Lead emite oferta formal       │
    │ • project_loi_date               │
    │                                  │
    │ Conversion: Moderate % of visits │
    │ Days from visit: 5-30            │
    │                                  │
    │ ⚠️ CRITICAL STAGE - Cambio legal │
    └────────────────┬─────────────────┘
                     │
         ┌───────────┴────────────┐
         ↓                        ↓
    [Aceptada]              [Rechazada]
         │
         ↓
    ┌──────────────────────────────────┐
    │ STAGE 7: CONTRACT SIGNED         │
    │ • Lead y seller firman contrato  │
    │ • project_contract_date          │
    │                                  │
    │ Conversion: Most of LOIs issued  │
    │ Days from LOI: 3-15              │
    │                                  │
    │ ✅ PUNTO SIN RETORNO             │
    └────────────────┬─────────────────┘
                     │
         ┌───────────┴────────────┐
         ↓                        ↓
    [Completado]            [Cancelado]
    (raro)
         │
         ↓
    ┌──────────────────────────────────┐
    │ STAGE 8: WON (Deal Cerrado)      │
    │ • Transacción completada         │
    │ • project_won_date               │
    │ • bt_transactions registrada     │
    │                                  │
    │ Conversion: Near 100% of signed  │
    │ Global: Calculate with Query 8   │
    │ Revenue: Comisión generada       │
    └──────────────────────────────────┘
```

---

## 📊 FUNNEL STRUCTURE (Por Etapa)

**⚠️ NOTE**: The following table shows STRUCTURE only. For live conversion rates and counts, use Query 8 (Data Validation - Sanity Checks) in sql-queries.md

| Etapa | Métrica SQL | Descripción | Días Típicos |
|-------|------------|-------------|--------------|
| **L0: Lead Created** | `COUNT(DISTINCT lead_id)` | Baseline - todos los leads | 0 |
| **L1: Project Created** | `COUNT(DISTINCT project_id)` | Lead inicia búsqueda | 0-30 |
| **L2: Visit Requested** | `project_visit_requested_date IS NOT NULL` | Lead solicita visualizar propiedad | 1-5 |
| **L3: Visit Confirmed** | `project_visit_confirmed_at IS NOT NULL` | Broker confirma asistencia | Variable |
| **L4: Visit Completed** | `project_visit_completed_date IS NOT NULL` | Lead visitó propiedad | 0-1 |
| **L5: LOI Issued** | `project_loi_date IS NOT NULL` | Lead emite oferta formal | 5-30 |
| **L6: Contract Signed** | `project_contract_date IS NOT NULL` | Contrato firmado | 3-15 |
| **L7: Won (Closed)** | `project_won_date IS NOT NULL` | Transacción completada | 0-7 |

**Global Conversion**: Calculate using Query 8 (Data Validation - Sanity Checks)

---

## 🔄 FUNNEL ALTERNATIVO: Lead → Activity (Sin Transacción)

No todos los leads cierran. Muchos:
- Miran propiedades pero no solicitan visita
- Solicitan pero cancelen
- Abandonan después de visita

```
┌──────────────────────────────────┐
│ LEAD CREATED (BASELINE)          │
│ See Query 8 for live count       │
└────────────┬─────────────────────┘
             │
      ┌──────┴──────┬───────────────┐
      ↓             ↓               ↓
   [Project]   [No Project]   [Deleted]
   [Created]   [Abandoned]    [Inactive]
      │
      ├─→ [Visit Requested] ─→ [Confirmed] ─→ [Completed] ─→ [LOI] ✅
      │
      ├─→ [No Visit Requested] ─→ [ABANDONO]
      │
      └─→ [Visit Requested but Not Confirmed] ─→ [ABANDONO]

✓ See Query 9 (Lead Cohort Analysis) for longitudinal conversion patterns
✓ See Query 10 (Conversation Impact) for driver analysis
```

---

## ⏱️ TIMELINE DEL JOURNEY (Días Típicos)

```
Day 0     → Lead Created (L0)
          → Campos capturados: domain, sector, contacto

Day 1-7   → Project Created (L1)
          → Promedio: 3 días

Day 8-12  → Visit Requested (L2)
          → Lead agrega spots y solicita

Day 13-15 → Visit Confirmed (L3)
          → Broker/seller confirma disponibilidad

Day 16-17 → Visit Completed (L4)
          → Lead visita la propiedad

Day 18-48 → LOI Issued (L5)
          → Negotiation period (5-30 días)
          → Average: 15 días

Day 49-63 → Contract Signed (L6)
          → Legal docs prepared (3-15 días)
          → Average: 7 días

Day 64-70 → Won / Deal Closed (L7)
          → Final signatures (0-7 días)
          → Revenue recognized
          → Commission calculated

TOTAL: Multiple weeks to months (varies by deal complexity)
```

---

## 🎯 CONVERSION RATES POR SECTOR

**Note**: Segment-level conversion rates vary. Use Query 5 (Sector Comparison) to calculate current rates per sector.

For each sector (Office, Retail, Industrial, Land, etc):
- Compare leads generated vs projects created
- Measure visit request conversion
- Calculate LOI and Won conversion rates
- Monitor trends month-over-month

---

## 👥 LEAD LEVEL PROGRESSION

```
     L0: Creación de cuenta - El lead se registró
     (Baseline - all leads)
         │
         ├─→ L1: Landing profiling - Completó formulario de requerimientos
         │   (Subset of leads - varies by month)
         │
         ├─→ L2: Spot Interested - Expresó interés en un espacio
         │   (Largest subset - majority qualified)
         │
         ├─→ L3: Outbound contact - Lead contactado proactivamente
         │   (Small subset - advanced stage)
         │
         └─→ L4: Treble source - Lead originado desde Trebble
             (Minimal subset - premium source)

⚠️ NOTA: Flags son ACUMULATIVOS, no secuenciales
   Un lead PUEDE ser L0+L1+L2 simultáneamente (múltiples canales)

Use Query 6 (Lead Level Distribution) for live L0-L4 counts and percentages
```

---

## 📈 KEY FUNNEL METRICS

### Conversion Rate by Stage

**Note**: All conversion rates vary by segment, month, and market conditions. Use live queries to calculate:

Conversion pathways to measure:
- Lead → Project: % of leads creating search
- Project → Visit Requested: % of projects requesting visits
- Visit Requested → Confirmed: % of requests confirmed
- Visit Confirmed → Completed: % of confirmations resulting in actual visits
- Visit Completed → LOI: % of visits leading to offers
- LOI → Contract: % of offers signed
- Contract → Won: % of contracts closing

CUMULATIVE: Lead → Won = Multiply all stage rates (varies significantly)

Use Query 9 (Lead Cohort Analysis) to calculate accurate cumulative conversion

### Velocity (Days between stages)
```
Lead → Project: 3 days (avg)
Project → Visit Request: 1 day
Visit Request → Completed: 3 days
Completed → LOI: 15 days
LOI → Contract: 7 days
Contract → Won: 1 day

TOTAL: Multiple weeks typical (varies significantly by deal complexity)
```

---

## 🚨 BOTTLENECKS (Donde se pierden leads)

| Bottleneck | Drop % | Reason | Action |
|-----------|--------|--------|--------|
| Lead → Project | 60.6% | No search interest | Nurture sequences |
| Project → Visit Req | 48.6% | Properties not matching | Improve matching algo |
| Visit Req → Confirmed | 15.9% | Broker no response | SLA enforcement |
| Visit Conf → Completed | 32.4% | No-shows | Reminder automation |
| Visit → LOI | 50% | Negotiation needed | Better support |
| LOI → Contract | 50% | Legal delays | Template acceleration |

---

## 💰 REVENUE FLOW

```
Deal Won (92 deals)
    │
    └─→ Transaction Value: $164.4M total
            │
            ├─→ Lease Revenue: $44.3M (annualized)
            │   • 200 lease deals @ avg $18.4K/month
            │
            └─→ Sale Revenue: $120.1M
                • 92 sale deals @ avg $1.3M per

Commission Calculation:
    │
    ├─→ Commission Rate: 3.2%
    ├─→ Total Commission Pool: $5.26M
    ├─→ Already Paid: $4.01M (76.3%)
    └─→ Pending: $1.25M (23.7%)

Top Earners (Top 3 brokers):
    • Luis García: $187K
    • María López: $156K
    • Carlos Rodríguez: $142K
```

---

## 🔗 RELATIONSHIP ENTRE FUNNELS

```
Visitor Funnel (anónimo)
    ↓
    ├─→ Abandona (bounce)
    │
    └─→ Chat con Bot (BT_CONVERSATION)
            ↓
            ├─→ Gets phone (proporciona contacto)
            │   ↓
            │   └─→ Lead Created (L2 típicamente)
            │
            └─→ No phone provided (no conversion)

Lead Funnel (identificado)
    ↓
    ├─→ Project Created (L1)
    │   ↓
    │   ├─→ Visit Requested (L2)
    │   │   ↓
    │   │   ├─→ Visit Completed (L4)
    │   │   │   ↓
    │   │   │   └─→ LOI (L5) → Contract (L6) → Won (L7) ✅
    │   │   │
    │   │   └─→ Cancelled
    │   │
    │   └─→ No Visit Request (abandono)
    │
    └─→ No Project Created (no interés)

Transaction Funnel (cierre)
    ↓
    └─→ BT_TRANSACTIONS
        └─→ Revenue registered + Commission calculated
```

---

# 📋 MÉTRICAS DETALLADAS CON QUERIES

---

## Métricas de Nivel de Lead

### 1. Leads (Conteo de Leads Únicos)

**Definición**: Cantidad de leads únicos en el período analizado.

**SQL Base**:
```sql
SELECT
  COUNT(DISTINCT lead_id) as leads
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
  AND CAST(lead_created_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Con Dimensiones**:
```sql
SELECT
  spot_sector,
  COALESCE(
    CASE
      WHEN lead_l4 THEN 'L4'
      WHEN lead_l3 THEN 'L3'
      WHEN lead_l2 THEN 'L2'
      WHEN lead_l1 THEN 'L1'
      ELSE 'L0'
    END,
    'L0'
  ) as lead_max_type,
  COUNT(DISTINCT lead_id) as leads
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY spot_sector, lead_max_type
ORDER BY spot_sector, lead_max_type;
```

**Interpretación**: Número de usuarios únicos que interactúan con la plataforma.

---

### 2. L0 Leads (Creación de Cuenta)

**Definición**: Leads que se registraron - Creación de cuenta.

**SQL**:
```sql
SELECT
  COUNT(DISTINCT lead_id) as l0_leads
FROM lk_leads
WHERE lead_l0 = TRUE
  AND lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
  AND CAST(lead_lead0_at AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Por Período**:
```sql
SELECT
  CAST(lead_lead0_at AS DATE) as creation_date,
  COUNT(DISTINCT lead_id) as l0_leads_created
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY CAST(lead_lead0_at AS DATE)
ORDER BY creation_date DESC;
```

---

### 3. L1 Leads (Landing Profiling)

**Definición**: Leads que completaron formulario de requerimientos en landing profiling o click en modal 100 segundos.

**SQL**:
```sql
SELECT
  COUNT(DISTINCT lead_id) as l1_leads
FROM lk_leads
WHERE lead_l1 = TRUE
  AND lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
  AND CAST(lead_lead1_at AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Atributos**:
- Marca transición desde "lead pasivo" a "lead activo"
- Primer indicador de engagement real

---

### 4. L2 Leads (Spot Interested)

**Definición**: Leads que han expresado interés en un espacio.

**SQL**:
```sql
SELECT
  COUNT(DISTINCT lead_id) as l2_leads
FROM lk_leads
WHERE lead_l2 = TRUE
  AND lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
  AND CAST(lead_lead2_at AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Por Sector**:
```sql
SELECT
  COALESCE(spot_sector, 'Retail') as spot_sector,
  COUNT(DISTINCT lead_id) as l2_leads,
  COUNT(DISTINCT lead_id) * 100.0 / SUM(COUNT(DISTINCT lead_id)) OVER ()
    as pct_of_total
FROM lk_leads
WHERE lead_l2 = TRUE
  AND lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY COALESCE(spot_sector, 'Retail')
ORDER BY l2_leads DESC;
```

**Interpretación**: Leads comprometidos que desean ver propiedades.

---

### 5. L3 Leads (Outbound Contact)

**Definición**: Leads contactados proactivamente por el equipo de ventas.

**SQL**:
```sql
SELECT
  COUNT(DISTINCT lead_id) as l3_leads
FROM lk_leads
WHERE lead_l3 = TRUE
  AND lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
  AND CAST(lead_lead3_at AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Duración Promedio L2 → L3**:
```sql
SELECT
  COALESCE(spot_sector, 'Retail') as spot_sector,
  COUNT(DISTINCT lead_id) as leads_with_loi,
  ROUND(AVG(DATE(lead_lead3_at) - DATE(lead_lead2_at)), 1) as avg_days_l2_to_l3,
  MIN(DATE(lead_lead3_at) - DATE(lead_lead2_at)) as min_days,
  MAX(DATE(lead_lead3_at) - DATE(lead_lead2_at)) as max_days
FROM lk_leads
WHERE lead_l3 = TRUE
  AND lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY COALESCE(spot_sector, 'Retail')
ORDER BY avg_days_l2_to_l3;
```

**Interpretación**: Leads con intención formal de compra.

---

### 6. L4 Leads (Treble Source)

**Definición**: Leads originados desde Trebble.

**SQL**:
```sql
SELECT
  COUNT(DISTINCT lead_id) as l4_leads
FROM lk_leads
WHERE lead_l4 = TRUE
  AND lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
  AND CAST(lead_lead4_at AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Interpretación**: Leads más cercanos a cierre o ya cerrados.

---

## Métricas de Proyectos (Properties)

### 7. Projects (Conteo de Proyectos)

**Definición**: Cantidad total de proyectos (propiedades que leads están considerando).

**SQL**:
```sql
SELECT
  COUNT(DISTINCT project_id) as total_projects
FROM lk_projects_v2
WHERE CAST(project_created_at AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Projects por Lead**:
```sql
SELECT
  COUNT(DISTINCT project_id) / COUNT(DISTINCT lead_id)::FLOAT as avg_projects_per_lead,
  COUNT(DISTINCT project_id) as total_projects,
  COUNT(DISTINCT lead_id) as total_leads
FROM lk_projects_v2 p
JOIN lk_leads l ON p.lead_id = l.lead_id
WHERE l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL;
```

**Projects por Sector**:
```sql
SELECT
  COALESCE(l.spot_sector, 'Retail') as spot_sector,
  COUNT(DISTINCT p.project_id) as projects
FROM lk_projects_v2 p
JOIN lk_leads l ON p.lead_id = l.lead_id
WHERE l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
GROUP BY COALESCE(l.spot_sector, 'Retail')
ORDER BY projects DESC;
```

---

### 8. Request Visits (Solicitudes de Visita)

**Definición**: Leads que han solicitado al menos una visita a una propiedad.

**SQL**:
```sql
SELECT
  COUNT(DISTINCT p.lead_id) as leads_requesting_visits
FROM lk_projects_v2 p
JOIN lk_leads l ON p.lead_id = l.lead_id
WHERE p.project_funnel_visit_created_date IS NOT NULL
  AND l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
  AND CAST(p.project_funnel_visit_created_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Visitas Solicitadas (count)**:
```sql
SELECT
  COUNT(*) as total_visit_requests
FROM lk_projects_v2
WHERE project_funnel_visit_created_date IS NOT NULL
  AND CAST(project_funnel_visit_created_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Interpretación**: Engagement profundo - leads que desean ver propiedades específicas.

---

### 9. Confirmed Visits

**Definición**: Leads con visitas confirmadas (lead confirmó asistencia).

**SQL**:
```sql
SELECT
  COUNT(DISTINCT p.lead_id) as leads_with_confirmed_visits
FROM lk_projects_v2 p
JOIN lk_leads l ON p.lead_id = l.lead_id
WHERE p.project_funnel_visit_confirmed_at IS NOT NULL
  AND l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
  AND CAST(p.project_funnel_visit_confirmed_at AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Tasa de Confirmación (% of Requested)**:
```sql
SELECT
  COUNT(DISTINCT CASE WHEN project_funnel_visit_confirmed_at IS NOT NULL THEN lead_id END)::FLOAT
  / COUNT(DISTINCT CASE WHEN project_funnel_visit_created_date IS NOT NULL THEN lead_id END)
  * 100 as confirmation_rate_pct
FROM lk_projects_v2 p
JOIN lk_leads l ON p.lead_id = l.lead_id
WHERE l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL;
```

---

### 10. Completed Visits (Visitas Realizadas)

**Definición**: Leads que han completado al menos una visita (llegaron a la propiedad).

**SQL**:
```sql
SELECT
  COUNT(DISTINCT p.lead_id) as leads_completed_visits
FROM lk_projects_v2 p
JOIN lk_leads l ON p.lead_id = l.lead_id
WHERE p.project_funnel_visit_realized_at IS NOT NULL
  AND l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
  AND CAST(p.project_funnel_visit_realized_at AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Visit Conversion Rate (Realized / Confirmed)**:
```sql
SELECT
  COUNT(DISTINCT CASE WHEN project_funnel_visit_realized_at IS NOT NULL THEN lead_id END)::FLOAT
  / COUNT(DISTINCT CASE WHEN project_funnel_visit_confirmed_at IS NOT NULL THEN lead_id END)
  * 100 as visit_realization_rate_pct
FROM lk_projects_v2 p
JOIN lk_leads l ON p.lead_id = l.lead_id
WHERE l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL;
```

---

### 11. LOIs (Cartas de Intención)

**Definición**: Leads que han emitido una Carta de Intención (Letter of Intent).

**SQL**:
```sql
SELECT
  COUNT(DISTINCT p.lead_id) as leads_with_loi
FROM lk_projects_v2 p
JOIN lk_leads l ON p.lead_id = l.lead_id
WHERE p.project_funnel_loi_date IS NOT NULL
  AND l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
  AND CAST(p.project_funnel_loi_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**LOI Count (cantidad de LOIs)**:
```sql
SELECT
  COUNT(*) as total_lois
FROM lk_projects_v2
WHERE project_funnel_loi_date IS NOT NULL
  AND CAST(project_funnel_loi_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Time to LOI desde visita**:
```sql
SELECT
  ROUND(AVG(project_funnel_loi_date - CAST(project_funnel_visit_realized_at AS DATE)), 1)
    as avg_days_visit_to_loi
FROM lk_projects_v2
WHERE project_funnel_visit_realized_at IS NOT NULL
  AND project_funnel_loi_date IS NOT NULL;
```

---

### 12. Contracts

**Definición**: Leads que han avanzado a etapa de contrato firmado.

**SQL**:
```sql
SELECT
  COUNT(DISTINCT p.lead_id) as leads_with_contract
FROM lk_projects_v2 p
JOIN lk_leads l ON p.lead_id = l.lead_id
WHERE p.project_funnel_contract_date IS NOT NULL
  AND l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
  AND CAST(p.project_funnel_contract_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Contract Count**:
```sql
SELECT
  COUNT(*) as total_contracts
FROM lk_projects_v2
WHERE project_funnel_contract_date IS NOT NULL
  AND CAST(project_funnel_contract_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

---

### 13. Won (Ganados/Cerrados)

**Definición**: Leads que han completado la transacción (proyecto ganado).

**SQL**:
```sql
SELECT
  COUNT(DISTINCT p.lead_id) as leads_won
FROM lk_projects_v2 p
JOIN lk_leads l ON p.lead_id = l.lead_id
WHERE p.project_won_date IS NOT NULL
  AND l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
  AND CAST(p.project_won_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Won Count**:
```sql
SELECT
  COUNT(*) as total_won_projects
FROM lk_projects_v2
WHERE project_won_date IS NOT NULL
  AND CAST(project_won_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Ciclo Completo: Lead Creation to Won**:
```sql
SELECT
  l.lead_id,
  l.lead_created_date,
  p.project_won_date,
  DATE(p.project_won_date) - l.lead_created_date as days_to_won,
  COALESCE(l.spot_sector, 'Retail') as spot_sector
FROM lk_leads l
JOIN lk_projects_v2 p ON l.lead_id = p.lead_id
WHERE p.project_won_date IS NOT NULL
  AND l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
ORDER BY l.lead_created_date DESC;
```

---

## Métricas de Conversión (Tasas)

### 14. L0 → L2 Conversion Rate

**Definición**: Porcentaje de leads L0 que eventualmente solicitan visita (L2).

**SQL**:
```sql
SELECT
  COUNT(DISTINCT CASE WHEN lead_l2 THEN lead_id END)::FLOAT
  / COUNT(DISTINCT CASE WHEN lead_l0 THEN lead_id END)
  * 100 as l0_to_l2_conversion_pct
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL;
```

**Por Sector**:
```sql
SELECT
  COALESCE(spot_sector, 'Retail') as spot_sector,
  COUNT(DISTINCT CASE WHEN lead_l2 THEN lead_id END)::FLOAT
    / COUNT(DISTINCT CASE WHEN lead_l0 THEN lead_id END)
    * 100 as l0_to_l2_conversion_pct
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY COALESCE(spot_sector, 'Retail')
ORDER BY l0_to_l2_conversion_pct DESC;
```

---

### 15. Visit Request → Realization Rate

**Definición**: Porcentaje de visitas solicitadas que fueron realizadas.

**SQL**:
```sql
SELECT
  COUNT(DISTINCT CASE WHEN project_funnel_visit_realized_at IS NOT NULL THEN project_id END)::FLOAT
  / COUNT(DISTINCT CASE WHEN project_funnel_visit_created_date IS NOT NULL THEN project_id END)
  * 100 as visit_realization_rate_pct
FROM lk_projects_v2;
```

---

### 16. LOI → Contract Rate

**Definición**: Porcentaje de LOIs que avanzan a contrato.

**SQL**:
```sql
SELECT
  COUNT(DISTINCT CASE WHEN project_funnel_contract_date IS NOT NULL THEN project_id END)::FLOAT
  / COUNT(DISTINCT CASE WHEN project_funnel_loi_date IS NOT NULL THEN project_id END)
  * 100 as loi_to_contract_rate_pct
FROM lk_projects_v2;
```

---

### 17. LOI → Won Rate

**Definición**: Porcentaje de LOIs que cierran (won).

**SQL**:
```sql
SELECT
  COUNT(DISTINCT CASE WHEN project_won_date IS NOT NULL THEN project_id END)::FLOAT
  / COUNT(DISTINCT CASE WHEN project_funnel_loi_date IS NOT NULL THEN project_id END)
  * 100 as loi_to_won_rate_pct
FROM lk_projects_v2;
```

---

## Métricas de Velocidad

### 18. Average Days to Next Stage

**L0 → L2**:
```sql
SELECT
  ROUND(AVG(DATE(lead_lead2_at) - lead_created_date), 1) as avg_days_l0_to_l2
FROM lk_leads
WHERE lead_l2 = TRUE
  AND lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL;
```

**Visit Request → Visit Realized**:
```sql
SELECT
  ROUND(AVG(CAST(project_funnel_visit_realized_at AS DATE)
    - project_funnel_visit_created_date), 1) as avg_days_request_to_realized
FROM lk_projects_v2
WHERE project_funnel_visit_created_date IS NOT NULL
  AND project_funnel_visit_realized_at IS NOT NULL;
```

---

## Resumen de Métricas por Categoría

| Categoría | Métricas |
|-----------|----------|
| **Lead Level** | L0, L1, L2, L3, L4 Leads |
| **Project Funnel** | Request Visits, Confirmed, Completed, LOI, Contracts, Won |
| **Conversion** | L0→L2, Visit Realization, LOI→Contract, LOI→Won |
| **Velocity** | Days to L2, Days to LOI, Days to Contract |

---

## Filtros Estándar a Aplicar

Todas las métricas deben aplicar:

```sql
WHERE lead_domain NOT IN ('spot2.mx')     -- Excluir test
  AND lead_deleted_at IS NULL              -- Excluir eliminados
  AND CAST(date_field AS DATE) <= CURRENT_DATE - INTERVAL '1 day'  -- Cutoff ayer
```

---

## 🔍 QUERY TEMPLATE: Full Funnel Analysis

```sql
WITH funnel_data AS (
  SELECT
    'L0_Leads' as stage,
    COUNT(DISTINCT l.lead_id) as count
  FROM lk_leads l
  WHERE l.lead_domain NOT IN ('spot2.mx')
    AND l.lead_deleted_at IS NULL

  UNION ALL

  SELECT 'L1_Projects' as stage,
    COUNT(DISTINCT p.project_id) as count
  FROM lk_projects p
  WHERE p.project_deleted_at IS NULL

  UNION ALL

  SELECT 'L2_Visit_Requested' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_visit_requested_date IS NOT NULL

  UNION ALL

  SELECT 'L3_Visit_Confirmed' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_visit_confirmed_at IS NOT NULL

  UNION ALL

  SELECT 'L4_Visit_Completed' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_visit_completed_date IS NOT NULL

  UNION ALL

  SELECT 'L5_LOI_Issued' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_loi_date IS NOT NULL

  UNION ALL

  SELECT 'L6_Contract_Signed' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_contract_date IS NOT NULL

  UNION ALL

  SELECT 'L7_Won' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_won_date IS NOT NULL
)
SELECT
  stage,
  count,
  LAG(count) OVER (ORDER BY stage) as prev_stage_count,
  ROUND(100.0 * count / LAG(count) OVER (ORDER BY stage), 1) as conversion_pct,
  ROUND(100.0 * count / (SELECT count FROM funnel_data WHERE stage = 'L0_Leads'), 1) as pct_of_total
FROM funnel_data
ORDER BY
  CASE stage
    WHEN 'L0_Leads' THEN 1
    WHEN 'L1_Projects' THEN 2
    WHEN 'L2_Visit_Requested' THEN 3
    WHEN 'L3_Visit_Confirmed' THEN 4
    WHEN 'L4_Visit_Completed' THEN 5
    WHEN 'L5_LOI_Issued' THEN 6
    WHEN 'L6_Contract_Signed' THEN 7
    WHEN 'L7_Won' THEN 8
  END;
```

---

## 🔍 QUERY TEMPLATES: Cohort vs Rolling

### 🔵 COHORT - Ejemplo: Contar Leads de una Cohorte

```sql
-- Leads de la cohorte de Enero 2025 (creados O reactivados en enero)
SELECT
  COUNT(DISTINCT lead_id) as cohort_leads
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
  AND (
    -- Condición A: Creados en enero 2025
    (CAST(lead_created_date AS DATE) >= '2025-01-01'
     AND CAST(lead_created_date AS DATE) < '2025-02-01')
    OR
    -- Condición B: Re-activados en enero 2025 (con >30 días de inactividad previo)
    (CAST(lead_last_activity_date AS DATE) >= '2025-01-01'
     AND CAST(lead_last_activity_date AS DATE) < '2025-02-01'
     AND CAST(lead_last_activity_date AS DATE) - CAST(lead_created_date AS DATE) > 30)
  );
```

### 🟢 ROLLING - Ejemplo: Contar Leads en un Rango de Fechas

```sql
-- Leads creados en el rango 2025-02-17 a 2025-02-23 (semana específica)
SELECT
  COUNT(DISTINCT lead_id) as rolling_leads
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
  AND CAST(lead_created_date AS DATE) >= '2025-02-17'
  AND CAST(lead_created_date AS DATE) <= '2025-02-23';
```

### 🔵 COHORT - Ejemplo: Projects de una Cohorte

```sql
-- Projects generados por leads de cohorte Enero 2025
WITH january_cohort AS (
  SELECT lead_id
  FROM lk_leads
  WHERE lead_domain NOT IN ('spot2.mx')
    AND lead_deleted_at IS NULL
    AND (
      CAST(lead_created_date AS DATE) >= '2025-01-01'
      AND CAST(lead_created_date AS DATE) < '2025-02-01'
    )
)
SELECT
  COUNT(DISTINCT project_id) as projects_by_cohort
FROM lk_projects p
WHERE p.project_deleted_at IS NULL
  AND p.lead_id IN (SELECT lead_id FROM january_cohort);
```

### 🟢 ROLLING - Ejemplo: Projects en un Rango de Fechas

```sql
-- Projects creados en el rango 2025-02-17 a 2025-02-23
SELECT
  COUNT(DISTINCT project_id) as projects_by_rolling
FROM lk_projects
WHERE project_deleted_at IS NULL
  AND CAST(project_created_at AS DATE) >= '2025-02-17'
  AND CAST(project_created_at AS DATE) <= '2025-02-23';
```

### 🔵 COHORT - Ejemplo: Análisis Longitudinal (Cómo Evolucionan los Leads)

```sql
-- Ver cómo evolucionan los leads de cada cohorte a través del funnel
WITH cohort_analysis AS (
  SELECT
    DATE_TRUNC('month', lead_created_date) as cohort_month,
    'Leads' as stage,
    COUNT(DISTINCT lead_id) as count
  FROM lk_leads
  WHERE lead_domain NOT IN ('spot2.mx')
    AND lead_deleted_at IS NULL
  GROUP BY DATE_TRUNC('month', lead_created_date)

  UNION ALL

  SELECT
    DATE_TRUNC('month', l.lead_created_date) as cohort_month,
    'Projects' as stage,
    COUNT(DISTINCT p.project_id) as count
  FROM lk_leads l
  JOIN lk_projects p ON l.lead_id = p.lead_id
  WHERE l.lead_domain NOT IN ('spot2.mx')
    AND l.lead_deleted_at IS NULL
    AND p.project_deleted_at IS NULL
  GROUP BY DATE_TRUNC('month', l.lead_created_date)

  UNION ALL

  SELECT
    DATE_TRUNC('month', l.lead_created_date) as cohort_month,
    'Won' as stage,
    COUNT(DISTINCT p.project_id) as count
  FROM lk_leads l
  JOIN lk_projects p ON l.lead_id = p.lead_id
  WHERE l.lead_domain NOT IN ('spot2.mx')
    AND l.lead_deleted_at IS NULL
    AND p.project_deleted_at IS NULL
    AND p.project_won_date IS NOT NULL
  GROUP BY DATE_TRUNC('month', l.lead_created_date)
)
SELECT
  cohort_month,
  stage,
  count
FROM cohort_analysis
ORDER BY cohort_month DESC,
  CASE stage WHEN 'Leads' THEN 1 WHEN 'Projects' THEN 2 WHEN 'Won' THEN 3 END;
```

---

## 📋 Definiciones Exactas de Lead Levels (L0-L4)

**Nota**: Los Lead Levels (L0-L4 en lk_leads) son canales de adquisición, NO etapas secuenciales del funnel.

### Lead Level L0: Creación de Cuenta
- **Definición**: El lead se registró
- **Indica**: Usuario nuevo creó cuenta en Spot2
- **Lead_l0 flag**: 0/1

### Lead Level L1: Landing Profiling
- **Definición**: Completó formulario de requerimientos en landing profiling o click en modal 100 segundos
- **Indica**: Lead vino por landing o profiling
- **Lead_l1 flag**: 0/1

### Lead Level L2: Spot Interested
- **Definición**: Expresó interés en un espacio
- **Indica**: Lead clic en contactar en detalle del spot
- **Lead_l2 flag**: 0/1

### Lead Level L3: Outbound Contact
- **Definición**: Lead contactado proactivamente por ventas
- **Indica**: Adquisición outbound
- **Lead_l3 flag**: 0/1

### Lead Level L4: Treble Source
- **Definición**: Lead originado desde Trebble
- **Indica**: Origen de tercera parte
- **Lead_l4 flag**: 0/1

---

## 📋 Etapas del Funnel (Project Progression)

Las siguientes etapas representan el progreso de un PROJECT a través del ciclo de ventas:

### Stage 1: Lead Created
- **SQL**: `COUNT(DISTINCT lead_id) WHERE lead_deleted_at IS NULL`

### Stage 2: Project Created
- **SQL**: `COUNT(DISTINCT project_id) WHERE project_deleted_at IS NULL`

### Stage 3: Visit Requested
- **SQL**: `project_visit_requested_date IS NOT NULL`

### Stage 4: Visit Confirmed
- **SQL**: `project_visit_confirmed_at IS NOT NULL`

### Stage 5: Visit Completed
- **SQL**: `project_visit_completed_date IS NOT NULL`

### Stage 6: LOI Issued
- **SQL**: `project_loi_date IS NOT NULL`

### Stage 7: Contract Signed
- **SQL**: `project_contract_date IS NOT NULL`

### Stage 8: Won
- **SQL**: `project_won_date IS NOT NULL`

---

**Última actualización**: 2026-02-24
**Versión**: 3.0.0
**Cambios**: Fusionado conversions-funnels.md + metrics.md en documento único con definiciones visuales + queries técnicas
