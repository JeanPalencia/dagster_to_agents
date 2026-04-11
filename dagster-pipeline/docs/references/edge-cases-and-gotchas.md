# Edge Cases & Gotchas - Cuidados Especiales Spot2

## 🚨 CRITICALES - Errores que arruinan análisis

### 1. El filtro `lead_domain NOT IN ('spot2.mx')` es OBLIGATORIO
**Problema**: Si no lo aplicas, incluyes datos de test/demo

**Síntomas**:
- Números inflados
- Leads sin actividad real
- Análisis sesgado hacia interna

**Correcto**:
```sql
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
```

**Incorrecto**:
```sql
WHERE lead_deleted_at IS NULL  -- ❌ SIN FILTRO DOMAIN
```

**Impacto**: Puede inflacionar tus números sin filtrar datos de test

---

### 2. Soft Delete (`lead_deleted_at IS NOT NULL`) vs Físico
**Problema**: Los datos no se eliminan físicamente, solo se marcan como borrados

**Qué significa**:
- `lead_deleted_at IS NULL` → Lead activo
- `lead_deleted_at IS NOT NULL` → Lead marcado como borrado (pero datos quedan en tabla)

**Cuándo pasó**: Se usa soft-delete para auditoría

**Correcto**:
```sql
WHERE lead_deleted_at IS NULL  -- ✅ Solo activos
```

**Error común**:
```sql
WHERE lead_id NOT IN (SELECT lead_id FROM deleted_leads_table)  -- ❌ No existe tabla así
```

**Impacto**: Incluir leads eliminados distorsiona métricas de "active users"

---

### 3. Cutoff de "Ayer" - NO uses "hoy"
**Problema**: Los datos de hoy pueden estar incompletos (procesamiento en tiempo real)

**Regla**:
- ✅ Análisis debe usar datos hasta `CURRENT_DATE - INTERVAL '1 day'`
- ❌ NUNCA `CURRENT_DATE` o `TODAY()` para agregaciones finales

**Por qué**:
- Datos se ingieren en batch durante el día
- Lead creado a las 11:59 PM aparece mañana
- Mañana el lead tiene conversación, visita, LOI
- Si incluyes datos incompletos de hoy, los números cambian constantemente

**Correcto**:
```sql
WHERE CAST(lead_created_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day'
```

**Incorrecto**:
```sql
WHERE CAST(lead_created_date AS DATE) = CURRENT_DATE  -- ❌ Datos incompletos
```

**Impacto**: Reportes que varían día a día sin razón

---

### 4. Lead Supply vs Lead Demand - Flujos COMPLETAMENTE DIFERENTES
**Problema**: Mezclar supply y demand da números sin sentido

**Diferencias**:
| Atributo | DEMAND (Busca) | SUPPLY (Publica) |
|----------|----------------|------------------|
| Objetivo | Rentar/Comprar espacios | Publicar espacios |
| Funnel | Lead→Project→Visit→Won | Lead→User→Spot→Activity |
| Duración ciclo | Multiple months | Ongoing (indefinido) |
| "Won" significa | Cerró un arrendamiento | Spot está publicado |
| Conversión típica | Lower conversion rate | N/A |

**Cuándo filtrar**:
```sql
-- Si necesitas SOLO demanda
WHERE lead_supply = 0

-- Si necesitas SOLO oferta
WHERE lead_supply = 1

-- Si necesitas AMBOS, reporta separado
SELECT
  lead_supply,
  CASE WHEN lead_supply = 0 THEN 'Demand' ELSE 'Supply' END as lead_type,
  COUNT(DISTINCT lead_id) as leads
FROM lk_leads
GROUP BY lead_supply
```

**Impacto**: Mezclar pueden invalidar completamente un análisis

---

### 5. Spots "Complex" NO son comercializables
**Problema**: Algunos spots son "Complex" (torres, centros comerciales) y NO se venden/rentan directamente

**Cómo filtrar**:
```sql
-- ✅ CORRECTO - Solo comercializables
WHERE spot_type IN ('Subspace', 'Single')

-- ❌ INCORRECTO - Incluye complejos
WHERE spot_type IN ('Subspace', 'Single', 'Complex')
```

**Qué es cada tipo**:
- `Subspace`: Individual rentable dentro de complejo (local en mall)
- `Single`: Propiedad única rentable/vendible
- `Complex`: Edificio entero o centro comercial (NO comercializable como unidad)

**Impacto**: Incluir complejos inflama el catálogo de propiedades sin valor real

---

### 6. `project_won_date` vs `project_funnel_*` - Cuál usar?
**Problema**: Hay muchos campos de fecha y es fácil confundirse

**Claridad**:
```sql
-- El ÚNICO campo de cierre final
project_won_date  ← ✅ AQUÍ está la transacción completada

-- Estos son puntos INTERMEDIOS del funnel (opcionales)
project_funnel_visit_created_date    -- Visita solicitada
project_funnel_visit_confirmed_at    -- Visita confirmada
project_funnel_visit_realized_at     -- Visita realizada
project_funnel_loi_date              -- LOI emitida
project_funnel_contract_date         -- Contrato firmado
```

**Regla**:
- Para contar "deals ganados": `project_won_date IS NOT NULL`
- Para funnel intermedio: Usa los puntos específicos
- NUNCA sumes multiples `project_funnel_*` (un proyecto pasa por TODOS)

**Incorrecto**:
```sql
COUNT(DISTINCT CASE WHEN project_funnel_visit_realized_at IS NOT NULL THEN project_id END) +
COUNT(DISTINCT CASE WHEN project_funnel_loi_date IS NOT NULL THEN project_id END)
-- ❌ Doble conteo (mismo proyecto aparece en ambas)
```

**Correcto**:
```sql
SELECT
  COUNT(DISTINCT CASE WHEN project_funnel_visit_realized_at IS NOT NULL THEN project_id END) as visits_completed,
  COUNT(DISTINCT CASE WHEN project_funnel_loi_date IS NOT NULL THEN project_id END) as lois_issued,
  COUNT(DISTINCT CASE WHEN project_won_date IS NOT NULL THEN project_id END) as won_projects
FROM lk_projects
```

**Impacto**: Doble conteo invalida métricas de conversión

---

### 7. Lead Levels (L0-L4) son ACUMULATIVOS, no secuenciales
**Problema**: Pensar que L0→L1→L2→L3→L4 es una secuencia

**Realidad**:
- Un lead PUEDE tener `lead_l0=1 AND lead_l1=1 AND lead_l2=1` (tiene múltiples flags)
- Significado: "Se registró (L0) Y completó landing (L1) Y expresó interés en spot (L2)"
- Son CANALES de adquisición, no etapas secuenciales

**L0-L4 Definiciones Exactas**:
```
lead_l0 = 1: Creación de cuenta - El lead se registró
lead_l1 = 1: Landing profiling - Completó formulario de requerimientos
lead_l2 = 1: Spot Interested - Expresó interés en un espacio
lead_l3 = 1: Outbound contact - Lead contactado proactivamente por ventas
lead_l4 = 1: Treble source - Lead originado desde Trebble
```

**Cómo encontrar MÁXIMO nivel**:
```sql
CASE
  WHEN lead_l4 = 1 THEN 'L4'
  WHEN lead_l3 = 1 THEN 'L3'
  WHEN lead_l2 = 1 THEN 'L2'
  WHEN lead_l1 = 1 THEN 'L1'
  ELSE 'L0'
END as lead_max_level
```

**Incorrecto**:
```sql
-- ❌ Estos NO suman a 100% (son acumulativos)
SELECT
  COUNT(CASE WHEN lead_l0=1 THEN 1 END) as l0,
  COUNT(CASE WHEN lead_l1=1 THEN 1 END) as l1,
  COUNT(CASE WHEN lead_l2=1 THEN 1 END) as l2
FROM lk_leads
```

**Impacto**: Reportes que no suman a 100% confunden stakeholders

---

## ⚠️ COMUNES - Errores que comete la gente

### 8. Usar `DATE()` en WHERE en lugar de `CAST()`
**Problema**: `DATE()` no usa índices, queries son lentas

**Lento** ❌:
```sql
WHERE DATE(lead_created_date) = '2026-02-23'
```

**Rápido** ✅:
```sql
WHERE CAST(lead_created_date AS DATE) = '2026-02-23'
```

**O incluso mejor**:
```sql
WHERE lead_created_date >= '2026-02-23'::timestamp
  AND lead_created_date < '2026-02-24'::timestamp
```

**Impacto**: Query tarda 10x más

---

### 9. Olvidar que algunos campos son NULLABLE
**Problema**: Comparar con NULL usando `=` no funciona

**Incorrecto** ❌:
```sql
WHERE project_won_date = NULL  -- ❌ Nunca es true
```

**Correcto** ✅:
```sql
WHERE project_won_date IS NULL
```

**Regla**: Siempre usa `IS NULL` o `IS NOT NULL`

---

### 10. Usar `COUNT(*)` en lugar de `COUNT(DISTINCT lead_id)`
**Problema**: `COUNT(*)` cuenta filas, no leads únicos

**Incorrecto** ❌:
```sql
SELECT COUNT(*) as leads
FROM lk_leads
-- Si hay 3 filas dup, devuelve 3, no 1
```

**Correcto** ✅:
```sql
SELECT COUNT(DISTINCT lead_id) as leads
FROM lk_leads
```

**Impacto**: Doble o triple conteo de leads

---

### 11. Conversaciones (bt_con_conversations) pueden ser NULL en muchos campos
**Problema**: No todos los leads tienen conversaciones

**Seguro**:
```sql
LEFT JOIN bt_con_conversations c ON l.lead_id = c.lead_id
-- Si no hay conversación, las métricas de conv aparecen como NULL
```

**Malo**:
```sql
INNER JOIN bt_con_conversations c ON l.lead_id = c.lead_id
-- ❌ Excluye leads sin conversaciones
```

**Impacto**: Pierdes leads que no usaron chatbot

---

### 12. Transactions son MUY pocas - No esperes que todos cierren
**Problema**: Closed deals son una pequeña fracción del total de proyectos

**Realidad**:
- Mayoría de leads NO cierran
- Es normal
- No significa que el sistema no funcione

**Correcto**:
```sql
SELECT
  COUNT(DISTINCT lead_id) as total_leads,
  COUNT(DISTINCT CASE WHEN project_won_date IS NOT NULL THEN lead_id END) as leads_won,
  ROUND(100.0 * COUNT(...) / COUNT(DISTINCT lead_id), 1) as conversion_pct
FROM lk_leads
-- Calcula tu conversion rate real con Query 8 (Data Validation)
```

---

## 🔍 DATA QUALITY ISSUES

### 13. Datos de hoy en adelante CAMBIAN constantemente
**Contexto**: Los datos se ingieren en real-time y se actualizan

**Miércoles 10:00 AM**: Lead creado ayer como "L0"
**Miércoles 4:00 PM**: MISMO lead ahora es "L2" (hizo click en contactar)
**Jueves 9:00 AM**: MISMO lead ahora es "L3" (solicitó visita)

**Implicación**: Un lead "creado el miércoles" termina como L3, pero si lo consultaste a las 10:00 AM viste L0

**Solución**: Usa `CURRENT_DATE - INTERVAL '1 day'` como cutoff

---

### 14. Algunos campos están siempre NULL - No significa error
**Campos que frecuentemente son NULL**:
- `project_funnel_contract_date` (muchos proyectos no llegan a contrato)
- `conv_variables` (no todos chats extraen variables)
- `user_phone` (algunos usuarios no lo proporciona)

**OK si es NULL**: Es dato normal, no error

---

### 15. Matches (lk_leads_matches) pueden tener relevance_score = 0
**Problema**: A veces el algoritmo genera matches con baja relevancia

**Realidad**:
- Algunos matches tienen scores bajos (malos)
- Es normal
- Los usuarios ignoran estos matches

**No filtres**:
```sql
-- ❌ MAL - Sesgado
WHERE match_relevance_score >= 60

-- ✅ BIEN - Todos los matches
WHERE match_status = 'Active'
```

---

## 💡 BEST PRACTICES

### 16. Siempre valida tus números
**Patrón de debugging**:
```sql
SELECT
  'Total Leads' as metric,
  COUNT(DISTINCT lead_id) as count
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL

UNION ALL

SELECT
  'Leads with Projects' as metric,
  COUNT(DISTINCT l.lead_id) as count
FROM lk_leads l
JOIN lk_projects p ON l.lead_id = p.lead_id

UNION ALL

SELECT
  'Projects with Won' as metric,
  COUNT(DISTINCT project_id) as count
FROM lk_projects
WHERE project_won_date IS NOT NULL

ORDER BY count DESC;
```

---

### 17. Documenta filtros aplicados en tu query
```sql
SELECT
  COUNT(DISTINCT lead_id) as leads
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')      -- Excluye test/demo
  AND lead_deleted_at IS NULL               -- Solo activos
  AND CAST(lead_created_date AS DATE)
    <= CURRENT_DATE - INTERVAL '1 day';    -- Datos completos
-- ^ Aclaración: Filtros estándar aplicados
```

---

### 18. Familiarízate con los OKRs esperados
**OKRs mensuales** (definidos en tabla `lk_okrs`):
- Cantidad de Leads
- Cantidad de Proyectos
- Visitas agendadas
- Visitas completadas
- LOIs
- Transacciones

**Antes de analizar**: Consulta `lk_okrs.md` para targets oficiales del mes actual. Si tus números varían significativamente de los targets, investiga por qué.

---

## 🎯 CHECKLIST Pre-análisis

Antes de presentar cualquier métrica, verifica:

- [ ] ¿Incluí `lead_domain NOT IN ('spot2.mx')`?
- [ ] ¿Excluí leads eliminados (`lead_deleted_at IS NULL`)?
- [ ] ¿Usé `CAST()` no `DATE()` en WHERE?
- [ ] ¿Usé `COUNT(DISTINCT ...)` no `COUNT(*)`?
- [ ] ¿Mi cutoff es `CURRENT_DATE - INTERVAL '1 day'`?
- [ ] ¿Separé Supply vs Demand si aplica?
- [ ] ¿Validé que mis números tengan sentido?
- [ ] ¿Documenté qué filtros apliqué?
- [ ] ¿Mis totales suman correctamente (no doble-conté)?
- [ ] ¿He visto estos números antes (sanity check)?

---

## 📋 Tabla Rápida de Errores

| Error | Síntoma | Solución |
|-------|---------|----------|
| Sin `lead_domain NOT IN` | Números inflados (incluye test data) | Agregar filtro |
| Sin soft-delete filter | Incluye leads eliminados | `lead_deleted_at IS NULL` |
| Usar CURRENT_DATE | Reportes cambian día a día | `CURRENT_DATE - INTERVAL '1 day'` |
| Mezclar Supply/Demand | Números sin sentido | Separar por `lead_supply` |
| `COUNT(*)` vs DISTINCT | Doble conteo | Usar `COUNT(DISTINCT lead_id)` |
| `DATE()` en WHERE | Queries muy lentas | Usar `CAST()` |
| Doble conteo funnel | Suma invalida | Usar un ÚNICO punto de funnel |

---

**Última actualización**: 2026-02-24
**Versión**: 1.0.0
