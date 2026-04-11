# SQL Queries - Spot2 Data Lakehouse

Documento consolidado con **queries completas** (copy-paste listas) y **reusable patterns** (building blocks adaptables).

---

## 📋 Cómo Usar Este Archivo

**Sección 1-10**: Queries COMPLETAS - Copia directamente, adapta minimalmente (período, sector)
**Sección 11-20**: Reusable PATTERNS - Adapta y combina para queries custom

---

## 🔍 SECCIÓN 1: QUERIES COMPLETAS - LISTAS PARA USAR

---

## 📊 DASHBOARD TEMPLATES

### Query 1: Executive Summary Dashboard
**Uso**: KPI dashboard para CEO/Fundadores
**Frecuencia**: Diaria

```sql
-- EXECUTIVE DASHBOARD - Daily Snapshot
SELECT
  -- Period
  CAST(CURRENT_DATE - INTERVAL '1 day' AS DATE) as report_date,

  -- Leads
  (SELECT COUNT(DISTINCT lead_id) FROM lk_leads
   WHERE CAST(lead_created_date AS DATE) = CURRENT_DATE - INTERVAL '1 day'
   AND lead_domain NOT IN ('spot2.mx')
   AND lead_deleted_at IS NULL) as new_leads_yesterday,

  -- Projects
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects
   WHERE CAST(project_created_at AS DATE) = CURRENT_DATE - INTERVAL '1 day'
   AND project_deleted_at IS NULL) as new_projects_yesterday,

  -- Visits
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects
   WHERE CAST(project_visit_completed_date AS DATE) = CURRENT_DATE - INTERVAL '1 day') as visits_completed_yesterday,

  -- LOIs
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects
   WHERE CAST(project_loi_date AS DATE) = CURRENT_DATE - INTERVAL '1 day') as lois_yesterday,

  -- Won
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects
   WHERE CAST(project_won_date AS DATE) = CURRENT_DATE - INTERVAL '1 day') as won_yesterday,

  -- Revenue
  (SELECT SUM(transaction_amount) FROM bt_transactions
   WHERE CAST(transaction_date AS DATE) = CURRENT_DATE - INTERVAL '1 day') as revenue_yesterday_mxn;
```

---

### Query 2: Monthly Performance vs OKR
**Uso**: Mensual para junta directiva
**Frecuencia**: Monthly

```sql
-- MONTHLY PERFORMANCE VS OKR
WITH current_month_data AS (
  SELECT
    DATE_TRUNC('month', CURRENT_DATE) as month,
    -- Leads
    (SELECT COUNT(DISTINCT lead_id) FROM lk_leads
     WHERE CAST(lead_created_date AS DATE)
       BETWEEN DATE_TRUNC('month', CURRENT_DATE)
       AND CURRENT_DATE - INTERVAL '1 day'
     AND lead_domain NOT IN ('spot2.mx')
     AND lead_deleted_at IS NULL) as leads_mtd,

    -- Projects
    (SELECT COUNT(DISTINCT project_id) FROM lk_projects
     WHERE CAST(project_created_at AS DATE)
       BETWEEN DATE_TRUNC('month', CURRENT_DATE)
       AND CURRENT_DATE - INTERVAL '1 day'
     AND project_deleted_at IS NULL) as projects_mtd,

    -- Visits Completed
    (SELECT COUNT(DISTINCT project_id) FROM lk_projects
     WHERE CAST(project_visit_completed_date AS DATE)
       BETWEEN DATE_TRUNC('month', CURRENT_DATE)
       AND CURRENT_DATE - INTERVAL '1 day') as visits_completed_mtd,

    -- LOIs
    (SELECT COUNT(DISTINCT project_id) FROM lk_projects
     WHERE CAST(project_loi_date AS DATE)
       BETWEEN DATE_TRUNC('month', CURRENT_DATE)
       AND CURRENT_DATE - INTERVAL '1 day') as lois_mtd,

    -- Revenue
    (SELECT COALESCE(SUM(transaction_amount), 0) FROM bt_transactions
     WHERE CAST(transaction_date AS DATE)
       BETWEEN DATE_TRUNC('month', CURRENT_DATE)
       AND CURRENT_DATE - INTERVAL '1 day') as revenue_mtd
)
SELECT
  cmd.month,
  cmd.leads_mtd,
  COALESCE(o_leads.okr_leads, 450) as target_leads,
  ROUND(100.0 * cmd.leads_mtd / COALESCE(o_leads.okr_leads, 450), 1) as leads_achievement_pct,

  cmd.projects_mtd,
  COALESCE(o_proj.okr_projects, 550) as target_projects,
  ROUND(100.0 * cmd.projects_mtd / COALESCE(o_proj.okr_projects, 550), 1) as projects_achievement_pct,

  cmd.visits_completed_mtd,
  COALESCE(o_visits.okr_completed_visits, 120) as target_visits,
  ROUND(100.0 * cmd.visits_completed_mtd / COALESCE(o_visits.okr_completed_visits, 120), 1) as visits_achievement_pct,

  cmd.lois_mtd,
  cmd.revenue_mtd
FROM current_month_data cmd
LEFT JOIN lk_okrs o_leads ON DATE_TRUNC('month', o_leads.okr_month_start_ts) = cmd.month
LEFT JOIN lk_okrs o_proj ON DATE_TRUNC('month', o_proj.okr_month_start_ts) = cmd.month
LEFT JOIN lk_okrs o_visits ON DATE_TRUNC('month', o_visits.okr_month_start_ts) = cmd.month;
```

---

### Query 3: Full Funnel - Last 7 Days
**Uso**: Ver trend corto plazo
**Frecuencia**: Diaria

```sql
-- FUNNEL - LAST 7 DAYS
SELECT
  'Leads' as stage,
  COUNT(DISTINCT lead_id) as count,
  0 as prev_count,
  100.0 as conversion_rate_pct
FROM lk_leads
WHERE CAST(lead_created_date AS DATE)
  BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'
AND lead_domain NOT IN ('spot2.mx')
AND lead_deleted_at IS NULL

UNION ALL

SELECT
  'Projects' as stage,
  COUNT(DISTINCT project_id) as count,
  (SELECT COUNT(DISTINCT lead_id) FROM lk_leads
   WHERE CAST(lead_created_date AS DATE)
     BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'
   AND lead_domain NOT IN ('spot2.mx')
   AND lead_deleted_at IS NULL) as prev_count,
  ROUND(100.0 * COUNT(DISTINCT project_id) /
    (SELECT COUNT(DISTINCT lead_id) FROM lk_leads
     WHERE CAST(lead_created_date AS DATE)
       BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'
     AND lead_domain NOT IN ('spot2.mx')
     AND lead_deleted_at IS NULL), 1) as conversion_rate_pct
FROM lk_projects
WHERE CAST(project_created_at AS DATE)
  BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'
AND project_deleted_at IS NULL

UNION ALL

SELECT
  'Visit Requested' as stage,
  COUNT(DISTINCT project_id) as count,
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects
   WHERE CAST(project_created_at AS DATE)
     BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'
   AND project_deleted_at IS NULL) as prev_count,
  ROUND(100.0 * COUNT(DISTINCT project_id) /
    (SELECT COUNT(DISTINCT project_id) FROM lk_projects
     WHERE CAST(project_created_at AS DATE)
       BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'
     AND project_deleted_at IS NULL), 1) as conversion_rate_pct
FROM lk_projects
WHERE CAST(project_funnel_visit_created_date AS DATE)
  BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'

UNION ALL

SELECT
  'Visit Completed' as stage,
  COUNT(DISTINCT project_id) as count,
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects
   WHERE project_funnel_visit_created_date IS NOT NULL) as prev_count,
  ROUND(100.0 * COUNT(DISTINCT project_id) /
    (SELECT COUNT(DISTINCT project_id) FROM lk_projects
     WHERE project_funnel_visit_created_date IS NOT NULL), 1) as conversion_rate_pct
FROM lk_projects
WHERE CAST(project_funnel_visit_realized_at AS DATE)
  BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'

UNION ALL

SELECT
  'LOI Issued' as stage,
  COUNT(DISTINCT project_id) as count,
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects
   WHERE project_funnel_visit_realized_at IS NOT NULL) as prev_count,
  ROUND(100.0 * COUNT(DISTINCT project_id) /
    (SELECT COUNT(DISTINCT project_id) FROM lk_projects
     WHERE project_funnel_visit_realized_at IS NOT NULL), 1) as conversion_rate_pct
FROM lk_projects
WHERE CAST(project_funnel_loi_date AS DATE)
  BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'

UNION ALL

SELECT
  'Won' as stage,
  COUNT(DISTINCT project_id) as count,
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects
   WHERE project_funnel_loi_date IS NOT NULL) as prev_count,
  ROUND(100.0 * COUNT(DISTINCT project_id) /
    (SELECT COUNT(DISTINCT project_id) FROM lk_projects
     WHERE project_funnel_loi_date IS NOT NULL), 1) as conversion_rate_pct
FROM lk_projects
WHERE CAST(project_won_date AS DATE)
  BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'
ORDER BY
  CASE stage
    WHEN 'Leads' THEN 1
    WHEN 'Projects' THEN 2
    WHEN 'Visit Requested' THEN 3
    WHEN 'Visit Completed' THEN 4
    WHEN 'LOI Issued' THEN 5
    WHEN 'Won' THEN 6
  END;
```

---

### Query 4: Weekly Trend - All Metrics
**Uso**: Ver semana a semana
**Frecuencia**: Semanal

```sql
-- WEEKLY TRENDS - LAST 8 WEEKS
SELECT
  DATE_TRUNC('week', CAST(lead_created_date AS DATE)) as week_start,
  COUNT(DISTINCT lead_id) as leads,
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects p
   WHERE DATE_TRUNC('week', CAST(p.project_created_at AS DATE))
     = DATE_TRUNC('week', CAST(l.lead_created_date AS DATE))
   AND p.project_deleted_at IS NULL) as projects,
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects p
   WHERE DATE_TRUNC('week', CAST(p.project_funnel_visit_realized_at AS DATE))
     = DATE_TRUNC('week', CAST(l.lead_created_date AS DATE))) as visits_completed,
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects p
   WHERE DATE_TRUNC('week', CAST(p.project_funnel_loi_date AS DATE))
     = DATE_TRUNC('week', CAST(l.lead_created_date AS DATE))) as lois,
  (SELECT COUNT(DISTINCT project_id) FROM lk_projects p
   WHERE DATE_TRUNC('week', CAST(p.project_won_date AS DATE))
     = DATE_TRUNC('week', CAST(l.lead_created_date AS DATE))) as won
FROM lk_leads l
WHERE CAST(l.lead_created_date AS DATE)
  > CURRENT_DATE - INTERVAL '56 days'
AND l.lead_domain NOT IN ('spot2.mx')
AND l.lead_deleted_at IS NULL
GROUP BY DATE_TRUNC('week', CAST(lead_created_date AS DATE))
ORDER BY week_start DESC;
```

---

### Query 5: Sector Comparison
**Uso**: Ver qué sector tiene mejor performance
**Frecuencia**: Semanal o mensual

```sql
-- SECTOR PERFORMANCE COMPARISON
SELECT
  l.spot_sector,
  COUNT(DISTINCT l.lead_id) as total_leads,

  -- Projects
  COUNT(DISTINCT p.project_id) as total_projects,
  ROUND(100.0 * COUNT(DISTINCT p.project_id) / COUNT(DISTINCT l.lead_id), 1) as lead_to_project_pct,

  -- Visits
  COUNT(DISTINCT CASE WHEN p.project_funnel_visit_realized_at IS NOT NULL THEN p.project_id END) as visits_completed,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.project_funnel_visit_realized_at IS NOT NULL THEN p.project_id END) /
    COUNT(DISTINCT p.project_id), 1) as project_to_visit_pct,

  -- LOIs
  COUNT(DISTINCT CASE WHEN p.project_funnel_loi_date IS NOT NULL THEN p.project_id END) as lois,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.project_funnel_loi_date IS NOT NULL THEN p.project_id END) /
    COUNT(DISTINCT CASE WHEN p.project_funnel_visit_realized_at IS NOT NULL THEN p.project_id END), 1) as visit_to_loi_pct,

  -- Won
  COUNT(DISTINCT CASE WHEN p.project_won_date IS NOT NULL THEN p.project_id END) as won_projects,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.project_won_date IS NOT NULL THEN p.project_id END) /
    COUNT(DISTINCT l.lead_id), 1) as lead_to_won_pct,

  -- Revenue
  COALESCE(SUM(t.transaction_amount), 0) as total_revenue_mxn
FROM lk_leads l
LEFT JOIN lk_projects p ON l.lead_id = p.lead_id
LEFT JOIN bt_transactions t ON p.project_id = t.project_id
WHERE l.lead_domain NOT IN ('spot2.mx')
AND l.lead_deleted_at IS NULL
AND CAST(l.lead_created_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day'
GROUP BY l.spot_sector
ORDER BY total_leads DESC;
```

---

### Query 6: Lead Level Distribution
**Uso**: Entender mix de leads por origen
**Frecuencia**: Diaria

```sql
-- LEAD LEVEL DISTRIBUTION
SELECT
  CASE
    WHEN lead_l4 = 1 THEN 'L4'
    WHEN lead_l3 = 1 THEN 'L3'
    WHEN lead_l2 = 1 THEN 'L2'
    WHEN lead_l1 = 1 THEN 'L1'
    ELSE 'L0'
  END as lead_max_level,
  COUNT(DISTINCT lead_id) as count,
  ROUND(100.0 * COUNT(DISTINCT lead_id) / SUM(COUNT(DISTINCT lead_id)) OVER (), 1) as pct_of_total,

  -- Conversion to project
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN EXISTS (
    SELECT 1 FROM lk_projects p WHERE p.lead_id = lk_leads.lead_id
  ) THEN lead_id END) / COUNT(DISTINCT lead_id), 1) as conversion_to_project_pct,

  -- Conversion to won
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN EXISTS (
    SELECT 1 FROM lk_projects p WHERE p.lead_id = lk_leads.lead_id AND p.project_won_date IS NOT NULL
  ) THEN lead_id END) / COUNT(DISTINCT lead_id), 1) as conversion_to_won_pct
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
AND lead_deleted_at IS NULL
GROUP BY lead_max_level
ORDER BY CASE lead_max_level
  WHEN 'L4' THEN 1
  WHEN 'L3' THEN 2
  WHEN 'L2' THEN 3
  WHEN 'L1' THEN 4
  ELSE 5
END;
```

---

### Query 7: Commission Tracking by Broker
**Uso**: Seguimiento de comisiones por broker
**Frecuencia**: Diaria o semanal

```sql
-- COMMISSION TRACKING BY BROKER
SELECT
  u.user_id,
  u.user_email,
  COUNT(DISTINCT t.transaction_id) as total_transactions,
  SUM(t.transaction_amount) as total_transaction_value,
  COALESCE(SUM(CAST(SUBSTRING(t.aud_job FROM 'commission_(\d+)') AS FLOAT)), 0) as total_commission_earned,

  -- YTD
  SUM(CASE WHEN CAST(t.transaction_date AS DATE) >= DATE_TRUNC('year', CURRENT_DATE)
    THEN t.transaction_amount ELSE 0 END) as commission_ytd
FROM lk_users u
LEFT JOIN bt_transactions t ON u.user_id = t.user_id
WHERE u.user_deleted_at IS NULL
GROUP BY u.user_id, u.user_email
HAVING COUNT(DISTINCT t.transaction_id) > 0
ORDER BY total_transaction_value DESC;
```

---

### Query 8: Data Validation - Sanity Checks
**Uso**: Detectar anomalías o data issues
**Frecuencia**: Diaria

```sql
-- DATA VALIDATION - SANITY CHECKS
SELECT
  'Total Leads' as check_name,
  COUNT(DISTINCT lead_id) as value,
  'Compare vs historical trend' as interpretation
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
AND lead_deleted_at IS NULL

UNION ALL

SELECT
  'Leads with Projects' as check_name,
  COUNT(DISTINCT CASE WHEN project_id IS NOT NULL THEN lead_id END) as value,
  'Compare vs historical baseline' as interpretation
FROM lk_leads l
LEFT JOIN lk_projects p ON l.lead_id = p.lead_id
WHERE l.lead_domain NOT IN ('spot2.mx')
AND l.lead_deleted_at IS NULL

UNION ALL

SELECT
  'Projects with Visits' as check_name,
  COUNT(DISTINCT project_id) as value,
  'Percentage of projects advancing funnel' as interpretation
FROM lk_projects
WHERE project_funnel_visit_realized_at IS NOT NULL

UNION ALL

SELECT
  'Average Days Lead to Won' as check_name,
  ROUND(AVG(CAST(p.project_won_date AS DATE) - CAST(l.lead_created_date AS DATE)), 0) as value,
  'Measure deal cycle length' as interpretation
FROM lk_leads l
JOIN lk_projects p ON l.lead_id = p.lead_id
WHERE p.project_won_date IS NOT NULL;
```

---

### Query 9: Lead Cohort Analysis
**Uso**: Entender performance por cohorte de creación
**Frecuencia**: Mensual

```sql
-- LEAD COHORT ANALYSIS - By Creation Month
SELECT
  DATE_TRUNC('month', CAST(l.lead_created_date AS DATE)) as cohort_month,
  COUNT(DISTINCT l.lead_id) as leads_in_cohort,

  -- Project conversion
  COUNT(DISTINCT CASE WHEN p.project_id IS NOT NULL THEN l.lead_id END) as converted_to_project,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.project_id IS NOT NULL THEN l.lead_id END) /
    COUNT(DISTINCT l.lead_id), 1) as project_conversion_pct,

  -- Visit conversion
  COUNT(DISTINCT CASE WHEN p.project_funnel_visit_realized_at IS NOT NULL THEN l.lead_id END) as converted_to_visit,

  -- Won conversion
  COUNT(DISTINCT CASE WHEN p.project_won_date IS NOT NULL THEN l.lead_id END) as converted_to_won,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.project_won_date IS NOT NULL THEN l.lead_id END) /
    COUNT(DISTINCT l.lead_id), 1) as won_conversion_pct,

  -- Avg days to conversion
  ROUND(AVG(CASE WHEN p.project_won_date IS NOT NULL
    THEN CAST(p.project_won_date AS DATE) - CAST(l.lead_created_date AS DATE)
    ELSE NULL END), 0) as avg_days_to_won
FROM lk_leads l
LEFT JOIN lk_projects p ON l.lead_id = p.lead_id
WHERE l.lead_domain NOT IN ('spot2.mx')
AND l.lead_deleted_at IS NULL
GROUP BY DATE_TRUNC('month', CAST(l.lead_created_date AS DATE))
ORDER BY cohort_month DESC;
```

---

### Query 10: Conversation Impact Analysis
**Uso**: Entender qué impacto tienen los chats
**Frecuencia**: Semanal

```sql
-- CONVERSATION IMPACT - Leads with vs without chats
SELECT
  'With Conversations' as group_name,
  COUNT(DISTINCT l.lead_id) as leads,
  COUNT(DISTINCT CASE WHEN p.project_id IS NOT NULL THEN l.lead_id END) as to_project,
  COUNT(DISTINCT CASE WHEN p.project_won_date IS NOT NULL THEN l.lead_id END) as to_won,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.project_won_date IS NOT NULL THEN l.lead_id END) /
    COUNT(DISTINCT l.lead_id), 1) as won_conversion_pct
FROM lk_leads l
LEFT JOIN lk_projects p ON l.lead_id = p.lead_id
WHERE EXISTS (SELECT 1 FROM bt_con_conversations c WHERE c.lead_id = l.lead_id)
AND l.lead_domain NOT IN ('spot2.mx')
AND l.lead_deleted_at IS NULL

UNION ALL

SELECT
  'Without Conversations' as group_name,
  COUNT(DISTINCT l.lead_id) as leads,
  COUNT(DISTINCT CASE WHEN p.project_id IS NOT NULL THEN l.lead_id END) as to_project,
  COUNT(DISTINCT CASE WHEN p.project_won_date IS NOT NULL THEN l.lead_id END) as to_won,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.project_won_date IS NOT NULL THEN l.lead_id END) /
    COUNT(DISTINCT l.lead_id), 1) as won_conversion_pct
FROM lk_leads l
LEFT JOIN lk_projects p ON l.lead_id = p.lead_id
WHERE NOT EXISTS (SELECT 1 FROM bt_con_conversations c WHERE c.lead_id = l.lead_id)
AND l.lead_domain NOT IN ('spot2.mx')
AND l.lead_deleted_at IS NULL;
```

---

## 🧩 SECCIÓN 2: REUSABLE PATTERNS - BUILDING BLOCKS

---

## 1. PATRONES BÁSICOS DE CONTEO

### Pattern: Conteo de Leads Únicos
```sql
SELECT
  COUNT(DISTINCT lead_id) as leads
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
  AND CAST(lead_created_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day';
```

**Variantes**:
- Con dimensión sector: `GROUP BY spot_sector`
- Con dimensión lead_level: `GROUP BY lead_type`
- Con período: Cambiar `lead_created_date` por período deseado

---

### Pattern: Conteo de Proyectos
```sql
SELECT
  COUNT(DISTINCT project_id) as projects,
  COUNT(DISTINCT lead_id) as unique_leads,
  ROUND(COUNT(DISTINCT project_id)::FLOAT / COUNT(DISTINCT lead_id), 2) as avg_projects_per_lead
FROM lk_projects
WHERE CAST(project_created_at AS DATE) <= CURRENT_DATE - INTERVAL '1 day'
  AND project_deleted_at IS NULL;
```

---

### Pattern: Conteo de Spots
```sql
SELECT
  COUNT(DISTINCT spot_id) as total_spots,
  COUNT(DISTINCT CASE WHEN spot_status_full = 'Public' THEN spot_id END) as public_spots,
  COUNT(DISTINCT CASE WHEN spot_deleted_at IS NULL THEN spot_id END) as active_spots,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN spot_status_full = 'Public' THEN spot_id END)
    / COUNT(DISTINCT spot_id), 1) as public_pct
FROM lk_spots
WHERE spot_deleted_at IS NULL;
```

---

## 2. PATRONES DE LEAD LEVELS

### Pattern: Distribución de Lead Levels
```sql
SELECT
  CASE
    WHEN lead_l4 = 1 THEN 'L4'
    WHEN lead_l3 = 1 THEN 'L3'
    WHEN lead_l2 = 1 THEN 'L2'
    WHEN lead_l1 = 1 THEN 'L1'
    ELSE 'L0'
  END as lead_type,
  COUNT(DISTINCT lead_id) as leads,
  ROUND(100.0 * COUNT(DISTINCT lead_id) / SUM(COUNT(DISTINCT lead_id)) OVER (), 1) as pct
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY lead_type
ORDER BY CASE lead_type WHEN 'L4' THEN 1 WHEN 'L3' THEN 2 WHEN 'L2' THEN 3 WHEN 'L1' THEN 4 ELSE 5 END;
```

---

### Pattern: Lead Type + Dimensión (Sector)
```sql
SELECT
  spot_sector,
  CASE
    WHEN lead_l4 = 1 THEN 'L4'
    WHEN lead_l3 = 1 THEN 'L3'
    WHEN lead_l2 = 1 THEN 'L2'
    WHEN lead_l1 = 1 THEN 'L1'
    ELSE 'L0'
  END as lead_type,
  COUNT(DISTINCT lead_id) as leads
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY spot_sector, lead_type
ORDER BY spot_sector, lead_type;
```

---

## 3. PATRONES DE FUNNEL (Conversiones)

### Pattern: Lead Level Funnel
```sql
WITH lead_funnel AS (
  SELECT
    'L0_Created' as stage,
    COUNT(DISTINCT lead_id) as count
  FROM lk_leads
  WHERE lead_l0 = 1
    AND lead_domain NOT IN ('spot2.mx')
    AND lead_deleted_at IS NULL

  UNION ALL

  SELECT
    'L1_Engaged' as stage,
    COUNT(DISTINCT lead_id) as count
  FROM lk_leads
  WHERE lead_l1 = 1
    AND lead_domain NOT IN ('spot2.mx')
    AND lead_deleted_at IS NULL

  UNION ALL

  SELECT
    'L2_Visited' as stage,
    COUNT(DISTINCT lead_id) as count
  FROM lk_leads
  WHERE lead_l2 = 1
    AND lead_domain NOT IN ('spot2.mx')
    AND lead_deleted_at IS NULL

  UNION ALL

  SELECT
    'L3_LOI' as stage,
    COUNT(DISTINCT lead_id) as count
  FROM lk_leads
  WHERE lead_l3 = 1
    AND lead_domain NOT IN ('spot2.mx')
    AND lead_deleted_at IS NULL

  UNION ALL

  SELECT
    'L4_Won' as stage,
    COUNT(DISTINCT lead_id) as count
  FROM lk_leads
  WHERE lead_l4 = 1
    AND lead_domain NOT IN ('spot2.mx')
    AND lead_deleted_at IS NULL
)
SELECT
  stage,
  count,
  ROUND(100.0 * count / LAG(count) OVER (ORDER BY stage), 1) as conversion_rate_pct
FROM lead_funnel
ORDER BY CASE stage
  WHEN 'L0_Created' THEN 1
  WHEN 'L1_Engaged' THEN 2
  WHEN 'L2_Visited' THEN 3
  WHEN 'L3_LOI' THEN 4
  WHEN 'L4_Won' THEN 5
END;
```

---

### Pattern: Project Funnel - Visit to Won
```sql
WITH project_funnel AS (
  SELECT
    'Request_Visit' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_funnel_visit_created_date IS NOT NULL

  UNION ALL

  SELECT
    'Completed_Visit' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_funnel_visit_realized_at IS NOT NULL

  UNION ALL

  SELECT
    'LOI_Issued' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_funnel_loi_date IS NOT NULL

  UNION ALL

  SELECT
    'Contract_Signed' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_funnel_contract_date IS NOT NULL

  UNION ALL

  SELECT
    'Won' as stage,
    COUNT(DISTINCT project_id) as count
  FROM lk_projects
  WHERE project_won_date IS NOT NULL
)
SELECT
  stage,
  count,
  ROUND(100.0 * count / LAG(count) OVER (ORDER BY stage), 1) as conversion_rate_pct
FROM project_funnel
ORDER BY CASE stage
  WHEN 'Request_Visit' THEN 1
  WHEN 'Completed_Visit' THEN 2
  WHEN 'LOI_Issued' THEN 3
  WHEN 'Contract_Signed' THEN 4
  WHEN 'Won' THEN 5
END;
```

---

## 4. PATRONES DE FECHAS Y PERÍODOS

### Pattern: Por Día (Daily)
```sql
SELECT
  CAST(lead_created_date AS DATE) as date,
  COUNT(DISTINCT lead_id) as leads,
  ROUND(AVG(COUNT(DISTINCT lead_id)) OVER (ORDER BY CAST(lead_created_date AS DATE)
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) as moving_avg_7day
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY CAST(lead_created_date AS DATE)
ORDER BY date DESC
LIMIT 30;
```

---

### Pattern: Por Semana (Weekly)
```sql
SELECT
  DATE_TRUNC('week', CAST(lead_created_date AS DATE)) as week_start,
  COUNT(DISTINCT lead_id) as leads,
  COUNT(DISTINCT project_id) as projects
FROM lk_leads l
LEFT JOIN lk_projects p ON l.lead_id = p.lead_id
WHERE l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
GROUP BY DATE_TRUNC('week', CAST(lead_created_date AS DATE))
ORDER BY week_start DESC;
```

---

### Pattern: Por Mes (Monthly)
```sql
SELECT
  DATE_TRUNC('month', CAST(lead_created_date AS DATE)) as month,
  COUNT(DISTINCT lead_id) as leads,
  COUNT(DISTINCT CASE WHEN lead_l2 = 1 THEN lead_id END) as l2_leads,
  COUNT(DISTINCT CASE WHEN lead_l3 = 1 THEN lead_id END) as l3_leads,
  COUNT(DISTINCT CASE WHEN lead_l4 = 1 THEN lead_id END) as l4_leads,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN lead_l2 = 1 THEN lead_id END) /
    COUNT(DISTINCT lead_id), 1) as l0_to_l2_conversion_pct
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY DATE_TRUNC('month', CAST(lead_created_date AS DATE))
ORDER BY month DESC;
```

---

### Pattern: Rolling Window (últimos 30 días)
```sql
SELECT
  COUNT(DISTINCT lead_id) as leads_last_30_days,
  COUNT(DISTINCT CASE WHEN CAST(lead_created_date AS DATE) > CURRENT_DATE - INTERVAL '7 days'
    THEN lead_id END) as leads_last_7_days,
  COUNT(DISTINCT CASE WHEN CAST(lead_created_date AS DATE) = CURRENT_DATE - INTERVAL '1 day'
    THEN lead_id END) as leads_yesterday
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
  AND CAST(lead_created_date AS DATE) > CURRENT_DATE - INTERVAL '30 days';
```

---

## 5. PATRONES DE JOINS ESTÁNDAR

### Pattern: Lead + Project
```sql
SELECT
  l.lead_id,
  l.spot_sector,
  l.lead_type,
  COUNT(DISTINCT p.project_id) as project_count,
  COUNT(DISTINCT CASE WHEN p.project_won_date IS NOT NULL THEN p.project_id END) as won_projects
FROM lk_leads l
LEFT JOIN lk_projects p ON l.lead_id = p.lead_id
WHERE l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
GROUP BY l.lead_id, l.spot_sector, l.lead_type
ORDER BY l.lead_id;
```

---

### Pattern: Project + Spot
```sql
SELECT
  p.project_id,
  p.lead_id,
  s.spot_id,
  s.spot_title,
  s.spot_sector,
  p.project_won_date
FROM lk_projects p
JOIN lk_spots s ON lk_projects_spots.spot_id = s.spot_id
WHERE p.project_deleted_at IS NULL
  AND s.spot_deleted_at IS NULL
  AND p.project_won_date IS NOT NULL
ORDER BY p.project_won_date DESC;
```

---

### Pattern: Lead + Conversation
```sql
SELECT
  l.lead_id,
  l.spot_sector,
  COUNT(DISTINCT c.conv_id) as conversation_count,
  AVG(c.conv_messages) as avg_messages_per_conv,
  SUM(c.conv_messages) as total_messages
FROM lk_leads l
LEFT JOIN bt_con_conversations c ON l.lead_id = c.lead_id
WHERE l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
GROUP BY l.lead_id, l.spot_sector
ORDER BY conversation_count DESC;
```

---

### Pattern: Lead + Matches + Spot
```sql
SELECT
  l.lead_id,
  lm.match_id,
  s.spot_id,
  s.spot_title,
  lm.match_relevance_score,
  lm.match_created_date
FROM lk_leads l
JOIN lk_leads_matches lm ON l.lead_id = lm.lead_id
JOIN lk_spots s ON lm.spot_id = s.spot_id
WHERE l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
ORDER BY lm.match_relevance_score DESC;
```

---

## 6. PATRONES DE VENTANAS (Window Functions)

### Pattern: Rank/Row Number
```sql
SELECT
  lead_id,
  spot_sector,
  project_won_date,
  ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY project_won_date) as project_sequence,
  LAG(project_won_date) OVER (PARTITION BY lead_id ORDER BY project_won_date) as previous_project_date,
  LEAD(project_won_date) OVER (PARTITION BY lead_id ORDER BY project_won_date) as next_project_date
FROM lk_projects
WHERE project_won_date IS NOT NULL
ORDER BY lead_id, project_won_date;
```

---

### Pattern: Cumulative Sum (Running Total)
```sql
SELECT
  CAST(lead_created_date AS DATE) as date,
  COUNT(DISTINCT lead_id) as daily_leads,
  SUM(COUNT(DISTINCT lead_id)) OVER (ORDER BY CAST(lead_created_date AS DATE)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_leads
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY CAST(lead_created_date AS DATE)
ORDER BY date;
```

---

### Pattern: Percentage of Total
```sql
SELECT
  spot_sector,
  COUNT(DISTINCT project_id) as projects,
  SUM(COUNT(DISTINCT project_id)) OVER () as total_projects,
  ROUND(100.0 * COUNT(DISTINCT project_id) / SUM(COUNT(DISTINCT project_id)) OVER (), 1) as pct_of_total
FROM lk_projects
WHERE project_deleted_at IS NULL
GROUP BY spot_sector
ORDER BY projects DESC;
```

---

## 7. PATRONES DE AGREGACIÓN CON CONDICIONALES

### Pattern: Métricas Multi-condicionales
```sql
SELECT
  spot_sector,
  COUNT(DISTINCT lead_id) as total_leads,
  COUNT(DISTINCT CASE WHEN lead_l0 = 1 THEN lead_id END) as l0_leads,
  COUNT(DISTINCT CASE WHEN lead_l1 = 1 THEN lead_id END) as l1_leads,
  COUNT(DISTINCT CASE WHEN lead_l2 = 1 THEN lead_id END) as l2_leads,
  COUNT(DISTINCT CASE WHEN lead_l3 = 1 THEN lead_id END) as l3_leads,
  COUNT(DISTINCT CASE WHEN lead_l4 = 1 THEN lead_id END) as l4_leads,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN lead_l2 = 1 THEN lead_id END) / COUNT(DISTINCT lead_id), 1) as l0_to_l2_pct
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY spot_sector
ORDER BY total_leads DESC;
```

---

### Pattern: CASE Statements Complejos
```sql
SELECT
  lead_id,
  spot_sector,
  CASE
    WHEN lead_l4 = 1 THEN 'L4_Won'
    WHEN lead_l3 = 1 AND lead_l4 = 0 THEN 'L3_LOI'
    WHEN lead_l2 = 1 AND lead_l3 = 0 THEN 'L2_Visited'
    WHEN lead_l1 = 1 AND lead_l2 = 0 THEN 'L1_Engaged'
    ELSE 'L0_Created'
  END as max_stage
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
ORDER BY max_stage;
```

---

## 8. FILTROS ESTÁNDAR (SIEMPRE APLICAR)

### Filtros de Limpieza
```sql
-- Excluir test/demo
AND lead_domain NOT IN ('spot2.mx')

-- Excluir eliminados
AND lead_deleted_at IS NULL

-- Cutoff de ayer (datos completos)
AND CAST(lead_created_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day'

-- Para spots
AND spot_deleted_at IS NULL
AND spot_status_full IN ('Public', 'Disabled')

-- Para proyectos
AND project_deleted_at IS NULL
```

---

## 9. PATRONES DE PERFORMANCE

### Pattern: Index-friendly queries
```sql
-- ✅ BUENO - Usa índices
SELECT lead_id, spot_sector
FROM lk_leads
WHERE lead_deleted_at IS NULL
  AND CAST(lead_created_date AS DATE) = CURRENT_DATE - INTERVAL '1 day'
ORDER BY lead_id;

-- ❌ MALO - No usa índices (función en WHERE)
SELECT lead_id, spot_sector
FROM lk_leads
WHERE DATE(lead_created_date) = CURRENT_DATE - INTERVAL '1 day'
```

---

## 10. PATRONES DE DEBUGGING

### Pattern: Row counts by stage
```sql
SELECT
  'Total Leads' as check_name,
  COUNT(DISTINCT lead_id) as count
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')

UNION ALL

SELECT
  'With Projects' as check_name,
  COUNT(DISTINCT lead_id) as count
FROM lk_leads l
WHERE l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
  AND EXISTS (SELECT 1 FROM lk_projects p WHERE p.lead_id = l.lead_id)

UNION ALL

SELECT
  'With Won Projects' as check_name,
  COUNT(DISTINCT lead_id) as count
FROM lk_leads l
WHERE l.lead_domain NOT IN ('spot2.mx')
  AND l.lead_deleted_at IS NULL
  AND EXISTS (SELECT 1 FROM lk_projects p WHERE p.lead_id = l.lead_id AND p.project_won_date IS NOT NULL)
ORDER BY count DESC;
```

---

## 📖 Cómo Usar Este Archivo

### Queries Completas (Sección 1)
1. **Copia el query completo** a tu editor
2. **Adapta filtros** según necesites (período, sector)
3. **Ejecuta directamente** - está listo para usar
4. **Customiza nombres de columnas** si lo necesitas

### Reusable Patterns (Sección 2)
1. **Selecciona el pattern más cercano** a tu necesidad
2. **Adapta campos** (SELECT, GROUP BY, ORDER BY)
3. **Combina patterns** para queries más complejos
4. **Aplica filtros estándar** (Sección 8)

### Ejemplo de Adaptación
```sql
-- Original: Last 7 days
WHERE CAST(lead_created_date AS DATE)
  BETWEEN CURRENT_DATE - INTERVAL '8 days' AND CURRENT_DATE - INTERVAL '1 day'

-- Adaptado: Specific month (Febrero 2026)
WHERE CAST(lead_created_date AS DATE)
  BETWEEN '2026-02-01' AND '2026-02-28'

-- Adaptado: Specific sector only
AND spot_sector = 'Retail'
```

---

**Última actualización**: 2026-02-24
**Versión**: 2.0.0 (Consolidado de query-templates + sql-patterns)
**Secciones**: 20 queries + 50+ patterns
