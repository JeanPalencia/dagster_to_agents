# Golden Tables Documentation

Complete reference documentation for all golden tables in the Geospot data lakehouse.

**⚠️ NOTE ON RECORD COUNTS**: All "Records" numbers below are from sample data (CSVs used for documentation). For live production counts, use the queries in `sql-queries.md` → Query 1 (Executive Summary).

## Tables Overview

| Table | Type | Primary Key | Fields | Records* | Data Owner | Business Owner |
|-------|------|-------------|--------|---------|------------|----------------|
| [lk_leads](./lk_leads.md) | Lookup | lead_id | 80 | Sample | Data Cross | Luis Muñiz |
| [lk_projects](./lk_projects.md) | Lookup | project_id | 63 | Sample | Data Cross | Luis Muñiz |
| [lk_users](./lk_users.md) | Lookup | user_id | 68 | Sample | Data Cross | Luis Muñiz |
| [lk_spots](./lk_spots.md) | Lookup | spot_id | 187 | Sample | Data Cross | Luis Muñiz |
| [lk_matches_visitors_to_leads](./lk_matches_visitors_to_leads.md) | Bridge | (user_pseudo_id, client_id) | 26 | Sample | Data Cross | Jean Palencia, Alessio Luiselli |
| [lk_okrs](./lk_okrs.md) | Lookup | okr_month_start_ts | 7 | Sample | Data Cross | Luis Muñiz |
| [bt_con_conversations](./bt_con_conversations.md) | Behavioral | conv_id | 17 | Sample | Data Cross | Cesar Acosta |
| [bt_lds_lead_spots](./bt_lds_lead_spots.md) | Behavioral | lds_id | 27 | Sample | Data Cross | Luis Muñiz |
| [bt_transactions](./bt_transactions.md) | Behavioral | transaction_id | 10 | Sample | Data Cross | Luis Muñiz |

*For live record counts, run the sanity checks in sql-queries.md → Query 8 (Data Validation)

### Ownership Legend

**Data Owner** (Data Cross - Arquitectura de Datos):
- Responsable técnico del pipeline, schema, validación, data quality
- Team:
  - Estefania Louzau (estefania.louzau@spot2.mx)
  - Jose Castellanos (jose.castellanos@spot2.mx)
  - Luciano Pacione (luciano.pacione@spot2.mx)

**Business Owner** (Data Science / Analytics):
- Responsable de definición de negocio, validación de datos, análisis y casos de uso
- Owners por tabla:
  - **Luis Muñiz** (luis.muniz@spot2.mx) - lk_leads, lk_projects, lk_users, lk_spots, lk_okrs, bt_lds_lead_spots, bt_transactions
  - **Jean Palencia** (jean.palencia@spot2.mx) - lk_matches_visitors_to_leads (atribución GA4)
  - **Alessio Luiselli** (alessio.luiselli@spot2.mx) - lk_matches_visitors_to_leads (atribución GA4)
  - **Cesar Acosta** (cesar.acosta@spot2.mx) - bt_con_conversations (chatbot)

---

## 📚 Mandatory Filters (ALWAYS apply in queries)

**Por qué**: Cada tabla tiene datos de prueba, eliminados lógicos, etc. Estos filtros son obligatorios.

### For lk_leads (ALWAYS apply)
```sql
WHERE lead_domain NOT IN ('spot2.mx')          -- Excluir test data
  AND lead_deleted_at IS NULL                   -- Solo activos (soft delete)
  AND CAST(lead_created_date AS DATE) <= CURRENT_DATE - INTERVAL '1 day'  -- Cutoff: ayer
```

### For lk_spots
```sql
WHERE spot_deleted_at IS NULL                   -- Solo activos
  AND spot_status_full IN ('Public', 'Disabled') -- Estados válidos
```

### For lk_projects
```sql
WHERE project_deleted_at IS NULL                -- Solo activos
```

### For bt_con_conversations
```sql
WHERE conv_end_date IS NOT NULL                 -- Solo conversaciones completadas
  AND conv_messages > 0                         -- Con contenido real
```

---

## 🔑 Relationships & Cardinalities (How to JOIN)

| From Table | From Field | To Table | To Field | Type | Cardinality | Example |
|------------|-----------|----------|----------|------|------------|---------|
| lk_projects | lead_id | lk_leads | lead_id | FK | 1:N | 1 lead → 1-50 projects |
| bt_lds_lead_spots | lead_id | lk_leads | lead_id | FK | 1:N | 1 lead → many events |
| bt_lds_lead_spots | spot_id | lk_spots | spot_id | FK | N:M | Many leads view same spot |
| lk_matches_visitors_to_leads | client_id | lk_leads | lead_id | FK | 1:N | 1 lead → 1 visitor match |
| bt_con_conversations | lead_id | lk_leads | lead_id | FK | 1:N | 1 lead → 0-100 conversations |
| bt_transactions | project_id | lk_projects | project_id | FK | N:1 | Many transactions per project (rare) |

---

## 💾 Audit Fields Standard (EVERY Gold Table)

All Gold layer tables have exactly 3 mandatory audit columns:

| Column | Type | Description | Always Non-Null |
|--------|------|-------------|-----------------|
| **aud_inserted_at** | timestamp | When row was FIRST inserted (immutable) | Yes |
| **aud_updated_at** | timestamp | When row was last updated (changed on each ETL run) | No |
| **aud_job** | string | Dagster job name that loaded/updated this row | Yes |

**Common Mistakes**:
- ❌ Don't use `created_at`, use `aud_inserted_at`
- ❌ Don't assume `aud_updated_at` tells you when business event happened (it's ETL time)
- ✅ Use `aud_inserted_at` for "when was this lead created" analysis

---

## 📖 Business Glossary (Key Terms)

### Lead Levels (L0-L4) - CUMULATIVE & HIERARCHICAL

| Level | Definition | Example |
|-------|-----------|---------|
| **L0** | Account created - Lead registered | Juan se registró |
| **L1** | Profile completed - Filled requirements form | María completó formulario |
| **L2** | Spot interested - Clicked property interest | Pedro expresó interés |
| **L3** | Outbound contact - Sales team reached out | Ana recibió llamada |
| **L4** | Treble source - Premium/manual source | Roberto vino de Trebble |

**For live L0-L4 distribution**, run: `sql-queries.md` → Query 6 (Lead Level Distribution)

**IMPORTANT**: Once a lead reaches L2, they STAY L2+ (never goes down). Use `lead_type` field for classification.

### Project Funnel Stages

| Stage | Field | Meaning | When Filled |
|-------|-------|---------|-------------|
| Created | project_created_at | Lead opened search | Immediately |
| Request Visit | project_funnel_visit_created_date | Lead requested viewing | When requested |
| Confirmed Visit | project_funnel_visit_confirmed_at | Broker confirmed | After coordination |
| Completed Visit | project_funnel_visit_realized_at | Lead actually visited | After visit |
| LOI | project_funnel_loi_date | Letter of Intent issued | If interested |
| Contract | project_funnel_contract_date | Contract signed | If progressing |
| Won | project_won_date | Deal closed | Final step |

### Supply vs Demand

- **Supply Lead** (lead_supply = 1): Offering space to rent/sell (publisher)
- **Demand Lead** (lead_supply = 0): Looking for space to rent/buy (searcher)

### Cohort vs Rolling (Metrics Analysis)

- **Cohort (Dynamic)**: Grouped by creation/reactivation month → longitudinal (how does January cohort evolve?)
- **Rolling**: Activity in any time window → operational (how much volume this week?)

---

## ⚠️ Common MCP Mistakes (How NOT to write queries)

### Mistake 1: Using lead_id as dimension (causes duplication)
```sql
-- ❌ WRONG - Returns many rows per lead
SELECT lead_id, COUNT(*) FROM lk_leads
LEFT JOIN lk_projects p ON lk_leads.lead_id = p.lead_id
GROUP BY lead_id;

-- ✅ RIGHT - Returns 1 row per lead
SELECT l.lead_id, COUNT(DISTINCT p.project_id) as project_count
FROM lk_leads l
LEFT JOIN lk_projects p ON l.lead_id = p.lead_id
GROUP BY l.lead_id;
```

### Mistake 2: Forgetting mandatory filters
```sql
-- ❌ WRONG - Includes test data + deleted leads
SELECT COUNT(*) FROM lk_leads;

-- ✅ RIGHT - Clean data only
SELECT COUNT(*) FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL;
```

### Mistake 3: Comparing without aggregating in 1:N joins
```sql
-- ❌ WRONG - Can't sum(transaction_amount) directly (multiple rows per project)
SELECT p.project_id, SUM(t.transaction_amount)
FROM lk_projects p
JOIN bt_transactions t ON p.project_id = t.project_id;

-- ✅ RIGHT - Aggregate first
SELECT p.project_id, COALESCE(SUM(t.transaction_amount), 0)
FROM lk_projects p
LEFT JOIN bt_transactions t ON p.project_id = t.project_id
GROUP BY p.project_id;
```

### Mistake 4: Using date functions that break indexes
```sql
-- ❌ WRONG - CAST in WHERE breaks indexes
SELECT * FROM lk_leads
WHERE DATE(lead_created_date) = CURRENT_DATE - 1;

-- ✅ RIGHT - Preserves index usage
SELECT * FROM lk_leads
WHERE CAST(lead_created_date AS DATE) = CURRENT_DATE - 1;
```

### Mistake 5: Forgetting cutoff rule (today's data is incomplete)
```sql
-- ❌ WRONG - Today's leads might still be loading
SELECT COUNT(*) FROM lk_leads
WHERE CAST(lead_created_date AS DATE) = CURRENT_DATE;

-- ✅ RIGHT - Use yesterday (data is complete)
SELECT COUNT(*) FROM lk_leads
WHERE CAST(lead_created_date AS DATE) = CURRENT_DATE - INTERVAL '1 day';
```

---

## 📖 Documentation Structure

Each table document includes:
- **Overview**: General description and business context
- **Table Structure**: Complete field listing with types and descriptions
- **Key Metrics**: Record counts, refresh frequency, SLAs
- **Data Quality**: Validation rules and quality checks
- **Related Tables**: Foreign key relationships
- **Notes**: Special considerations and known issues

---

## 📚 How to Use This Documentation

### For MCP / AI-Powered Tools
1. **Start here** with Mandatory Filters + Relationships + Glossary (this README)
2. **Then** look at sql-queries.md for patterns
3. **Finally** check individual table docs for field-level details

### For Humans
1. Read individual table docs (lk_leads.md, etc) for full context
2. Use sql-queries.md for ready-to-use queries
3. Refer to this README for quick reference

## How to Update

1. Navigate to the specific table markdown file
2. Update the relevant section
3. Commit changes with descriptive message
4. Update this README if adding/removing tables or changing filters
