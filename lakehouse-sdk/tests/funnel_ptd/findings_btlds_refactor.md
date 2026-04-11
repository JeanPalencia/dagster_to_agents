# Hallazgos: Refactorización funnel_ptd — bt_lds_lead_spots como fuente principal

## Paso 0: Migración lk_leads → lk_leads_v2

- lk_leads_v2 tiene 48,665 leads vs 48,730 en lk_leads (65 leads legacy eliminados)
- 0 diferencias en fechas para los 48,665 leads comunes
- Columnas lead_l0–lead_l4 cambian de INTEGER a BOOLEAN (corregido: `=1` → booleano directo)
- Validación: 42 columnas, 32 filas, 100% paridad

## Redundancias detectadas

| # | Transformación actual | Ubicación | Equivalente en bt_lds | Estado |
|---|----------------------|-----------|----------------------|--------|
| 1 | MIN(lead_lead0_at..lead_lead4_at) → lead_first_datetime | stg_gs_fptd_leads | Event 1: lds_event_at = lead_type_datetime_start | REDUNDANTE |
| 2 | Filtros lead relevancia (flags, domain, deleted) | raw_gs_lk_leads SQL WHERE | lead_lds_relevant_id = 1 ya aplicado en bt_lds | REDUNDANTE |
| 3 | Filtro >= 2021-01-01 y < today | stg_gs_fptd_leads | Aplicable sobre event 1 lds_event_at | NECESARIO en STG |
| 4 | Join leads+projects + cómputo cohort_date_real | silver_shared._build_projects_base | lds_cohort_at ya calculado en bt_lds | REDUNDANTE |
| 5 | Determinación New/Reactivated (30 días) | core assets (cohort_type) | lds_cohort_type ya calculado en bt_lds | REDUNDANTE |
| 6 | Cast de fechas funnel proyecto | stg_gs_fptd_projects | Pivot de events 2,4,5,6,7,10 con cast | NECESARIO en STG |

## Mapeo de columnas: bt_lds events → columnas funnel_ptd

| Columna funnel_ptd | Fuente actual | Fuente bt_lds | Método |
|--------------------|--------------|--------------| -------|
| lead_id | lk_leads_v2 | event 1 lead_id | Directo |
| lead_first_datetime | MIN(lead_lead0_at..4_at) | event 1 lds_event_at | Directo |
| lead_first_date | lead_first_datetime::date | event 1 lds_event_at::date | Directo |
| project_id | lk_projects_v2 | event 2 project_id | Directo |
| project_created_at | lk_projects_v2.project_created_at | event 2 lds_event_at | Directo |
| visit_scheduled_at | lk_projects_v2.project_funnel_visit_created_date | MIN(event 4 lds_event_at)::date | Agregado por project |
| visit_confirmed_at | lk_projects_v2.project_funnel_visit_confirmed_at | MIN(event 5 lds_event_at) | Agregado por project |
| visit_completed_at | lk_projects_v2.project_funnel_visit_realized_at | MIN(event 6 lds_event_at) | Agregado por project |
| project_funnel_loi_date | lk_projects_v2.project_funnel_loi_date | MIN(event 7 lds_event_at)::date | Agregado por project |
| project_won_date | lk_projects_v2.project_won_date | event 10 lds_event_at::date | Directo |
| cohort_date_real | Calculado en _build_projects_base | lds_cohort_at | Directo |
| cohort_type | Calculado en core assets | lds_cohort_type | Directo |
| project_enable_id | lk_projects_v2.project_enable_id | NO disponible en bt_lds | Mantener lk_projects_v2 minimal |

## Dato faltante en bt_lds

- `project_enable_id`: solo existe en lk_projects_v2. Se mantiene un raw reducido con solo project_id + project_enable_id.
