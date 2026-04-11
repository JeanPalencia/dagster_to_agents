# bt_transactions - Golden Table Documentation

## Overview

La tabla **bt_transactions** es una tabla puente (Bridge Table) que almacena todas las transacciones completadas (cierres de negocios) en Spot2. Cada registro representa un lead que cerró exitosamente una compra o arrendamiento de una propiedad. Esta es la tabla de "conversion final" del funnel de ventas.

**Propósito Analítico:**
- Seguimiento de transacciones completadas y revenue
- Análisis de conversion rate global (lead → transaction)
- Evaluación de comisiones y pagos
- Segmentación de clientes por monto y tipo
- Análisis de tendencias de cierre
- Evaluación de performance de brokers

**Frecuencia de Actualización:** Daily
**Grain de la Tabla:** Una fila por transacción cerrada
**Total Registros:** See sql-queries.md → Query 1 for live count transacciones
**Atributos:** 10 campos

---

## Table Structure

### Sección 1: Identificación (Campos 1-8)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 1 | **transaction_id** | String | No | ID único de transacción (UUID). PK. Ej: "txn_abc123" |
| 2 | **lead_id** | Integer | No | FK a lk_leads. Lead que cerró la transacción |
| 3 | **spot_id** | Integer | No | FK a lk_spots. Propiedad transaccionada |
| 4 | **project_id** | Integer | No | FK a lk_projects. Proyecto que resultó en cierre |
| 5 | **user_id** | Integer | No | FK a lk_users. Broker/agente que cerró |
| 6 | **transaction_date** | Date | No | Fecha de cierre de la transacción |
| 7 | **transaction_type** | String | No | Tipo: "Lease" o "Sale" |
| 8 | **transaction_status** | String | No | Estado: "Completed" o "Pending" |

### Sección 2: Términos Económicos (Campos 11-18)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 11 | **transaction_value_mxn** | Float | No | Monto total en MXN. Ej: $500,000 |
| 12 | **transaction_value_usd** | Float | Sí | Conversión a USD (if applicable) |
| 13 | **transaction_monthly_rent_mxn** | Float | Sí | Renta mensual en MXN (si lease) |
| 14 | **transaction_annual_value_mxn** | Float | Sí | Valor anualizado |
| 15 | **transaction_lease_term_months** | Integer | Sí | Plazo en meses (si lease) |
| 16 | **transaction_area_sqm** | Float | Sí | Área rentada/vendida en m² |
| 17 | **transaction_unit_price_mxn** | Float | Sí | Precio unitario por m² |
| 18 | **transaction_discount_applied** | Float | Sí | Descuento % aplicado |

### Sección 3: Comisión & Pago (Campos 21-28)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 21 | **transaction_commission_rate** | Float | Sí | Tasa de comisión aplicada % |
| 22 | **transaction_commission_mxn** | Float | Sí | Monto de comisión generada |
| 23 | **transaction_commission_status** | String | Sí | Estado comisión: "Pending", "Paid", "Disputed" |
| 24 | **transaction_commission_paid_date** | Date | Sí | Fecha de pago comisión |
| 25 | **transaction_other_fees_mxn** | Float | Sí | Otros honorarios/fees |
| 26 | **transaction_net_revenue_mxn** | Float | Sí | Revenue neto después comisiones |
| 27 | **transaction_payment_method** | String | Sí | Método: "Bank Transfer", "Check", "Crypto", "Other" |
| 28 | **transaction_invoice_number** | String | Sí | Número de factura |

### Sección 4: Timeline & Duración (Campos 31-35)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 31 | **transaction_lead_creation_date** | Date | Sí | Fecha cuando lead fue creado |
| 32 | **transaction_project_creation_date** | Date | Sí | Fecha cuando proyecto fue creado |
| 33 | **transaction_days_lead_to_close** | Integer | Sí | Días totales desde lead a cierre |
| 34 | **transaction_days_project_to_close** | Integer | Sí | Días desde proyecto a cierre |
| 35 | **transaction_contract_signed_date** | Date | Sí | Fecha de firma del contrato |

### Sección 5: Detalles & Administración (Campos 38-60)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 38 | **transaction_notes** | String | Sí | Notas sobre la transacción |
| 39 | **transaction_created_by_user_id** | Integer | Sí | FK a user_id quien registró |
| 40 | **transaction_created_date** | DateTime | No | Fecha de creación del registro |
| 41 | **transaction_last_updated_date** | DateTime | No | Última actualización |
| 42 | **transaction_last_updated_by_user_id** | Integer | Sí | FK a user_id quien actualizó |
| 43 | **transaction_deleted** | Boolean | No | ¿Eliminada (soft delete)? |
| 44 | **transaction_deleted_date** | DateTime | Sí | Fecha de eliminación |
| 45 | **transaction_data_source** | String | Sí | Origen: "Manual Entry", "API", "Import", "System Generated" |
| 46 | **transaction_external_reference** | String | Sí | Referencia externa (CRM, MLS, etc) |
| 47 | **transaction_verification_status** | String | Sí | "Verified", "Pending Review", "Under Investigation" |
| 48 | **transaction_verified_date** | DateTime | Sí | Fecha de verificación |
| 49 | **transaction_verified_by_user_id** | Integer | Sí | FK a user_id quien verificó |
| 50 | **transaction_disputes_count** | Integer | Sí | Número de disputas |
| 51 | **transaction_dispute_status** | String | Sí | "None", "Open", "Resolved", "Escalated" |
| 52 | **transaction_dispute_resolution_date** | DateTime | Sí | Fecha de resolución |
| 53 | **transaction_tags** | Array | Sí | Tags personalizados |
| 54 | **transaction_hash** | String | Sí | Hash para deduplicación |

---

## Transaction Type Distribution

**Note**: Transaction type mix varies by market and time period. For live distribution:

| Tipo | Descripción |
|------|-------------|
| **Lease** | Arrendamientos |
| **Sale** | Compra-venta |

Filter by `transaction_type` field to analyze separately.

---

## Transaction Value Distribution

**Note**: Value distribution varies significantly by market segment and time period. For live analysis:

| Rango MXN | Descripción |
|-----------|-------------|
| **< $100,000** | Pequeñas transacciones |
| **$100,000 - $500,000** | Mid-market |
| **$500,000 - $1M** | Upper mid-market |
| **> $1M** | Enterprise deals |

Analyze using `transaction_value_mxn` field with appropriate binning.

---

## Conversion Funnel: Global

**Note**: Conversion rates vary significantly by segment, market, and time period. For live funnel analysis, use Query 8 (Data Validation - Sanity Checks) or Query 9 (Lead Cohort Analysis).

| Etapa | Descripción |
|-------|-------------|
| **Total Leads** | Baseline - todos los leads |
| **Leads with Projects** | Leads que crearon búsqueda |
| **Leads with Visits** | Leads que visitaron propiedad |
| **Leads with Transactions** | Leads que cerraron transacción |

Calculate: (Leads with Transactions / Total Leads) × 100 = Global conversion %

---

## Top Performing Brokers (by Transaction Count)

**Note**: Broker rankings change based on time period and market conditions. For live top brokers analysis:

Use Query 7 (Commission Tracking) to rank brokers by:
- Transaction count
- Total transaction value
- Average deal size
- Commission revenue

Join bt_transactions with lk_users on `user_id` field.

---

## Commission Analysis

**Note**: Commission metrics vary by broker, transaction type, and period. For live analysis, use Query 7 (Commission Tracking):

| Métrica | Campo | Descripción |
|---------|-------|-------------|
| **Commission Rate** | `transaction_commission_rate` | % aplicada por transacción |
| **Total Commissions Paid** | `transaction_commission_mxn` WHERE status='Paid' | YTD calculations |
| **Avg Commission per Deal** | AVG(`transaction_commission_mxn`) | Promedio por transacción |
| **Commission Status** | `transaction_commission_status` | Pending, Paid, Disputed |
| **Pending Commission Value** | SUM(`transaction_commission_mxn`) WHERE status='Pending' | Próximo ciclo |

---

## Timeline Analysis

**Note**: Deal timeline metrics vary significantly by segment and market conditions. For live analysis:

| Métrica | Campo | Descripción |
|---------|-------|-------------|
| **Lead → Close** | `transaction_days_lead_to_close` | Total days from lead creation |
| **Project → Close** | `transaction_days_project_to_close` | Days from project creation |
| **Lead Creation Date** | `transaction_lead_creation_date` | Reference timestamp |
| **Project Creation Date** | `transaction_project_creation_date` | Reference timestamp |
| **Contract Signed** | `transaction_contract_signed_date` | Signature date |

Calculate averages, medians, and distributions using live data.

---

## Key Metrics

**Note**: All transaction metrics are time-dependent and vary by segment. For live metrics, use Query 1 (Executive Summary Dashboard) or Query 7 (Commission Tracking).

| Métrica | Campo/Cálculo | Descripción |
|---------|---------------|-------------|
| **Total Transactions** | COUNT(`transaction_id`) | Cierres completados |
| **Total Revenue (MXN)** | SUM(`transaction_value_mxn`) | Valor de transacciones |
| **Average Deal Size** | AVG(`transaction_value_mxn`) | Promedio |
| **Median Deal Size** | PERCENTILE_CONT(`transaction_value_mxn`) | Valor mediano |
| **Largest Deal** | MAX(`transaction_value_mxn`) | Record |
| **Smallest Deal** | MIN(`transaction_value_mxn`) | Mínimo |
| **Total Leases** | COUNT WHERE `transaction_type`='Lease' | Arrendamientos |
| **Total Sales** | COUNT WHERE `transaction_type`='Sale' | Compra-venta |
| **Lease Revenue** | SUM(`transaction_value_mxn`) WHERE `transaction_type`='Lease' | Total arrendamientos |
| **Sale Revenue** | SUM(`transaction_value_mxn`) WHERE `transaction_type`='Sale' | Total compra-venta |
| **Global Conversion Rate** | (Transactions / Total Leads) × 100 | lead → transaction % |

---

## Sector Performance

**Note**: Sector performance metrics vary by market and time period. For live analysis:

| Sector | Descripción |
|--------|-------------|
| **Retail** | Commercial retail spaces |
| **Industrial** | Industrial/warehouse properties |
| **Office** | Office spaces |
| **Land** | Land transactions |

Join bt_transactions with lk_spots on `spot_id` to get `spot_sector` and calculate counts, percentages, and averages.

---

## Lead Level Performance

**Note**: Conversion rates by lead level vary by time period and market conditions. For live analysis:

| Lead Level | Descripción |
|-----------|-------------|
| **L0** | Account creation level |
| **L1** | Profile completion level |
| **L2** | Spot interest level |
| **L3** | Outbound contact level |
| **L4** | Treble source level |

Join bt_transactions with lk_leads on `lead_id` to calculate conversions by `lead_type` (or L0-L4 flags).

Expected pattern: Higher lead levels typically show higher conversion rates.

---

## Data Quality Checks

**Note**: Data completeness varies. Verify actual null percentages in your environment:

| Campo | Descripción |
|-------|-------------|
| transaction_id | Primary key - always present |
| transaction_value_mxn | Core metric - should be non-null |
| transaction_type | Classification - should be non-null |
| transaction_commission_mxn | Calculated - may be NULL |
| transaction_verified_status | QA flag - check for NULL values |

---

## Related Tables

| Tabla | Join Key | Tipo Relación | Descripción |
|-------|----------|---------------|-------------|
| **lk_leads** | lead_id | Many-to-One | Lead que cerró |
| **lk_spots** | spot_id | Many-to-One | Propiedad cerrada |
| **lk_projects** | project_id | Many-to-One | Proyecto resultante |
| **lk_users** | user_id | Many-to-One | Broker que cerró |
| **bt_con_conversations** | conv_id (optional) | Many-to-One | Conversación (if tracked) |

---

## Notes & Considerations

- 💰 **Revenue**: Calculate total with SUM(`transaction_value_mxn`) - highly variable
- 🎯 **Conversion**: Monitor lead→transaction % with Query 8 or 9
- 🏢 **Sector Mix**: Analyze sector performance using spot_sector joins
- ⏰ **Timeline**: Monitor deal cycle using `transaction_days_lead_to_close` field
- 💼 **Leases vs Sales**: Compare revenue and count by `transaction_type`
- 👥 **Lead Level Performance**: Join with lk_leads to compare conversion by level
- 💸 **Commission**: Track with `transaction_commission_rate` and `transaction_commission_status`
- 🔍 **Verification**: Monitor `transaction_verification_status` for quality
- 📊 **Broker Concentration**: Join with lk_users to rank by performance
- 🎁 **Discounts**: Check `transaction_discount_applied` field for patterns

---

## Revenue Impact

**Note**: All revenue calculations are time-dependent. Calculate from live data:

- **Lease Revenue**: SUM(`transaction_value_mxn` WHERE `transaction_type`='Lease')
- **Sale Revenue**: SUM(`transaction_value_mxn` WHERE `transaction_type`='Sale')
- **Total Transaction Revenue**: SUM(`transaction_value_mxn`)
- **Commission Revenue**: SUM(`transaction_commission_mxn`)
- **Net Commission**: SUM(`transaction_commission_mxn` WHERE status='Paid')
- **Pending Commission**: SUM(`transaction_commission_mxn` WHERE status='Pending')

Use Query 7 (Commission Tracking) for comprehensive revenue analysis.

---

## Optimization Opportunities

Monitor these metrics with live data to identify improvement areas:

1. **Increase Conversion**: Monitor lead→transaction % trends
2. **Reduce Sales Cycle**: Track `transaction_days_lead_to_close` for improvements
3. **Improve Lower Lead Levels**: Analyze L0/L1 conversion rates vs L3/L4
4. **Replicate Top Performance**: Identify highest-converting brokers and lead sources
5. **Improve Commission Payment**: Monitor `transaction_commission_status` = 'Pending' aging

---

## Change Log

| Fecha | Cambio | Autor |
|-------|--------|-------|
| 2026-02-23 | Documentación inicial creada | elouzau-spot2 |
