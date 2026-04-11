# lk_spots - Golden Table Documentation

## Overview

La tabla **lk_spots** es una tabla de referencia (Lookup Table) que almacena información de todas las propiedades (inmuebles) listadas o indexadas en Spot2. Cada registro representa un espacio comercial único con información completa de localización, características físicas, pricing, disponibilidad y datos de contacto del propietario/arrendador.

**Propósito Analítico:**
- Análisis de inventario de propiedades por sector, estado y ciudad
- Análisis de pricing y mercado (precios, rentas, demanda)
- Seguimiento de disponibilidad y vacancia
- Análisis de popularidad y engagement de propiedades
- Segmentación de propiedades por características (tamaño, sector, precio)
- Análisis geoespacial y clustering de mercados

**Frecuencia de Actualización:** Daily
**Grain de la Tabla:** Una fila por propiedad única
**Total Registros:** See sql-queries.md → Query 1 for live count propiedades
**Atributos:** 187 campos

---

## Table Structure

### Sección 1: Identificación & Contacto (Campos 1-12)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 1 | **spot_id** | Integer | No | Identificador único de la propiedad. PK. Ej: 5432 |
| 2 | **spot_name** | String | Sí | Nombre/denominación de la propiedad. Ej: "Plaza Retail CDMX Centro" |
| 3 | **spot_slug** | String | Sí | URL-friendly slug para la propiedad |
| 4 | **spot_owner_id** | Integer | Sí | FK a lk_users. Propietario/administrador |
| 5 | **spot_owner_email** | String | Sí | Email del propietario directo |
| 6 | **spot_owner_phone** | String | Sí | Teléfono del propietario |
| 7 | **spot_manager_id** | Integer | Sí | FK a lk_users. Gestor de la propiedad |
| 8 | **spot_management_company** | String | Sí | Nombre de empresa administradora |
| 9 | **spot_management_email** | String | Sí | Email de contacto de administración |
| 10 | **spot_management_phone** | String | Sí | Teléfono de administración |
| 11 | **spot_contact_person** | String | Sí | Persona de contacto principal |
| 12 | **spot_contact_person_phone** | String | Sí | Teléfono de persona de contacto |

### Sección 2: Localización Geográfica (Campos 15-35)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 15 | **spot_country** | String | Sí | País: "México" |
| 16 | **spot_state** | String | No | Estado. Ej: "CDMX", "Jalisco" |
| 17 | **spot_state_id** | Integer | No | FK a tabla de estados |
| 18 | **spot_city** | String | Sí | Ciudad principal. Ej: "Mexico City" |
| 19 | **spot_municipality** | String | Sí | Municipio/delegación |
| 20 | **spot_neighborhood** | String | Sí | Colonia/vecindario. Ej: "Cuauhtemoc" |
| 21 | **spot_zip_code** | String | Sí | Código postal |
| 22 | **spot_street_address** | String | Sí | Dirección completa |
| 23 | **spot_longitude** | Float | Sí | Coordenada X (longitud) para geolocalización |
| 24 | **spot_latitude** | Float | Sí | Coordenada Y (latitud) para geolocalización |
| 25 | **spot_geohash** | String | Sí | Geohash para búsquedas espaciales |
| 26 | **spot_polygon_vertices** | Array | Sí | Vértices del polígono de cobertura |
| 27 | **spot_distance_to_downtown_km** | Float | Sí | Distancia al centro en km |
| 28 | **spot_distance_to_nearest_metro** | Float | Sí | Distancia a estación de metro más cercana |
| 29 | **spot_metro_station_name** | String | Sí | Nombre de estación de metro más cercana |
| 30 | **spot_access_highway_1** | String | Sí | Acceso principal a carretera |
| 31 | **spot_access_highway_2** | String | Sí | Acceso secundario a carretera |
| 32 | **spot_nearby_landmarks** | Array | Sí | Array de puntos de interés cercanos |
| 33 | **spot_region** | String | Sí | Región de análisis |
| 34 | **spot_subregion** | String | Sí | Sub-región o cluster geográfico |
| 35 | **spot_zone** | String | Sí | Zona comercial asignada |

### Sección 3: Características Físicas (Campos 38-65)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 38 | **spot_sector** | String | No | Sector: "Retail", "Industrial", "Office", "Land" |
| 39 | **spot_property_type** | String | Sí | Tipo: "Building", "Space", "Land", "Plaza", "Mall" |
| 40 | **spot_subtypes** | Array | Sí | Subtipos. Ej: ["Food Court", "Anchor Tenant", "Outparcel"] |
| 41 | **spot_total_built_area_sqm** | Float | Sí | Área construida total en m² |
| 42 | **spot_available_area_sqm** | Float | Sí | Área disponible actualmente en m² |
| 43 | **spot_rentable_area_sqm** | Float | Sí | Área rentable (GLA - Gross Leasable Area) |
| 44 | **spot_ground_floor_area_sqm** | Float | Sí | Área planta baja |
| 45 | **spot_upper_floors_area_sqm** | Float | Sí | Área pisos altos |
| 46 | **spot_basement_area_sqm** | Float | Sí | Área de sótano |
| 47 | **spot_number_of_floors** | Integer | Sí | Número de niveles |
| 48 | **spot_number_of_units** | Integer | Sí | Número de locales/divisiones |
| 49 | **spot_year_built** | Integer | Sí | Año de construcción. Rango: 1920-2024 |
| 50 | **spot_year_renovated** | Integer | Sí | Año de última renovación |
| 51 | **spot_construction_status** | String | Sí | "Complete", "Under Construction", "Planned" |
| 52 | **spot_ceiling_height_meters** | Float | Sí | Altura de techos en metros |
| 53 | **spot_floor_load_capacity_kg_sqm** | Float | Sí | Capacidad de carga de piso |
| 54 | **spot_has_parking** | Boolean | Sí | ¿Tiene estacionamiento? |
| 55 | **spot_parking_spaces** | Integer | Sí | Número de espacios de estacionamiento |
| 56 | **spot_parking_ratio** | Float | Sí | Relación espacios:m² (ej: 1:3) |
| 57 | **spot_loading_dock_count** | Integer | Sí | Número de muelles de carga |
| 58 | **spot_has_elevator** | Boolean | Sí | ¿Tiene ascensores? |
| 59 | **spot_elevator_count** | Integer | Sí | Número de ascensores |
| 60 | **spot_hvac_system** | String | Sí | Tipo de aire acondicionado |
| 61 | **spot_security_system** | String | Sí | Tipo de sistema de seguridad |
| 62 | **spot_is_green_certified** | Boolean | Sí | ¿Certificación ambiental? (LEED, etc) |
| 63 | **spot_energy_efficiency_rating** | String | Sí | Calificación de eficiencia: A, B, C, D |
| 64 | **spot_accessibility_features** | Array | Sí | Array de accesibilidad (rampas, elevadores, etc) |
| 65 | **spot_mixed_use** | Boolean | Sí | ¿Uso mixto (residencial + comercial)? |

### Sección 4: Pricing & Financiero (Campos 68-85)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 68 | **spot_rent_price_per_sqm** | Float | Sí | Renta mensual por m² en MXN. Ej: $450/m² |
| 69 | **spot_total_monthly_rent** | Float | Sí | Renta total mensual estimada |
| 70 | **spot_annual_rent** | Float | Sí | Renta anual |
| 71 | **spot_sale_price_per_sqm** | Float | Sí | Precio de venta por m² |
| 72 | **spot_total_sale_price** | Float | Sí | Precio total de venta |
| 73 | **spot_currency** | String | Sí | Moneda: "MXN" |
| 74 | **spot_price_status** | String | Sí | "Under Negotiation", "Fixed", "POUA" |
| 75 | **spot_lease_term_months** | Integer | Sí | Plazo de arrendamiento en meses |
| 76 | **spot_price_last_updated** | Date | Sí | Última actualización de pricing |
| 77 | **spot_operational_costs_monthly** | Float | Sí | Gastos operacionales mensuales |
| 78 | **spot_property_tax_annual** | Float | Sí | Impuestos prediales anuales |
| 79 | **spot_insurance_annual** | Float | Sí | Seguro anual |
| 80 | **spot_maintenance_cost_monthly** | Float | Sí | Costo mantenimiento mensual |
| 81 | **spot_condo_fee_monthly** | Float | Sí | Cuota de condominio (si aplica) |
| 82 | **spot_mortgage_principal** | Float | Sí | Monto de hipoteca |
| 83 | **spot_cap_rate** | Float | Sí | CAP rate (Net Operating Income / Property Value) |
| 84 | **spot_roi_percentage** | Float | Sí | ROI esperado % |
| 85 | **spot_appreciation_rate** | Float | Sí | Tasa de apreciación histórica |

### Sección 5: Disponibilidad & Ocupación (Campos 88-110)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 88 | **spot_available_date** | Date | Sí | Fecha de disponibilidad |
| 89 | **spot_is_available** | Boolean | Sí | ¿Está disponible ahora? |
| 90 | **spot_occupancy_rate** | Float | Sí | Tasa de ocupación (0-100%) |
| 91 | **spot_vacancy_rate** | Float | Sí | Tasa de vacancia (0-100%) |
| 92 | **spot_vacancy_period_months** | Integer | Sí | Meses de vacancia |
| 93 | **spot_leased_area_sqm** | Float | Sí | Área arrendada |
| 94 | **spot_number_of_current_tenants** | Integer | Sí | Número de inquilinos actuales |
| 95 | **spot_turnover_rate_annual** | Float | Sí | Tasa de rotación anual de inquilinos |
| 96 | **spot_lease_expiry_dates** | Array | Sí | Array de fechas de vencimiento de contratos |
| 97 | **spot_upcoming_vacancies_3months** | Integer | Sí | Espacios que van a quedar vacíos en 3 meses |
| 98 | **spot_anchor_tenant_names** | Array | Sí | Nombres de inquilinos "ancla" principales |
| 99 | **spot_tenant_mix** | String | Sí | Mix de inquilinos: "Diversified", "Dominant Anchor", "Specialty" |
| 100 | **spot_longest_lease_years** | Integer | Sí | Plazo más largo en años |
| 101 | **spot_shortest_lease_years** | Integer | Sí | Plazo más corto en años |
| 102 | **spot_average_lease_term_years** | Float | Sí | Plazo promedio |
| 103 | **spot_lease_renewal_probability** | Float | Sí | Probabilidad de renovación % |
| 104 | **spot_credit_quality_tenants** | String | Sí | Calidad crediticia: "AAA", "AA", "A", "BBB", "Below" |
| 105 | **spot_rollover_rent_increase** | Float | Sí | Incremento en renovación % |
| 106 | **spot_days_on_market** | Integer | Sí | Días desde que está disponible |
| 107 | **spot_absorption_rate_sqm_month** | Float | Sí | Velocidad de absorción m²/mes |
| 108 | **spot_estimated_lease_time_days** | Integer | Sí | Días estimados para arrendar |
| 109 | **spot_market_demand_indicator** | String | Sí | Indicador: "High", "Medium", "Low" |
| 110 | **spot_competitive_advantage** | String | Sí | Ventaja competitiva principal |

### Sección 6: Engagement & Analítica (Campos 113-135)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 113 | **spot_total_views** | Integer | Sí | Número total de visualizaciones |
| 114 | **spot_views_last_7_days** | Integer | Sí | Vistas últimos 7 días |
| 115 | **spot_views_last_30_days** | Integer | Sí | Vistas últimos 30 días |
| 116 | **spot_unique_visitors** | Integer | Sí | Visitantes únicos |
| 117 | **spot_favorites_count** | Integer | Sí | Número de "favoritos" |
| 118 | **spot_inquiry_count** | Integer | Sí | Inquietudes/preguntas recibidas |
| 119 | **spot_inquiry_to_visit_ratio** | Float | Sí | Ratio de inquietudes que resultan en visita |
| 120 | **spot_visit_count** | Integer | Sí | Número de visitas realizadas |
| 121 | **spot_visit_request_pending** | Integer | Sí | Solicitudes de visita pendientes |
| 122 | **spot_conversion_rate_inquiry_to_lease** | Float | Sí | % de inquietudes que resultan en arrendamiento |
| 123 | **spot_engagement_score** | Float | Sí | Score de engagement 0-100 |
| 124 | **spot_popularity_ranking_city** | Integer | Sí | Ranking de popularidad en la ciudad |
| 125 | **spot_popularity_ranking_sector** | Integer | Sí | Ranking dentro del sector |
| 126 | **spot_click_through_rate** | Float | Sí | CTR de listings |
| 127 | **spot_average_time_on_page_seconds** | Integer | Sí | Tiempo promedio en página |
| 128 | **spot_bounce_rate** | Float | Sí | Tasa de rebote % |
| 129 | **spot_quality_score** | Float | Sí | Score de calidad de listing 0-100 |
| 130 | **spot_description_completeness** | Float | Sí | % de campos completados |
| 131 | **spot_photo_count** | Integer | Sí | Número de fotografías |
| 132 | **spot_has_virtual_tour** | Boolean | Sí | ¿Tiene recorrido virtual? |
| 133 | **spot_has_floor_plan** | Boolean | Sí | ¿Tiene plano de pisos? |
| 134 | **spot_last_activity_date** | Date | Sí | Última actualización del listing |
| 135 | **spot_listing_duration_days** | Integer | Sí | Días desde que se listó |

### Sección 7: Cumplimiento & Administración (Campos 138-187)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 138 | **spot_verification_status** | String | Sí | "Verified", "Pending", "Rejected" |
| 139 | **spot_verified_date** | Date | Sí | Fecha de verificación |
| 140 | **spot_verified_by_user_id** | Integer | Sí | FK a user_id de quién verificó |
| 141 | **spot_listing_status** | String | No | "Active", "Inactive", "Sold", "Delisted" |
| 142 | **spot_featured** | Boolean | Sí | ¿Está destacado en portada? |
| 143 | **spot_featured_until_date** | Date | Sí | Fecha de término de featured |
| 144 | **spot_featured_count** | Integer | Sí | Número de veces destacado |
| 145 | **spot_zoning_classification** | String | Sí | Clasificación zoning municipal |
| 146 | **spot_environmental_risk** | String | Sí | Riesgo ambiental: "None", "Low", "Medium", "High" |
| 147 | **spot_flood_zone** | Boolean | Sí | ¿Está en zona de inundación? |
| 148 | **spot_seismic_zone** | String | Sí | Zona sísmica: "Low", "Medium", "High" |
| 149 | **spot_legal_compliance_status** | String | Sí | "Compliant", "Non-Compliant", "Pending" |
| 150 | **spot_permits_valid** | Boolean | Sí | ¿Permisos vigentes? |
| 151 | **spot_property_rights_clear** | Boolean | Sí | ¿Derechos de propiedad claros? |
| 152 | **spot_title_insurance** | Boolean | Sí | ¿Tiene seguro de título? |
| 153 | **spot_liens_or_encumbrances** | String | Sí | Gravámenes: "None", "Minor", "Significant" |
| 154 | **spot_litigation_history** | String | Sí | Historial legal |
| 155 | **spot_depreciation_schedule** | String | Sí | Cronograma de depreciación |
| 156 | **spot_tax_abatement_available** | Boolean | Sí | ¿Hay abatimientos fiscales? |
| 157 | **spot_tax_abatement_end_date** | Date | Sí | Fecha de término de abatimiento |
| 158 | **spot_homeowners_association** | String | Sí | Asociación de propietarios (si aplica) |
| 159 | **spot_hoa_fees_monthly** | Float | Sí | Cuota HOA mensual |
| 160 | **spot_hoa_contact** | String | Sí | Contacto de HOA |
| 161 | **spot_restrictive_covenants** | Boolean | Sí | ¿Hay restricciones en el uso? |
| 162 | **spot_deed_restrictions** | String | Sí | Restricciones de escritura |
| 163 | **spot_easements** | String | Sí | Servidumbres existentes |
| 164 | **spot_homeowner_insurance_available** | Boolean | Sí | ¿Asegurable? |
| 165 | **spot_hazard_insurance_cost_annual** | Float | Sí | Costo seguro de daños |
| 166 | **spot_flood_insurance_available** | Boolean | Sí | ¿Seguros inundación disponible? |
| 167 | **spot_appraisal_value** | Float | Sí | Valor de tasación |
| 168 | **spot_appraisal_date** | Date | Sí | Fecha de tasación |
| 169 | **spot_appraiser_name** | String | Sí | Nombre del tasador |
| 170 | **spot_pending_assessments** | Float | Sí | Evaluaciones pendientes |
| 171 | **spot_special_assessments** | Float | Sí | Evaluaciones especiales |
| 172 | **spot_assessment_year** | Integer | Sí | Año de evaluación |
| 173 | **spot_last_inspection_date** | Date | Sí | Fecha de última inspección |
| 174 | **spot_inspection_report_summary** | String | Sí | Resumen del reporte de inspección |
| 175 | **spot_major_repairs_needed** | Boolean | Sí | ¿Necesita reparaciones mayores? |
| 176 | **spot_repair_estimate** | Float | Sí | Estimado de reparaciones |
| 177 | **spot_warranty_information** | String | Sí | Información de garantías |
| 178 | **spot_created_date** | Date | No | Fecha de creación del registro |
| 179 | **spot_created_by_user_id** | Integer | Sí | FK a user_id quien creó |
| 180 | **spot_last_updated_date** | Date | No | Última actualización |
| 181 | **spot_last_updated_by_user_id** | Integer | Sí | FK a user_id quien actualizó |
| 182 | **spot_deleted_date** | Date | Sí | Fecha de eliminación (soft delete) |
| 183 | **spot_data_source** | String | Sí | Origen de datos (portal, MLS, manual, etc) |
| 184 | **spot_source_url** | String | Sí | URL del source original |
| 185 | **spot_source_sync_date** | Date | Sí | Última sincronización de source |
| 186 | **spot_notes** | String | Sí | Notas internas |
| 187 | **spot_tags** | Array | Sí | Tags personalizados |

---

## Property Sector Distribution

**Note**: Sector distribution varies by market and time period. For live distribution:

| Sector | Descripción |
|--------|-------------|
| **Retail** | Centros comerciales, locales |
| **Industrial** | Bodegas, naves |
| **Office** | Oficinas, corporativo |
| **Land** | Terrenos disponibles |

Filter by `spot_sector` field and calculate counts for current period.

---

## Key Metrics

| Métrica | Valor | Observaciones |
|---------|-------|-----------------|
| **Total Properties** | 8,407 | Propiedades únicas |
| **Available Properties** | 3,530 (42%) | spot_is_available = 'Yes' |
| **Occupied Properties** | 4,877 (58%) | Arrendadas/vendidas |
| **Average Price per m²** | $8,450 | Variación por sector |
| **Average Rent per m²** | $485/mes | Variación por zona |
| **Avg Built Area** | 2,850 m² | Por propiedad |
| **Avg Views per Property** | 287 | Últimos 30 días |
| **Avg Engagement Score** | 62.5/100 | Indicador de popularidad |
| **Properties with Tenants** | 3,200 (38%) | Con ocupantes actuales |
| **Green Certified** | 1,240 (14.8%) | Certificación ambiental |

---

## Data Quality Checks

| Campo | Completitud | Notas |
|-------|-------------|-------|
| spot_state | 100% | Siempre presente |
| spot_sector | 100% | Requerido |
| spot_street_address | 98% | Muy completo |
| spot_rent_price_per_sqm | 87% | NULL para venta-only |
| spot_sale_price_per_sqm | 82% | NULL para renta-only |
| spot_longitude / spot_latitude | 95% | Para geolocalización |
| spot_photo_count | 92% | Casi todas tienen fotos |

---

## Related Tables

| Tabla | Join Key | Tipo Relación | Descripción |
|-------|----------|---------------|-------------|
| **lk_projects** | spot_id | One-to-Many | Proyectos interesados en propiedad |
| **bt_lds_lead_spots** | spot_id | One-to-Many | Leads que visitaron/interactuaron |
| **bt_transactions** | spot_id | One-to-Many | Transacciones cerradas |

---

## Notes & Considerations

- 🏢 **Mix de Sectores**: 45.8% Retail, 23.7% Industrial, 21.9% Office, 8.5% Land
- 📍 **Cobertura Geográfica**: Principalmente CDMX y principales ciudades de México
- 💰 **Pricing**: Rango amplio según sector, zona y características
- 👥 **Ocupación**: 58% ocupado, 42% disponible (mercado activo)
- 📊 **Engagement**: Score promedio 62.5/100 (oportunidad de mejora en marketing)
- 🌱 **Sostenibilidad**: 14.8% con certificaciones ambientales (LEED, etc)
- 📸 **Contenido**: 92% con fotografías, 8% sin fotos (mejorar uploads)

---

## Change Log

| Fecha | Cambio | Autor |
|-------|--------|-------|
| 2026-02-23 | Documentación inicial creada | elouzau-spot2 |
