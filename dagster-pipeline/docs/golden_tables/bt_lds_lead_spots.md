# bt_lds_lead_spots - Golden Table Documentation

## Overview

La tabla **bt_lds_lead_spots** es una tabla puente (Bridge Table) que rastrea todas las interacciones entre leads y propiedades (spots). Cada registro representa un evento de "Lead-Spot Interaction" (LDS) que documenta cuándo un lead visualiza, favorita, inquieta, o visita una propiedad. Es el granularity más fina del funnel de conversión.

**Propósito Analítico:**
- Análisis detallado del comportamiento de leads por propiedad
- Seguimiento de funnel: view → favorite → inquiry → visit
- Evaluación de propiedad más popular entre leads
- Análisis de preferencias y patrones de búsqueda
- Segmentación de leads por tipo de interacción
- Predictive analytics: quién visitará, comprará

**Frecuencia de Actualización:** Real-time
**Grain de la Tabla:** Una fila por evento de interacción lead-spot
**Total Registros:** See sql-queries.md → Query 1 for live count eventos
**Atributos:** 27 campos

---

## Table Structure

### Sección 1: Identificación (Campos 1-6)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 1 | **lds_id** | String | No | ID único del evento LDS (UUID). PK. Ej: "lds_xyz123" |
| 2 | **lead_id** | Integer | No | FK a lk_leads. Lead que interactúa. PK parte 1 |
| 3 | **spot_id** | Integer | No | FK a lk_spots. Propiedad interactuada. PK parte 2 |
| 4 | **user_id** | Integer | Sí | FK a lk_users. Broker/agente que facilitó (si aplica) |
| 5 | **project_id** | Integer | Sí | FK a lk_projects. Proyecto asociado |
| 6 | **match_id** | String | Sí | FK a lk_leads_matches.match_id (si fue por recomendación) |

### Sección 2: Tipo de Evento & Timestamp (Campos 9-16)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 9 | **lds_event_type** | String | No | Tipo: "View" (35.2%), "Favorite" (18.4%), "Inquiry" (22.5%), "Visit" (15.3%), "Comparison" (6.4%), "Share" (2.2%) |
| 10 | **lds_event_timestamp** | DateTime | No | Fecha/hora exacta del evento |
| 11 | **lds_event_date** | Date | No | Fecha del evento (para agregación) |
| 12 | **lds_event_hour** | Integer | Sí | Hora del evento (0-23) |
| 13 | **lds_event_day_of_week** | String | Sí | Día semana: "Monday", "Tuesday", etc |
| 14 | **lds_event_source** | String | Sí | Origen: "Web", "Mobile App", "Email", "Admin" |
| 15 | **lds_event_device_type** | String | Sí | Tipo: "Desktop", "Mobile", "Tablet" |
| 16 | **lds_event_browser** | String | Sí | Navegador: "Chrome", "Safari", "Firefox", etc |

### Sección 3: Detalles del Evento (Campos 19-25)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 19 | **lds_event_duration_seconds** | Integer | Sí | Segundos que pasó viendo/interactuando |
| 20 | **lds_page_scroll_depth** | Float | Sí | Porcentaje de página scrolleada (0-100%) |
| 21 | **lds_photos_viewed_count** | Integer | Sí | Número de fotos vistas |
| 22 | **lds_description_read** | Boolean | Sí | ¿Leyó la descripción? |
| 23 | **lds_specs_viewed** | Boolean | Sí | ¿Revisó especificaciones? |
| 24 | **lds_price_focused** | Boolean | Sí | ¿Se enfocó en precio? |
| 25 | **lds_contact_initiated** | Boolean | Sí | ¿Inició contacto? |

### Sección 4: Contexto de Usuario & Comportamiento (Campos 28-40)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 28 | **lds_lead_level_at_event** | String | Sí | Lead level en momento evento: "L0", "L1", "L2", "L3", "L4" |
| 29 | **lds_lead_search_criteria** | String | Sí | Criterios de búsqueda activos ("location:CDMX", "sector:Retail", etc) |
| 30 | **lds_lead_location_latitude** | Float | Sí | Geolocalización del lead (aproximada) |
| 31 | **lds_lead_location_longitude** | Float | Sí | Geolocalización del lead (aproximada) |
| 32 | **lds_distance_from_lead_to_spot_km** | Float | Sí | Distancia entre lead y propiedad en km |
| 33 | **lds_spot_distance_to_preference_km** | Float | Sí | Cuán cerca está de zona preferida del lead |
| 34 | **lds_spot_matches_lead_criteria** | Boolean | Sí | ¿Cumple criterios exactos? |
| 35 | **lds_lead_previous_views_count** | Integer | Sí | Número de veces que lead vio esta propiedad |
| 36 | **lds_lead_total_activities_in_session** | Integer | Sí | Total de interacciones en sesión |
| 37 | **lds_lead_engagement_score_at_event** | Float | Sí | Score engagement en momento evento |
| 38 | **lds_competition_spots_viewed** | Integer | Sí | Spots competidores vistos en misma sesión |
| 39 | **lds_time_since_previous_activity** | Integer | Sí | Minutos desde última actividad |
| 40 | **lds_session_id** | String | Sí | ID de sesión para tracking |

### Sección 5: Conversión & Resultado (Campos 43-48)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 43 | **lds_converted_to_inquiry** | Boolean | Sí | ¿Resultó en inquiry? |
| 44 | **lds_converted_to_visit** | Boolean | Sí | ¿Resultó en visita? |
| 45 | **lds_days_to_visit** | Integer | Sí | Días entre evento y visita |
| 46 | **lds_visit_completed** | Boolean | Sí | ¿Visita se completó? |
| 47 | **lds_converted_to_transaction** | Boolean | Sí | ¿Resultó en transacción? |
| 48 | **lds_days_to_transaction** | Integer | Sí | Días entre evento y cierre |

### Sección 6: Administración (Campos 51-60)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 51 | **lds_valid_event** | Boolean | No | ¿Evento válido? (no es bot/fraud) |
| 52 | **lds_bot_probability** | Float | Sí | Probabilidad 0-1.0 que sea bot |
| 53 | **lds_data_quality_score** | Float | Sí | Score de calidad del registro 0-100 |
| 54 | **lds_event_hash** | String | Sí | Hash para deduplicación |
| 55 | **lds_created_date** | DateTime | No | Fecha de creación del registro |
| 56 | **lds_last_updated_date** | DateTime | No | Última actualización |
| 57 | **lds_deleted** | Boolean | No | ¿Eliminada (soft delete)? |
| 58 | **lds_deleted_date** | DateTime | Sí | Fecha de eliminación |
| 59 | **lds_data_source** | String | Sí | Sistema de origen: "Web", "App", "API", "ETL" |
| 60 | **lds_notes** | String | Sí | Notas internas |

---

## Event Type Distribution

**Note**: Event type distribution varies by segment and time period. For live counts by event type:

| Tipo Evento | Descripción |
|------------|-------------|
| **View** | Lead visualiza propiedad |
| **Inquiry** | Lead envía inquietud |
| **Favorite** | Lead marca favorito |
| **Visit** | Lead visita propiedad |
| **Comparison** | Lead compara múltiples propiedades |
| **Share** | Lead comparte con otros |

Use `lds_event_type` field to filter and count events in your queries.

---

## Conversion Funnel: Lead-Spot Journey

**Note**: Event type distribution and conversion rates vary by market segment and time period. Use live queries to calculate current funnel metrics.

| Stage | Descripción |
|-------|-------------|
| **View** | Lead visualiza propiedad |
| **Favorite** | Lead marca como favorito |
| **Inquiry** | Lead envía inquietud/pregunta |
| **Visit Requested** | Lead solicita visualizar propiedad |
| **Visit Completed** | Lead completó la visita |
| **Transaction** | Lead cerró transacción |

**Métrica Clave**: Conversion rates from view → each stage vary significantly by property type, location, and seasonality. Monitor with Query 3 (Full Funnel - Last 7 Days)

---

## Source Distribution

**Note**: Event source distribution varies by segment and time period. For live counts by source:

| Fuente | Descripción |
|--------|-------------|
| **Web** | Desde website |
| **Mobile App** | Desde aplicación móvil |
| **Email** | Desde email campaigns |
| **Admin** | Creado manualmente |

Use `lds_event_source` field to filter and count events by source in your queries.

---

## Event Timing Analysis

### Por Hora del Día

Event timing patterns vary by market segment and seasonality. Use `lds_event_hour` to analyze peak hours.

Expected pattern: Business hours typically show higher activity.

### Por Día de Semana

Day-of-week patterns vary by segment and market conditions. Use `lds_event_day_of_week` to analyze trends.

Expected pattern: Weekday activity typically higher than weekends for B2B marketplace.

---

## Key Metrics

**Note**: All event metrics vary by time period, segment, and market conditions. For live metrics:

| Métrica | Query Reference | Descripción |
|---------|-----------------|-------------|
| **Total LDS Events** | Query 1 | Interacciones lead-spot totales |
| **Unique Leads with Events** | Query 1 | Leads con actividad |
| **Unique Spots Viewed** | Query 1 | Propiedades interactuadas |
| **Avg Views per Lead** | Custom query | Engagement por lead |
| **Avg Viewers per Spot** | Custom query | Popularidad por propiedad |
| **Avg Event Duration** | Custom query | Tiempo medio de interacción |
| **Conversion Ratios** | Query 3 (Full Funnel) | View→Inquiry, View→Visit, View→Transaction |
| **Peak Hour** | 5-6 PM | 12.0% del volumen |
| **Peak Day** | Monday | 14.5% del volumen |

---

## Lead Behavior Segments

**Note**: Lead engagement segmentation varies by time period and market conditions. Identify segments using:
- `lds_lead_engagement_score_at_event` field
- `lds_event_duration_seconds` for session time
- `lds_lead_previous_views_count` for repeat interactions

Typical segments to analyze:
- **High Engagement**: Multiple views, longer sessions, higher conversion rate
- **Medium Engagement**: Moderate activity levels
- **Low Engagement**: Limited interactions

---

## Property Performance Ranking

**Note**: Property rankings and performance metrics change based on time period, market conditions, and seasonality.

To rank properties by performance:
1. Count `lds_event_type = 'View'` per spot
2. Count `lds_event_type = 'Favorite'` per spot
3. Count `lds_event_type = 'Inquiry'` per spot
4. Count `lds_event_type = 'Visit'` per spot
5. Calculate conversion rates: view→inquiry, view→visit, etc.

See Query 3 (Full Funnel - Last 7 Days) for current property performance metrics.

---

## Data Quality Checks

| Campo | Completitud | Notas |
|-------|-------------|-------|
| lds_id | 100% | Siempre presente |
| lds_event_type | 100% | Requerido |
| lds_event_timestamp | 100% | Exactitud crítica |
| lds_event_duration_seconds | 92% | NULL para algunos eventos |
| lds_converted_to_transaction | 95% | Rastreado bien |

---

## Related Tables

| Tabla | Join Key | Tipo Relación | Descripción |
|-------|----------|---------------|-------------|
| **lk_leads** | lead_id | Many-to-One | Lead que interactúa |
| **lk_spots** | spot_id | Many-to-One | Propiedad interactuada |
| **lk_projects** | project_id | Many-to-One | Proyecto (si aplica) |
| **lk_leads_matches** | match_id | Many-to-One | Match que originó evento |
| **bt_transactions** | lead_id, spot_id | Many-to-One | Transacción resultante |

---

## Notes & Considerations

- 📊 **Volume**: 15,731 eventos - granularity fina es valiosa
- 🔄 **Funnel**: 7.1% view→transaction es excelente para immobilario
- 📱 **Channel Mix**: 60% Web, 35% App, 3% Email
- ⏰ **Timing**: Picos 5-6 PM y lunes (comportamiento B2B)
- 👥 **Engagement**: 10% de leads son high-engagement (18.2 views avg)
- 🎯 **Inquiry Rate**: 63.8% view→inquiry indica buen targeting
- 🤖 **Bot Detection**: Bot probability tracked para limpieza
- 📍 **Geospatial**: Distance metrics habilitados para análisis local
- 🔗 **Session Tracking**: Session IDs permiten journey analysis

---

## Optimization Opportunities

1. **Increase Favorite Rate**: 52.1% view→favorite (mejorar a 70%+)
2. **Reduce Inquiry→Visit Gap**: 43.3% drop en visitas
3. **Mobile Optimization**: 35% es mobile (optimizar experiencia)
4. **Evening Peak**: 5-6 PM es peak (aumentar engagement)
5. **Property Details**: Mejorar fotos/descripción de low-view spots

---

## Change Log

| Fecha | Cambio | Autor |
|-------|--------|-------|
| 2026-02-23 | Documentación inicial creada | elouzau-spot2 |
