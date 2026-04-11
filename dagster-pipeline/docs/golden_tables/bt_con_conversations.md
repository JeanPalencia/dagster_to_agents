# bt_con_conversations - Golden Table Documentation

## Overview

La tabla **bt_con_conversations** es una tabla puente (Bridge Table) que almacena todas las conversaciones entre leads y brokers/agentes dentro de la plataforma. Cada registro representa una conversación completa (thread) con múltiples mensajes entre participantes. Rastrea contenido, timestamps, estado de lectura, y tópicos.

**Propósito Analítico:**
- Análisis de engagement y comunicación entre brokers y leads
- Seguimiento de velocidad de respuesta (SLA)
- Análisis de tópicos de conversación
- Evaluación de calidad de servicio
- Seguimiento de conversaciones por lead y broker
- Análisis de conversión: conversación → visita → transacción

**Frecuencia de Actualización:** Real-time
**Grain de la Tabla:** Una fila por conversación (thread)
**Total Registros:** See sql-queries.md → Query 1 for live count conversaciones
**Atributos:** 17 campos base + 45 conv_variables (métricas calculadas)

---

## Table Structure

### Sección 1: Identificación (Campos 1-5)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 1 | **conv_id** | String | No | ID único de conversación (UUID). PK. Ej: "conv_a1b2c3d4" |
| 2 | **lead_id** | Integer | No | FK a lk_leads. Lead participante |
| 3 | **user_id** | Integer | No | FK a lk_users. Broker/agente respondedor |
| 4 | **spot_id** | Integer | Sí | FK a lk_spots. Propiedad de la conversación (si aplica) |
| 5 | **project_id** | Integer | Sí | FK a lk_projects. Proyecto asociado (si aplica) |

### Sección 2: Timestamps & Duración (Campos 8-12)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 8 | **conv_created_date** | DateTime | No | Fecha/hora de primer mensaje |
| 9 | **conv_last_message_date** | DateTime | Sí | Fecha/hora del último mensaje |
| 10 | **conv_last_message_by_lead** | DateTime | Sí | Última vez que lead escribió |
| 11 | **conv_last_message_by_broker** | DateTime | Sí | Última vez que broker escribió |
| 12 | **conv_conversation_duration_days** | Integer | Sí | Días entre primer y último mensaje |

### Sección 3: Contenido & Tópicos (Campos 15-19)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 15 | **conv_subject** | String | Sí | Asunto/tema de la conversación. Ej: "Interés en Retail CDMX" |
| 16 | **conv_topic** | String | Sí | Clasificación: "Property Inquiry", "Availability", "Pricing", "Other" |
| 17 | **conv_topic_keywords** | Array | Sí | Array de palabras clave: ["rent", "price", "available", "discount"] |
| 18 | **conv_sentiment** | String | Sí | Sentiment general: "Positive", "Neutral", "Negative" |
| 19 | **conv_complexity_score** | Integer | Sí | Complejidad 1-10 (1=simple, 10=muy compleja) |

### Sección 4: Métricas de Engagement (Campos 22-32)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 22 | **conv_message_count** | Integer | No | Total de mensajes en thread |
| 23 | **conv_message_count_by_lead** | Integer | Sí | Mensajes del lead |
| 24 | **conv_message_count_by_broker** | Integer | Sí | Mensajes del broker |
| 25 | **conv_avg_message_length** | Integer | Sí | Promedio de caracteres por mensaje |
| 26 | **conv_avg_response_time_hours** | Float | Sí | Tiempo promedio de respuesta en horas |
| 27 | **conv_max_response_time_hours** | Float | Sí | Respuesta más lenta en horas |
| 28 | **conv_min_response_time_hours** | Float | Sí | Respuesta más rápida en horas |
| 29 | **conv_messages_unread_lead** | Integer | Sí | Mensajes sin leer por lead |
| 30 | **conv_messages_unread_broker** | Integer | Sí | Mensajes sin leer por broker |
| 31 | **conv_is_open** | Boolean | No | ¿Conversación abierta? Values: "Yes", "No" |
| 32 | **conv_closed_date** | DateTime | Sí | Fecha de cierre |

### Sección 5: Conversión & Outcome (Campos 35-42)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 35 | **conv_resulted_in_visit** | Boolean | Sí | ¿Resultó en visita? |
| 36 | **conv_visit_date** | DateTime | Sí | Fecha de visita si aplica |
| 37 | **conv_resulted_in_project** | Boolean | Sí | ¿Resultó en proyecto? |
| 38 | **conv_project_date** | DateTime | Sí | Fecha de creación proyecto |
| 39 | **conv_resulted_in_transaction** | Boolean | Sí | ¿Resultó en transacción/cierre? |
| 40 | **conv_transaction_date** | DateTime | Sí | Fecha de cierre |
| 41 | **conv_conversion_funnel_stage** | String | Sí | Etapa: "Inquiry", "Engaged", "Visited", "Won", "Lost" |
| 42 | **conv_outcome** | String | Sí | "Converted", "Dropped", "Inactive", "Spam" |

### Sección 6: Quality & Compliance (Campos 45-60)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 45 | **conv_quality_rating** | Integer | Sí | Rating de calidad 1-5 estrellas |
| 46 | **conv_quality_feedback** | String | Sí | Comentarios sobre la conversación |
| 47 | **conv_sla_response_met** | Boolean | Sí | ¿Cumplió SLA de respuesta? (< 4 horas) |
| 48 | **conv_sla_resolution_met** | Boolean | Sí | ¿Cumplió SLA de resolución? (< 48 horas) |
| 49 | **conv_inappropriate_content** | Boolean | Sí | ¿Contiene contenido inapropiado? |
| 50 | **conv_spam_flagged** | Boolean | Sí | ¿Fue marcado como spam? |
| 51 | **conv_requires_escalation** | Boolean | Sí | ¿Requiere escalación? |
| 52 | **conv_escalation_reason** | String | Sí | Razón de escalación |
| 53 | **conv_escalated_to_user_id** | Integer | Sí | FK a user_id que escalonó |
| 54 | **conv_archived** | Boolean | No | ¿Archivada? |
| 55 | **conv_archived_date** | DateTime | Sí | Fecha de archivo |
| 56 | **conv_deleted** | Boolean | No | ¿Eliminada (soft delete)? |
| 57 | **conv_deleted_date** | DateTime | Sí | Fecha de eliminación |
| 58 | **conv_data_source** | String | Sí | Origen: "Web", "Mobile App", "Email", "API" |
| 59 | **conv_notes** | String | Sí | Notas internas |
| 60 | **conv_last_updated_date** | DateTime | No | Última actualización |

---

## Conv_Variables: Métricas Calculadas (45 variables)

Dimensión de análisis profundo de patrones de conversación:

| Variable ID | Nombre | Tipo | Rango | Descripción |
|------------|--------|------|-------|-------------|
| cv_001 | lead_engagement_score | Float | 0-100 | Score de engagement del lead |
| cv_002 | broker_responsiveness_score | Float | 0-100 | Score de responsividad del broker |
| cv_003 | conversation_momentum | Float | -100 to 100 | Tendencia de conversación |
| cv_004 | urgency_indicator | Integer | 0-10 | Nivel de urgencia detectado |
| cv_005 | price_sensitivity | Boolean | true/false | ¿Lead es sensible a precio? |
| cv_006 | location_flexibility | Boolean | true/false | ¿Flexible en ubicación? |
| cv_007 | size_flexibility | Boolean | true/false | ¿Flexible en tamaño? |
| cv_008 | timeline_pressure | Integer | 0-10 | Presión de tiempo detectada |
| cv_009 | competitor_mention | Boolean | true/false | ¿Menciona competidor? |
| cv_010 | objection_count | Integer | 0-N | Número de objeciones |
| cv_011 | question_count | Integer | 0-N | Número de preguntas |
| cv_012 | action_item_count | Integer | 0-N | Número de acciones pendientes |
| cv_013 | next_step_clear | Boolean | true/false | ¿Próximo paso claro? |
| cv_014 | buyer_intent_signal | String | "None","Low","Medium","High" | Señal de intención de compra |
| cv_015 | negotiation_stage | String | "Early","Mid","Late","Closed" | Etapa de negociación |
| cv_016 | budget_discussed | Boolean | true/false | ¿Se discutió presupuesto? |
| cv_017 | timeline_discussed | Boolean | true/false | ¿Se discutió timeline? |
| cv_018 | terms_discussed | Boolean | true/false | ¿Se discutieron términos? |
| cv_019 | decision_maker_identified | Boolean | true/false | ¿Se identificó tomador de decisiones? |
| cv_020 | stakeholders_count | Integer | 1-N | Número de stakeholders |
| cv_021 | message_tone_professional | Float | 0-1.0 | Profesionalismo detectado |
| cv_022 | message_tone_friendly | Float | 0-1.0 | Amabilidad detectada |
| cv_023 | message_tone_urgent | Float | 0-1.0 | Urgencia en tono |
| cv_024 | request_conciseness | Float | 0-1.0 | Qué tan conciso es el lead |
| cv_025 | broker_helpfulness | Float | 0-1.0 | Percepción de utilidad del broker |
| cv_026 | information_gaps | Integer | 0-N | Número de gaps de información |
| cv_027 | follow_up_due_count | Integer | 0-N | Follow-ups requeridos |
| cv_028 | days_since_last_message | Integer | 0-365+ | Días de inactividad |
| cv_029 | conversation_velocity | Float | msgs/day | Velocidad de mensajes |
| cv_030 | time_to_first_response | Float | hours | Tiempo a primera respuesta |
| cv_031 | total_wait_time | Float | hours | Tiempo total de espera |
| cv_032 | lead_response_pattern | String | "Responsive","Sporadic","Delayed" | Patrón de respuesta |
| cv_033 | broker_availability_pattern | String | "24/7","Business Hours","Sporadic" | Disponibilidad broker |
| cv_034 | content_relevance_score | Float | 0-100 | Score de relevancia contenido |
| cv_035 | conversion_probability | Float | 0-1.0 | Probabilidad de conversión |
| cv_036 | churn_risk_score | Float | 0-100 | Score de riesgo de abandono |
| cv_037 | lifetime_value_potential | String | "Low","Medium","High","VIP" | LTV potencial |
| cv_038 | requires_special_handling | Boolean | true/false | ¿Requiere atención especial? |
| cv_039 | seasonal_keyword_detected | Boolean | true/false | ¿Patrón estacional? |
| cv_040 | market_trend_mention | Boolean | true/false | ¿Menciona tendencias? |
| cv_041 | regulatory_concern_mentioned | Boolean | true/false | ¿Preocupaciones regulatorias? |
| cv_042 | sustainability_interest | Boolean | true/false | ¿Interés en sostenibilidad? |
| cv_043 | premium_service_upgrade_eligible | Boolean | true/false | ¿Elegible para upgrade? |
| cv_044 | cross_sell_opportunity | Boolean | true/false | ¿Oportunidad de cross-sell? |
| cv_045 | referral_potential | Boolean | true/false | ¿Potencial de referencia? |

---

## Conversation Topic Distribution

**Note**: Topic distribution varies by market segment and time period. For live topic counts:

| Tópico | Descripción |
|--------|-------------|
| **Property Inquiry** | Preguntas sobre propiedad específica |
| **Availability** | Disponibilidad y ocupación |
| **Pricing** | Negociación de precios |
| **Other** | General, logistics, etc |

Use `conv_topic` field to filter and analyze conversation topics in your queries.

---

## Conversation Outcome Distribution

**Note**: Outcome distribution varies by segment and time period. For live outcome counts:

| Resultado | Descripción |
|-----------|-------------|
| **Converted** | Conversión a transacción |
| **Visited** | Lead realizó visita |
| **Dropped** | Lead dejó de responder |
| **Inactive** | Conversación en pausa |

Use `conv_outcome` field to filter and analyze conversation outcomes in your queries.

---

## Key Metrics

**Note**: All conversation metrics vary by time period and segment. For live metrics:

| Métrica | Query Reference | Descripción |
|---------|-----------------|-------------|
| **Total Conversations** | Query 1 | Threads únicos |
| **Open Conversations** | Query 1 | Conversaciones activas |
| **Closed Conversations** | Query 1 | Completadas o inactivas |
| **Avg Messages per Conv** | Query 10 | Total mensajes promedio |
| **Avg Response Time** | Query 10 | Tiempo broker responde |
| **Conversion Rates** | Query 10 | Conv→Visit, Conv→Project, Conv→Transaction |
| **SLA Performance** | Query 10 | Response < 4h, Resolution < 48h |
| **Avg Conversation Duration** | Query 10 | Primer a último mensaje |
| **Quality Rating** | Query 10 | Satisfacción general |

---

## Event Type Analysis (13 tipos de eventos)

**Note**: Event type distribution varies. For live event counts by type, query bt_con_conversations filtering by event type values.

| Evento | Descripción |
|--------|-------------|
| **Message Sent** | Cualquier lado envía mensaje |
| **Message Read** | Mensaje leído por receptor |
| **Visit Scheduled** | Visita agendada desde conversación |
| **Offer Made** | Propuesta formal enviada |
| **Negotiation** | Renegociación activa |
| **Contract Sent** | Contrato enviado |
| **Escalation** | Escalado a supervisor |
| **Resolved** | Conversación resuelta |
| **Closed Won** | Conversación cerrada - ganada |
| **Closed Lost** | Conversación cerrada - perdida |
| **Reopened** | Reabierta después cierre |
| **Archived** | Archivada sin resolución |
| **Spam Reported** | Reportado como spam |

---

## Response Time SLA Analysis

**Note**: SLA performance metrics vary by segment and time period. Monitor with Query 10 (Conversation Impact Analysis):

| SLA | Target | Descripción |
|-----|--------|-------------|
| **First Response** | < 4 hours | Tiempo a primer mensaje |
| **Substantive Response** | < 24 hours | Tiempo a respuesta sustantiva |
| **Resolution** | < 48 hours | Tiempo a resolución |

Use `conv_avg_response_time_hours`, `conv_max_response_time_hours`, `conv_sla_response_met`, and `conv_sla_resolution_met` fields to calculate current SLA performance.

---

## Data Quality Checks

**Note**: Data completeness varies. Check actual null counts in your environment:

| Campo | Descripción |
|-------|-------------|
| conv_id | Primary key - always present |
| conv_message_count | Calculated field |
| conv_avg_response_time_hours | NULL for open conversations |
| conv_outcome | NULL for active conversations |
| conv_sentiment | NLP-classified field - may be NULL |

---

## Related Tables

| Tabla | Join Key | Tipo Relación | Descripción |
|-------|----------|---------------|-------------|
| **lk_leads** | lead_id | Many-to-One | Lead participante |
| **lk_users** | user_id | Many-to-One | Broker/agente participante |
| **lk_spots** | spot_id | Many-to-One | Propiedad (si aplica) |
| **lk_projects** | project_id | Many-to-One | Proyecto (si aplica) |
| **bt_transactions** | conv_id | Many-to-One | Transacción resultante |

---

## Notes & Considerations

- 💬 **Activity**: Monitor open vs closed conversations for engagement opportunities
- ⏱️ **SLA Performance**: Track response and resolution time trends with Query 10
- 📈 **Conversion Quality**: Measure conversation→visit and conversation→transaction rates with Query 10
- 🔄 **Visit Conversion**: Compare direct visit requests vs conversation-initiated visits
- 📊 **Volume**: Analyze average messages per conversation as engagement indicator
- 🎯 **Topics**: Use `conv_topic` to understand conversation drivers
- 😊 **Satisfaction**: Monitor `conv_quality_rating` for satisfaction trends
- ⚠️ **Spam Risk**: Track `conv_spam_flagged` and `conv_inappropriate_content` for quality issues
- 🚀 **Momentum**: Use conv_variables (cv_001-cv_045) for advanced conversation analysis

---

## Change Log

| Fecha | Cambio | Autor |
|-------|--------|-------|
| 2026-02-23 | Documentación inicial creada | elouzau-spot2 |
