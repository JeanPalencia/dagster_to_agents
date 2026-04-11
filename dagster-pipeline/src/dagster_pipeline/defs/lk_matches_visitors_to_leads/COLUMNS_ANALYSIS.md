# Análisis de columnas: lead_events vs funnel_with_channel

## Columnas que produce `lead_events.sql` (raw GA4)

| Columna          | Uso downstream |
|------------------|-----------------|
| event_date       | Filtro de fechas |
| event_datetime   | **Crítico**: orden, primer evento por usuario, ventana de match con chats |
| user_pseudo_id   | **Crítico**: clave de usuario |
| event_name       | **Crítico**: filtro clientRequestedWhatsappForm, blog, L1, wpp_desktop_no_match |
| email            | Filtro @spot2, has_contact, match vía cliente |
| phone            | **Crítico**: has_contact, phone_clean, match por teléfono |
| from_param       | Opcional en create_match_result |
| spotId           | Opcional en create_match_result |
| page_location    | Spot ID, create_match_result, salida final |
| page_referrer    | No usado en pipeline actual |
| source, medium, campaign_name | Salida final, channel_info en matching |
| device_category  | **Crítico**: WhatsApp desktop vs mobile (wpp_desktop_no_match) |
| channel          | Salida channel_first, channel_info |
| traffic_type     | No usado en pipeline actual |

## Columnas que tiene `funnel_with_channel` (según especificación)

- event_date, entry_point, segunda_bifurcacion, user_pseudo_id, user_sospechoso,
- email, source, medium, campaign_name, Channel, page_locations, conversion_point, Traffic_type

## Mapeo aplicado

| funnel_with_channel | → | lead_events (esperado) |
|---------------------|---|------------------------|
| Channel             | → | channel                |
| page_locations      | → | page_location          |
| Traffic_type        | → | traffic_type           |

## Variables críticas que faltan en funnel_with_channel

1. **event_datetime**  
   Necesario para: primer evento por usuario, filtro por rango de fechas, match por tiempo con conversaciones.  
   **Solución actual**: se deriva de `event_date` como inicio del día (granularidad día, no evento).

2. **event_name**  
   Necesario para: filtrar eventos de lead (clientRequestedWhatsappForm, clientSubmitFormBlog*, etc.) y para `wpp_desktop_no_match`.  
   **Solución actual**: se rellena con placeholder `clientRequestedWhatsappForm` para que el pipeline no rompa. Para resultados correctos, la vista debe exponer `event_name` (granularidad por evento).

3. **phone**  
   Necesario para: match directo chat–lead por teléfono y detección de has_contact.  
   **Solución actual**: se rellena con NaN; el match por email sigue funcionando.

4. **device_category**  
   Necesario para: clasificación WhatsApp desktop vs mobile y para `wpp_desktop_no_match`.  
   **Solución actual**: se rellena con None.

## Recomendación

Para que el pipeline tenga el mismo comportamiento que con `lead_events.sql`, la vista `funnel_with_channel` debería exponer (o una vista derivada a nivel evento):

- **event_datetime** (timestamp del evento)
- **event_name** (nombre del evento de lead)
- **phone** (teléfono si existe en el evento)
- **device_category** (desktop/mobile)

Si la vista sigue siendo a nivel usuario-día, el match por tiempo será por día, no por evento, y no se distinguirán tipos de evento (WhatsApp, blog, etc.).
