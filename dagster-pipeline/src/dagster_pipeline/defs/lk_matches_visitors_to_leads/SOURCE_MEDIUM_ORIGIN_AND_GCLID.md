# Origen de source/medium: Job viejo vs nuevo y el caso gclid (usuario 248186321.1770420847)

**Caso:** El usuario 248186321.1770420847 aparece como **Organic Search** (medium = organic) en el Job Viejo y como **Paid Search** (medium = cpc, con gclid) en el Job Nuevo y en la tabla de BigQuery.

---

## 1. ¿De dónde saca el job viejo `source` y `medium`?

Sí: el job viejo usa **exactamente** los campos de sesión de GA4:

**Archivo:** `visitors_to_leads/queries/lead_events.sql` (líneas 76-79)

```sql
-- Traffic source information
session_traffic_source_last_click.cross_channel_campaign.source AS source,
session_traffic_source_last_click.cross_channel_campaign.medium AS medium,
session_traffic_source_last_click.cross_channel_campaign.campaign_name AS campaign_name,
```

Es decir:

- **source** = `session_traffic_source_last_click.cross_channel_campaign.source`
- **medium** = `session_traffic_source_last_click.cross_channel_campaign.medium`
- **campaign_name** = `session_traffic_source_last_click.cross_channel_campaign.campaign_name`

No se usa en esa query:

- `page_location` (ni parámetros de URL como gclid)
- `event_params` para gclid/fbclid
- Ningún otro campo para “corregir” la atribución.

El canal se calcula **solo** con ese source/medium/campaign_name en el CASE WHEN del mismo SQL (Organic Search cuando `medium = 'organic'`, Paid Search cuando `medium IN ('cpc','spot','paid')` + source/campaign de búsqueda, etc.).

---

## 2. ¿Puede GA4 “perder” el CPC y volver a organic?

Sí. Es un problema conocido del export de GA4 a BigQuery:

- En muchos casos, el **primer evento de la sesión** (landing con **gclid** en la URL) se atribuye mal en los campos de tráfico que exporta GA4: aparece como **organic** o **direct** en lugar de **google / cpc**.
- Eso afecta tanto a los campos “clásicos” de tráfico como, en la práctica, a **session_traffic_source_last_click**: incluso cuando por lógica de “last non-direct click” debería ser Paid Search, los valores de `session_traffic_source_last_click.manual_campaign` (y el bloque cross_channel_campaign que usamos) pueden seguir llegando como **google / organic** para sesiones que en realidad vinieron de Google Ads (con gclid).

Referencia: [GA4 BigQuery misattribution (part 1)](https://www.ga4bigquery.com/how-to-fix-major-ga4-misattribution-bug-for-paid-search-events-and-sessions-gclid/) y [part 3 (session_traffic_source_last_click)](https://www.ga4bigquery.com/how-to-fix-the-major-ga4-bigquery-export-misattribution-part-3-use-the-session_traffic_source_last_click-record/):

- Con gclid en la URL, el evento puede seguir atribuido a organic o direct.
- session_traffic_source_last_click (desde julio 2024) mejora la situación pero, en la práctica, para sesiones de Google Ads los valores de manual_campaign pueden seguir siendo **google / organic** u otros no Paid Search.

Por tanto:

- No hace falta que la “sesión expire” ni una navegación especialmente larga: el bug puede darse **en el primer evento** de la sesión (landing con gclid).
- Si además la sesión es larga o hay muchos eventos, el modelo “last non-direct click” puede terminar usando otro touch (por ejemplo una navegación posterior “direct” o “organic”) y reforzar que en el export aparezca **organic** en lugar de **cpc**.

En resumen: **sí, el campo de GA4 que usa el job viejo puede “perder” el rastro del CPC y verse como organic** incluso cuando en la URL hubo gclid.

---

## 3. Comparación con el pipeline nuevo (funnel_with_channel)

En el **nuevo pipeline** (`lead_events_from_funnel_lk.sql`):

- **raw_events** también obtiene source/medium de GA4:
  - `session_traffic_source_last_click.cross_channel_campaign.source AS source`
  - `session_traffic_source_last_click.cross_channel_campaign.medium AS medium`
  - (mismo origen que el job viejo en la parte de eventos crudos).

- Pero el **canal** que se usa en el output **no** se calcula a partir de ese source/medium en el SQL del repo. Se toma de la tabla/vista de BigQuery:
  - `analitics_spot2.funnel_with_channel`
  - `COALESCE(f.Channel, 'Unassigned') AS channel`

Es decir: **quién define el canal en el nuevo pipeline es la lógica con la que esté construida `funnel_with_channel`** (en otro proyecto/script de BigQuery). Esa lógica puede:

- Usar **page_location** (o equivalentes) para detectar **gclid** en la URL y forzar **Paid Search**.
- Usar **session_traffic_source_last_click.google_ads_campaign** (si la vista tiene acceso a eventos GA4) para marcar sesiones de Google Ads como Paid Search.
- Aplicar una jerarquía distinta (por ejemplo “si hay gclid → Paid Search” antes de mirar medium).

Por eso el **mismo usuario/evento** puede tener:

- En el **viejo:** source/medium tal como vienen en el export (organic) → canal = Organic Search.
- En el **nuevo:** canal = **Paid Search** porque `funnel_with_channel` corrige la atribución usando gclid (o similar).

---

## 4. Pregunta clave: ¿El job viejo ignora los parámetros de la URL (page_location)?

**Sí.** En el job viejo:

- **No** se usa `page_location` (ni ningún parámetro de URL como gclid) para decidir el **canal**.
- Solo se usa lo que GA4 ya trae “masticado” en:
  - `session_traffic_source_last_click.cross_channel_campaign.source`
  - `session_traffic_source_last_click.cross_channel_campaign.medium`
  - `session_traffic_source_last_click.cross_channel_campaign.campaign_name`

En esa misma query, `page_location` **sí** se extrae (desde `event_params`) y se devuelve en el SELECT, pero **no** interviene en el CASE WHEN del canal. Es decir: el job viejo **solo confía en lo que GA4 le da** para source/medium/campaign y no corrige con URL ni gclid.

Por tanto, para el usuario 248186321.1770420847:

- **Job viejo:** Lee source/medium de GA4; si el export tiene ahí organic (por el bug de atribución), el CASE WHEN devuelve **Organic Search**. No mira gclid en page_location.
- **Job nuevo / tabla BigQuery:** Si la tabla o la vista que alimenta el nuevo pipeline usa **gclid** (por ejemplo en page_location) o alguna lógica equivalente para Paid Search, clasifica correctamente ese lead como **Paid Search** con medium = cpc.

---

## 5. Resumen

| Pregunta | Respuesta |
|----------|-----------|
| ¿El job viejo usa session_traffic_source_last_click? | **Sí.** source, medium y campaign_name vienen de `session_traffic_source_last_click.cross_channel_campaign`. |
| ¿Ese campo puede “perder” el CPC y verse como organic? | **Sí.** Es un bug conocido del export de GA4: sesiones con gclid pueden aparecer como organic/direct; incluso con session_traffic_source_last_click, manual_campaign puede seguir como google/organic. |
| ¿El nuevo pipeline extrae distinto? | En **raw_events** usa el mismo source/medium de GA4; el **canal** lo toma de **funnel_with_channel**, que puede usar gclid/page_location u otra lógica y por eso mostrar Paid Search. |
| ¿El job viejo ignora page_location para el canal? | **Sí.** Solo usa source/medium/campaign_name de GA4; no usa parámetros de la URL (page_location, gclid) para definir el canal. |

Para alinear el comportamiento del job viejo al “real” (gclid = Paid Search), habría que añadir en la query vieja una corrección que, cuando en `page_location` exista `gclid`, fuerce source/medium (o el canal) a Paid Search, de forma similar a como lo hace la lógica detrás de `funnel_with_channel`.
