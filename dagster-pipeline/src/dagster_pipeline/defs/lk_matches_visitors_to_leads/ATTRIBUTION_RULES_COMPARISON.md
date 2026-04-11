# Comparación de reglas de atribución: Job viejo vs Job nuevo (LK)

**Contexto:** 11 leads con discrepancia de canal entre el pipeline original (`visitors_to_leads`) y el nuevo (`lk_matches_visitors_to_leads`) en febrero 2026.

| Job viejo       | Job nuevo    | Cantidad |
|-----------------|-------------|----------|
| Organic Search  | Paid Search | 5        |
| Paid Search     | Organic Search | 1     |
| Paid Social     | Paid Search | 1        |
| Unassigned      | Paid Search | 1        |
| Organic Search  | Paid Social | 1        |
| Paid Search     | Cross-Network | 1     |
| Nulo-Vacío      | Unassigned  | 1        |

---

## 1. Job viejo: dónde y cómo se define el channel

El canal en el pipeline viejo se calcula **en la query SQL** `visitors_to_leads/queries/lead_events.sql`, dentro del CTE `events_with_channel`. Se usa un único **CASE WHEN** que evalúa en **orden fijo** (el primer match gana). Las variables de entrada son solo:

- `source` → `session_traffic_source_last_click.cross_channel_campaign.source`
- `medium` → `session_traffic_source_last_click.cross_channel_campaign.medium`
- `campaign_name` → `session_traffic_source_last_click.cross_channel_campaign.campaign_name`

**No** se usan en esta query: `gclid`, `fbclid`, `utm_*` de event_params ni parámetros de URL. Solo session-scoped source/medium/campaign_name de GA4.

### Jerarquía exacta del job viejo (orden de evaluación)

1. **Nulo-Vacío**  
   - `COALESCE(TRIM(medium), '') = ''`  
   - O `(source like '%l.wl.co%' OR 't.co%' OR '%github.com%' OR '%statics.teams.cdn.office.net%') AND medium = 'referral'`

2. **Organic Search**  
   - `campaign_name = '(organic)'` y source ≠ adwords y medium ≠ spot  
   - O `medium = 'organic'`  
   - O source y medium vacíos y campaign_name NOT LIKE '%comunicado%'  
   - O source en search.yam.com / copilot / search.google.com / msn.com y medium = 'referral'  
   - O source tipo google|bing|yahoo|duckduckgo|ecosia|search|adwords y medium **not in** ('cpc','spot','paid','referral') y medium no contiene cpc/spot/paid/(not set)  
   - O source like search.yahoo.com y medium = 'referral'

3. **Organic LLMs**  
   - source contiene chatgpt|perplexity|claude|bard|gemini|anthropic|openai|llm|ai chat|ai assistant

4. **Mail**  
   - source = 'mail' o source tipo gmail|outlook|... (y no search.)

5. **Display**  
   - campaign_name LIKE '%_display_%' o '%_disp_%'

6. **Direct**  
   - source = '(direct)'

7. **Referral**  
   - campaign_name = '(referral)' o medium LIKE '%referral%', y source no es red social

8. **Cross-network**  
   - campaign_name LIKE '%cross-network%' o '%pmax%' o source LIKE '%syndicatedsearch.goog%' o (source LIKE '%google%' AND medium IN ('','cross-network')) o nova.taboolanews.com + referral

9. **Paid Video**  
   - campaign_name LIKE '%youtube%' o r'yt_'

10. **Paid Search**  
    - medium IN ('cpc','spot','paid') y (source tipo google|bing|yahoo|... o campaign_name LIKE '%search%' o '%srch%')  
    - O source = 'adwords' y medium = '(not set)'

11. **Paid Social**  
    - medium IN ('cpc','paid','paid_social') y source tipo facebook|instagram|meta|linkedin|...

12. **Organic Social**  
    - medium IN ('social','rss') o (source tipo red social y medium NOT IN ('cpc','paid') y campaign_name sin 'meta')

13. **Unassigned**  
    - Todo lo que no matcheó arriba.

---

## 2. Job nuevo: de dónde sale el channel

En el pipeline LK el canal **no** se calcula en el repo. Viene de la tabla/vista de BigQuery:

```sql
analitics_spot2.funnel_with_channel
```

En `lead_events_from_funnel_lk.sql`:

- Se hace **LEFT JOIN** de `raw_events` (eventos GA4) con `funnel_base` (`funnel_with_channel`) por:
  - `r.user_pseudo_id = f.user`
  - `PARSE_DATE('%Y%m%d', r.event_date) = f.event_date`
- El canal que se usa es: `COALESCE(f.Channel, 'Unassigned')`.

Por tanto, el job nuevo usa **la lógica con la que esté construida** `funnel_with_channel` (en otro proyecto/script de BigQuery). En **este repositorio no está definida** esa vista; solo se consume.

Consecuencias:

- Si `funnel_with_channel` usa **gclid / fbclid / utm_campaign** (o cualquier otra señal que el viejo no usa), el nuevo puede clasificar distinto.
- Si la **jerarquía** de canales (orden de prioridad) en esa vista es distinta a la del job viejo, aparecen exactamente los tipos de diferencias que ves (Organic Search → Paid Search, etc.).

---

## 3. Pregunta clave: ¿Por qué Organic Search (viejo) → Paid Search (nuevo)?

En el job viejo, **medium = 'organic'** por sí solo ya asigna **Organic Search** (paso 2), y **Paid Search** solo se asigna después (paso 10), cuando medium IN ('cpc','spot','paid') y source/campaign de búsqueda.

Posibles causas en el job nuevo (lógica dentro de `funnel_with_channel`):

1. **Prioridad de señales de pago**  
   Si la vista considera primero “¿hay gclid / utm_source=google con campaña de pago?” y asigna Paid Search, y **después** no re-evalúa medium=organic, entonces un tráfico con medium=organic pero con gclid (o campaña paga en URL) se clasifica como **Paid Search** en el nuevo y **Organic Search** en el viejo.

2. **Jerarquía invertida**  
   Si en `funnel_with_channel` la regla “Paid Search” se evalúa **antes** que “Organic Search”, entonces cualquier combinación que cumpla ambas (p. ej. source=google, medium=organic pero campaign con “search” o “BrandTerms”) daría Paid Search en el nuevo y Organic Search en el viejo.

3. **Uso de utm_campaign / campaign_name distinto**  
   Si el nuevo usa `utm_campaign` o un `campaign_name` derivado de otra fuente (por ejemplo con “BrandTerms” o “Conversiones”) y lo trata como señal de pago, podría marcar Paid aunque medium sea organic.

---

## 4. Hipótesis 1: ¿El nuevo pipeline detecta gclid / utm_campaign / fbclid que el viejo ignora?

- **Job viejo:** Solo usa `source`, `medium`, `campaign_name` de **session** (GA4). No usa event_params como gclid o fbclid en esta query.
- **Job nuevo:** Toma `Channel` de `funnel_with_channel`. Si esa vista en BigQuery se alimenta de lógica que usa:
  - presencia de **gclid** (Google Ads) → Paid Search,
  - **fbclid** (Meta) → Paid Social,
  - **utm_campaign** con ciertos patrones → Paid,

entonces **sí**: el nuevo puede estar usando señales que el viejo ignora, y por eso un lead con medium=organic pero con gclid en la sesión podría ser Organic Search en el viejo y Paid Search en el nuevo.

---

## 5. Hipótesis 2: ¿Jerarquía distinta (si hay señal de pago, se ignora organic)?

Si en `funnel_with_channel` la lógica es del tipo:

- “Si hay señal de pago (gclid, campaign de pago, etc.) → Paid Search (o Paid Social)”
- y eso se evalúa **antes** que “si medium = organic → Organic Search”,

entonces un lead con **medium=organic** pero con **gclid** o **utm_campaign** de pago sería:

- Viejo: Organic Search (porque el viejo solo mira medium y asigna en el paso 2).
- Nuevo: Paid Search (o el canal pago que use la vista).

Eso explicaría los 5 casos **Organic Search → Paid Search**.

---

## 6. Ejemplos de combinaciones que provocarían los 11 cambios

A continuación, ejemplos **típicos** de (source, medium, campaign_name) —y en el nuevo, posible uso de gclid/utm— que son coherentes con cada discrepancia. Son ilustrativos; la confirmación requiere revisar la definición real de `funnel_with_channel`.

### Organic Search → Paid Search (5 leads)

- **Viejo:** `medium = 'organic'` → Organic Search.  
- **Nuevo:** Si la vista prioriza “campaña de pago” o “gclid” sobre medium:
  - source = `google`, medium = `organic`, campaign_name = `(not set)` o vacío, pero **URL con gclid** o **utm_campaign** tipo “BrandTerms” / “Conversiones”.
  - O source = `google`, medium = `organic`, campaign_name con “search” o “BrandTerms” y la vista lo clasifica como Paid Search antes de mirar organic.

### Paid Search → Organic Search (1 lead)

- **Viejo:** medium = `cpc` (o spot/paid) + source tipo google → Paid Search.  
- **Nuevo:** Si la vista no considera ese medium como pago (p. ej. solo mira gclid y no hay gclid), o asigna “Organic” por otra regla:
  - source = `google`, medium = `cpc`, campaign_name = `(not set)` y **sin gclid** en la sesión; la vista podría devolver Organic Search.

### Paid Social → Paid Search (1 lead)

- **Viejo:** medium en ('cpc','paid','paid_social') y source tipo meta/facebook → Paid Social.  
- **Nuevo:** Si la vista agrupa todo “pago” en Paid Search o prioriza “campaña de búsqueda”:
  - source = `facebook`, medium = `cpc`, campaign_name con “search” o que la vista interpreta como búsqueda → Paid Search en el nuevo.

### Unassigned → Paid Search (1 lead)

- **Viejo:** No matchea ninguna regla (p. ej. source/medium raros o vacíos) → Unassigned.  
- **Nuevo:** Si la vista usa gclid o utm y detecta sesión de Google Ads:
  - source = `(direct)` o vacío, medium = `(not set)`, pero **gclid** presente → Paid Search.

### Organic Search → Paid Social (1 lead)

- **Viejo:** medium = `organic` (o regla de Organic Search) → Organic Search.  
- **Nuevo:** Si la vista prioriza “red social” + “pago”:
  - source = `facebook` (o instagram), medium = `organic`, pero **fbclid** en URL → Paid Social en el nuevo.

### Paid Search → Cross-Network (1 lead)

- **Viejo:** medium = cpc + source google → Paid Search.  
- **Nuevo:** Si la vista prioriza “cross-network” por campaign_name o source:
  - source = `google`, medium = `cpc`, campaign_name con “cross-network” o “pmax” → Cross-Network en el nuevo.

### Nulo-Vacío → Unassigned (1 lead)

- **Viejo:** medium vacío o de la lista especial (l.wl.co, t.co, etc.) + referral → Nulo-Vacío.  
- **Nuevo:** Si en el JOIN por user+event_date **no hay fila** en `funnel_with_channel`, se usa `COALESCE(f.Channel, 'Unassigned')` → Unassigned. No existe “Nulo-Vacío” en la vista; todo lo no matcheado o sin fila en funnel cae en Unassigned.

---

## 7. Resumen y próximos pasos

| Pregunta | Conclusión |
|----------|------------|
| ¿Dónde se define el channel en el viejo? | En `visitors_to_leads/queries/lead_events.sql`, CTE `events_with_channel`, un solo CASE WHEN en orden fijo, solo source/medium/campaign_name de sesión. |
| ¿Dónde se define en el nuevo? | En la vista/tabla `analitics_spot2.funnel_with_channel` (BigQuery). No está en este repo. |
| ¿Por qué Organic Search → Paid Search? | Muy probablemente: el nuevo prioriza señales de pago (gclid, utm_campaign, campaign_name) sobre medium=organic, o tiene distinto orden de reglas. |
| ¿gclid/utm/fbclid? | El viejo no los usa. Si `funnel_with_channel` sí los usa, explica varias de las diferencias. |
| ¿Jerarquía distinta? | Sí: si en la vista “Paid” se evalúa antes que “Organic”, aparecen exactamente estos cruces. |

**Recomendación:** Revisar en BigQuery la definición de `analitics_spot2.funnel_with_channel` (script o vista): orden de las reglas de Channel y uso o no de gclid, fbclid, utm_campaign. Con eso se puede alinear la atribución al viejo o documentar las diferencias de negocio de forma explícita.
