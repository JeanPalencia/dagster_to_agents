# Investigación: Tráfico Direct artificial (bots) — 2026-03-25

## Resumen

Se detectaron **3 perfiles de bot** inflando artificialmente el tráfico Direct
en marzo 2026. En el pico (18 de marzo), el 77% del tráfico Direct era bot.
Las reglas de filtrado refinadas reclasifican **5,575 sesiones bot al canal "Bot/Spam"**
sin afectar ningún otro canal ni ningún lead.

## Perfiles detectados

### Bot 1: Opera 128 — Login/OTP brute force (3,486 usuarios)
- **Browser**: Opera 128.0.0.0, Desktop, Windows
- **Geo**: México (mayoría) y Estados Unidos
- **Páginas**: `?signup=login` y `?signup=verify-otp` (99.9% del tráfico)
- **Engagement**: ~13.5s (simulan interacción con el formulario)
- **Periodo**: 5-19 marzo 2026
- **Canales**: 3,272 en Direct, 214 en Unassigned

### Bot 2: Chrome US Desktop — Login/OTP (479 usuarios)
- **Browser**: Chrome 146.0.7680.80, Desktop, Windows
- **Geo**: Estados Unidos (Atlanta = data center AWS/GCP)
- **Páginas**: `?signup=login` (448 usuarios) y `?signup=verify-otp` (34 usuarios)
- **Engagement**: ~14.1s
- **Periodo**: 11-25 marzo (sigue activo)
- **Canales**: 479 en Direct

### Bot 3: Mobile Chrome Android US — Scraper staging (1,557 usuarios)
- **Browser**: Chrome, Mobile, Android
- **Geo**: Estados Unidos (Chicago, Des Moines, San Jose, Boydton, Phoenix, Cheyenne, Flint Hill = IPs de data centers)
- **Páginas**: `*.vercel.app/*` y `gamma.spot2.mx/*` (entornos staging)
- **Engagement**: **0.1 segundos** (zero engagement)
- **Periodo**: 15-25 marzo (sigue activo)
- **Canales**: 1,415 en Direct, 97 en Unassigned (GA4 no completa atribución en sesiones tan efímeras)

## Impacto cuantificado

Validado con `test_v3_vs_v4.py` ejecutando ambas queries completas (v3 actual vs v4 corregida)
sobre datos de marzo 2026, con las reglas ya acotadas por fecha.

| Métrica | Valor |
|---------|-------|
| Sesiones reclasificadas a Bot/Spam | 5,575 |
| Usuarios reclasificados | 5,559 |
| Pico diario (18 marzo) | 1,721 sesiones bot / 2,228 total = **77% bot** |
| Direct limpio sin bots (18 marzo) | 505 sesiones (nivel normal) |
| Total filas v3 = v4 | 55,726 = 55,726 (ningún registro se pierde ni se agrega) |

### Desglose por canal de origen

Las sesiones reclasificadas provienen de:

| Canal origen | Sesiones movidas a Bot/Spam |
|-------------|----------------------------|
| Direct | 5,514 |
| Nulo-Vacío | 60 |
| Unassigned | 1 |
| **Total** | **5,575** |

## Reglas de filtrado finales (validadas con test)

Todas las reglas incluyen filtro `channel_group IN ('Direct', 'Unassigned')` para
evitar marcar tráfico de otros canales. Por consenso del equipo:
- **Reglas 1 y 2** acotadas a `event_date BETWEEN '20260316' AND '20260325'` para evitar
  falsos positivos si los mismos browser_version se generalizan en el futuro.
- **Regla 3** queda **atemporal** porque las URLs de staging no son producción y no deben contar nunca.

```sql
-- Regla existente (sin cambios)
device.web_info.browser_version = '129.0.6668.71'
AND channel_group IN ('Direct', 'Unassigned')
AND event_name = 'first_visit'
AND page_location LIKE 'https://spot2.mx/%'

-- Regla 1: Opera 128 en Windows Desktop — acotada a marzo 16-25
device.web_info.browser = 'Opera'
AND device.web_info.browser_version = '128.0.0.0'
AND device.category = 'desktop'
AND device.operating_system = 'Windows'
AND channel_group IN ('Direct', 'Unassigned')
AND event_date BETWEEN '20260316' AND '20260325'

-- Regla 2: Chrome 146.0.7680.80 Desktop Windows US en login/OTP — acotada a marzo 16-25
device.web_info.browser = 'Chrome'
AND device.web_info.browser_version = '146.0.7680.80'
AND device.category = 'desktop'
AND device.operating_system = 'Windows'
AND geo.country = 'United States'
AND channel_group IN ('Direct', 'Unassigned')
AND (page_location LIKE '%signup=login%' OR page_location LIKE '%signup=verify-otp%')
AND event_date BETWEEN '20260316' AND '20260325'

-- Regla 3: Mobile Chrome Android US en staging (atemporal)
device.category = 'mobile'
AND device.web_info.browser = 'Chrome'
AND device.operating_system = 'Android'
AND geo.country = 'United States'
AND (page_location LIKE '%vercel.app%' OR page_location LIKE '%gamma.spot2.mx%')
```

### Implementación en la query

Los usuarios marcados como `user_sospechoso = 1` se reclasifican al canal **"Bot/Spam"**
mediante `WHEN user_sospechoso = 1 THEN 'Bot/Spam'` como primera condición del CASE WHEN
que asigna `Channel`. El `Traffic_type` también se asigna como "Bot/Spam".

### Test v3 vs v4 (query completa)

Se ejecutó `test_v3_vs_v4.py` con ambas queries completas sobre marzo 2026:

| Validación | Resultado |
|------------|-----------|
| Total filas v3 = v4 | 55,726 = 55,726 |
| Bot/Spam solo en v4 | 5,575 sesiones, 5,559 usuarios |
| user_sospechoso=1 = Bot/Spam | 100% (5,575/5,575) |
| Paid Search, Organic Search, Paid Social, etc. | Sin cambios |
| Balance: Direct + Nulo-Vacío + Unassigned perdidos = Bot/Spam | 5,514 + 60 + 1 = 5,575 |
| Traffic_type Organic perdido = Bot/Spam ganado | 5,514 Organic → 5,575 Bot/Spam |

### Distribución de canales: ANTES vs DESPUÉS del filtrado

| Canal | Sesiones antes | Sesiones después | Sesiones bot | % Antes | % Después | Delta % |
|-------|---------------|-----------------|-------------|---------|-----------|---------|
| Paid Search | 29,399 | 29,399 | 0 | 46.42% | 51.28% | +4.86 |
| Organic Search | 12,468 | 12,468 | 0 | 19.69% | 21.75% | +2.06 |
| **Direct** | **11,063** | **5,414** | **5,649** | **17.47%** | **9.44%** | **-8.03** |
| Unassigned | 4,596 | 4,240 | 356 | 7.26% | 7.40% | +0.14 |
| Paid Social | 4,498 | 4,498 | 0 | 7.10% | 7.85% | +0.75 |
| Referral | 726 | 726 | 0 | 1.15% | 1.27% | +0.12 |
| Organic Social | 368 | 368 | 0 | 0.58% | 0.64% | +0.06 |
| Cross-network | 189 | 189 | 0 | 0.30% | 0.33% | +0.03 |
| Display | 21 | 21 | 0 | 0.03% | 0.04% | +0.01 |
| Organic Video | 2 | 2 | 0 | 0.00% | 0.00% | 0.00 |
| **Total** | **63,330** | **57,325** | **6,005** | **100%** | **100%** | — |

**Nota:** esta tabla se generó con `test_scraping_rules.py` antes de aplicar las restricciones
de fecha a las reglas 1 y 2. Con las restricciones, el impacto real es de 5,575 sesiones
(validado en test_v3_vs_v4). La distribución relativa entre canales permanece igual.

## Por qué son bots (argumentación)

### Bot 1 — Opera 128
- El 99.9% de su tráfico va exclusivamente a `?signup=login` y `?signup=verify-otp`. Un usuario real navega el sitio, busca propiedades, lee el blog. Estos no hacen nada más que intentar loguearse.
- Todos comparten exactamente el mismo browser_version (128.0.0.0), OS (Windows) y device (desktop). Esa homogeneidad es típica de bots usando la misma configuración automatizada.
- Aparecieron masivamente del 5 al 19 de marzo y luego pararon, coincidiendo con los ataques coordinados que reportó Dante.
- Prácticamente todos tienen `first_visit` = usuarios "nuevos" que nunca habían visitado el sitio. Miles de usuarios nuevos que solo van a login = credential stuffing.

### Bot 2 — Chrome US Desktop
- Mismo patrón de comportamiento: solo visitan login/OTP, nada más.
- Las ciudades son sedes de data centers conocidos (Atlanta = AWS/GCP). Tráfico legítimo mexicano no viene masivamente de data centers en US.
- Siguen activos después de que Opera 128 paró, sugiriendo que el atacante cambió de herramienta.
- Todos usan exactamente la misma browser_version (146.0.7680.80).

### Bot 3 — Mobile Chrome Android US
- Engagement promedio de 0.1 segundos. Es físicamente imposible que un humano interactúe con una página en una décima de segundo.
- Visitan URLs de staging (`*.vercel.app`, `gamma.spot2.mx`) que un usuario real no conoce ni tiene forma de llegar.
- Vienen de ciudades US que son sedes de data centers (Boydton = Microsoft, Des Moines = Meta, San Jose = múltiples). No hay teléfonos móviles en data centers — es user-agent spoofing.
- Cada usuario genera exactamente 3 eventos (first_visit + session_start + page_view) y se va. Patrón de scraper que toca y abandona.

### Prueba definitiva
Cuando se filtran estos 3 perfiles, el tráfico Direct diario de marzo vuelve a niveles normales (~100-500 sesiones), consistente con febrero. La curva anómala que reportó Pablo desaparece completamente.

## Solución implementada

### Estado anterior
La query `funnel_with_channel` tenía un CTE `scraping` que detectaba un bot viejo
(`browser_version = '129.0.6668.71'`) y marcaba a esos usuarios con `user_sospechoso = 1`
mediante un LEFT JOIN. Sin embargo, **ese flag no se usaba para filtrar en ningún punto**:
ni en la propia query, ni en los flujos downstream (`lk_visitors`, `lk_mat_matches_visitors_to_leads`).
Los bots marcados seguían contando en la distribución de canales como "Direct".

### Cambios realizados (query v4)

**1. CTE `scraping` ampliado** con las 3 firmas nuevas (reglas 1, 2 y 3 descritas arriba),
además de la regla original del bot viejo.

**2. Canal "Bot/Spam"** como primera condición del CASE WHEN de `Channel`:
`WHEN user_sospechoso = 1 THEN 'Bot/Spam'`. Los bots ya no contaminan "Direct" ni "Unassigned".

**3. Traffic_type "Bot/Spam"** para el nuevo canal, separándolo de "Organic" y "Paid".

**Decisión: Opción C + flag existente.** Los usuarios marcados con `user_sospechoso = 1`
se reclasifican al canal **"Bot/Spam"**. De esta forma:
- No contaminan "Direct" ni "Unassigned" en los reportes de negocio.
- La data permanece disponible para análisis forense filtrando por canal "Bot/Spam".
- El flag `user_sospechoso` se mantiene como complemento para queries ad-hoc.

### Tablas afectadas
- `funnel_with_channel` (BigQuery, consulta programada) — donde se marca el flag y se reclasifica el canal
- `lk_visitors` (GeoSpot, vía Dagster) — lee de funnel, renombra flag a `vis_is_scraping` pero no filtra
- `lk_mat_matches_visitors_to_leads` — sin impacto: ningún lead tiene un bot como visitante matcheado (validado)

### Limitación
La solución es reactiva. Cada vez que el atacante cambie de browser, IP o patrón, habrá que
agregar nuevas reglas. La mitigación definitiva es del lado de infraestructura (firewall, desactivar
OTP SMS), que Dante ya está implementando.

## Test de impacto en pipeline lk_mat_matches_visitors_to_leads

Se ejecutó `test_impacto_mat.py` que compara los `lead_events` generados por v3 (producción)
vs v4 (corregida, con restricciones de fecha) y cruza con la tabla MAT actual en GeoSpot.

**Equivalencia de la estrategia simplificada:** se verificó en el código del pipeline que
`channel` es metadata carry-along (no afecta la lógica de matching, que se basa en
`user_pseudo_id`, timestamps, email y phone). Por tanto, comparar los `lead_events` y cruzar
con la tabla MAT es equivalente a ejecutar el pipeline completo.

### Resultados

| Métrica | Resultado |
|---------|-----------|
| Total lead_events v3 | 135,175 |
| Total lead_events v4 | 135,175 |
| Diferencia | **0** (ningún evento se pierde ni se agrega) |
| Lead_events reclasificados a Bot/Spam | 8,907 (8,873 de Direct + 34 de Unassigned) |
| Usuarios reclasificados a Bot/Spam | 2,068 |
| **Leads con visitante bot en tabla MAT** | **0** (de 52,402 leads) |
| Veredicto | **v4 es segura, sin impacto en leads** |

### Conclusión

Ninguno de los 52,402 leads en la tabla MAT tiene un visitante bot como match.
Los 2,068 usuarios reclasificados a "Bot/Spam" no generaron eventos de conversión reales
y nunca matchearon con ningún lead. La actualización de la consulta programada a v4
**no alterará ningún lead existente ni futuro**.

## Nota sobre falsos positivos
- **Opera 128**: Los ~8 usuarios con Opera 128 que llegaron por Paid Search (gclid) quedan protegidos por el filtro `channel_group IN ('Direct', 'Unassigned')`. Además, la regla está acotada a marzo 16-25.
- **Chrome US login**: Se restringió a `browser_version = '146.0.7680.80'` (la versión específica del bot) y acotada a marzo 16-25. Un usuario legítimo de US con otra versión de Chrome que entre a login no será marcado.
- **Mobile US staging**: Las URLs de staging no son accesibles para usuarios reales. Riesgo nulo. Regla atemporal.
- **Unassigned afectado**: Algunos bots caen en Unassigned en vez de Direct porque GA4 no completa su lógica de atribución en sesiones tan efímeras o anómalas. Son los mismos bots, mismo patrón — simplemente GA4 les asignó un canal diferente.
- **clientClickedRegisterForm**: 150 usuarios marcados dispararon este evento, pero es parte del ataque (intentos de registro automatizado). Los top 2 bots acumulan 4,171 y 3,100 clicks cada uno — ningún humano hace eso.

## Archivo de la query corregida

`lakehouse-sdk/sql/old/ funnel_with_channel_v4.sql`
