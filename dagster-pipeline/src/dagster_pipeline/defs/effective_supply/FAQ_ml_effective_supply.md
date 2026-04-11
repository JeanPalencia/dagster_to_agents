# FAQ — ML Effective Supply

> Preguntas frecuentes sobre el sub-flujo ML de oferta efectiva.
> Orientado a negocio y producto con nivel tecnico.

---

## 1. Que tablas produce el modelo y como se consultan?

El sub-flujo ML produce **4 tablas** en PostgreSQL, mas la tabla base que alimenta
todo el analisis. Cada tabla responde a una pregunta de negocio diferente:

### 1.1 `lk_effective_supply` — Panel base de oferta efectiva

**Pregunta**: Para cada spot activo, cuantas vistas/contactos/proyectos tuvo y que
tan relevante es?

**Campos clave**: `spot_id`, `view_count`, `contact_count`, `project_count`,
`total_score` (0-1), `is_top_p80` (Yes/No), `is_top_p80_rent`, `is_top_p80_sale`,
atributos del spot (estado, sector, tipo, precio, area, etc.).

```sql
-- Top 20 spots mas relevantes en Renta en CDMX
SELECT spot_id, spot_sector, spot_corridor, spot_area_in_sqm,
       spot_price_sqm_mxn_rent, total_score,
       view_count, contact_count, project_count
FROM lk_effective_supply
WHERE is_top_p80_rent = 'Yes'
  AND spot_state = 'Ciudad de México'
ORDER BY total_score DESC
LIMIT 20;
```

### 1.2 `lk_effective_supply_propensity_scores` — Probabilidad predictiva por spot

**Pregunta**: Que probabilidad tiene cada spot de ser Top P80, basandose solo en sus
atributos (sin mirar eventos historicos)?

**Campos clave**: `spot_id`, `market_universe` (Rent/Sale), `p_top_p80` (0 a 1).

```sql
-- Oportunidades ocultas: spots con alta probabilidad predicha
-- pero que actualmente NO estan en el Top P80
SELECT ps.spot_id, ps.p_top_p80, es.total_score,
       es.spot_state, es.spot_sector, es.spot_corridor
FROM lk_effective_supply_propensity_scores ps
JOIN lk_effective_supply es ON ps.spot_id = es.spot_id
WHERE ps.market_universe = 'Sale'
  AND ps.p_top_p80 >= 0.6
  AND es.is_top_p80_sale = 'No'
ORDER BY ps.p_top_p80 DESC;
```

### 1.3 `lk_effective_supply_drivers` — Que variables importan mas

**Pregunta**: Que atributos del spot determinan mas si sera Top P80?

**Campos clave**: `market_universe`, `feature_name`, `importance` (0-1), `rank` (1 = mas importante).

```sql
-- Ranking de importancia de features por universo
SELECT market_universe, rank, feature_name,
       ROUND(importance::numeric, 4) AS importance
FROM lk_effective_supply_drivers
ORDER BY market_universe, rank;
```

### 1.4 `lk_effective_supply_category_effects` — Efecto de cada valor

**Pregunta**: Dentro de cada variable, que valores favorecen o desfavorecen la demanda?

**Campos clave**: `market_universe`, `feature_name`, `category_name`, `support_n`,
`p_top`, `lift` (>1 = mejor que promedio).

```sql
-- Top 10 estados por lift en Renta
SELECT category_name AS estado, support_n AS spots,
       ROUND(p_top::numeric, 4) AS prob_top,
       ROUND(lift::numeric, 2) AS lift
FROM lk_effective_supply_category_effects
WHERE market_universe = 'Rent'
  AND feature_name = 'estado'
ORDER BY lift DESC
LIMIT 10;
```

```sql
-- Rangos de precio por m² y su efecto en Sale
SELECT category_name AS rango_precio_m2, support_n AS spots,
       ROUND(lift::numeric, 2) AS lift
FROM lk_effective_supply_category_effects
WHERE market_universe = 'Sale'
  AND feature_name = 'precio_m²'
ORDER BY lift DESC;
```

### 1.5 `lk_effective_supply_rules` — Combinaciones ganadoras

**Pregunta**: Que combinaciones concretas de estado + sector + tipo + zona + precio + area
tienen la mayor probabilidad de ser Top P80?

**Campos clave**: `market_universe`, `rule_id`, `spot_state`, `spot_sector`, `spot_type`,
`spot_municipality`, `spot_corridor`, `price_sqm_range`, `price_sqm_pctl_range`,
`price_sqm_is_discriminant_id`, `price_sqm_is_discriminant`, `area_range`, `area_pctl_range`,
`area_is_discriminant_id`, `area_is_discriminant`,
`rule_text`, `support_n`, `p_top`, `lift`, `avg_score_percentile`, `strength`.

**Definiciones de metricas de reglas**:

- **`p_top`**: tasa observada de spots Top P80 entre los que cumplen la regla. Es una
  probabilidad clasica (frecuentista): p_top = n_top_en_regla / n_total_en_regla.
  Responde: "de cada 100 spots que cumplen esta regla, cuantos son Top P80?"
  Ejemplo: p_top = 0.65 → 65 de cada 100 spots son Top.

- **`lift`**: p_top / p_top_global. Cuantas veces mas probable es ser Top P80 si cumples
  esta regla, comparado con un spot aleatorio del universo. Siempre >= 1.5x (las reglas
  debiles se filtran automaticamente).
  Ejemplo: lift = 3.0x → 3 veces mas probable que el promedio.

- **`avg_score_percentile`** (0-100): percentil promedio de `total_score` de los spots
  que cumplen la regla, calculado dentro del universo (Rent o Sale). Indica la posicion
  promedio en el ranking de relevancia.
  Responde: "que tan arriba estan estos spots en el ranking general?"
  Ejemplo: avg_score_percentile = 85 → en promedio, estos spots estan en el top 15%.

- **`strength`**: intuitive label based on lift:
  - "Very High" (lift >= 3.0x): exceptional combination
  - "High" (lift >= 2.0x): strong combination
  - "Moderate" (lift >= 1.5x): notable combination

- **`price_sqm_pctl_range`** / **`area_pctl_range`**: rango de percentiles del subgrupo
  (ej: "P25-P70"). Si `is_discriminant_id = 1`, indica la ventana optima encontrada por el
  algoritmo de sliding window. Si `is_discriminant_id = 0`, es "P5-P95" (rango completo).

- **`price_sqm_is_discriminant_id`** / **`area_is_discriminant_id`**: flag (0/1) que indica
  si el rango numerico aporta >= 5% de lift incremental. 1 = discriminante (el rango realmente
  mejora la probabilidad), 0 = solo contexto informativo (marcado con `*` en el rango).

- **`price_sqm_is_discriminant`** / **`area_is_discriminant`**: version string del flag
  anterior ("Yes" / "No"), para uso directo en dashboards y filtros.

```sql
-- Todas las reglas de Renta, ordenadas por fuerza y probabilidad
SELECT rule_id, spot_state, spot_sector, spot_type, spot_corridor, spot_municipality,
       price_sqm_range, price_sqm_pctl_range, price_sqm_is_discriminant_id,
       area_range, area_pctl_range, area_is_discriminant_id,
       support_n AS spots,
       ROUND(p_top::numeric, 4) AS prob_top,
       ROUND(lift::numeric, 2) || 'x' AS lift,
       ROUND(avg_score_percentile::numeric, 1) AS pctl,
       strength
FROM lk_effective_supply_rules
WHERE market_universe = 'Rent'
ORDER BY lift DESC, p_top DESC;
```

```sql
-- Solo reglas de fuerza "Very High" para presentar a negocio
SELECT rule_id, rule_text, support_n, strength,
       ROUND(avg_score_percentile::numeric, 1) AS pctl
FROM lk_effective_supply_rules
WHERE market_universe = 'Rent'
  AND strength = 'Very High'
ORDER BY avg_score_percentile DESC;
```

### 1.6 `lk_effective_supply_model_metrics` — Calidad del modelo

**Pregunta**: Que tan bueno es el modelo actual? Como se compara con ejecuciones anteriores?
Ha pasado el control de calidad (QA gate)?

**Campos clave**: `market_universe`, `model_version`, `model_variant`,
`gate_passed_id`, `gate_passed`,
`auc_roc_random`, `pr_auc_random`, `brier_random`,
`auc_roc_group`, `pr_auc_group`, `brier_group`,
`gap_auc`, `best_iteration`, `window_end_date`, `window_months`.

**Definiciones**:
- `auc_roc_random` / `auc_roc_group`: Area bajo la curva ROC en el test aleatorio (15%)
  y en el holdout geografico. Valores cercanos a 1.0 son excelentes.
- `pr_auc_random` / `pr_auc_group`: Area bajo la curva Precision-Recall. Mas informativa
  que AUC-ROC cuando las clases estan desbalanceadas (80/20).
- `brier_random` / `brier_group`: Error cuadratico medio de las probabilidades predichas.
  Valores mas bajos son mejores (0 = perfecto, 0.25 = random con clases 50/50).
- `gap_auc`: AUC_random - AUC_group. Un gap positivo grande indica overfitting geografico.
  Valores negativos son aceptables (el modelo generaliza bien a municipios no vistos).
- `gate_passed`: indica si el modelo paso el control de calidad automatico
  (auc_group >= 0.60 y gap_auc <= 0.05).
- `best_iteration`: iteracion donde CatBoost detuvo el entrenamiento (early stopping).

```sql
-- Historico de metricas por universo y version
SELECT market_universe, model_version, model_variant,
       gate_passed,
       ROUND(auc_roc_random::numeric, 4) AS auc_random,
       ROUND(auc_roc_group::numeric, 4) AS auc_group,
       ROUND(gap_auc::numeric, 4) AS gap,
       ROUND(pr_auc_random::numeric, 4) AS pr_auc,
       ROUND(brier_random::numeric, 4) AS brier,
       best_iteration
FROM lk_effective_supply_model_metrics
ORDER BY model_version DESC, market_universe;
```

### Jerarquia de las tablas

Las 6 tablas forman una jerarquia de detalle:

| Nivel | Tabla | Responde |
|---|---|---|
| 1. Dato | `lk_effective_supply` | Que pasa con cada spot (conteos, scores) |
| 2. Prediccion | `propensity_scores` | Que deberia pasar segun el modelo |
| 3. Explicacion macro | `drivers` | Que variables importan |
| 4. Explicacion micro | `category_effects` | Que valores importan dentro de cada variable |
| 5. Receta de negocio | `rules` | Combinaciones concretas y accionables |
| 6. Auditoria | `model_metrics` | Que tan bueno es el modelo y su historico de calidad |

---

## 2. Cual es el uso real de CatBoost? Que productos obtenemos de el?

CatBoost se usa como **motor central de aprendizaje**, pero sus productos se dividen
en **3 usos distintos**. Cada uno alimenta una tabla diferente:

### 2.1 Uso directo: Probabilidad por spot → `propensity_scores`

El modelo CatBoost genera `P(is_top_p80 = 1)` para cada spot. Esta probabilidad
se escribe directamente en `lk_effective_supply_propensity_scores`.

**Flujo**: `ml_train` → `ml_score` → tabla

Este es el unico producto que usa la **prediccion numerica** del modelo.

### 2.2 Uso directo: Importancia de variables → `drivers`

Del modelo entrenado se extrae la importancia relativa de cada feature
(via `model.get_feature_importance()`). Esto produce el ranking de que variables
pesan mas en la decision.

**Flujo**: `ml_train` → `ml_drivers` → tabla

### 2.3 Uso indirecto: Orden de features para las reglas → `rules`

Las reglas **NO usan la probabilidad de CatBoost** ni ningun arbol del modelo.
Lo que usan es el **ranking de importancia** (de la tabla `drivers`) para decidir
en que orden recorrer las features al construir las reglas jerarquicas.

El algoritmo de reglas opera directamente sobre los datos reales:
- Toma la importancia de CatBoost para ordenar features
- Calcula `p_top` y `lift` con frecuencias reales de los datos
- Aplica pruning con umbrales de lift y soporte minimo

**Flujo**: `ml_drivers` (importancia) + `ml_build_universes` (datos) → `ml_rules` → tabla

### 2.4 Sin CatBoost: Efectos por categoria → `category_effects`

Esta tabla **no usa CatBoost en absoluto**. Es un calculo puramente estadistico
(frecuencia, tasa, lift) sobre los datos originales, segmentados por cada valor
de cada variable.

**Flujo**: `ml_build_universes` (datos) → `ml_category_effects` → tabla

### Resumen visual

```
CatBoost (ml_train)
  │
  ├── predict_proba() ──────────► propensity_scores (probabilidad por spot)
  │
  ├── get_feature_importance() ─► drivers (ranking de variables)
  │                                  │
  │                                  └── orden de features ──► rules (combinaciones)
  │                                                             ▲
  │                                                             │
  └── (no interviene) ─────────► category_effects               │
                                   (lift por valor,         datos reales
                                    calculo estadistico)    de ml_build_universes
```

### Por que este diseno?

1. **Propensity scores**: necesitamos la potencia predictiva de CatBoost (AUC ~0.87)
   para rankear spots individuales con precision.

2. **Drivers**: la importancia del modelo refleja que variables realmente contribuyen
   a la prediccion, no solo correlaciones simples.

3. **Rules**: CatBoost es demasiado complejo para ser interpretable directamente
   (2,000 arboles). En lugar de intentar "abrir la caja negra", usamos su
   inteligencia (el orden de importancia) para guiar un algoritmo simple y transparente
   que opera sobre datos reales. Esto hace que las reglas sean:
   - **Auditables**: cualquier analista puede verificar los numeros
   - **Interpretables**: texto legible para negocio
   - **Confiables**: p_top y lift vienen de conteos reales, no de aproximaciones

4. **Category effects**: no necesitan modelo. Son un analisis descriptivo puro que
   complementa a los drivers con detalle por valor.

---

## 3. El `p_top_p80` de CatBoost es realmente una probabilidad? Y el `p_top` de las reglas?

Son dos metricas con el mismo nombre conceptual pero de **naturaleza distinta**.

### 3.1 `p_top_p80` de CatBoost — Probabilidad estimada (no exacta, pero muy cercana)

CatBoost produce su salida via `predict_proba()`, que devuelve un valor entre 0 y 1.
Tecnicamente, esta salida **es una probabilidad calibrada**, no un odds ratio crudo
como el que devolveria una regresion logistica antes de aplicar la funcion sigmoide.

La razon es que CatBoost con `loss_function = "Logloss"` optimiza directamente la
**log-verosimilitud binaria** (cross-entropy), que es la misma funcion objetivo de la
regresion logistica. Internamente, CatBoost:
1. Produce un log-odds (score crudo) acumulando las predicciones de todos los arboles
2. Aplica la **funcion sigmoide** `σ(x) = 1 / (1 + e^{-x})` para convertirlo a probabilidad
3. `predict_proba()` devuelve el resultado ya transformado

Esto significa que el output SI es una probabilidad en el sentido formal, no un ranking
arbitrario ni un odds ratio. Sin embargo, hay un matiz importante:

**Que tan bien calibrada esta?**

Para que la probabilidad sea confiable (ej: si el modelo dice 0.70, realmente el 70% de
esos spots son Top P80), necesitamos verificar la **calibracion**. Nuestro Brier score
mide esto:

| Universo | Brier score |
|---|---|
| Rent | 0.1256 |
| Sale | 0.1101 |

Un Brier score perfecto es 0.0 y uno aleatorio para ~20% prevalencia seria ~0.16.
Nuestros valores estan significativamente mejor que el aleatorio, lo que indica una
**calibracion razonablemente buena** sin necesidad de post-calibracion (Platt scaling).

**En resumen**: `p_top_p80` es una probabilidad estimada, interpretable como tal,
con buena calibracion. No es un ranking arbitrario ni un odds ratio.

### 3.2 `p_top` de las reglas — Probabilidad frecuentista (exacta)

El `p_top` en la tabla de reglas es una **probabilidad clasica (frecuentista)** pura:

```
p_top = (spots Top P80 en la regla) / (spots totales en la regla)
```

Por ejemplo, si una regla tiene `p_top = 0.78` y `support_n = 140`, significa que
de los 140 spots que cumplen esa combinacion, exactamente 109 estan en el Top P80.
Es un conteo directo sobre los datos reales, sin ninguna estimacion ni modelo.

El calculo en el codigo es simplemente `target.mean()` sobre el subconjunto filtrado,
donde `target` es 0 o 1:

```python
p_top = float(working_df["target"].mean())  # fraccion de 1s en el grupo
```

**Ventaja**: es exacta y verificable con una consulta SQL directa sobre los datos.
**Limitacion**: depende del tamano de muestra. Una regla con `n=30` tiene un p_top
menos estable que una con `n=500`. Por eso exigimos un soporte minimo de 30 spots.

### 3.3 Comparacion directa

| Metrica | `p_top_p80` (propensity) | `p_top` (reglas) |
|---|---|---|
| **Tipo** | Probabilidad estimada por modelo | Probabilidad frecuentista exacta |
| **Fuente** | CatBoost `predict_proba()` | Conteo real: positivos / total |
| **Granularidad** | Por spot individual | Por combinacion (regla) |
| **Depende del modelo?** | Si | No |
| **Verificable con SQL?** | No (necesitas el modelo) | Si (es un COUNT/GROUP BY) |
| **Calibracion** | Buena (Brier ~0.11-0.13) | Exacta por definicion |
| **Estabilidad** | Alta (usa todos los datos) | Depende de `support_n` |

### Cuando usar cada una?

- **`p_top_p80`**: para rankear spots individuales, encontrar oportunidades ocultas,
  o alimentar decisiones automatizadas (ej: priorizar spots en un CRM).
- **`p_top` de reglas**: para comunicar a negocio "en este segmento, X% de los spots
  son Top P80", construir estrategias por segmento, o definir criterios de busqueda.

---

## 4. Por que CatBoost y no regresion logistica? Los datos cumplen las hipotesis?

### 4.1 Las hipotesis de la regresion logistica y por que nuestros datos las violan

La regresion logistica asume:

1. **Linealidad en el log-odds**: la relacion entre cada feature y el logaritmo de odds
   del target debe ser lineal. En nuestro caso:
   - El precio por m² tiene una relacion **no monotona** con el target: spots muy baratos
     y spots muy caros pueden tener baja demanda, mientras que rangos intermedios tienen alta.
   - El area muestra patrones similares: nichos especificos (ej. 100-500 m² en retail)
     tienen alta demanda, no una relacion lineal continua.
   - Para capturar esto en logistica, necesitariamos crear interacciones y polinomios
     manualmente, lo cual es fragil y dificil de mantener.

2. **Independencia de features**: logistica asume que las features no tienen interacciones
   fuertes. En nuestro caso:
   - Estado y sector interactuan fuertemente: industrial en Nuevo Leon es muy diferente
     a industrial en Yucatan.
   - Precio y zona interactuan: $300/m² es caro en Puebla pero barato en Polanco.
   - Logistica necesitaria terminos de interaccion explicitos para cada par relevante,
     lo cual es combinatorialmente inviable con 8 features.

3. **Categoricas requieren encoding explicito**: logistica necesita one-hot encoding o
   similar para variables categoricas. Con nuestros datos:
   - `municipality_enc` (geographic encoding per state) tiene ~116-138 niveles → otras tantas columnas dummy
   - `corridor_enc` (misma estrategia) tiene ~47-54 niveles → otras tantas columnas
   - Esto genera una matriz sparse de ~200 columnas, problemas de dimensionalidad,
     y coeficientes inestables para categorias con poco soporte.

4. **Sin multicolinealidad**: logistica sufre con features correlacionadas. Incluso
   despues de eliminar `price_total`, `price_sqm` y `area` siguen correlacionados
   (spots grandes tienden a precios/m² mas bajos). CatBoost es robusto ante esto.

### 4.2 Que ofrece CatBoost que logistica no puede

| Aspecto | Regresion logistica | CatBoost |
|---|---|---|
| **Relaciones no lineales** | No (necesita features manuales) | Si (arboles las capturan naturalmente) |
| **Interacciones entre features** | Requiere terminos explicitos | Las descubre automaticamente |
| **Categoricas de alta cardinalidad** | One-hot encoding (dimensionalidad explosion) | Manejo nativo con target statistics |
| **Categoricas ordinales vs nominales** | No distingue | Las trata como nominales nativamente |
| **Datos faltantes (nulls)** | Requiere imputacion previa | Los maneja nativamente |
| **Robustez a outliers** | Sensible | Robusto (arboles splits, no magnitudes) |
| **Calibracion** | Excelente por diseno | Buena con Logloss (Brier ~0.11-0.13) |
| **Interpretabilidad directa** | Coeficientes claros | Caja negra (pero tenemos drivers + rules) |

### 4.3 Hipotesis de CatBoost (gradient boosting): cuales son y como las cumplimos

Aunque CatBoost es mucho mas flexible que la logistica, no es un modelo "sin supuestos".
Tiene hipotesis, pero son mas laxas y las cumplimos:

**1. Observaciones i.i.d. (independientes e identicamente distribuidas)**

Es el supuesto mas importante de cualquier gradient boosting. Requiere que los datos
de entrenamiento sean muestras independientes de una misma distribucion.

- **Lo cumplimos?** Si. Cada fila es un spot unico (grano spot_id). No hay repeticiones,
  ni series temporales, ni dependencia secuencial entre spots. Los spots se muestrean
  del mismo periodo de 6 meses cerrados, lo que garantiza una distribucion estable.
- **Riesgo potencial**: dependencia geografica (spots vecinos pueden parecerse).
  Lo mitigamos con el **group holdout**: evaluamos el modelo en municipios completos
  que nunca vio durante el entrenamiento. El AUC group (0.86-0.92) confirma que
  generaliza bien a zonas nuevas.

**2. Estacionariedad de la distribucion**

El modelo asume que la relacion entre features y target no cambia entre train y scoring.

- **Lo cumplimos?** Si, dentro de cada ventana mensual. El modelo se re-entrena cada mes
  con datos frescos (ventana movil de 6 meses cerrados), por lo que no asumimos que la
  relacion sea estable a largo plazo — la actualizamos periodicamente.
- **Esto es clave**: si entrenamos una sola vez y nunca actualizamos, la estacionariedad
  se violaria con el tiempo. El re-entrenamiento mensual lo previene por diseno.

**3. Representatividad del training set**

El training set debe cubrir adecuadamente el espacio de features para que el modelo
generalice.

- **Lo cumplimos?** Si. Entrenamos con la totalidad de spots marketables activos
  (25,291 Rent, 14,037 Sale), no con una muestra. Esto cubre todas las combinaciones
  observadas de estado, sector, tipo, precio y area.
- **Limitacion conocida**: categorias muy raras (municipios con < 10 spots globales) se agrupan
  en `__OTHER__` via encoding adaptativo por estado (coverage 90%, floor 3, min_support 10),
  lo cual es la forma correcta de manejar la baja representatividad en lugar de pretender
  que el modelo aprenda de 5 observaciones.

**4. Balance de clases razonable**

Gradient boosting puede manejar desbalance, pero un desbalance extremo (ej. 1:1000)
degrada el rendimiento y la calibracion.

- **Lo cumplimos?** Si. Nuestro target es ~20% positivo (~80/20), que es un desbalance
  moderado y manejable. No necesitamos oversampling ni class weights.
  La PR-AUC (0.52-0.64) y el Brier score (0.11-0.13) confirman que el modelo no
  esta sesgado hacia la clase mayoritaria.

**5. Features categoricas: valores discretos (no floats ni NaN)**

CatBoost requiere que las categoricas sean valores discretos, no flotantes.

- **Lo cumplimos?** Si. Todas las categoricas son IDs enteros convertidos a string,
  o tokens especiales (`__OTHER__`, `__NULL__`). Los valores faltantes se convierten
  explicitamente a `__NULL__` antes de entrenar, no se dejan como NaN.

**6. Funcion de perdida convexa (loss function)**

La convergencia del gradient boosting requiere que la funcion de perdida sea convexa
(para que los gradientes apunten en la direccion correcta).

- **Lo cumplimos?** Si. Usamos `Logloss` (cross-entropy binaria), que es estrictamente
  convexa. Es la eleccion estandar para clasificacion binaria.

**Nota importante sobre el punto 5 — Precio y area son continuos, no categoricos**

La hipotesis de "categoricas discretas" aplica **solo a las features declaradas como
categoricas** ante CatBoost. Precio y area NO se declaran como categoricas.

En nuestro pipeline, las features se separan explicitamente en dos listas:

- `CAT_FEATURES` (6): sector, tipo, modalidad, estado, municipality_enc, corridor_enc
  (municipio y corredor usan geographic encoding per state)
  → se pasan a CatBoost como `cat_features` (strings discretos)
- `NUM_FEATURES` (2): log_price_sqm, log_area_sqm
  → se pasan como features numericas (floats continuos)

CatBoost recibe ambas listas y las trata de forma diferente internamente:
- Las **categoricas** las procesa con ordered target statistics (su mecanismo propietario)
- Las **numericas** las procesa como cualquier arbol de decision: busca el mejor punto
  de corte (split) en el rango continuo del valor

Adicionalmente, antes de llegar a CatBoost, aplicamos `log1p()` a precio y area.
Esto no es un requisito del modelo (CatBoost funcionaria sin la transformacion),
pero mejora el rendimiento porque:
- Comprime el rango de valores extremos (spots de $500,000/m² no distorsionan)
- Hace que los splits del arbol sean mas uniformes a lo largo del rango
- Es una practica estandar para features monetarias y de superficie con distribucion
  log-normal (muchos valores bajos, pocos valores muy altos)

**En las reglas y category effects**, precio y area se tratan de forma diferente:
- En `category_effects`: se discretizan en **quintiles** (5 bins) para calcular lift
  por rango, ya que un lift "por valor continuo" no tendria sentido
- En `rules`: se aplica un **barrido exhaustivo con ventana deslizante** sobre 19 percentiles
  (P5, P10, ..., P95) del subgrupo de spots que cumplen las condiciones categoricas de esa
  regla. El algoritmo evalua las 171 combinaciones posibles de (inicio, fin) y elige la que
  maximiza p_top. Si la mejor ventana aporta >= 5% de lift incremental, se usa como rango
  discriminante (sin asterisco). Si no, se muestra el rango P5-P95 observado como contexto
  informativo (con asterisco `*`).
  **Importante**: los percentiles se calculan sobre el **subgrupo** (no el universo global),
  por lo que dos reglas del mismo estado y sector pero con diferente corredor pueden mostrar
  rangos de precio distintos. Ejemplo: Condesa muestra `$404–$1,000` y Roma muestra
  `$550–$1,297` porque cada corredor tiene su propia distribucion de precios.

En ambos casos, la discretizacion es **solo para la tabla de salida** (presentacion),
no para el entrenamiento de CatBoost.

**Resumen**: los supuestos de CatBoost son principalmente de diseno de datos (i.i.d.,
representatividad, loss convexa), no distribucionales (no exige normalidad, linealidad,
ni homocedasticidad). Nuestro pipeline los cumple todos, y los dos puntos de mayor
riesgo (dependencia geografica y estacionariedad) estan activamente mitigados por el
group holdout y el re-entrenamiento mensual.

### 4.4 CatBoost vs XGBoost y LightGBM

Elegimos CatBoost sobre XGBoost y LightGBM por una razon especifica: **el manejo
nativo de categoricas**.

Nuestro dataset tiene 6 features categoricas y solo 2 numericas. XGBoost y LightGBM
requieren encoding previo (one-hot, target encoding, etc.), lo cual:
- Introduce decisiones arbitrarias de encoding
- Puede causar leakage si se hace target encoding sin cuidado
- Aumenta la dimensionalidad

CatBoost implementa internamente **ordered target statistics**: calcula la media del
target para cada categoria usando solo las observaciones anteriores en un orden aleatorio,
evitando leakage por diseno. Este es exactamente nuestro caso de uso: muchas categoricas,
alta cardinalidad, y necesidad de evitar leakage.

### 4.5 Lo que si perdemos vs logistica (y como lo compensamos)

La principal ventaja de logistica es la **interpretabilidad directa**: cada coeficiente
tiene un significado claro ("un aumento de 1 en log_price_sqm incrementa el log-odds
en β"). CatBoost no ofrece esto — es una caja negra de 2,000 arboles.

Compensamos esta limitacion con las 3 tablas de explicabilidad:
- **Drivers**: sustituyen a los coeficientes (que features importan)
- **Category effects**: sustituyen a los odds ratios (que valores favorecen)
- **Rules**: sustituyen a la interpretacion manual (combinaciones accionables)

En conjunto, estas 3 tablas ofrecen una explicabilidad **superior** a la de logistica
para el usuario de negocio, porque estan en lenguaje natural y no requieren entender
log-odds ni exponenciales.

### 4.6 Resumen para la defensa

> "Usamos CatBoost porque nuestros datos tienen 6 variables categoricas (incluyendo
> municipio con 100+ valores), relaciones no lineales entre precio/area y demanda,
> e interacciones fuertes entre zona y tipo de inmueble. La regresion logistica
> requeriria ingenieria manual de features, interacciones explicitas y one-hot encoding
> que harian el modelo fragil y dificil de mantener. CatBoost maneja todo esto
> nativamente, con mejor rendimiento predictivo (AUC ~0.87 vs un estimado ~0.72-0.75
> para logistica en este tipo de datos). La interpretabilidad se resuelve con las
> tablas de drivers, category effects y rules, que son mas legibles para negocio
> que los coeficientes de una logistica."

---

## 5. Para que sirve cada split (train/val/test/group) y que mide cada metrica?

### 5.1 Los 4 conjuntos de datos y su proposito

Nuestro pipeline no usa 3 conjuntos sino **4**, cada uno con un rol diferente:

```
Todos los spots (25,291 Rent / 14,037 Sale)
│
├── 70% TRAIN ──────── El modelo aprende patrones aqui
│
├── 15% VALIDATION ─── El modelo se auto-evalua aqui durante el entrenamiento
│                      (early stopping: "dejo de entrenar si ya no mejoro")
│
├── 15% TEST ──────── Evaluacion final "a ciegas" (metricas que reportamos)
│
└── 20% GROUP TEST ── Evaluacion geografica: municipios completos que el modelo
                       nunca vio (stress test de generalizacion)
```

#### TRAIN (70%)

**Que hace el modelo con el**: aprende. Itera sobre estos datos hasta 2,000 veces
(arboles), ajustando gradualmente sus predicciones para minimizar el error.

**Analogia**: es el "libro de texto" del alumno. Lo estudia a fondo.

#### VALIDATION (15%)

**Que hace el modelo con el**: se autoevalua **durante** el entrenamiento. Despues de
cada arbol nuevo, CatBoost mide su AUC sobre validacion. Si despues de 100 arboles
consecutivos (`od_wait=100`) el AUC de validacion no mejora, para de entrenar.

**Por que es necesario**: sin validacion, el modelo entrenaria los 2,000 arboles
completos y terminaria **memorizando** el training set (overfitting). La validacion
actua como un freno: "ya no estas aprendiendo patrones generales, estas memorizando".

**Analogia**: es el "examen de practica". El alumno lo usa para saber si ya estudio
suficiente o si necesita seguir. Pero como lo ve repetidamente, no es confiable para
la nota final.

**Importante**: la validacion NO se usa para reportar metricas finales, porque el
modelo la "ve" indirectamente durante el entrenamiento (el early stopping optimiza
sobre ella). Las metricas de validacion son optimistas.

#### TEST (15%)

**Que hace el modelo con el**: nada durante el entrenamiento. Solo se usa una vez,
al final, para calcular las metricas que reportamos (AUC, PR-AUC, Brier).

**Por que es necesario**: es la unica evaluacion honesta. El modelo nunca vio estos
datos ni directa ni indirectamente. Si las metricas son buenas aqui, el modelo
realmente generaliza a spots nuevos.

**Analogia**: es el "examen final". El alumno nunca lo vio antes. La nota aqui es
la que cuenta.

**En nuestro codigo**: las metricas que reportamos como `AUC_random`, `PR-AUC` y
`Brier` vienen de este split:

```python
y_proba_test = model.predict_proba(test_pool)[:, 1]
metrics_random = _compute_metrics(y_test_arr, y_proba_test)
```

#### GROUP TEST (20% de municipios)

**Que hace el modelo con el**: una segunda evaluacion, pero con una condicion especial:
todos los spots de ~23-28 municipios completos (20% de los retenidos) se reservan.
El modelo nunca vio ningun spot de esos municipios.

**Por que es necesario**: el test aleatorio mezcla spots de todas las zonas, asi que
el modelo podria "reconocer" patrones de una zona que vio parcialmente en train.
El group test pregunta algo mas duro: "puedes predecir bien en una ciudad donde
nunca has visto un solo spot?"

**Analogia**: el examen final es con preguntas de temas que SI estudio. El group test
es con preguntas de un tema nuevo que NO estudio — evalua si entendio los principios
generales, no si memorizo ejemplos.

**En nuestro codigo**: se reporta como `AUC_group`:

```python
y_proba_group = model.predict_proba(group_pool)[:, 1]
metrics_group = _compute_metrics(y_group_test, y_proba_group)
```

### 5.2 Las metricas y que mide cada una

Calculamos 4 metricas. Cada una responde a una pregunta diferente:

#### AUC-ROC (Area Under the ROC Curve)

**Que mide**: la capacidad del modelo de **rankear** correctamente. Si tomo un spot
Top P80 al azar y un spot no-Top al azar, que probabilidad hay de que el modelo le
asigne un score mayor al Top?

**Rango**: 0.5 (azar) a 1.0 (perfecto).

**Nuestros valores**:
| Universo | AUC test | AUC group | Interpretacion |
|---|---|---|---|
| Rent | 0.8062 | 0.8608 | Rankea correctamente ~81-86% de las veces |
| Sale | 0.8695 | 0.8962 | Rankea correctamente ~87-90% de las veces |

**Cuando es util**: para saber si el modelo distingue bien entre spots buenos y malos.
Es la metrica mas usada porque es robusta al desbalance de clases (80/20).

**Limitacion**: no dice nada sobre la calibracion (si dice 0.70, realmente es 70%?).

#### PR-AUC (Area Under the Precision-Recall Curve)

**Que mide**: la calidad de las predicciones **cuando nos enfocamos en los positivos**.
Si el modelo dice "este spot es Top P80", que tan frecuentemente acierta (precision)?
Y de todos los spots que realmente son Top P80, cuantos encuentra (recall)?

**Rango**: base rate (~0.20 para azar) a 1.0 (perfecto). Un modelo aleatorio tendria
~0.20 porque ~20% de los spots son Top P80.

**Nuestros valores**:
| Universo | PR-AUC | vs azar (0.20) | Mejora |
|---|---|---|---|
| Rent | 0.5218 | +0.32 | 2.6x mejor que azar |
| Sale | 0.6303 | +0.43 | 3.2x mejor que azar |

**Cuando es util**: es mas exigente que AUC-ROC en datasets desbalanceados. Si
PR-AUC es alto, significa que cuando el modelo dice "este es Top", generalmente
tiene razon, y ademas encuentra la mayoria de los Tops reales.

**Por que es menor que AUC-ROC**: siempre lo es en datos desbalanceados. No significa
que el modelo sea malo — un PR-AUC de 0.52-0.64 con 20% de prevalencia es solido.

#### Brier Score

**Que mide**: la **calibracion** — que tan cercanas son las probabilidades predichas
a las frecuencias reales. Es el error cuadratico medio entre la probabilidad predicha
y el resultado real (0 o 1).

**Rango**: 0.0 (calibracion perfecta) a 1.0 (maxima descalibracion).
Para referencia: un modelo que siempre predice la prevalencia global (~0.20) tendria
Brier = 0.20 × 0.80 = 0.16.

**Nuestros valores**:
| Universo | Brier | vs referencia (0.16) | Interpretacion |
|---|---|---|---|
| Rent | 0.1256 | -0.034 mejor | Bien calibrado |
| Sale | 0.1101 | -0.050 mejor | Muy bien calibrado |

**Cuando es util**: si vamos a usar el score numerico (p_top_p80) como probabilidad
real — por ejemplo, para decir a un cliente "este spot tiene 75% de probabilidad de
estar en el Top". Sin buena calibracion, ese 75% no significaria nada.

**Por que nos importa**: porque la tabla `propensity_scores` publica `p_top_p80`
como probabilidad. Si Brier fuera malo, tendriamos que recalibrar (Platt scaling).
Como es bueno, no es necesario.

#### Gap AUC (AUC_random - AUC_group)

**Que mide**: la **generalizacion geografica**. Es la diferencia entre el AUC en el
test aleatorio y el AUC en el group test (municipios no vistos).

**Rango ideal**: cercano a 0 (o negativo). Si es muy positivo, significa que el modelo
funciona bien en zonas conocidas pero mal en zonas nuevas (overfitting geografico).

**Nuestros valores**:
| Universo | AUC random | AUC group | Gap | Interpretacion |
|---|---|---|---|---|
| Rent | 0.8062 | 0.8608 | -0.055 | Group MEJOR que random (excelente) |
| Sale | 0.8695 | 0.8962 | -0.027 | Group MEJOR que random (excelente) |

**Dato notable**: nuestro gap es **negativo**, lo que significa que el modelo predice
MEJOR en municipios nuevos que en el test aleatorio. Esto ocurre porque los municipios
del holdout resultaron ser mas "predecibles" que el promedio (ej: municipios grandes
con patrones claros). No es preocupante — lo preocupante seria un gap positivo grande.

**Es un quality gate**: si `gap > 0.05`, el modelo falla y se intenta una variante
con mas regularizacion. Nuestro umbral exige que la generalizacion geografica no se
degrade mas de 5 puntos.

### 5.3 Los quality gates (criterios de aprobacion)

Antes de aceptar un modelo, debe pasar dos condiciones:

| Gate | Condicion | Que evita |
|---|---|---|
| `GATE_AUC_GROUP_MIN >= 0.60` | AUC en group test >= 0.60 | Modelo inutil en zonas nuevas |
| `GATE_GAP_AUC_MAX <= 0.05` | Gap AUC <= 0.05 | Overfitting a zonas de entrenamiento |

Si no pasa, se prueba automaticamente una variante mas conservadora (mas regularizacion,
menos features). Hay 4 variantes en cascada:

1. `base` (depth=6, l2=6) → la mas expresiva
2. `reg` (depth=4, l2=12) → mas conservadora
3. `no_muni` (sin municipio) → elimina la feature geografica mas granular
4. `no_muni_no_corr` (sin municipio ni corredor) → solo estado + atributos del spot

En la practica, ambos universos pasan con la variante `base` al primer intento.

### 5.4 Resumen visual

```
                    ┌──────────────────────────────┐
                    │         TRAIN (70%)           │
                    │  El modelo aprende aqui       │
                    └──────────────┬───────────────┘
                                   │ entrena
                                   ▼
                    ┌──────────────────────────────┐
                    │      VALIDATION (15%)         │
                    │  Early stopping: "ya basta?"  │
                    │  (el modelo la ve durante el  │
                    │   entrenamiento)              │
                    └──────────────┬───────────────┘
                                   │ modelo final
                                   ▼
              ┌────────────────────┴────────────────────┐
              │                                         │
              ▼                                         ▼
┌───────────────────────┐              ┌──────────────────────────┐
│      TEST (15%)       │              │   GROUP TEST (20% munis) │
│ Spots aleatorios que  │              │ Municipios COMPLETOS que │
│ nunca vio             │              │ nunca vio                │
│                       │              │                          │
│ → AUC-ROC             │              │ → AUC-ROC (group)        │
│ → PR-AUC              │              │ → Gap AUC                │
│ → Brier               │              │                          │
│                       │              │ "Funciona en zonas       │
│ "Funciona en general?"│              │  totalmente nuevas?"     │
└───────────────────────┘              └──────────────────────────┘
```

---

## 6. Como se generan las reglas? (Resumen ejecutivo del algoritmo)

Las reglas de la tabla `lk_effective_supply_rules` se generan con un **algoritmo
propio llamado Hierarchical Drill-Down**, implementado en `ml_rules.py`. No es un
arbol de decision ni un algoritmo de una libreria externa — es un procedimiento
personalizado disenado para respetar la logica de negocio inmobiliaria.

### Por que no se usa un arbol de decision estandar?

Se evaluo `DecisionTreeClassifier` de sklearn en versiones anteriores, pero se
descarto porque:
- Genera reglas con AND redundantes y confusas (ej: `estado ≠ CDMX AND estado = Jalisco`)
- No respeta la jerarquia geografica (puede mezclar municipios de diferentes estados)
- No permite controlar que el estado sea siempre el primer segmentador
- Fragmenta reglas estadisticamente equivalentes sin mecanismo de fusion

### Resumen del algoritmo en 7 pasos

El algoritmo se ejecuta por separado para cada universo (Rent y Sale):

**Paso 1 — Estado (raiz fija)**: evalua todos los estados con al menos 30 spots.
No se aplica pre-filtro por lift a nivel estado, ya que un estado con lift bajo global
puede contener combinaciones de nicho con lift alto. Tipicamente ~29 estados para Rent
y ~28 para Sale.

**Paso 2 — Zona geografica (municipio y corredor, independientes)**: para cada estado,
evalua independientemente si agregar municipio y/o corredor como segundo nivel.
Ambas ramas generan sus propias reglas si aportan >= 5% de lift incremental.
No coexisten en la misma regla individual, pero un estado puede tener reglas con
municipio y tambien con corredor. Los municipios/corredores se seleccionan con un
encoding adaptativo por estado (cobertura 90%, piso 3, soporte minimo global 10 spots).

**Paso 3 — Features categoricas (sector, tipo)**: expande combinatoriamente los valores
de sector y tipo que mejoran la tasa del estado, en el orden de importancia de CatBoost.
Resultado: ~17 candidatos granulares.

**Paso 4 — Rangos numericos (ventana deslizante)**: para cada candidato, evalua 171
combinaciones de rangos de precio_sqm y area sobre percentiles P5-P95 con step de 5pp.
Elige la ventana que maximiza p_top. Si aporta >= 5% de lift, el rango es discriminante;
si no, se muestra como contexto con asterisco.

**Paso 5 — Fusion Marascuilo**: reglas con proporciones estadisticamente indistinguibles
(test de Marascuilo, alpha=0.05) se fusionan en una sola regla, uniendo categorias y
recalculando metricas.

**Paso 6 — Filtrado y enriquecimiento**: se descartan reglas con lift < 1.5x y se
enriquecen con `avg_score_percentile` (posicion promedio en el ranking) y `strength`
(etiqueta: Very High/High/Moderate).

**Paso 7 — Output**: se genera `rule_text` legible, se deduplican y se numeran por p_top
descendente.

### Como interpretar una regla

Ejemplo:
```
#4  lift=3.53x  p=0.71  n=31  str=Very High
→  Mexico + Toluca-Lerma + Industrial + Subspace + $109–$145 (P5-P65) + 1,184–16,769 m² (P10-P95)
```

Lectura: "De los 31 spots en Estado de Mexico, corredor Toluca-Lerma, sector Industrial,
tipo Subspace, con precio entre $109 y $145 por m² y area entre 1,184 y 16,769 m²,
el 71% esta en el Top P80. Eso es 3.53 veces mas que el promedio del universo."

Si el rango de precio tuviera asterisco (`$109–$145 *`), significaria: "el rango de precios
tipico de estos spots es $109–$145, pero el precio no es lo que los hace Top — es la
combinacion de zona, sector y tipo."

### Documentacion completa

El algoritmo esta documentado paso a paso con formulas, diagramas y constantes en
la seccion 8 de `plan_ml_effective_supply_v2.md`.
