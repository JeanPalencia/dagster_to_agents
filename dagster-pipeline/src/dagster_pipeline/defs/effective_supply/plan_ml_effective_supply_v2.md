# ML Effective Supply — Documentacion del sub-flujo

> Documentacion consolidada con las 3 fases completadas.
> Ultima actualizacion: febrero 2026 (geographic encoding adaptativo por estado, renombre k100->enc, evaluacion independiente de municipio/corredor).
> Archivo original: `plan_ml_prediccion_de_supply_efectivo_rent_vs_sale.md`

---

## Resumen ejecutivo

Pipeline ML mensual que entrena modelos CatBoost separados para Rent y Sale,
prediciendo P(is_top_p80 = 1) usando solo atributos del spot (sin leakage).

Produce 4 tablas de salida en PostgreSQL:

| Tabla | Fase | Estado | Grano |
|---|---|---|---|
| `lk_effective_supply_propensity_scores` | 1 | COMPLETADA | spot_id x market_universe |
| `lk_effective_supply_drivers` | 1 | COMPLETADA | market_universe x feature_name |
| `lk_effective_supply_category_effects` | 2 | COMPLETADA | market_universe x feature_name x category |
| `lk_effective_supply_rules` | 3 | COMPLETADA | market_universe x rule_id |
| `lk_effective_supply_model_metrics` | - | COMPLETADA | market_universe x model_version |

---

## 0) Principios (no negociables)

1. **Separacion por universo**: Rent (Rent + Rent&Sale) y Sale (Sale + Rent&Sale).
2. **Sin leakage**: NUNCA usar view/contact/project counts ni scores como features.
3. **Ventana movil**: ultimos 6 meses cerrados, recalculado mensualmente.
4. **Outputs BI-friendly**: columna `market_universe` en todas las tablas.

---

## 1) Fuente de datos y universos

Fuente: `gold_lk_effective_supply` (37,763 spots marketables, sin complejos ni eliminados).

- **Rent**: spots con `is_for_rent_id = 1` (25,291 spots, ~20% target=1)
  - Precios: `spot_price_total_mxn_rent`, `spot_price_sqm_mxn_rent`
- **Sale**: spots con `is_for_sale_id = 1` (14,037 spots, ~20% target=1)
  - Precios: `spot_price_total_mxn_sale`, `spot_price_sqm_mxn_sale`

> Spots "Rent & Sale" participan en ambos universos.
> Spots complejos (spot_type_id NOT IN (1,3)) y soft-deleted se excluyen desde `stg_gs_lk_spots`.

---

## 2) Target

Pre-calculado en `gold_lk_effective_supply`:
- Rent: `is_top_p80_rent_id` (threshold ~0.628)
- Sale: `is_top_p80_sale_id` (threshold ~0.353)

---

## 3) Features

### Categoricas (6)
- `spot_sector_id`, `spot_type_id`, `spot_modality_id`, `spot_state_id`
- `municipality_enc`, `corridor_enc` (coverage-based per-state geographic encoding)

### Numericas (2)
- `log_price_sqm` = log1p(price_sqm)
- `log_area_sqm` = log1p(spot_area_in_sqm)

> **Nota**: `log_price_total` fue eliminada del modelo por redundancia.
> El precio total es el producto de precio por m² y area (`price_sqm * area`),
> por lo que incluirlo causaba colinealidad sin aportar informacion adicional.
> La eliminacion tuvo impacto despreciable en las metricas (< 0.5% en AUC).

### Geographic encoding (coverage-based per state)
Municipio y corredor tienen alta cardinalidad. En lugar de un K fijo global (que
favorece a los estados grandes y deja sin representacion a los pequenos), se usa
una estrategia adaptativa por estado:

1. **Cobertura por estado** (`GEO_ENC_COVERAGE=0.90`): para cada estado, se retienen
   los municipios/corredores mas frecuentes hasta cubrir >= 90% de los spots de
   ese estado.
2. **Piso por estado** (`GEO_ENC_FLOOR=3`): cada estado retiene al menos 3 valores
   (o todos si tiene menos).
3. **Soporte minimo global** (`GEO_ENC_MIN_SUPPORT=10`): un valor solo se retiene si
   tiene >= 10 spots en todo el universo. Esto previene micro-categorias que
   causarian overfitting.

El resultado es un conjunto adaptativo de categorias que varia por universo:
- Rent: ~116 municipios, ~54 corredores retenidos
- Sale: ~138 municipios, ~47 corredores retenidos

Los valores no retenidos se agrupan en `__OTHER__` y los nulls en `__NULL__`.

---

## 4) Split y validacion

- Estratificado: 70% train / 15% val / 15% test
- Group holdout: 20% de `municipality_enc` unicos para stress test geografico
- Metricas: AUC-ROC, PR-AUC, Brier score
- Gate: `auc_group >= 0.60` y `gap_auc <= 0.05`

---

## 5) Entrenamiento y fallback

Modelo: CatBoostClassifier con early stopping.

Cascada de fallback (si no pasa el gate):
1. `base`: depth=6, l2=6, min_leaf=50
2. `reg`: depth=4, l2=12, min_leaf=100
3. `no_muni`: igual que reg, sin municipality_enc
4. `no_muni_no_corr`: sin municipality_enc ni corridor_enc

### Resultados tipicos (ultima corrida — coverage-based per-state encoding, 8 features, sin price_total)
| Universo | Variante | AUC random | AUC group | Gap AUC | PR-AUC | Brier |
|---|---|---|---|---|---|---|
| Rent | base | 0.8062 | 0.8608 | -0.0546 | 0.5218 | 0.1256 |
| Sale | base | 0.8695 | 0.8962 | -0.0266 | 0.6303 | 0.1101 |

Ambos pasan el gate al primer intento.

### Top-3 drivers por universo
| Rank | Rent | Importancia | Sale | Importancia |
|---|---|---|---|---|
| 1 | spot_state_id | 1.0000 | spot_state_id | 1.0000 |
| 2 | log_price_sqm | 0.8209 | municipality_enc | 0.5620 |
| 3 | spot_sector_id | 0.7755 | log_area_sqm | 0.4503 |

---

## 6) Fase 1: Propensity Scores + Drivers (COMPLETADA)

### 6.1 Propensity Scores
- Tabla: `lk_effective_supply_propensity_scores`
- 39,328 filas (25,291 rent + 14,037 sale)
- Columnas: spot_id, market_universe, p_top_p80, model_version, window_end_date, window_months, model_variant, audit fields

### 6.2 Drivers (Feature Importance)
- Tabla: `lk_effective_supply_drivers`
- 16 filas (8 features x 2 universos)
- Usa CatBoost built-in importance (fallback desde permutation importance por incompatibilidad sklearn/CatBoost)
- Columnas: market_universe, feature_name, importance, importance_method, rank, model_version, window_end_date, model_variant, audit fields

### Archivos implementados
```
ml/__init__.py
ml/constants.py
ml/ml_build_universes.py
ml/ml_feature_engineering.py
ml/ml_train.py
ml/ml_score.py
ml/ml_drivers.py
gold/gold_lk_effective_supply_propensity_scores.py
gold/gold_lk_effective_supply_drivers.py
publish/lk_effective_supply_propensity_scores.py
publish/lk_effective_supply_drivers.py
```

### SQL
```
lakehouse-sdk/sql/lk_effective_supply_propensity_scores.sql
lakehouse-sdk/sql/lk_effective_supply_drivers.sql
```

### Ajustes vs plan original
- Nombres: `ml_supply_*` -> `lk_effective_supply_*` (convencion Data Lakehouse)
- Permutation importance: fallback automatico a CatBoost built-in (incompatibilidad `__sklearn_tags__` con CatBoostClassifier)
- Calibracion Platt (seccion 7.2 original): no implementada (opcional, metricas de calibracion son buenas)
- `log_price_total` eliminada por redundancia con `log_price_sqm` + `log_area_sqm`

---

## 7) Fase 2: Category Effects (COMPLETADA)

### Objetivo
Para cada feature categorica (y bins numericos), por universo, calcular el efecto de cada valor:
- `support_n`: cuantos spots tienen esa categoria
- `p_top`: tasa real de is_top_p80=1 en esa categoria
- `lift`: p_top / p_top_global (>1 = mejor que promedio)

Responde: "que valores de estado/sector/municipio/precio/area favorecen la oferta efectiva?"

### Implementacion
- Asset: `ml_category_effects` — calcula metricas por categoria usando datos originales (IDs), resuelve a nombres legibles
- Soporte minimo: 30 spots (configurable en `constants.py` via `MIN_SUPPORT`)
- Categoricas: `spot_sector_id`, `spot_type_id`, `spot_modality_id`, `spot_state_id`, `spot_municipality_id`, `spot_corridor_id`
- Numericas discretizadas en quintiles: `price_sqm`, `spot_area_in_sqm`
  - Formato legible: rangos con formato de moneda para precios y m² para area

### Tabla: `lk_effective_supply_category_effects`
Grano: market_universe x feature_name x category_value
- market_universe, feature_name, category_id, category_name
- support_n, p_top, lift
- model_version, window_end_date, audit fields

### Archivos
```
ml/ml_category_effects.py
gold/gold_lk_effective_supply_category_effects.py
publish/lk_effective_supply_category_effects.py
lakehouse-sdk/sql/lk_effective_supply_category_effects.sql
```

---

## 8) Fase 3: Rules — Hierarchical Drill-Down (COMPLETADA)

### 8.1 Motivacion y objetivo

**Objetivo**: extraer reglas combinadas interpretables que respondan a la pregunta de negocio:
> "Que combinacion concreta de estado + zona + sector + tipo + precio + area tiene la
> mayor probabilidad de estar en el Top P80?"

Ejemplo de regla generada:
> "Mexico + corredor Toluca-Lerma + Industrial + Subspace + precio_m² $109–$145 (P5-P65) + area 1,184–16,769 m² (P10-P95)
> → P(top) = 71%, lift 3.53x, strength Very High"

**Por que un algoritmo propio y no un arbol de decision estandar?**

Se evaluaron arboles de decision (`DecisionTreeClassifier` de sklearn) en versiones
anteriores (Fase 3 v1 y v2), pero se descartaron por las siguientes razones:

1. **Reglas redundantes y confusas**: un arbol con one-hot encoding genera reglas como
   `estado ≠ Ciudad de Mexico AND estado = Jalisco`, que son tecnicamentecorrectas pero
   ilegibles para negocio. Incluso con target encoding ordinal, las reglas compuestas con
   AND concatenados eran dificiles de interpretar.

2. **Ignora la geografia**: el arbol no sabe que un municipio pertenece a un estado, ni
   que un corredor puede abarcar varios municipios. Puede generar reglas que mezclen
   municipio de Jalisco con estado de Ciudad de Mexico, lo cual no tiene sentido de negocio.

3. **No respeta prioridades de negocio**: el estado es siempre la primera dimension para
   segmentar mercados inmobiliarios (cada estado tiene su propia dinamica). Un arbol de
   decision puede poner sector o precio antes de estado si estadisticamente conviene,
   produciendo reglas que no siguen la logica de negocio.

4. **Fragmentacion artificial**: el arbol puede generar muchas reglas granulares que son
   estadisticamente indistinguibles (p_top similar), lo cual infla la tabla sin aportar
   informacion nueva. No tiene un mecanismo nativo de fusion.

El algoritmo **Hierarchical Drill-Down** resuelve estos 4 problemas mediante un diseno
que combina la inteligencia del modelo (importancia de CatBoost) con reglas de negocio
explicitas (jerarquia geografica, exclusion mutua, fusion estadistica).

### 8.2 Inputs del algoritmo

El asset `ml_rules` recibe 3 inputs:

1. **`ml_build_universes`** (dict[str, pl.DataFrame]):
   - DataFrames de Rent y Sale con todos los spots del universo
   - Columnas: `spot_id`, `target` (0/1 = is_top_p80), todas las features originales
     (IDs y nombres: spot_state_id, spot_state, spot_sector_id, spot_sector, etc.)
   - Incluye precios originales (`price_sqm`, `spot_area_in_sqm`) sin transformar

2. **`ml_drivers`** (dict[str, pl.DataFrame]):
   - Importancia de cada feature segun CatBoost, por universo
   - Se usa para determinar el **orden de exploracion** de features en el drill-down
   - Columnas: `feature_name`, `importance` (0-1, normalizada)

3. **`gold_lk_effective_supply`** (pl.DataFrame):
   - Tabla gold completa con `total_score` para todos los spots
   - Se usa en el paso de enriquecimiento para calcular `avg_score_percentile`

### 8.3 Paso 1: Seleccion de estados (nivel raiz, siempre fijo)

El estado es **siempre** el primer nivel de la jerarquia, por dos razones:
- Es sistematicamente el segmentador mas importante segun CatBoost (rank 1 en ambos universos)
- Los mercados inmobiliarios son locales: el comportamiento de oferta/demanda difiere
  radicalmente entre estados

**Procedimiento**:
1. Calcular `p_top_global` = tasa global de Top P80 en el universo (~0.20)
2. Seleccionar **todos los estados** con al menos `RULES_MIN_SUPPORT = 30` spots
3. No se aplica pre-filtro por lift a nivel estado — un estado con lift bajo global
   puede contener combinaciones de nicho con lift alto (ej: Queretaro)
4. El control de calidad se aplica al final sobre cada regla individual (`RULES_MIN_LIFT`)

**Resultado**: ~29 estados (Rent) o ~28 estados (Sale) con soporte suficiente.

Cada estado tambien genera una regla "fallback" (solo estado, sin mas
variables) como baseline. Esta regla se conserva solo si pasa el filtro de lift minimo
en pasos posteriores.

### 8.4 Paso 2: Evaluacion geografica independiente (municipio y corredor)

Para cada estado, se evalua **independientemente** si agregar municipio y/o corredor
como segundo nivel geografico. Ambas ramas se evaluan por separado y cada una genera
sus propias reglas si aporta lift suficiente. No coexisten en la misma regla individual
(una regla tiene municipio **o** corredor, no ambos), pero un estado puede generar
reglas con municipio y tambien con corredor.

**Procedimiento**:
1. Aplicar coverage-based per-state encoding a municipios y corredores:
   cobertura 90% por estado, floor 3, soporte minimo global 10 spots
2. Para cada valor de municipio con soporte >= 30, calcular `p_top_muni`
3. Para cada valor de corredor con soporte >= 30, calcular `p_top_corr`
4. Seleccionar los valores que mejoran `p_top_estado` (lift > 1 vs estado)
5. Calcular el lift incremental de usar municipio y corredor:
   - `lift_inc_muni = (p_top_muni_combinado - p_top_estado) / p_top_estado`
   - `lift_inc_corr = (p_top_corr_combinado - p_top_estado) / p_top_estado`
6. Si municipio aporta >= 5% (`RULES_MIN_LIFT_INCREMENT`): se generan reglas con municipio
7. Si corredor aporta >= 5%: se generan reglas con corredor (independiente de municipio)
8. Si ninguno aporta >= 5%: sin nivel geografico para ese estado

**Resultado**: para cada estado, una lista de `geo_items` que puede incluir:
- Valores de municipio que aportan lift
- Valores de corredor que aportan lift
- O vacío si ninguno aporta (solo estado)

### 8.5 Paso 3: Expansion combinatoria de features categoricas

Con el estado y la opcion geografica definidos, se expanden las features categoricas
restantes en orden de importancia de CatBoost.

**Features drillables** (excluidas estado, geo y modalidad):
- `spot_sector_id` (sector: Industrial, Retail, Office, Land)
- `spot_type_id` (tipo: Single, Subspace, Building, Complex)

La modalidad (`spot_modality_id`) se excluye porque es redundante con el split
por market_universe (Rent/Sale).

**Procedimiento por estado**:
1. Determinar los valores de tipo y sector que mejoran `p_top_estado`:
   - Para cada valor de `spot_type_id`, calcular p_top en el estado
   - Conservar solo los que superan `p_top_estado`
   - Idem para `spot_sector_id`
2. Generar candidatos por **combinacion cartesiana**:
   - Para cada valor geo (o [None] si no hay geo):
     - Para cada valor tipo (o [None] si ninguno mejora):
       - Para cada valor sector (o [None] si ninguno mejora):
         - Filtrar el DataFrame al subconjunto que cumple todas las condiciones
         - Verificar soporte >= 30 spots
         - Calcular `p_top_candidato`
         - Solo conservar si `p_top_candidato > p_top_estado` (mejora sobre baseline)

**Resultado**: lista de candidatos granulares. Cada candidato es un dict con:
```
{
    "estado": "Ciudad de Mexico",
    "municipio": "Benito Juarez" (o None),
    "corredor": None (o "Condesa"),
    "tipo": "Single" (o None),
    "sector": "Retail" (o None),
    "support_n": 38,
    "p_top": 0.7368,
    "lift": 3.66,
}
```

Tipicamente se generan 10-20 candidatos granulares por universo.

### 8.6 Paso 4: Evaluacion de rangos numericos — ventana deslizante

Para cada candidato generado en el paso 3, se evalua si acotar el rango de
`price_sqm` (precio por metro cuadrado) y/o `spot_area_in_sqm` (area) mejora
la probabilidad de ser Top P80.

**Algoritmo: Sliding Window sobre percentiles del subgrupo**

A diferencia de la discretizacion en quintiles fijos (usada en `category_effects`),
las reglas usan un barrido exhaustivo de **ventanas deslizantes** que prueban todos
los rangos posibles a una resolucion de 5 percentiles.

**Procedimiento para cada feature numerica (precio_sqm, area)**:

1. **Filtrar a valores validos**: spots del candidato con valor no-nulo y > 0

2. **Generar puntos de corte**: 19 percentiles del subgrupo:
   P5, P10, P15, P20, P25, P30, P35, P40, P45, P50,
   P55, P60, P65, P70, P75, P80, P85, P90, P95

   **Importante**: estos percentiles se calculan sobre el **subgrupo** (los spots que
   ya cumplen las condiciones categoricas del candidato), no sobre el universo global.
   Por eso dos candidatos con distinto corredor pueden tener puntos de corte diferentes.

3. **Evaluar todas las combinaciones (lo, hi)**:
   Para cada par (i, j) donde i < j (171 combinaciones en total = 19*18/2):
   - `lo_val` = valor del percentil i
   - `hi_val` = valor del percentil j
   - Contar spots en [lo_val, hi_val]: `n`
   - Sumar targets en ese rango: `sum_target`
   - Calcular `p_top = sum_target / n`
   - Descartar si `n < RULES_MIN_SUPPORT` (30)

4. **Optimizacion con sumas acumuladas**:
   En lugar de filtrar el DataFrame 171 veces, se usa una tecnica eficiente:
   - Ordenar los spots por el valor numerico
   - Construir un array acumulado del target: `cum_target[k] = sum(target[0..k-1])`
   - Para cada ventana (idx_lo, idx_hi):
     `n = idx_hi - idx_lo`
     `sum_target = cum_target[idx_hi] - cum_target[idx_lo]`
   - Se usa `numpy.searchsorted` para mapear valores de percentil a indices

   Esto reduce la complejidad de O(171 * n_spots) a O(n_spots * log(n_spots)) (el sort)
   mas O(171) operaciones aritmeticas.

5. **Seleccionar la mejor ventana**: la que maximiza `p_top`

6. **Evaluar lift incremental**:
   - `lift_inc = (p_top_ventana - p_top_candidato) / p_top_candidato`
   - Si `lift_inc >= RULES_MIN_LIFT_INCREMENT` (5%): la ventana es **discriminante**
     → se aplica el rango (sin asterisco), se actualiza `p_top` y se filtra el DataFrame
   - Si no: la ventana no aporta suficiente mejora

7. **Rango no discriminante (fallback con asterisco)**:
   Si ninguna ventana aporta >= 5% de lift, se muestra el rango observado P5-P95
   del subgrupo como **contexto informativo**, marcado con asterisco `*`.
   Este rango indica "los spots de esta regla estan tipicamente en este intervalo
   de precio/area", pero el precio/area no es lo que los hace Top P80.

**Notacion en el output**:
- `$404 – $1,000` → rango discriminante (sin asterisco): filtrar a este rango **mejora**
  la probabilidad de ser Top P80 en al menos 5%
- `$128 – $210 *` → rango no discriminante (con asterisco): rango observado como contexto,
  el precio no es un diferenciador significativo para esta combinacion

**Por que los rangos difieren entre reglas del mismo estado?**
Porque los percentiles se calculan sobre el subgrupo filtrado. Si dos reglas comparten
el mismo estado pero tienen distinto corredor, los spots son diferentes y por tanto la
distribucion de precios es diferente. Ejemplo:
- Regla A: CDMX + Condesa + Retail → precio P5-P95 del subgrupo = $404–$1,000
- Regla B: CDMX + Roma + Retail → precio P5-P95 del subgrupo = $550–$1,297

### 8.7 Paso 5: Fusion Marascuilo

Despues de generar todos los candidatos (con sus rangos numericos), se aplica un paso
de fusion estadistica para evitar **fragmentacion artificial**: multiples reglas que
difieren en una categoria pero tienen proporciones de Top P80 estadisticamente
indistinguibles.

**Ejemplo**: si "Condesa + Retail" tiene p_top=0.74 y "Roma + Retail" tiene p_top=0.72,
y la diferencia no es estadisticamente significativa, es mejor fusionarlas en una sola
regla "Condesa, Roma + Retail" con p_top recalculado.

**Test utilizado: Marascuilo (multiple comparisons de proporciones)**

El test de Marascuilo es una extension del test de chi-cuadrado para k > 2 proporciones.
Permite comparar multiples proporciones simultaneamente controlando la tasa de error.

**Procedimiento**:

1. **Agrupar candidatos por contexto compartido**: se agrupan por
   `(spot_state, price_sqm_range, area_range)`. Solo candidatos que comparten este contexto
   son evaluados para fusion.

2. **Para cada grupo con k > 1 candidatos**:
   a. Calcular la distancia critica para cada par (i, j):
      ```
      r_ij = sqrt(pi*(1-pi)/ni + pj*(1-pj)/nj) * sqrt(chi2_crit(k-1, alpha))
      ```
      donde `pi`, `pj` son los p_top, `ni`, `nj` son los support_n, y `chi2_crit` es
      el valor critico de chi-cuadrado para k-1 grados de libertad con
      `alpha = RULES_MARASCUILO_ALPHA = 0.05`.

   b. Dos candidatos son **indistinguibles** si `|pi - pj| < r_ij`

   c. **Construir cliques maximales** (no transitivos):
      - Ordenar candidatos por p_top descendente
      - Para cada candidato no asignado (semilla):
        - Crear un grupo con la semilla
        - Agregar cada candidato restante SOLO si es indistinguible con TODOS los miembros
          actuales del grupo
      - Esto evita el problema de transitividad: si A~B y B~C pero A!~C, no se fusionan
        los tres — se forma el clique {A,B} y C queda solo (o forma otro clique)

3. **Fusionar miembros del clique**:
   - Unir categorias: si un clique tiene {Condesa, Roma, Del Valle}, la regla fusionada
     mostrara `corredor ∈ {Condesa, Roma, Del Valle}`
   - Recalcular metricas sobre la **union** de spots (no promediar p_top):
     - Construir un filtro OR que combina las condiciones de todos los miembros
     - Calcular p_top y support_n sobre el DataFrame filtrado

### 8.8 Paso 6: Filtrado y enriquecimiento

Despues de la fusion, se aplican filtros y se enriquecen las reglas:

**6a. Filtro de lift minimo**:
- Se descartan reglas con `lift < RULES_MIN_LIFT` (1.5x)
- Esto elimina las reglas de "solo estado" (fallback) que no son accionables
  (una regla que dice "Jalisco tiene 25% de Top P80" vs 20% global es verdadera
  pero poco util para negocio)
- Tipicamente esto reduce de ~17 a ~13 reglas por universo

**6b. Calculo de avg_score_percentile**:
- Para cada regla, se busca en `gold_lk_effective_supply` los spots que cumplen
  las condiciones categoricas de la regla
- Se calcula el percentil de `total_score` de cada spot (rank ordinal / n_total)
  dentro del universo (Rent o Sale)
- Se promedia: `avg_score_percentile = mean(percentile_rank) * 100`
- Indica la posicion promedio de estos spots en el ranking de relevancia
- Ejemplo: avg_score_percentile=81.9 → los spots estan, en promedio, en el top 18%
- Manejo especial: valores "Others" en municipio/corredor se saltan en el filtro
  para evitar NaN (esos spots estan distribuidos en muchos municipios/corredores)

**6c. Asignacion de strength**:
- Etiqueta categorica basada en el lift de la regla:
  - `lift >= 3.0` → "Very High"
  - `lift >= 2.0` → "High"
  - `lift >= 1.5` → "Moderate"
- Configurada en `constants.py` via `STRENGTH_THRESHOLDS`

### 8.9 Paso 7: Construccion del output

**7a. Generacion de rule_text**:
- Se construye un texto legible concatenando las condiciones con ` → `:
  ```
  estado = Mexico → corredor ∈ {Toluca - Lerma} → sector ∈ {Industrial}
  → tipo ∈ {Subspace} → precio_m² ∈ $109 – $145 → area ∈ 1,184 – 16,769 m²
  ```
- Las condiciones ausentes (NULL) se omiten del texto

**7b. Deduplicacion**:
- Se eliminan reglas con `rule_text` identico (pueden surgir de la fusion)

**7c. Ordenamiento y numeracion**:
- Se ordena por `p_top` descendente (la regla con mayor probabilidad primero)
- Se asigna `rule_id` secuencial (1 = mejor regla)
- Se agrega `market_universe` (Rent/Sale)

**7d. Construccion del DataFrame**:
- Se seleccionan las columnas definidas en `RULE_COLUMNS`:
  `market_universe`, `rule_id`, `spot_state`, `spot_sector`, `spot_type`,
  `spot_municipality`, `spot_corridor`, `price_sqm_range`, `price_sqm_pctl_range`,
  `price_sqm_is_discriminant_id`, `price_sqm_is_discriminant`,
  `area_range`, `area_pctl_range`,
  `area_is_discriminant_id`, `area_is_discriminant`,
  `rule_text`, `support_n`, `p_top`, `lift`,
  `avg_score_percentile`, `strength`

### 8.10 Diagrama de flujo completo del algoritmo

```
                     ml_build_universes (datos originales)
                     ml_drivers (importancia CatBoost)
                     gold_lk_effective_supply (para percentiles)
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │  Por cada universo (Rent/Sale)│
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ PASO 1: Seleccion de estados  │
                    │ todos con n >= 30             │
                    │ → ~29 estados tipicamente     │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ PASO 2: Geo independiente      │
                    │ municipio AND/OR corredor      │
                    │ por estado (>= 5% lift inc.)  │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ PASO 3: Expansion categorica   │
                    │ sector x tipo (importancia)    │
                    │ → ~17 candidatos granulares    │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ PASO 4: Ventana deslizante     │
                    │ 19 percentiles, 171 ventanas   │
                    │ precio_sqm + area              │
                    │ discriminante o contexto (*)    │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ PASO 5: Fusion Marascuilo      │
                    │ alpha=0.05, cliques greedy     │
                    │ union categorias + recalculo   │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ PASO 6: Filtro + enriquecimiento│
                    │ lift >= 1.5x                   │
                    │ avg_score_percentile, strength │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
                    │ PASO 7: Output                 │
                    │ rule_text, dedup, rule_id      │
                    │ → ~13 reglas Rent, ~10 Sale    │
                    └──────────────────────────────┘
```

### Tabla: `lk_effective_supply_rules`
Grano: market_universe x rule_id x model_version

Columnas individuales por segmentador:
- `spot_state` (VARCHAR, siempre presente)
- `spot_sector`, `spot_type` (VARCHAR, NULL si no relevante)
- `spot_municipality` (VARCHAR, NULL si se uso corridor)
- `spot_corridor` (VARCHAR, NULL si se uso municipality)
- `price_sqm_range` (VARCHAR, rango de precio por m²)
- `price_sqm_pctl_range` (VARCHAR, rango de percentiles, ej: "P25-P70")
- `price_sqm_is_discriminant_id` (INTEGER, 1 si el rango aporta >= 5% lift, 0 si es contexto)
- `price_sqm_is_discriminant` (VARCHAR, "Yes" o "No")
- `area_range` (VARCHAR, rango de area en m²)
- `area_pctl_range` (VARCHAR, rango de percentiles, ej: "P10-P60")
- `area_is_discriminant_id` (INTEGER, 1 si el rango aporta >= 5% lift, 0 si es contexto)
- `area_is_discriminant` (VARCHAR, "Yes" o "No")

Metricas:
- `rule_text` (TEXT, regla completa legible para negocio)
- `support_n` (INTEGER, spots que cumplen la regla)
- `p_top` (DOUBLE, tasa observada de is_top_p80=1 entre los spots que cumplen la regla)
- `lift` (DOUBLE, p_top / p_top_global; cuantas veces mejor que el promedio)
- `avg_score_percentile` (DOUBLE, percentil promedio de total_score de los spots que
  cumplen la regla, en escala 0-100; indica que tan "arriba" estan estos spots en el ranking
  general del universo)
- `strength` (VARCHAR, strength label based on lift:
  "Very High" >= 3x, "High" >= 2x, "Moderate" >= 1.5x)

Metadata:
- model_version, window_end_date, audit fields

### Ejemplo de reglas generadas (Rent, febrero 2026)
```
#1  lift=4.65x  p=0.94  n=31  str=Very High  →  Mexico + Cuautitlan + Industrial + Subspace
                                                  + precio_m² $150–$210 * (P5-P95) + area 579–2,936 m² (P20-P60)
#2  lift=3.66x  p=0.74  n=38  str=Very High  →  CDMX + Condesa + Retail + Single
                                                  + precio_m² $404–$1,000 (P25-P85) + area 60–600 m² * (P5-P95)
#3  lift=3.57x  p=0.72  n=32  str=Very High  →  CDMX + Roma + Retail + Single
                                                  + precio_m² $550–$1,297 (P35-P95) + area 17–552 m² * (P5-P95)
#4  lift=3.53x  p=0.71  n=31  str=Very High  →  Mexico + Toluca-Lerma + Industrial + Subspace
                                                  + precio_m² $109–$145 (P5-P65) + area 1,184–16,769 m² (P10-P95)
```

### Constantes configurables (en `constants.py`)

| Constante | Valor | Descripcion |
|---|---|---|
| `RULES_MIN_SUPPORT` | 30 | Spots minimos por regla |
| `RULES_MIN_LIFT` | 1.5 | Lift minimo para conservar la regla |
| `RULES_MIN_LIFT_INCREMENT` | 0.05 | Mejora minima (5%) para incluir una feature |
| `RULES_MARASCUILO_ALPHA` | 0.05 | Nivel de significancia del test de fusion |
| `RULES_RANGE_STEP` | 0.05 | Paso de percentil para ventana deslizante (5pp) |
| `RULES_RANGE_MIN_PCTL` | 0.05 | Percentil minimo (P5) |
| `RULES_RANGE_MAX_PCTL` | 0.95 | Percentil maximo (P95) |
| `GEO_ENC_COVERAGE` | 0.90 | Cobertura objetivo por estado (90%) |
| `GEO_ENC_FLOOR` | 3 | Minimo de categorias por estado |
| `GEO_ENC_MIN_SUPPORT` | 10 | Soporte minimo global (spots) por categoria |
| `STRENGTH_THRESHOLDS` | [(3.0, "Very High"), (2.0, "High"), (1.5, "Moderate")] | Strength labels |

### Archivos
```
ml/ml_rules.py          — Algoritmo completo (funciones _best_sliding_window,
                           _compute_universe_rules, _marascuilo_groups, etc.)
ml/constants.py          — Todos los umbrales y constantes configurables
gold/gold_lk_effective_supply_rules.py
publish/lk_effective_supply_rules.py
lakehouse-sdk/sql/lk_effective_supply_rules.sql
```

---

## 8.5) Tabla: `lk_effective_supply_model_metrics`

Persiste las metricas de evaluacion del modelo CatBoost para cada ejecucion mensual,
permitiendo trazabilidad historica de la calidad del modelo.

**Grano**: market_universe x model_version (2 filas por ejecucion: Rent y Sale)

**Columnas principales**:
- `market_universe`, `model_version`, `model_variant`
- `gate_passed_id` (0/1), `gate_passed` ("Yes"/"No")
- `auc_roc_random`, `pr_auc_random`, `brier_random` — metricas sobre test aleatorio (15%)
- `auc_roc_group`, `pr_auc_group`, `brier_group` — metricas sobre holdout geografico
- `gap_auc` — diferencia AUC_random - AUC_group (senhal de overfitting)
- `best_iteration` — iteracion de early-stopping de CatBoost
- `window_end_date`, `window_months`
- Campos de auditoria estandar

**Archivos**:
```
gold/gold_lk_effective_supply_model_metrics.py
publish/lk_effective_supply_model_metrics.py
lakehouse-sdk/sql/lk_effective_supply_model_metrics.sql
```

---

## 9) Scheduling

Todos los assets ML estan incluidos en el job mensual existente:
- Job: `effective_supply_monthly_job`
- Schedule: `0 3 1 * *` (3 AM, dia 1 de cada mes, hora CDMX)
- Seleccion: upstream de todos los `*_to_geospot` terminales

---

## 10) Arquitectura de assets (DAG)

```
STG (3 assets: stg_bq_spot_events, stg_gs_bt_lds_lead_spots, stg_gs_lk_spots)
  └─> core_effective_supply_events (union de eventos)
      └─> core_effective_supply (join con lk_spots, scores, flags)
          └─> gold_lk_effective_supply
              ├─> lk_effective_supply_to_s3 + lk_effective_supply_to_geospot
              │
              └─> ml_build_universes (split Rent / Sale)
                  │
                  ├─> ml_feature_engineering (coverage-based encoding, log1p)
                  │   └─> ml_train (CatBoost con fallback)
                  │       ├─> ml_score ─> gold_propensity_scores ─> publish (S3 + PG)
                  │       └─> ml_drivers ─> gold_drivers ─> publish (S3 + PG)
                  │
                  ├─> ml_category_effects ─> gold_category_effects ─> publish (S3 + PG)
                  │
                  └─> ml_rules (usa ml_drivers + gold_lk_effective_supply para percentiles)
                      └─> gold_rules ─> publish (S3 + PG)
```

Dependencias clave:
- `ml_category_effects` depende de `ml_build_universes` (datos originales sin encoding)
- `ml_rules` depende de `ml_build_universes` + `ml_drivers` + `gold_lk_effective_supply`
  (drivers para orden de importancia, gold para calcular `avg_score_percentile`)
- `ml_score` y `ml_drivers` dependen de `ml_train` + `ml_feature_engineering`

---

## 11) Checklist de entrega a negocio

- [x] Score por spot: `p_top_p80` (Fase 1)
- [x] Top drivers por universo (Fase 1)
- [x] Top categorias por lift con soporte (Fase 2)
- [x] Top reglas combinadas con P(top) y soporte (Fase 3)
- [ ] Nota metodologica: ventana 6m, actualizacion mensual
- [ ] Dashboard Metabase con filtros interactivos

---

## 12) Glosario

- **Top P80**: spots con total_score >= percentil 80 dentro del universo (Rent o Sale)
- **Lift**: p_top_categoria / p_top_global (>1 = mejor que promedio)
- **Stress test geografico**: test set con municipios no vistos en train para medir generalizacion
- **Geographic encoding (coverage-based per state)**: para cada estado, retener los municipios/corredores
  mas frecuentes hasta cubrir 90% de los spots, con un piso de 3 por estado y un soporte minimo
  global de 10 spots. Los no retenidos = `__OTHER__`, los nulls = `__NULL__`. Columnas: `municipality_enc`, `corridor_enc`
- **Propensity score**: P(is_top_p80 = 1 | atributos del spot), calculado por CatBoost
- **Category effect**: efecto marginal de cada valor categorico (o bin numerico) sobre el target
- **Hierarchical Drill-Down**: algoritmo personalizado de extraccion de reglas que recorre las
  features en orden de importancia de CatBoost, comenzando siempre por estado, evaluando
  municipio y corredor de forma independiente (cada uno genera sus propias reglas si aporta lift),
  y podando features que no aporten al menos un 5% de lift incremental
- **RULES_MIN_LIFT**: umbral minimo de lift (1.5x) para conservar una regla; reglas por debajo
  se consideran no accionables y se descartan
- **RULES_MIN_LIFT_INCREMENT**: umbral minimo de mejora incremental (5%) para que una feature
  sea incluida en una regla; configurado en `constants.py`
- **Ventana deslizante (sliding window)**: tecnica de evaluacion de rangos numericos que
  prueba todas las 171 combinaciones posibles de (inicio, fin) sobre 19 percentiles
  (P5 a P95 con step de 5pp) del subgrupo de spots, y selecciona la que maximiza p_top.
  Reemplaza la evaluacion por quintiles fijos (5 bins de 20%) para obtener rangos mas precisos
- **Rango discriminante (sin asterisco)**: rango numerico cuya ventana optima aporta >= 5%
  de lift incremental sobre el candidato. Indica que filtrar a ese rango DE VERDAD mejora
  la probabilidad de ser Top P80
- **Rango no discriminante (con asterisco *)**: rango P5-P95 observado del subgrupo, mostrado
  como contexto informativo. El precio/area no es un diferenciador para esta combinacion.
  El rango se calcula sobre el subgrupo filtrado (no global), por lo que dos reglas con
  distinto corredor pueden mostrar rangos diferentes aunque ambos lleven `*`
- **avg_score_percentile**: percentil promedio de `total_score` de los spots que cumplen una
  regla. Se calcula sobre la gold del universo (0-100). Indica la posicion promedio de estos
  spots en el ranking general. Ejemplo: avg_score_percentile=85 significa que los spots de esta
  regla estan, en promedio, en el percentil 85 del universo
- **strength**: categorical label for rule strength based on lift:
  - "Very High": lift >= 3.0x (spots in this rule are at least 3x more likely to be Top P80)
  - "High": lift >= 2.0x
  - "Moderate": lift >= 1.5x
- **price_sqm_pctl_range / area_pctl_range**: rango de percentiles del subgrupo que fue evaluado
  (ej: "P25-P70"). Si es discriminante, indica la ventana optima; si no, es "P5-P95" (el rango completo observado)
- **price_sqm_is_discriminant_id / area_is_discriminant_id**: flag 0/1 que indica si el rango numerico
  aporta >= 5% de lift incremental (1 = discriminante, 0 = solo contexto informativo)
- **Fusion Marascuilo**: tecnica estadistica que identifica reglas granulares cuyas proporciones
  de Top P80 no son significativamente diferentes (alpha=0.05) y las fusiona en una sola regla
  mas robusta, evitando fragmentacion artificial

---

## 13) Historial de cambios relevantes

| Fecha | Cambio |
|---|---|
| Feb 2026 | Fase 1 completada: propensity scores + drivers |
| Feb 2026 | Fase 2 completada: category effects con bins numericos y nombres legibles |
| Feb 2026 | Fase 3 v1: reglas con DecisionTreeClassifier |
| Feb 2026 | Fase 3 v2: cambio a target encoding ordinal + geographic encoding para reglas |
| Feb 2026 | Fase 3 v3: reemplazo total por Hierarchical Drill-Down (algoritmo actual) |
| Feb 2026 | Eliminacion de `price_total` / `log_price_total` del modelo (redundante con price_sqm + area) |
| Feb 2026 | Eliminacion de `modalidad` de reglas (redundante con market_universe) |
| Feb 2026 | Fusion Marascuilo: reglas granulares indistinguibles se fusionan automaticamente |
| Feb 2026 | Rangos con P95 como tope superior (en lugar de `+` abierto) |
| Feb 2026 | Filtro de lift minimo (1.5x) para eliminar reglas debiles |
| Feb 2026 | Nuevas columnas: `avg_score_percentile` y `strength` para facilitar interpretacion |
| Feb 2026 | `ml_rules` ahora depende tambien de `gold_lk_effective_supply` para calcular percentiles |
| Feb 2026 | Reemplazo de quintiles fijos por ventana deslizante (sliding window, 171 combinaciones) para rangos de precio/area en reglas |
| Feb 2026 | Documentacion extensiva del algoritmo Hierarchical Drill-Down (seccion 8 reescrita completa) |
| Feb 2026 | Renombre de columnas de reglas a ingles (spot_state, spot_type, etc.) + 4 nuevos campos (pctl_range, is_discriminant_id) |
| Feb 2026 | Strength labels cambiados a ingles: Very High, High, Moderate |
| Feb 2026 | Nueva tabla `lk_effective_supply_model_metrics` para persistir metricas de evaluacion del modelo (AUC, PR-AUC, Brier, gap, gate) |
| Feb 2026 | Evaluacion independiente de municipio y corredor en reglas (antes XOR, ahora ambos si aportan lift) |
| Feb 2026 | Eliminacion del pre-filtro de estados por lift: todos los estados con n >= 30 se evaluan |
| Feb 2026 | Geographic encoding adaptativo por estado (coverage 90% + floor 3 + min_support 10) reemplaza K=100 global |
| Feb 2026 | Renombre de columnas y constantes: `*_k100` -> `*_enc`, `TOPK_*` -> `GEO_ENC_*` |
