# Plan completo: ML para predecir Supply “Efectivo” (Top P80) por universo (Rent vs Sale)

> Objetivo: construir **dos pipelines independientes** (Rent y Sale) que produzcan:
> 1) un **modelo predictivo** que estime `P(is_top_p80 = 1 | características del spot)`,
> 2) un ranking de **drivers** (importancia de variables),
> 3) un set de **reglas / combinaciones** accionables para negocio,
> 4) un esquema de **validación automática anti-memorización geográfica**,
> 5) outputs unificados al final en PostgreSQL con una columna `market_universe`.

---

## 0) Principios (no negociables)

1) **Separación por universo**: todo el modelado se hace por separado para:
   - `Rent` (spots modalidad Rent o Rent&Sale, usando precios de renta)
   - `Sale` (spots modalidad Sale o Rent&Sale, usando precios de venta)

2) **Evitar tautologías / leakage**: el modelo NO debe usar como features:
   - `view_count`, `contact_count`, `project_count`
   - `view_score`, `contact_score`, `project_score`, `total_score`

   Estos campos definen/están altamente correlacionados con el target, por lo que deben quedar fuera.

3) **Ventana móvil**: cada corrida usa los **últimos 6 meses cerrados**. Se recalcula mensualmente.

4) **Outputs BI-friendly**: al final se unifican outputs de Rent y Sale con `market_universe` para dashboards.

---

## 1) Definiciones de datasets

### 1.1 Fuente de datos
Tabla/DF de trabajo (ya existe): `lk_effective_supply` con:
- segmentadores: `spot_sector(_id)`, `spot_type(_id)`, `spot_modality(_id)`, `spot_state(_id)`, `spot_municipality(_id)`, `spot_corridor(_id)`
- numéricos: `spot_area_in_sqm`, precios totales y por m² para rent y sale
- *NO usar como features*: conteos y scores

### 1.2 Construcción de universos

#### Universo Rent
Filtrar spots con `spot_modality` ∈ {`Rent`, `Rent & Sale`}.
- Precio total: `spot_price_total_mxn_rent`
- Precio m²: `spot_price_sqm_mxn_rent`

#### Universo Sale
Filtrar spots con `spot_modality` ∈ {`Sale`, `Rent & Sale`}.
- Precio total: `spot_price_total_mxn_sale`
- Precio m²: `spot_price_sqm_mxn_sale`

> Nota: los spots `Rent & Sale` participan en ambos universos.

---

## 2) Target (Top P80 por universo)

Para cada universo:
1) calcular `p80 = quantile(total_score, 0.80, method='nearest')` **dentro del universo**.
2) target binario:
   - `is_top_p80 = 1` si `total_score >= p80`
   - `is_top_p80 = 0` en otro caso

Guardar además:
- `p80_threshold` por universo y por corrida (para auditoría)

---

## 3) Features (solo atributos del spot)

### 3.1 Categóricas (recomendadas)
Usar IDs (más estable) pero tratarlos como **categoría**, no como continuo.

- `spot_sector_id`
- `spot_type_id`
- `spot_modality_id`  (en Rent: {Rent, Rent&Sale}; en Sale: {Sale, Rent&Sale})
- `spot_state_id`
- `spot_municipality_id`
- `spot_corridor_id`

### 3.2 Numéricas (por universo)

**Rent**:
- `price_total = spot_price_total_mxn_rent`
- `price_sqm   = spot_price_sqm_mxn_rent`

**Sale**:
- `price_total = spot_price_total_mxn_sale`
- `price_sqm   = spot_price_sqm_mxn_sale`

Ambos:
- `area_sqm = spot_area_in_sqm`

### 3.3 Transformaciones numéricas
Aplicar para estabilidad y colas largas:
- `log_price_total = log1p(price_total)`
- `log_price_sqm   = log1p(price_sqm)`
- `log_area_sqm    = log1p(area_sqm)`

Opcional (si hay outliers extremos): winsorizar en p1–p99 ANTES de log1p.

---

## 4) Control de cardinalidad: Top-K=100 + OTHER (municipio y corredor)

Objetivo: evitar memorización por alta cardinalidad y cola larga.

### 4.1 Regla Top-K (por universo y por corrida)

Para `spot_municipality_id`:
- calcular frecuencia por categoría en el universo
- obtener lista `top_muni_ids` = Top 100 por frecuencia
- definir:
  - `municipality_k100 = (spot_municipality_id)` si está en `top_muni_ids`
  - `municipality_k100 = '__OTHER__'` si no
  - `municipality_k100 = '__NULL__'` si es null

Para `spot_corridor_id`:
- misma lógica → `corridor_k100`

> Recomendación: convertir a string para el modelo, para evitar que se trate como numérico.

Ejemplo de valores:
- `'123'`, `'456'`, ..., `'__OTHER__'`, `'__NULL__'`

### 4.2 Nota
- `top_muni_ids` y `top_corr_ids` cambian mensualmente con la ventana.
- Guardarlos como artefacto/metadata ayuda a reproducibilidad.

---

## 5) Split y validación

Como no hay marcas de mes dentro de la ventana agregada, se usa validación no temporal:

### 5.1 Split principal (estratificado)
- `train`: 70%
- `val`: 15%
- `test`: 15%
Estratificar por `is_top_p80`.

### 5.2 Stress test anti-memorización (group holdout)
Construir un segundo test set basado en grupos completos:
- Elegir aleatoriamente (seed fijo) el 20% de valores únicos de `municipality_k100`
  - excluir `__OTHER__` y `__NULL__` de la selección
- `test_group` = filas cuyo `municipality_k100` está en ese subconjunto
- `train_group` = el resto

Evaluar el mismo modelo entrenado en `train_group` y medir performance en `test_group`.

### 5.3 Métricas
Calcular al menos:
- AUC ROC
- PR-AUC (por desbalance)
- Brier score (calibración)

Definir:
- `gap_auc = auc_random_test - auc_group_test`

### 5.4 Gate automático (QA)
Umbrales iniciales recomendados (ajustables):
- `auc_group_test >= 0.60`
- `gap_auc <= 0.05`

Si falla → activar fallback (sección 6).

---

## 6) Entrenamiento y fallback automático

### 6.1 Modelo principal recomendado
**CatBoostClassifier** (tabular + categóricas alta cardinalidad).

Inputs:
- Categóricas: `sector_id`, `type_id`, `modality_id`, `state_id`, `municipality_k100`, `corridor_k100`
- Numéricas: `log_price_total`, `log_price_sqm`, `log_area_sqm`

Parámetros iniciales (sugeridos, ajustar tras 1–2 corridas):
- `loss_function='Logloss'`
- `eval_metric='AUC'`
- `iterations=2000` (con early stopping)
- `learning_rate=0.05`
- `depth=6`
- `l2_leaf_reg=6`
- `min_data_in_leaf=50`
- `random_seed=<seed fijo>`
- `od_type='Iter'`, `od_wait=100` (early stopping)
- `verbose=False`

> Nota: El dataset no es enorme (~40k), entrenar 2 modelos/mes es razonable.

### 6.2 Fallback (si falla el gate)

**Fallback 1 (más suave):**
- Reentrenar aumentando regularización:
  - `depth=4`
  - `l2_leaf_reg=12`
  - `min_data_in_leaf=100`
- Mantener municipio/corridor K100.

**Fallback 2 (si sigue fallando):**
- Dropear `municipality_k100` del set de features.

**Fallback 3 (si sigue fallando):**
- Dropear también `corridor_k100`.

Publicar el primer modelo que pase el gate.

Registrar en metadata:
- `model_variant`: {`base`, `reg`, `no_muni`, `no_muni_no_corr`}
- métricas en random test y group test

---

## 7) Probabilidades y calibración

### 7.1 Output principal
Para cada spot en el universo:
- `p_top_p80` = probabilidad predicha de `is_top_p80=1`.

### 7.2 Calibración (opcional pero recomendado)
Si notas que las probabilidades están “infladas” o “aplastadas”:
- aplicar calibración Platt (logistic) sobre `val`.

Guardar además:
- `p_top_p80_calibrated` (si aplica)

---

## 8) Drivers (importancia) – barato y estable

### 8.1 Permutation importance (recomendado)
Calcular permutation importance sobre `val` o `test`:
- lista de features
- `importance` normalizado 0–1
- `rank`

**Nota:** es más estable que la importancia interna del boosting.

### 8.2 Estabilidad
Opcional (si quieres robustez): repetir permutation importance 3 veces con seeds distintas y promediar.

---

## 9) Categorías que influyen (tablas accionables)

Para cada feature categórica (por universo):
- calcular por categoría:
  - `support_n`
  - `p_top = mean(is_top_p80)`
  - `lift = p_top / p_top_global`

Filtrar por soporte mínimo (ej. >= 50).

> Importante: hacer esto **con las categorías originales** (IDs) y luego join a diccionario para nombres.

Outputs:
- Top categorías por lift y por p_top.
- Bottom categorías (anti-ejemplos), con soporte suficiente.

---

## 10) Reglas / combinaciones (para negocio)

Objetivo: frases tipo “si traes supply con X, prob ~Y”.

### 10.1 Discretización (bins) de numéricas
Crear bins por cuantiles (por universo):
- `price_sqm_bin`: quintiles (5 bins) o deciles (10 bins)
- `area_bin`: quintiles
- (opcional) `price_total_bin`

### 10.2 Árbol pequeño para reglas
Entrenar un DecisionTreeClassifier (o similar) con:
- profundidad 3–5
- `min_samples_leaf` alto (ej. 200)
- features: categóricas (ya K100) + bins numéricos

Extraer rutas hoja como reglas:
- `rule_text`
- `support_n`
- `p_top` en esa hoja
- `lift` vs global

Guardar top reglas por:
- mayor `p_top` con soporte mínimo
- y/o mayor `lift` con soporte mínimo

---

## 11) Artefactos y tablas de salida (para PostgreSQL)

### 11.1 Tablas sugeridas (por simplicidad de BI)

#### A) `ml_supply_propensity_scores`
Grano: spot-universe.
Campos mínimos:
- `spot_id`
- `market_universe` ('Rent' | 'Sale')
- `p_top_p80` (y/o calibrada)
- `model_version` (ej. YYYYMM)
- `window_end_date`
- `window_months` (=6)
- `model_variant`

#### B) `ml_supply_drivers`
Grano: universe-feature.
- `market_universe`
- `feature_name`
- `importance`
- `rank`
- `model_version`, `window_end_date`, `model_variant`

#### C) `ml_supply_category_effects`
Grano: universe-feature-category.
- `market_universe`
- `feature_name`
- `category_id`
- `category_name` (via join diccionario)
- `support_n`
- `p_top`
- `lift`
- `model_version`, `window_end_date`

#### D) `ml_supply_rules`
Grano: universe-rule.
- `market_universe`
- `rule_id`
- `rule_text`
- `support_n`
- `p_top`
- `lift`
- `model_version`, `window_end_date`

### 11.2 Metadata / auditoría (muy recomendado)
Guardar por universo:
- `p80_threshold`
- `top_k=100` (muni/corr)
- métricas: AUC/PR-AUC/Brier en random y group
- `gap_auc`

---

## 12) Implementación en Dagster + Polars (guía paso a paso)

> Nota: esta sección está escrita para ser “IA-friendly” y servir de checklist.

### 12.1 Assets sugeridos (por universo)

1) `build_universe_df(universe)`
   - input: DF base de la ventana 6m
   - output: DF filtrado Rent o Sale con columnas unificadas `price_total`, `price_sqm`, `area_sqm`.

2) `add_targets(universe_df)`
   - calcula p80 dentro del universo
   - agrega `is_top_p80`

3) `add_topk_geo(universe_df)`
   - calcula Top100 municipio/corridor por frecuencia
   - agrega `municipality_k100`, `corridor_k100` (strings)
   - guarda listas TopK como metadata

4) `build_features_matrix(universe_df)`
   - crea `log_*`
   - selecciona features finales (sin leakage)

5) `split_data(features_df)`
   - split estratificado 70/15/15
   - además split group por municipality_k100 para stress test
   - usar seed fijo y guardarlo

6) `train_model_with_gate(splits)`
   - entrena CatBoost base
   - evalúa métricas random test + group test
   - si falla: fallback 1/2/3
   - devuelve modelo final + metadata

7) `score_spots(model, features_df)`
   - genera `p_top_p80`

8) `compute_drivers(model, splits)`
   - permutation importance

9) `compute_category_effects(universe_df)`
   - lift por categoría (con soporte mínimo)

10) `extract_rules(universe_df)`
   - bins + árbol pequeño + extracción de reglas

11) `publish_outputs_to_pg(all_outputs)`
   - unificar Rent/Sale agregando `market_universe`
   - escribir tablas a PostgreSQL

### 12.2 Seeds y reproducibilidad
- usar un `seed` fijo por corrida (por ejemplo derivado de `window_end_date`)
- loggear:
  - seed
  - top_k lists
  - thresholds y métricas

---

## 13) Precauciones y notas

1) **No leakage**: nunca entrenar usando conteos/scores.
2) **TopK puede cambiar** mes a mes; está bien, pero guardarlo como metadata.
3) **Group stress test** no debe usar `__OTHER__` para formar grupos (porque mezcla demasiados).
4) **Soporte mínimo** para reportes: no reportar categorías con `support_n` muy bajo.
5) **Probabilidades**: si negocio las tomará literalmente, calibrar.
6) **Monitoreo mensual**: guardar performance y comparar vs meses anteriores.

---

## 14) Checklist de entrega a negocio

- [ ] “Top drivers” por universo (tabla)
- [ ] Top categorías por lift (sector/type/state/corridor/municipio) con soporte
- [ ] Top reglas (combinaciones) con `P(top)` y `support_n`
- [ ] Score por spot: `p_top_p80`
- [ ] Nota metodológica: ventana 6m, actualización mensual

---

## 15) Glosario breve

- **Top P80**: spots con `total_score` en el percentil 80 o superior dentro del universo.
- **Lift**: `p_top_categoria / p_top_global`.
- **Stress test geográfico**: evaluación donde el test tiene municipios no vistos en train.
- **Top-K encoding**: conservar solo las 100 categorías más frecuentes; el resto es `__OTHER__`.

