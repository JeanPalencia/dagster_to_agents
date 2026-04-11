# Glosario — Dashboard Effective Marketable Supply

---

## Definición de campos (conteos y scores)

### Conteos
- **view_count**: Número de vistas web del spot (visitas a la página de detalle). Señal de interés.
- **contact_count**: Número de intentos de contacto asociados al spot (clic en WhatsApp, formulario, llamada, etc.). Señal de intención más fuerte que una vista.
- **project_count**: Número de veces que el spot fue asignado a un proyecto (incluido formalmente en un flujo de lead/proyecto). Es la señal de mayor valor porque refleja demanda más calificada.

### Scores parciales (0 a 1)
Cada score parcial es una versión normalizada robusta de su conteo correspondiente, diseñada para:
- comparar spots en una escala común 0–1,
- reducir la influencia de outliers,
- preservar interpretabilidad.

- **view_score**: score derivado de view_count
- **contact_score**: score derivado de contact_count
- **project_score**: score derivado de project_count

Interpretación:
- 0 = sin señal (conteo = 0) o actividad muy baja.
- Valores cercanos a 1 = nivel alto relativo a la distribución general.

Nota: un score de 1 no significa el máximo absoluto; valores por encima del umbral robusto se saturan (clipping) a 1 por diseño.

### Total score (0 a 1)
Indicador compuesto de relevancia que combina los tres scores parciales con pesos de prioridad de negocio:

**total_score = 0.7 × project_score + 0.2 × contact_score + 0.1 × view_score**

Interpretación:
- Entre dos spots con vistas similares, el que tenga más contactos y especialmente más asignaciones a proyectos tendrá un total_score más alto.
- El total_score se usa para rankear spots y seleccionar la oferta más relevante según la demanda observada.

### Filtro Spot Top P80
Identifica spots con total_score ≥ P80 (percentil 80), es decir, el Top 20% por score, incluyendo empates en el corte.

Existen **tres variantes** de este flag:

- **is_top_p80** (general): Top 20% calculado sobre todos los spots activos.
- **is_top_p80_rent**: Top 20% calculado solo sobre el universo de Renta (spots con modalidad Rent o Rent & Sale).
- **is_top_p80_sale**: Top 20% calculado solo sobre el universo de Venta (spots con modalidad Sale o Rent & Sale).

Los tops por universo son necesarios porque los spots de renta son mucho más frecuentes que los de venta, lo que sesga el top general a favor de renta. Un spot puede ser Top en venta dentro de su universo pero no aparecer en el top general. Los tops separados garantizan una representación justa de cada mercado. Spots con modalidad "Rent & Sale" participan en ambos universos.

Para cada variante:
- **Yes**: el spot pertenece al top 20%.
- **No**: el spot está por debajo del umbral P80.

Estos flags se pre-calculan en el pipeline para evitar problemas de redondeo al consultar.

Nota: los complejos (spot_type_id = 2) están excluidos de esta tabla — solo se incluyen spots individuales (spot_type_id = 1) y subespacios (spot_type_id = 3).

---

## Inferencia (modelo ML)

### ¿Qué hace el modelo?
Un modelo CatBoost predice, para cada spot, la probabilidad de pertenecer al Top P80 **basándose solo en sus atributos** (estado, sector, tipo, zona, precio, área), sin mirar eventos históricos. Esto permite entender qué oferta es más relevante, efectiva o interesante para la demanda.

### Tablas de inferencia

| Tabla | Pregunta que responde |
|---|---|
| **propensity_scores** | ¿Qué probabilidad tiene cada spot de ser Top P80? |
| **drivers** | ¿Qué atributos importan más para ser Top P80? |
| **category_effects** | ¿Qué valores específicos favorecen o desfavorecen la demanda? |
| **rules** | ¿Qué combinaciones concretas tienen mayor probabilidad? |
| **model_metrics** | ¿Pasó el modelo su control de calidad? |

### Propensity scores (probabilidad por spot)
- **p_top_p80** (0 a 1): probabilidad predicha de ser Top P80.
- Se calcula por **market_universe** (Rent o Sale) de forma independiente.
- Un spot con p_top_p80 = 0.70 tiene 70% de probabilidad predicha de ser Top.
- Spots con p_top_p80 alto pero is_top_p80 = No son **oportunidades ocultas**.

### Drivers (importancia de variables)
- Ranking de qué atributos del spot determinan más si será Top P80.
- **importance** (0 a 1): peso relativo (1.0 = la variable más importante).
- Se calcula por universo; el ranking puede diferir entre Rent y Sale.

### Category effects (efecto por valor)
- Para cada variable y cada valor (ej: estado = "Jalisco"), muestra la tasa de Top P80 y el **lift**.
- **lift > 1**: ese valor favorece ser Top. **lift < 1**: lo desfavorece.
- Incluye también rangos de precio y área en quintiles.

### Rules (combinaciones ganadoras)
- Combinaciones de estado + sector + tipo + zona + precio + área con alta probabilidad de ser Top P80.
- Cada regla incluye **Municipio o Corredor** (nunca ambos en la misma regla, pero un estado puede tener reglas con cada uno).
- **p_top**: tasa observada de Top P80 entre los spots que cumplen la regla.
- **lift**: cuántas veces más probable es ser Top respecto al promedio (siempre ≥ 1.5x).
- **avg_score_percentile**: posición promedio en el ranking (ej: 85 = top 15%).
- **strength**: Very High (≥ 3x), High (≥ 2x), Moderate (≥ 1.5x).
- Rangos de precio y área con `*` = solo contexto informativo (no discriminante).

### Model metrics (control de calidad)
- **gate_passed**: ¿Pasó el modelo ambas condiciones de calidad?
  - `auc_roc_group ≥ 0.60` (discrimina bien en zonas no vistas)
  - `gap_auc ≤ 0.05` (no hay overfitting geográfico)
- Si gate_passed = No, el modelo usó una variante de fallback más conservadora.
- Valores típicos: AUC ~0.82–0.86, PR-AUC ~0.57–0.60, Brier ~0.11–0.12.
