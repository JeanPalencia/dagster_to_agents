# Handoff — Supply & Growth

Jean Palenca \-\> Luis Muñiz

| Fecha de inicio: 19 de marzo 2026 |
| :---- |
| Fecha límite: 27 de marzo 2026 |
|  |

Algunos proyectos que vi con Devin

---

## Proyecto 1: ML Effective Supply Pipeline (Dagster)

Pipeline mensual de ML que predice el potencial de demanda de listings inmobiliarios usando modelos CatBoost separados para Renta y Venta.

| Campo | Detalle |
| :---- | :---- |
| **Repo** | Spot2HQ/dagster |
| **Job** | effective\_supply\_monthly\_job |
| **Schedule** | effective\_supply\_monthly\_schedule (mensual) |

### Fuentes de datos

| Fuente | Descripción | Origen |
| :---- | :---- | :---- |
| gold\_lk\_effective\_supply | Tabla principal de spots comercializables | Gold layer |
| stg\_bq\_spot\_contact\_view\_event\_counts | Eventos de contacto/vista | BigQuery |
| stg\_gs\_bt\_lds\_spot\_added | Spots añadidos | GeoSpot |
| stg\_gs\_lk\_spots | Catálogo de spots | GeoSpot |

### Assets del pipeline (orden de ejecución)

| \# | Asset | Qué hace |
| :---- | :---- | :---- |
| 1 | core\_effective\_supply\_events | Unión de eventos de distintas fuentes |
| 2 | core\_effective\_supply | Pivotea tipos de evento, calcula scores de relevancia (log \+ p95 \+ clipping) |
| 3 | ml\_build\_universes | Separa en universos Renta y Venta |
| 4 | ml\_feature\_engineering | Encoding geográfico por cobertura, transformaciones log1p |
| 5 | ml\_train | Entrena modelos CatBoost por universo |
| 6 | ml\_score | Genera propensity scores |
| 7 | ml\_drivers | Importancia de features |
| 8 | ml\_category\_effects | Efectos y lift por categoría |
| 9 | ml\_rules | Reglas jerárquicas de drill-down |

### Tablas de salida (PostgreSQL vía GeoSpot API)

| Tabla | Contenido |
| :---- | :---- |
| lk\_effective\_supply\_propensity\_scores | Probabilidad p\_top\_p80 por spot |
| lk\_effective\_supply\_drivers | Ranking de features que más contribuyen |
| lk\_effective\_supply\_category\_effects | Análisis de lift por categoría |
| lk\_effective\_supply\_rules | Combinaciones de atributos de alta probabilidad |
| lk\_effective\_supply\_model\_metrics | Métricas de evaluación del modelo |
| lk\_effective\_supply | Tabla base (también publicada a S3) |

### Consumidores downstream

* Dashboard en Metabase con filtros interactivos

* **CRM Prioritization** — ranking de propiedades para outreach del equipo de ventas

* **Portfolio Scoring** — identificación de administradoras de alto potencial

### Sesión KT

| Fecha |  |
| :---- | :---- |
| **Grabación** |  |
| **Emisor entregó** | ☐ |
| **Receptor comprende** | ☐ |
| **Notas** |  |

---

## Proyecto 2: Funnel Period-to-Date Pipeline (Dagster)

Pipeline que calcula métricas de performance del funnel en tiempo real y genera forecasts usando curvas de supervivencia Kaplan-Meier.

| Campo | Detalle |
| :---- | :---- |
| **Repo** | Spot2HQ/dagster |
| **Job** | funnel\_ptd\_job |
| **Trigger** | funnel\_ptd\_after\_gold\_sensor (se dispara al terminar gold jobs upstream) |

### Tabla de salida

| Tabla | Destino |
| :---- | :---- |
| rpt\_funnel\_ptd | GeoSpot PostgreSQL |

### Consumidores downstream

* Monitoreo de performance del funnel de ventas

* Proyecciones y forecasting de negocio

### Sesión KT

| Fecha |  |
| :---- | :---- |
| **Grabación** |  |
| **Emisor entregó** | ☐ |
| **Receptor comprende** | ☐ |
| **Notas** |  |

---

## Proyecto 3: Historic Population & Growth (Data Service)

Funciones que calculan porcentajes de crecimiento poblacional entre periodos (2000-2020, 2010-2020) para análisis de mercado.

| Campo | Detalle |
| :---- | :---- |
| **Repo** | Spot2HQ/data-service |
| **Archivo** | geo\_spot/data/layers/historic\_population.py |

### Funciones clave

| Función | Qué hace |
| :---- | :---- |
| process\_historic\_population | Calcula crecimiento poblacional entre años |
| process\_historic\_population\_v3 | Formato estructurado para charting con población total y % |
| get\_data\_insights | Recupera datos de insights |
| get\_filter\_data | Filtra datos por criterios |
| get\_aggregations\_from\_data | Agrega datos |
| plus\_is\_what\_percent\_of | Cálculo de porcentajes (utilidad) |

### Endpoint relacionado

DagsterView en geo\_spot/datalakehouse/views.py — endpoint POST para cargar datos de S3 a PostgreSQL.

### Sesión KT

| Fecha |  |
| :---- | :---- |
| **Grabación** |  |
| **Emisor entregó** | ☐ |
| **Receptor comprende** | ☐ |
| **Notas** |  |

---

## Accesos requeridos para el receptor

| Recurso | Acceso otorgado |
| :---- | :---- |
| Repo Spot2HQ/dagster | ☐ |
| Repo Spot2HQ/data-service | ☐ |
| BigQuery (lectura) | ☐ |
| GeoSpot PostgreSQL | ☐ |
| S3 buckets de datos | ☐ |
| Dagster Cloud / UI | ☐ |
| Metabase dashboards | ☐ |
| 1Password vault | ☐ |
| Canales de Slack del equipo | ☐ |

---

## Confirmación Final

### Emisor

☐ Confirmo que entregué toda la información, accesos y conocimiento de los proyectos de Supply y Growth.

| Nombre | Fecha | Firma |
| :---- | :---- | :---- |
|  |  |  |

### Receptor

☐ Confirmo que revisé todo, tengo los accesos y puedo continuar sin bloqueos.

| Nombre | Fecha | Firma |
| :---- | :---- | :---- |
|  |  |  |

### Notas adicionales