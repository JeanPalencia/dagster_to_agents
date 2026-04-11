# Ad Hoc

Mini proyectos y entregas puntuales (estáticas) que aprovechan las conexiones y utilidades del `lakehouse-sdk` (SSM, BigQuery, MySQL, S3, etc.) pero **no** requieren un pipeline Dagster (sin cron, sin jobs, sin schedules).

## Cuándo usar este directorio

- Peticiones de datos ad hoc
- Análisis exploratorios
- Entregas únicas de reportes
- Queries puntuales contra BigQuery, MySQL, PostgreSQL, etc.

## Convención de nombres

Cada petición va en su propio subdirectorio autocontenido con el formato:

```
nombre_descriptivo_YYYY-MM-DD
```

Donde `YYYY-MM-DD` es la fecha del día de la petición. Ejemplo:

```
gsc_spots_performance_2026-03-26
```

## Estructura de cada proyecto

Cada subdirectorio puede incluir:

- Scripts `.py` para ejecutar queries o transformaciones
- Archivos `.sql` con las queries utilizadas
- Resultados: `.csv`, `.png`, `.xlsx`
- Un `README.md` propio si el proyecto lo amerita

## Lo que NO va aquí

Nada que deba ejecutarse de forma automática o recurrente. Eso pertenece a `dagster-pipeline/`.
