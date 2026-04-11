-- q_browse_first_branches.sql
-- Devuelve las "primeras ramas" del árbol de browse pages: el primer
-- segmento de cada url_path registrada en la ventana dinámica.
--
-- Resultado típico: 8 filas (bodegas, coworking, locales-comerciales,
-- naves-industriales, oficinas, renta, terrenos, venta).
--
-- Usar para construir dinámicamente la regex de filtrado en BigQuery
-- sin hardcodear valores que pueden cambiar (ej. bodegas y coworking
-- se agregaron después de la ejecución ~200).
--
-- Performance: < 1s

SELECT DISTINCT
    SUBSTRING_INDEX(SUBSTRING(url_path, 2), '/', 1) AS first_branch
FROM seo_indexable_urls
WHERE execution_id BETWEEN
    (
        SELECT MIN(execution_id)
        FROM seo_indexability_executions
        WHERE started_at >= DATE_SUB(
            CAST(DATE_FORMAT(CURDATE(), '%Y-%m-01') AS DATE),
            INTERVAL 6 MONTH
        )
    )
    AND
    (
        SELECT MAX(execution_id)
        FROM seo_indexability_executions
        WHERE started_at >= DATE_SUB(
            CAST(DATE_FORMAT(CURDATE(), '%Y-%m-01') AS DATE),
            INTERVAL 6 MONTH
        )
    )
ORDER BY first_branch;
