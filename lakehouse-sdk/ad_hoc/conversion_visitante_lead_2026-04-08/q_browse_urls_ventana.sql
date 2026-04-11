-- q_browse_urls_ventana.sql
-- URLs únicas de browse pages en ventana dinámica: 6 meses cerrados + mes actual.
-- Toma los datos de la ejecución más reciente en que apareció cada URL.
--
-- Performance: ~5 min en analysis replica (sin índices en seo_indexable_urls).
--   Si se necesita más rápido, usar el wrapper Python de dos pasos.

SELECT
    i.url_path,
    i.sector_slug,
    i.operation_slug,
    i.state_slug,
    i.corridor_slug,
    i.municipality_slug,
    i.settlement_slug,
    i.levelable_type,
    i.spots_count_at_evaluation,
    i.median_price_by_square_space_mxn,
    i.evaluation_date
FROM seo_indexable_urls i
INNER JOIN (
    SELECT url_path, MAX(execution_id) AS latest_exec_id
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
    GROUP BY url_path
) latest
  ON  i.url_path      = latest.url_path
  AND i.execution_id   = latest.latest_exec_id;
