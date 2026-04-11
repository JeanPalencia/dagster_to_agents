SELECT
  DATE(timeframe_timestamp) AS fecha,
  spot_sector,
  spot_modality,
  SUM(spot_count)           AS spots_activos
FROM dm_spot_historical_metrics
WHERE timeframe_id  = 1
  AND spot_status   = 'Public'
  AND spot_sector   IS NOT NULL
  and date(timeframe_timestamp)>= '2025-09-01'
GROUP BY DATE(timeframe_timestamp), spot_sector, spot_modality
ORDER BY fecha DESC, spots_activos DESC
