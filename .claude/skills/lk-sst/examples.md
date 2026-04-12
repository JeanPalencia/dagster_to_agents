# Metric Examples — SST Metrics

## Example 1: Daily public inventory by sector and modality

**Business question**: "How many public spots are active each day, broken down by sector and modality?"

This is the **validation metric** — its results must match `dm_spot_historical_metrics`.

### SQL (Metabase / BI)

```sql
WITH date_series AS (
  SELECT generate_series('2025-09-01'::date, CURRENT_DATE, '1 day')::date AS fecha
)
SELECT
  ds.fecha,
  s.spot_sector,
  s.spot_modality,
  COUNT(DISTINCT h.spot_id) AS spots_activos
FROM date_series ds
JOIN lk_spot_status_history h
  ON h.source_sst_created_at::date <= ds.fecha
  AND (h.next_source_sst_created_at::date >= ds.fecha
       OR h.next_source_sst_created_at IS NULL)
JOIN lk_spots s ON h.spot_id = s.spot_id
WHERE h.spot_status_id = 1              -- Public
  AND s.spot_sector_id IS NOT NULL
  AND h.spot_hard_deleted_id = 0         -- No
GROUP BY ds.fecha, s.spot_sector, s.spot_modality
ORDER BY ds.fecha DESC, spots_activos DESC
```

No JOIN to `lk_spot_status_dictionary` needed — filter directly on `h.spot_status_id` (indexed). String columns (`spot_sector`, `spot_modality`) only in SELECT for display.

### Equivalent using dm_spot_historical_metrics (for validation)

```sql
SELECT
  DATE(timeframe_timestamp) AS fecha,
  spot_sector,
  spot_modality,
  SUM(spot_count) AS spots_activos
FROM dm_spot_historical_metrics
WHERE timeframe_id = 1
  AND spot_status = 'Public'
  AND spot_sector IS NOT NULL
  AND date(timeframe_timestamp) >= '2025-09-01'
GROUP BY DATE(timeframe_timestamp), spot_sector, spot_modality
ORDER BY fecha DESC, spots_activos DESC
```

### Python (ad-hoc script)

```python
import pandas as pd
import psycopg2

QUERY = """
WITH date_series AS (
  SELECT generate_series('2025-09-01'::date, CURRENT_DATE, '1 day')::date AS fecha
)
SELECT
  ds.fecha,
  s.spot_sector,
  s.spot_modality,
  COUNT(DISTINCT h.spot_id) AS spots_activos
FROM date_series ds
JOIN lk_spot_status_history h
  ON h.source_sst_created_at::date <= ds.fecha
  AND (h.next_source_sst_created_at::date >= ds.fecha
       OR h.next_source_sst_created_at IS NULL)
JOIN lk_spots s ON h.spot_id = s.spot_id
WHERE h.spot_status_id = 1              -- Public
  AND s.spot_sector_id IS NOT NULL
  AND h.spot_hard_deleted_id = 0         -- No
GROUP BY ds.fecha, s.spot_sector, s.spot_modality
ORDER BY ds.fecha DESC, spots_activos DESC
"""

conn = psycopg2.connect(host=..., database="geospot", ...)
df = pd.read_sql(QUERY, conn)
conn.close()
```

---

## Example 2: Monthly transitions from Public to Disabled by sector

**Business question**: "How many spots transitioned from Public to Disabled each month, by sector?"

### SQL

```sql
SELECT
  DATE_TRUNC('month', h.source_sst_created_at)::date AS month,
  s.spot_sector,
  d_reason.spot_status_reason,
  COUNT(DISTINCT h.spot_id) AS transition_count
FROM lk_spot_status_history h
JOIN lk_spots s ON h.spot_id = s.spot_id
LEFT JOIN lk_spot_status_dictionary d_reason
  ON h.spot_status_reason_id = d_reason.spot_status_reason_id
  AND d_reason.spot_status_reason IS NOT NULL
WHERE h.prev_spot_status_id = 1       -- from Public
  AND h.spot_status_id = 3            -- to Disabled
  AND h.source_sst_created_at >= '2025-09-01'
  AND h.spot_hard_deleted_id = 0         -- No
GROUP BY month, s.spot_sector, d_reason.spot_status_reason
ORDER BY month DESC, transition_count DESC
```

### Python

```python
QUERY = """
SELECT
  DATE_TRUNC('month', h.source_sst_created_at)::date AS month,
  s.spot_sector,
  d_reason.spot_status_reason,
  COUNT(DISTINCT h.spot_id) AS transition_count
FROM lk_spot_status_history h
JOIN lk_spots s ON h.spot_id = s.spot_id
LEFT JOIN lk_spot_status_dictionary d_reason
  ON h.spot_status_reason_id = d_reason.spot_status_reason_id
  AND d_reason.spot_status_reason IS NOT NULL
WHERE h.prev_spot_status_id = 1
  AND h.spot_status_id = 3
  AND h.source_sst_created_at >= '2025-09-01'
  AND h.spot_hard_deleted_id = 0         -- No
GROUP BY month, s.spot_sector, d_reason.spot_status_reason
ORDER BY month DESC, transition_count DESC
"""

df = pd.read_sql(QUERY, conn)
```

---

## Example 3: Average days in Public state by spot type

**Business question**: "How long do spots stay Public on average, broken down by type?"

### SQL

```sql
SELECT
  s.spot_type,
  s.spot_sector,
  COUNT(*) AS n_transitions,
  ROUND(AVG(h.minutes_until_next_state / 60.0 / 24.0), 1) AS avg_days,
  ROUND(
    PERCENTILE_CONT(0.5) WITHIN GROUP (
      ORDER BY h.minutes_until_next_state / 60.0 / 24.0
    ), 1
  ) AS median_days,
  ROUND(MIN(h.minutes_until_next_state / 60.0 / 24.0), 1) AS min_days,
  ROUND(MAX(h.minutes_until_next_state / 60.0 / 24.0), 1) AS max_days
FROM lk_spot_status_history h
JOIN lk_spots s ON h.spot_id = s.spot_id
WHERE h.spot_status_id = 1                      -- Public
  AND h.next_spot_status_id IS NOT NULL          -- exclude still-active
  AND h.minutes_until_next_state IS NOT NULL
  AND h.minutes_until_next_state > 0
  AND h.source_sst_created_at >= '2025-01-01'
  AND h.spot_hard_deleted_id = 0         -- No
GROUP BY s.spot_type, s.spot_sector
HAVING COUNT(*) >= 10
ORDER BY avg_days DESC
```

---

## Example 4: Weekly inventory by geographic state

**Business question**: "How many spots (and total sqm) were Public each week, by geographic state?"

### SQL

```sql
WITH week_series AS (
  SELECT generate_series(
    DATE_TRUNC('week', '2025-09-01'::date),
    DATE_TRUNC('week', CURRENT_DATE),
    '1 week'
  )::date AS week_start
),
active_per_week AS (
  SELECT DISTINCT ON (ws.week_start, h.spot_id)
    ws.week_start,
    h.spot_id,
    s.spot_state_id,
    s.spot_state,
    s.spot_area_in_sqm
  FROM week_series ws
  JOIN lk_spot_status_history h
    ON h.source_sst_created_at::date <= ws.week_start
    AND (h.next_source_sst_created_at::date >= ws.week_start
         OR h.next_source_sst_created_at IS NULL)
  JOIN lk_spots s ON h.spot_id = s.spot_id
  WHERE h.spot_status_id = 1             -- Public
    AND h.spot_hard_deleted_id = 0       -- No
)
SELECT
  week_start,
  spot_state,
  COUNT(*) AS spot_count,
  ROUND(SUM(COALESCE(spot_area_in_sqm, 0))::numeric, 0) AS total_sqm
FROM active_per_week
WHERE spot_state_id IS NOT NULL
GROUP BY week_start, spot_state
ORDER BY week_start DESC, spot_count DESC
```

---

## Example 5: Full DM-style daily metric (all core dimensions)

**Business question**: "Replicate `dm_spot_historical_metrics` for the Day timeframe — all statuses, all core dimensions, with count and area."

This uses **Full mode** (construction rule #8): no status filter, no `spot_hard_deleted` filter. All statuses are included as GROUP BY dimensions so downstream queries can slice freely.

### SQL (single timeframe — Day)

```sql
WITH base AS (
  SELECT
    ds.period_date,
    h.spot_id,
    -- Core dimensions from lk_spots
    s.spot_type_id, s.spot_type,
    s.spot_sector_id, s.spot_sector,
    s.spot_state_id, s.spot_state,
    s.spot_municipality_id, s.spot_municipality,
    s.spot_modality_id, s.spot_modality,
    s.user_affiliation_id, s.user_affiliation,
    s.user_industria_role_id, s.user_industria_role,
    s.spot_origin_id, s.spot_origin,
    -- Status dimensions from dictionary
    h.spot_status_id, d_status.spot_status,
    h.spot_status_reason_id, d_reason.spot_status_reason,
    h.spot_status_full_id, d_full.spot_status_full,
    MAX(s.spot_area_in_sqm) AS spot_area_in_sqm
  FROM generate_series('2025-09-01'::date, CURRENT_DATE, '1 day') ds(period_date)
  JOIN lk_spot_status_history h
    ON h.source_sst_created_at::date <= ds.period_date
    AND (h.next_source_sst_created_at::date >= ds.period_date
         OR h.next_source_sst_created_at IS NULL)
  JOIN lk_spots s ON h.spot_id = s.spot_id
  LEFT JOIN lk_spot_status_dictionary d_status
    ON h.spot_status_id = d_status.spot_status_id
    AND d_status.spot_status IS NOT NULL
  LEFT JOIN lk_spot_status_dictionary d_reason
    ON h.spot_status_reason_id = d_reason.spot_status_reason_id
    AND d_reason.spot_status_reason IS NOT NULL
  LEFT JOIN lk_spot_status_dictionary d_full
    ON h.spot_status_full_id = d_full.spot_status_full_id
    AND d_full.spot_status_full IS NOT NULL
  GROUP BY
    ds.period_date, h.spot_id,
    s.spot_type_id, s.spot_type,
    s.spot_sector_id, s.spot_sector,
    s.spot_state_id, s.spot_state,
    s.spot_municipality_id, s.spot_municipality,
    s.spot_modality_id, s.spot_modality,
    s.user_affiliation_id, s.user_affiliation,
    s.user_industria_role_id, s.user_industria_role,
    s.spot_origin_id, s.spot_origin,
    h.spot_status_id, d_status.spot_status,
    h.spot_status_reason_id, d_reason.spot_status_reason,
    h.spot_status_full_id, d_full.spot_status_full
)
SELECT
  1 AS timeframe_id,
  'Day' AS timeframe,
  period_date AS timeframe_timestamp,
  spot_type_id, spot_type,
  spot_sector_id, spot_sector,
  spot_state_id, spot_state,
  spot_municipality_id, spot_municipality,
  spot_modality_id, spot_modality,
  user_affiliation_id, user_affiliation,
  user_industria_role_id, user_industria_role,
  spot_origin_id, spot_origin,
  spot_status_id, spot_status,
  spot_status_reason_id, spot_status_reason,
  spot_status_full_id, spot_status_full,
  COUNT(*) AS spot_count,
  COALESCE(SUM(spot_area_in_sqm), 0) AS spot_total_sqm
FROM base
GROUP BY
  period_date,
  spot_type_id, spot_type,
  spot_sector_id, spot_sector,
  spot_state_id, spot_state,
  spot_municipality_id, spot_municipality,
  spot_modality_id, spot_modality,
  user_affiliation_id, user_affiliation,
  user_industria_role_id, user_industria_role,
  spot_origin_id, spot_origin,
  spot_status_id, spot_status,
  spot_status_reason_id, spot_status_reason,
  spot_status_full_id, spot_status_full
ORDER BY period_date DESC
```

To add more timeframes (Week, Month, Quarter, Year), use the UNION ALL pattern from Pattern 6 in SKILL.md. To add extra segmenters beyond the core set, add columns from `lk_spots` to both the `base` CTE and the outer GROUP BY.

### Querying the result (like dm_spot_historical_metrics)

Once materialized, query it the same way as the DM table:

```sql
SELECT
  DATE(timeframe_timestamp) AS fecha,
  spot_sector,
  spot_modality,
  SUM(spot_count) AS spots_activos
FROM my_dm_table
WHERE timeframe_id = 1
  AND spot_status = 'Public'
  AND spot_sector IS NOT NULL
GROUP BY DATE(timeframe_timestamp), spot_sector, spot_modality
ORDER BY fecha DESC, spots_activos DESC
```

---

## Example 6: Complete historical inventory including hard-deleted spots

**Business question**: "How many spots were Public on each day across ALL history, including spots that were physically deleted from the production database?"

Hard-deleted spots no longer exist in `lk_spots` (the production table `spots` on Spot2.mx removes them physically). The SST pipeline marks them with `spot_hard_deleted_id = 1` (Yes) in `lk_spot_status_history`. An INNER JOIN with `lk_spots` silently drops them. See construction rule #9.

### Variant A: Without lk_spots (pure count by status)

No JOIN with `lk_spots` — maximum historical coverage, no spot dimensions. No JOIN with `lk_spot_status_dictionary` either — filter directly on `h.spot_status_id` (indexed).

```sql
WITH date_series AS (
  SELECT generate_series('2025-01-01'::date, CURRENT_DATE, '1 day')::date AS fecha
)
SELECT
  ds.fecha,
  h.spot_hard_deleted,
  COUNT(DISTINCT h.spot_id) AS spot_count
FROM date_series ds
JOIN lk_spot_status_history h
  ON h.source_sst_created_at::date <= ds.fecha
  AND (h.next_source_sst_created_at::date >= ds.fecha
       OR h.next_source_sst_created_at IS NULL)
WHERE h.spot_status_id = 1              -- Public
GROUP BY ds.fecha, h.spot_hard_deleted
ORDER BY ds.fecha DESC, h.spot_hard_deleted
```

Note: no `spot_hard_deleted_id = 0` filter — we want both. The `spot_hard_deleted` column in the output lets you see how many deleted spots were Public on each day.

### Variant B: With LEFT JOIN to lk_spots (dimensions with NULLs for deleted)

LEFT JOIN preserves hard-deleted spots. Their dimension columns (`spot_sector`, `spot_modality`, etc.) will be NULL since the spot no longer exists in `lk_spots`. No JOIN to `lk_spot_status_dictionary` — filter directly on `h.spot_status_id`.

```sql
WITH date_series AS (
  SELECT generate_series('2025-01-01'::date, CURRENT_DATE, '1 day')::date AS fecha
)
SELECT
  ds.fecha,
  h.spot_hard_deleted,
  s.spot_sector,
  s.spot_modality,
  COUNT(DISTINCT h.spot_id) AS spot_count
FROM date_series ds
JOIN lk_spot_status_history h
  ON h.source_sst_created_at::date <= ds.fecha
  AND (h.next_source_sst_created_at::date >= ds.fecha
       OR h.next_source_sst_created_at IS NULL)
LEFT JOIN lk_spots s ON h.spot_id = s.spot_id
WHERE h.spot_status_id = 1              -- Public
GROUP BY ds.fecha, h.spot_hard_deleted,
         s.spot_sector, s.spot_modality
ORDER BY ds.fecha DESC, h.spot_hard_deleted, spot_count DESC
```

Rows with `spot_hard_deleted_id = 1` (Yes) will have `spot_sector = NULL` and `spot_modality = NULL`. You can use `COALESCE(s.spot_sector, 'Hard-deleted')` to make the output more readable.

---

## Example 7: Monthly marketable inventory with duration threshold

**Business question**: "How many commercializable spots (Public + Disabled Internal Use) exist each month, excluding accidental publications under 1 hour?"

This example combines two concepts from the skill: **marketable inventory** (`spot_status_full_id IN (100, 309)`) and **duration threshold** (exclude transitions shorter than 1 hour). See SKILL.md "Business concepts" and construction rule #10.

### Variant A: Simple duration filter (using minutes_until_next_state)

```sql
WITH month_series AS (
  SELECT generate_series(
    '2025-01-01'::date,
    DATE_TRUNC('month', CURRENT_DATE)::date,
    '1 month'
  )::date AS month_start
)
SELECT
  ms.month_start,
  s.spot_sector,
  s.spot_origin,
  s.user_industria_role,
  COUNT(DISTINCT h.spot_id) AS spot_count
FROM month_series ms
JOIN lk_spot_status_history h
  ON h.source_sst_created_at::date <= (ms.month_start + INTERVAL '1 month' - INTERVAL '1 day')::date
  AND (h.next_source_sst_created_at::date >= ms.month_start
       OR h.next_source_sst_created_at IS NULL)
JOIN lk_spots s ON h.spot_id = s.spot_id
WHERE h.spot_status_full_id IN (100, 309)   -- Marketable: Public + Disabled Internal Use
  AND h.spot_hard_deleted_id = 0         -- No
  AND s.spot_type_id IN (1, 3)              -- Filter by spot type if needed
  AND (h.minutes_until_next_state IS NULL   -- Still active, always passes
       OR h.minutes_until_next_state >= 60) -- At least 1 hour in this state
GROUP BY ms.month_start, s.spot_sector, s.spot_origin, s.user_industria_role
ORDER BY ms.month_start DESC, spot_count DESC
```

### Variant B: Precise duration filter (timestamp arithmetic, within the period)

More accurate when a state starts near a month boundary. Uses timestamp arithmetic to compute duration and ensures the state lasted at least 1 hour overall.

```sql
WITH month_series AS (
  SELECT generate_series(
    '2025-01-01'::date,
    DATE_TRUNC('month', CURRENT_DATE)::date,
    '1 month'
  )::date AS month_start
),
marketable_states AS (
  SELECT
    ms.month_start,
    h.spot_id,
    h.source_sst_created_at AS state_start,
    COALESCE(h.next_source_sst_created_at, 'infinity'::timestamp) AS state_end
  FROM month_series ms
  JOIN lk_spot_status_history h
    ON h.source_sst_created_at < (ms.month_start + INTERVAL '1 month')
    AND COALESCE(h.next_source_sst_created_at, 'infinity'::timestamp) > ms.month_start::timestamp
  WHERE h.spot_status_full_id IN (100, 309)
    AND h.spot_hard_deleted_id = 0       -- No
    AND (
      h.next_source_sst_created_at IS NULL                                        -- Still active
      OR (h.next_source_sst_created_at - h.source_sst_created_at) >= INTERVAL '1 hour'  -- Closed >= 1h
    )
)
SELECT
  ms2.month_start,
  s.spot_sector,
  s.spot_origin,
  s.user_industria_role,
  COUNT(DISTINCT ms2.spot_id) AS spot_count
FROM marketable_states ms2
JOIN lk_spots s ON ms2.spot_id = s.spot_id
WHERE s.spot_type_id IN (1, 3)
GROUP BY ms2.month_start, s.spot_sector, s.spot_origin, s.user_industria_role
ORDER BY ms2.month_start DESC, spot_count DESC
```

**Key differences from Variant A**: Variant B uses `<` and `>` on raw timestamps (not `::date` casts) for month boundary precision, and checks duration with `INTERVAL` arithmetic instead of the pre-computed `minutes_until_next_state`. Both are valid; Variant A is simpler, Variant B is more precise at period boundaries.

---

## Dagster rpt_* pattern (for any of the above)

To turn any SQL metric into a Dagster pipeline report table, follow this structure:

```
defs/{flow_name}/
├── __init__.py
├── shared.py          # re-export query_bronze_source, write_polars_to_s3, load_to_geospot
├── jobs.py
├── bronze/
│   └── raw_gs_{metric_name}.py    # runs the SQL via query_bronze_source
├── silver/
│   └── stg/
│       └── stg_gs_{metric_name}.py  # cast types
├── gold/
│   └── gold_rpt_{metric_name}.py    # add_audit_fields
└── publish/
    └── rpt_{metric_name}.py         # _to_s3 + _to_geospot (mode="replace")
```

For simple metrics (single SQL → single output), Core can be skipped.
The Gold asset adds audit fields and optionally a lightweight transformation.
Publish writes CSV to S3 and then loads to GeoSpot via API with `mode="replace"`.
