---
name: lk-sst
description: Build ad-hoc metrics from the lk_spot_status_history enriched event log in GeoSpot PostgreSQL. Use when the user asks to create metrics, reports, inventories, counts, or analysis about spot states, transitions, durations, or historical status. Supports SQL output (for Metabase/BIs) and Python/Dagster output (for rpt_* pipeline tables).
license: Apache-2.0
metadata:
  author: Luis Muñiz Valledor
  organization: Spot2
  version: "1.0"
---

# lk-sst — Ad-hoc metrics from lk_spot_status_history

## Mental model

`lk_spot_status_history` is a **state-change log**, not a simple event table. Each row represents a **time range** during which a spot was in a given state:

- **Start**: `source_sst_created_at` (when the spot entered this state)
- **End**: `next_source_sst_created_at` (when it transitioned to the next state; NULL = still active)

To answer "how many spots were in state X on date Y", use **temporal overlap** — filter rows whose time range covers the target date. Never pre-expand into a cartesian product of dates x dimensions.

## Tables

Three GeoSpot PostgreSQL tables work together:

| Table | Role | JOIN key |
|-------|------|----------|
| `lk_spot_status_history` | Enriched state-change log (always required) | — |
| `lk_spots` | Spot dimensions (geography, type, sector, modality, area, prices, user) | `spot_id` |
| `lk_spot_status_dictionary` | Human-readable status names | `spot_status_id`, `spot_status_reason_id`, or `spot_status_full_id` |

For complete column lists and available dimensions, see [table-reference.md](table-reference.md).

### When to JOIN each table

- **lk_spots**: whenever you need geographic, commercial, or user dimensions (sector, type, modality, state, municipality, area, prices, user_affiliation, etc.). Default is INNER JOIN, but use LEFT JOIN when you need to include hard-deleted spots (their dimensions will be NULL). If you don't need dimensions at all, you can skip this JOIN entirely for maximum historical coverage. See construction rule #9 for details.
- **lk_spot_status_dictionary**: whenever you need to filter or group by status **name** (e.g. `spot_status = 'Public'`) instead of ID

### Dimension sets

When building metrics, pick dimension columns from two sources. Each column has an `_id` (bigint) and a name (varchar) variant — always include both when grouping.

**Core dimensions** (used by `dm_spot_historical_metrics` — most common and recommended):

| Source | Columns (name) | Columns (ID) |
|--------|----------------|--------------|
| `lk_spots` | `spot_type`, `spot_sector`, `spot_state`, `spot_municipality`, `spot_modality`, `user_affiliation`, `user_industria_role`, `spot_origin` | `spot_type_id`, `spot_sector_id`, `spot_state_id`, `spot_municipality_id`, `spot_modality_id`, `user_affiliation_id`, `user_industria_role_id`, `spot_origin_id` |
| `lk_spot_status_dictionary` | `spot_status`, `spot_status_reason`, `spot_status_full` | `spot_status_id`, `spot_status_reason_id`, `spot_status_full_id` |

For ad-hoc metrics you typically pick 1-3 of these. To replicate `dm_spot_historical_metrics`, include **all 11** in a single GROUP BY.

**Extended dimensions**: `lk_spots` has ~187 columns. Beyond the core set, you can add any column as an extra segmenter — prices (`spot_price_total_mxn_rent`, `spot_price_sqm_mxn_sale`, etc.), fine geography (`spot_region`, `spot_corridor`, `spot_zip_code`), quality (`spot_score`, `spot_class`), user details (`user_level`, `user_broker_next`), listing metadata (`spot_exclusive`, `spot_is_listing`), and more. See [table-reference.md](table-reference.md) section "Additional dimensions" for the full list. When you need a segmenter not in the core set, explore `lk_spots` to find it and add the corresponding JOIN column.

## Metric patterns

### 1. Point-in-time count

"How many spots were in state X on each day/week/month?"

```sql
WITH date_series AS (
  SELECT generate_series(
    '{start_date}'::date, '{end_date}'::date, '1 {interval}'
  )::date AS period_date
)
SELECT
  ds.period_date,
  {dimension_columns},
  COUNT(DISTINCT h.spot_id) AS spot_count
FROM date_series ds
JOIN lk_spot_status_history h
  ON h.source_sst_created_at::date <= ds.period_date
  AND (h.next_source_sst_created_at::date >= ds.period_date
       OR h.next_source_sst_created_at IS NULL)
-- Optional JOINs for dimensions/names:
-- JOIN lk_spots s ON h.spot_id = s.spot_id
-- JOIN lk_spot_status_dictionary d ON h.spot_status_id = d.spot_status_id
WHERE {status_filter}
  AND h.spot_hard_deleted_id = 0         -- No
GROUP BY ds.period_date, {dimension_columns}
ORDER BY ds.period_date DESC
```

**Interval options**: `'1 day'`, `'1 week'`, `'1 month'`, `'1 quarter'`, `'1 year'`

### 2. Transition count

"How many spots transitioned from state A to state B in a period?"

```sql
SELECT
  DATE_TRUNC('{period}', h.source_sst_created_at) AS period,
  {dimension_columns},
  COUNT(DISTINCT h.spot_id) AS transition_count
FROM lk_spot_status_history h
-- JOIN lk_spot_status_dictionary d ON h.spot_status_id = d.spot_status_id
-- JOIN lk_spot_status_dictionary dp ON h.prev_spot_status_id = dp.spot_status_id
WHERE h.prev_spot_status_id = {from_status_id}
  AND h.spot_status_id = {to_status_id}
  AND h.source_sst_created_at >= '{start_date}'
  AND h.spot_hard_deleted_id = 0         -- No
GROUP BY period, {dimension_columns}
ORDER BY period
```

Use `sstd_status_id` for pre-encoded transition pairs: `status * 100000 + prev_status * 100`.

### 3. Duration in state

"What's the average time a spot spends in state X?"

```sql
SELECT
  {dimension_columns},
  AVG(h.minutes_until_next_state) AS avg_minutes,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY h.minutes_until_next_state) AS median_minutes,
  COUNT(*) AS n_transitions
FROM lk_spot_status_history h
WHERE h.spot_status_id = {status_id}
  AND h.next_spot_status_id IS NOT NULL  -- exclude still-active states
  AND h.spot_hard_deleted_id = 0         -- No
  AND h.source_sst_created_at >= '{start_date}'
GROUP BY {dimension_columns}
```

### 4. Point-in-time count + area

"How many spots and total sqm in state X on each day, with dimensions?"

Uses a CTE to deduplicate by `(period, spot_id)` first (a spot may have multiple transitions within one period), keeping the maximum area. Then aggregates count + area.

```sql
WITH date_series AS (
  SELECT generate_series('{start}'::date, '{end}'::date, '1 {interval}')::date AS period_date
),
deduped AS (
  SELECT
    ds.period_date,
    h.spot_id,
    {dimension_columns},
    MAX(s.spot_area_in_sqm) AS spot_area_in_sqm
  FROM date_series ds
  JOIN lk_spot_status_history h
    ON h.source_sst_created_at::date <= ds.period_date
    AND (h.next_source_sst_created_at::date >= ds.period_date
         OR h.next_source_sst_created_at IS NULL)
  JOIN lk_spots s ON h.spot_id = s.spot_id
  -- JOIN lk_spot_status_dictionary d ON h.spot_status_id = d.spot_status_id
  WHERE {status_filter}
    AND h.spot_hard_deleted_id = 0       -- No
  GROUP BY ds.period_date, h.spot_id, {dimension_columns}
)
SELECT
  period_date,
  {dimension_columns},
  COUNT(*) AS spot_count,
  COALESCE(SUM(spot_area_in_sqm), 0) AS spot_total_sqm
FROM deduped
GROUP BY period_date, {dimension_columns}
ORDER BY period_date DESC
```

This two-stage aggregation (deduplicate per spot, then count) matches how `dm_spot_historical_metrics` computes `spot_count` and `spot_total_sqm`.

### 5. Flow analysis

"Most common state paths."

```sql
SELECT
  h.sstd_status_id,
  d_prev.spot_status AS from_status,
  d_curr.spot_status AS to_status,
  COUNT(*) AS n_transitions
FROM lk_spot_status_history h
JOIN lk_spot_status_dictionary d_curr ON h.spot_status_id = d_curr.spot_status_id
LEFT JOIN lk_spot_status_dictionary d_prev ON h.prev_spot_status_id = d_prev.spot_status_id
WHERE h.sstd_status_id IS NOT NULL
  AND h.source_sst_created_at >= '{start_date}'
GROUP BY h.sstd_status_id, d_prev.spot_status, d_curr.spot_status
ORDER BY n_transitions DESC
LIMIT 20
```

### 6. Multi-timeframe UNION

"Produce the same metric at Day, Week, Month, Quarter, and Year granularity in a single output."

This is how `dm_spot_historical_metrics` is structured — one table with a `timeframe_id` column so downstream queries can filter by granularity.

```sql
WITH base AS (
  -- Reuse any point-in-time CTE here (Pattern 1 or 4).
  -- Replace {interval} with the finest granularity needed,
  -- then truncate for coarser periods below.
  SELECT
    ds.period_date,
    h.spot_id,
    {dimension_columns},
    MAX(s.spot_area_in_sqm) AS spot_area_in_sqm
  FROM generate_series('{start}'::date, '{end}'::date, '1 day') ds(period_date)
  JOIN lk_spot_status_history h
    ON h.source_sst_created_at::date <= ds.period_date
    AND (h.next_source_sst_created_at::date >= ds.period_date
         OR h.next_source_sst_created_at IS NULL)
  JOIN lk_spots s ON h.spot_id = s.spot_id
  -- JOIN lk_spot_status_dictionary only if status names are needed in output
  WHERE h.spot_hard_deleted_id = 0       -- No
  GROUP BY ds.period_date, h.spot_id, {dimension_columns}
)
SELECT 1 AS timeframe_id, 'Day' AS timeframe,
  period_date AS timeframe_timestamp, {dimension_columns},
  COUNT(*) AS spot_count, COALESCE(SUM(spot_area_in_sqm),0) AS spot_total_sqm
FROM base GROUP BY period_date, {dimension_columns}

UNION ALL

SELECT 2, 'Week',
  DATE_TRUNC('week', period_date)::date, {dimension_columns},
  COUNT(DISTINCT spot_id), COALESCE(SUM(spot_area_in_sqm),0)
FROM base GROUP BY DATE_TRUNC('week', period_date), {dimension_columns}

UNION ALL

SELECT 3, 'Month',
  DATE_TRUNC('month', period_date)::date, {dimension_columns},
  COUNT(DISTINCT spot_id), COALESCE(SUM(spot_area_in_sqm),0)
FROM base GROUP BY DATE_TRUNC('month', period_date), {dimension_columns}

UNION ALL

SELECT 4, 'Quarter',
  DATE_TRUNC('quarter', period_date)::date, {dimension_columns},
  COUNT(DISTINCT spot_id), COALESCE(SUM(spot_area_in_sqm),0)
FROM base GROUP BY DATE_TRUNC('quarter', period_date), {dimension_columns}

UNION ALL

SELECT 5, 'Year',
  DATE_TRUNC('year', period_date)::date, {dimension_columns},
  COUNT(DISTINCT spot_id), COALESCE(SUM(spot_area_in_sqm),0)
FROM base GROUP BY DATE_TRUNC('year', period_date), {dimension_columns}

ORDER BY timeframe_id, timeframe_timestamp DESC
```

For most ad-hoc needs a single timeframe suffices (Patterns 1/4). Use this UNION pattern only when building a DM-style "all-in-one" table.

## Construction rules

1. **Always filter** `h.spot_hard_deleted_id = 0` (No) unless the user explicitly asks to include deleted spots.
2. **Use COUNT(DISTINCT spot_id)** for spot counts — a spot can appear in multiple transitions.
3. **For area metrics** use `spot_area_in_sqm` from `lk_spots` (not from the history table). Deduplicate per spot per date with `DISTINCT ON` or `MAX()`.
4. **Use generate_series** to expand date ranges — never pre-compute a full cartesian product of dates x dimensions.
5. **Prefer date casting** `::date` on timestamps for day-level grouping; `DATE_TRUNC('month', ...)` for coarser periods.
6. **NULL in next_source_sst_created_at** means the spot is still in that state (current/active). Include these with `OR h.next_source_sst_created_at IS NULL`.
7. **Boundary is inclusive (`>=`)**: use `next_source_sst_created_at::date >= period_date`, not `>`. The pre-computed `dm_spot_historical_metrics` counts the transition day as part of the outgoing state. Using `>` instead of `>=` under-counts by ~2-4% and fails validation.
8. **Full vs Filtered mode**:
   - **Filtered** (default for ad-hoc): filter by a specific status (e.g. `WHERE h.spot_status_id = 1`) and always include `h.spot_hard_deleted_id = 0`. Use for targeted metrics.
   - **Full** (for DM-style tables): do NOT filter by status or `spot_hard_deleted_id`. Instead, include `spot_status_id`, `spot_status_reason_id`, `spot_status_full_id`, and `spot_hard_deleted_id` as GROUP BY dimensions. Downstream queries then filter the resulting table (e.g. `WHERE spot_status_id = 1` and `SUM(spot_count)`).
9. **JOIN strategy with lk_spots** — choose based on whether hard-deleted spots matter:
   - **INNER JOIN** (default): `JOIN lk_spots s ON h.spot_id = s.spot_id`. Excludes spots that were hard-deleted from the production database. Use when you only care about spots that still exist and need their dimensions.
   - **LEFT JOIN**: `LEFT JOIN lk_spots s ON h.spot_id = s.spot_id`. Includes all spots; hard-deleted ones will have NULL in all `lk_spots` dimension columns. Use when you need the complete historical time series with partial dimensions.
   - **No JOIN**: don't join `lk_spots` at all. Use only `lk_spot_status_history` + `lk_spot_status_dictionary`. Maximum historical coverage, no spot dimensions. Best for pure count-by-status metrics.

   Background: the source of hard-deletes is the production table `spots` in MySQL (Spot2.mx platform), where the app physically removes rows. `lk_spots` (GeoSpot/PostgreSQL) is a cleaned and enriched analytical copy that inherits those deletions — it is not the source. The SST pipeline detects missing spots by comparing against `stg_gs_active_spot_ids` and marks them with `spot_hard_deleted_id = 1` (Yes) in `lk_spot_status_history`.

   When using LEFT JOIN or no JOIN to include hard-deleted spots, **omit** the `h.spot_hard_deleted_id = 0` filter. You can instead include `h.spot_hard_deleted_id` (or `h.spot_hard_deleted` for display) as a column or GROUP BY dimension to distinguish them.
10. **Duration threshold** — exclude accidental/short-lived transitions:

    Spots are sometimes published by mistake and reverted within seconds or minutes. These ultra-short transitions contaminate inventory metrics. **When building inventory or count metrics, the agent MUST ask the user whether to exclude short-lived transitions**, even if the user doesn't mention it. Propose a default threshold of **1 hour** and let the user decide (they may adjust to 30 min, 2 hours, 5 min, etc., or skip the filter entirely).

    Two approaches:

    - **Simple (overall duration)**: use the pre-computed `minutes_until_next_state` column. Still-active states (NULL) always pass.
      ```sql
      AND (h.minutes_until_next_state IS NULL OR h.minutes_until_next_state >= 60)
      ```

    - **Precise (duration within the observation period)**: use timestamp arithmetic to verify the state lasted long enough within the specific period. More accurate when a state starts near a period boundary. Still-active states use `'infinity'::timestamp` as end.
      ```sql
      AND (
        h.next_source_sst_created_at IS NULL  -- still active, always passes
        OR (h.next_source_sst_created_at - h.source_sst_created_at) >= INTERVAL '1 hour'
      )
      ```

    The simple approach is recommended for most ad-hoc metrics. Use the precise approach when period-boundary accuracy matters (e.g., monthly snapshots where a 30-minute state at month-end should not count).
11. **Prefer `*_id` columns for filters and JOINs** — every string column (e.g. `spot_sector`, `spot_status`) has a numeric `*_id` counterpart that is **indexed** in GeoSpot. Rules:
    - **WHERE / JOIN / GROUP BY**: always use `*_id` columns (integer comparison, indexed lookups).
    - **SELECT**: include string columns only when they are needed for display in the final output (reports, dashboards).
    - **Comment literal IDs**: when using a literal `*_id` value, add a comment with the string equivalent for readability: `h.spot_status_id = 1  -- Public`.
    - **Eliminate unnecessary JOINs**: if you only need to filter by status (not display the name), skip the JOIN to `lk_spot_status_dictionary` entirely and filter directly on `h.spot_status_id`, `h.spot_status_full_id`, etc.
    - **GROUP BY optimization**: group by `*_id` columns; if the string name is needed in the output, include it in GROUP BY as well (PostgreSQL requires it) but the `*_id` does the heavy lifting.

## Business concepts

Concepts the agent should know when building metrics:

### Marketable / Commercializable inventory

Spots available for transactions (rent/sell), even if not publicly visible on the Spot2.mx platform:

- `spot_status_full_id = 100` — **Public** (visible on platform)
- `spot_status_full_id = 309` — **Disabled Internal Use** (not visible, but available for direct transactions within the company)

Filter: `h.spot_status_full_id IN (100, 309)`

**Why `spot_status_full_id`?** Because the marketable concept requires reason-level specificity. `spot_status_id = 3` (Disabled) includes many reasons (Publisher, Occupied, Won, QA, etc.) — only reason 9 (Internal Use) qualifies as marketable.

**Formula**: `spot_status_full_id = spot_status_id * 100 + COALESCE(spot_status_reason_id, 0)`. Both filter paths are equally valid:
- Direct: `h.spot_status_full_id IN (100, 309)`
- Combined: `(h.spot_status_id = 1) OR (h.spot_status_id = 3 AND h.spot_status_reason_id = 9)`

### Status names and synonyms

| status_id | Formal name | Business synonym |
|-----------|-------------|-----------------|
| 1 | Public | Active / Activo |
| 2 | Draft | — |
| 3 | Disabled | — |
| 4 | Archived | — |

When a user says "active spots" or "spots activos", they mean `spot_status_id = 1` (Public).

## Output modes

### SQL (for Metabase / BI tools)

Generate a standalone SQL query that runs directly on GeoSpot PostgreSQL. Use CTEs for clarity. Include comments explaining each section.

### Python / Dagster (for rpt_* pipeline tables)

Follow the Medallion Architecture from `dagster-pipeline/ARCHITECTURE.md`:

1. **Bronze**: `raw_gs_*` asset that runs the SQL query via `query_bronze_source(query, source_type="geospot_postgres", context=context)`
2. **STG**: `stg_gs_*` asset that casts types
3. **Core**: `core_*` asset with pure transformations (if needed)
4. **Gold**: `gold_rpt_*` asset that adds audit fields via `add_audit_fields(df, job_name="rpt_...")`
5. **Publish**: `rpt_*_to_s3` + `rpt_*_to_geospot` assets using `write_polars_to_s3` and `load_to_geospot` with `mode="replace"`

Naming convention for report tables: `rpt_{descriptive_name}` (e.g. `rpt_spot_inventory_daily`).

Reference implementation: `dagster-pipeline/src/dagster_pipeline/defs/funnel_ptd/publish/rpt_funnel_ptd.py`.

## Validation

After building a metric, validate it against `dm_spot_historical_metrics` (the pre-computed table) when possible. Run:

```bash
cd dagster-pipeline
uv run python ../.claude/skills/lk-sst/scripts/validate_metric.py
```

See [examples.md](examples.md) for concrete metric examples with both SQL and Python.
