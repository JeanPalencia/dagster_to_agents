# Table Reference — SST Metrics

## 1. lk_spot_status_history

Enriched state-change log. Each row = one state the spot was in, with prev/next context.

**Source of truth for column definitions**: `dagster-pipeline/src/dagster_pipeline/defs/spot_state_transitions/processing.py` (`FINAL_COLUMNS`, `FINAL_RENAME`) and `gold/gold_lk_spot_status_history.py`.

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `source_sst_id` | bigint | Original ID in the source table |
| `spot_id` | bigint | Spot identifier (FK to lk_spots) |
| `spot_status_id` | bigint | Current status: 1=Public (aka Active), 2=Draft, 3=Disabled, 4=Archived |
| `spot_status_reason_id` | bigint | Reason for the status (nullable). E.g. 9=Internal Use, 6=Occupied, 7=Won |
| `spot_status_full_id` | bigint | Composite: `spot_status_id * 100 + COALESCE(spot_status_reason_id, 0)`. E.g. 100=Public, 309=Disabled Internal Use |
| `prev_spot_status_id` | bigint | Previous status (NULL if first record for spot) |
| `prev_spot_status_reason_id` | bigint | Previous reason |
| `prev_spot_status_full_id` | bigint | Previous composite: `prev_status * 100 + prev_reason` |
| `next_spot_status_id` | bigint | Next status (NULL if current/latest state) |
| `next_spot_status_reason_id` | bigint | Next reason |
| `next_spot_status_full_id` | bigint | Next composite |
| `sstd_status_final_id` | bigint | `status * 100000` — status-only transition ID |
| `sstd_status_id` | bigint | `status * 100000 + prev_status * 100` — transition pair (excludes Public-to-Public) |
| `sstd_status_full_final_id` | bigint | `status * 100000 + reason * 1000` |
| `sstd_status_full_id` | bigint | `status * 100000 + reason * 1000 + prev_status * 100 + prev_reason` (excludes identical self-transitions) |
| `prev_source_sst_created_at` | timestamp | When the previous state started |
| `next_source_sst_created_at` | timestamp | When the next state started (NULL = still in this state) |
| `minutes_since_prev_state` | double | Minutes from previous transition to this one |
| `minutes_until_next_state` | double | Minutes from this transition to the next one |
| `source_sst_created_at` | timestamp | **State start**: when the spot entered this state |
| `source_sst_updated_at` | timestamp | Last update timestamp in the source system |
| `source_id` | bigint | Source system: 2 = MySQL spot_state_transitions, others = snapshots |
| `source` | varchar | Source name (e.g. 'spot_state_transitions') |
| `spot_hard_deleted_id` | bigint | 0 = active in lk_spots, 1 = hard-deleted |
| `spot_hard_deleted` | varchar | 'No' / 'Yes' |
| `aud_inserted_at` | timestamp | Audit: insertion timestamp |
| `aud_inserted_date` | date | Audit: insertion date |
| `aud_updated_at` | timestamp | Audit: update timestamp |
| `aud_updated_date` | date | Audit: update date |
| `aud_job` | varchar | Audit: 'lk_spot_status_history' |

### Key temporal concept

```
Row N for spot_id=42:
  source_sst_created_at      = 2025-10-01 09:00:00   ← state START
  next_source_sst_created_at = 2025-10-15 14:30:00   ← state END (next transition)
  spot_status_id = 1 (Public)

This means spot 42 was Public from Oct 1 to Oct 15.
To check if spot 42 was Public on Oct 10:
  source_sst_created_at <= '2025-10-10' AND next_source_sst_created_at >= '2025-10-10'  → TRUE
```

### Status reference (verified against lk_spot_status_dictionary)

**Statuses** (`spot_status_id`):

| ID | Name | Business synonym |
|----|------|-----------------|
| 1 | Public | Active / Activo |
| 2 | Draft | — |
| 3 | Disabled | — |
| 4 | Archived | — |

**Full statuses** (`spot_status_full_id = spot_status_id * 100 + COALESCE(spot_status_reason_id, 0)`):

| full_id | Name | Composition |
|---------|------|-------------|
| 100 | Public | status=1 |
| 200 | Draft | status=2 |
| 201 | Draft User | status=2, reason=1 |
| 202 | Draft Onboarding API | status=2, reason=2 |
| 203 | Draft Onboarding Scraping | status=2, reason=3 |
| 204 | Draft QA | status=2, reason=4 |
| 300 | Disabled | status=3 |
| 305 | Disabled Publisher | status=3, reason=5 |
| 306 | Disabled Occupied | status=3, reason=6 |
| 307 | Disabled Won | status=3, reason=7 |
| 308 | Disabled QA | status=3, reason=8 |
| 309 | Disabled Internal Use | status=3, reason=9 |
| 400 | Archived | status=4 |
| 410 | Archived Internal | status=4, reason=10 |
| 411 | Archived Owner | status=4, reason=11 |
| 412 | Archived Unpublished | status=4, reason=12 |
| 413 | Archived Outdated | status=4, reason=13 |
| 414 | Archived Quality | status=4, reason=14 |

Note: `full_id = 315` (status=3, reason=15) exists in production data but is not yet mapped in the dictionary.

---

## 2. lk_spots

Spot dimension table with 187 columns. Only the most relevant for metrics are listed here.

**Source of truth**: `dagster-pipeline/src/dagster_pipeline/defs/data_lakehouse/gold/gold_lk_spots_new.py` (`GOLD_LK_SPOTS_LEGACY_ORDER`).

### Commonly used dimensions for metrics

| Column | Type | Description | Example values |
|--------|------|-------------|---------------|
| `spot_id` | bigint | Primary key | |
| `spot_area_in_sqm` | double | Spot area in square meters | 150.0, 5000.0 |
| `spot_type_id` | bigint | Type ID | |
| `spot_type` | varchar | Type name | 'Office', 'Retail', 'Industrial', 'Land' |
| `spot_sector_id` | bigint | Sector ID | |
| `spot_sector` | varchar | Sector name | 'Industrial', 'Office', 'Retail', 'Land' |
| `spot_state_id` | bigint | Geographic state ID | |
| `spot_state` | varchar | Geographic state name | 'Ciudad de México', 'Nuevo León', 'Jalisco' |
| `spot_municipality_id` | bigint | Municipality ID | |
| `spot_municipality` | varchar | Municipality name | 'Miguel Hidalgo', 'Monterrey' |
| `spot_modality_id` | bigint | Modality ID | |
| `spot_modality` | varchar | Modality | 'Rent', 'Sale', 'Rent/Sale' |
| `user_affiliation_id` | bigint | User affiliation ID | |
| `user_affiliation` | varchar | User affiliation | 'Direct', 'Broker' |
| `user_industria_role_id` | bigint | Industria role ID | |
| `user_industria_role` | varchar | Industria role | 'Owner', 'Broker', 'Developer' |
| `spot_origin_id` | bigint | Spot origin ID | |
| `spot_origin` | varchar | How the spot was created | 'Platform', 'Import', 'API' |
| `spot_status_id` | bigint | Current status in lk_spots | |
| `spot_status_full_id` | bigint | Current full status | |
| `spot_latitude` | double | Latitude | |
| `spot_longitude` | double | Longitude | |
| `spot_created_at` | timestamp | When the spot was created | |

### Additional dimensions (less common but available)

- Price columns: `spot_price_total_mxn_rent`, `spot_price_total_mxn_sale`, `spot_price_sqm_mxn_rent`, `spot_price_sqm_mxn_sale` (and USD equivalents)
- Contact: `contact_company`, `contact_category`
- User: `user_id`, `user_email`, `user_level`, `user_broker_next`
- Quality: `spot_score`, `spot_photo_count`, `spot_class`
- Geography: `spot_region`, `spot_corridor`, `spot_zip_code`, `spot_settlement`
- Listing: `spot_listing_status`, `spot_exclusive`, `spot_is_listing`
- Dates: `spot_created_at`, `spot_updated_at`, `spot_valid_through`, `spot_deleted_at`

---

## 3. lk_spot_status_dictionary

Maps status IDs to human-readable names. Small lookup table.

### Columns

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `spot_status_id` | bigint | Status ID | 1, 2, 3 |
| `spot_status` | varchar | Status name | 'Public', 'Draft', 'Disabled', 'Archived' |
| `spot_status_reason_id` | bigint | Reason ID | 1, 2, 8 |
| `spot_status_reason` | varchar | Reason name | 'Rented', 'Sold', 'Other' |
| `spot_status_full_id` | bigint | Combined status+reason ID | 100, 308 |
| `spot_status_full` | varchar | Combined name | 'Public', 'Disabled Internal Use', 'Archived Outdated' |

### JOIN patterns

```sql
-- Get status name
JOIN lk_spot_status_dictionary d ON h.spot_status_id = d.spot_status_id
  AND d.spot_status IS NOT NULL

-- Get reason name
JOIN lk_spot_status_dictionary dr ON h.spot_status_reason_id = dr.spot_status_reason_id
  AND dr.spot_status_reason IS NOT NULL

-- Get full status name
JOIN lk_spot_status_dictionary df ON h.spot_status_full_id = df.spot_status_full_id
  AND df.spot_status_full IS NOT NULL
```

Note: the dictionary table may have rows where only one of the three ID columns is populated. Always filter for `IS NOT NULL` on the column you're joining, and `DROP_DUPLICATES` / `DISTINCT` on the join key to avoid row multiplication.

---

## 4. ID-to-string dictionary

Quick-reference mapping for all `*_id` fields used in filters, JOINs, and GROUP BY. Use `*_id` in SQL logic (indexed); use the string equivalent only in SELECT for display (see SKILL.md construction rule #11).

### lk_spot_status_history

#### spot_hard_deleted_id

| ID | String |
|----|--------|
| 0 | No |
| 1 | Yes |

#### spot_status_id

| ID | Name | Business synonym |
|----|------|-----------------|
| 1 | Public | Active / Activo |
| 2 | Draft | — |
| 3 | Disabled | — |
| 4 | Archived | — |

#### spot_status_full_id

Formula: `spot_status_id * 100 + COALESCE(spot_status_reason_id, 0)`

| full_id | Name | Composition |
|---------|------|-------------|
| 100 | Public | status=1 |
| 200 | Draft | status=2 |
| 201 | Draft User | status=2, reason=1 |
| 202 | Draft Onboarding API | status=2, reason=2 |
| 203 | Draft Onboarding Scraping | status=2, reason=3 |
| 204 | Draft QA | status=2, reason=4 |
| 300 | Disabled | status=3 |
| 305 | Disabled Publisher | status=3, reason=5 |
| 306 | Disabled Occupied | status=3, reason=6 |
| 307 | Disabled Won | status=3, reason=7 |
| 308 | Disabled QA | status=3, reason=8 |
| 309 | Disabled Internal Use | status=3, reason=9 |
| 315 | (unmapped) | status=3, reason=15 |
| 400 | Archived | status=4 |
| 410 | Archived Internal | status=4, reason=10 |
| 411 | Archived Owner | status=4, reason=11 |
| 412 | Archived Unpublished | status=4, reason=12 |
| 413 | Archived Outdated | status=4, reason=13 |
| 414 | Archived Quality | status=4, reason=14 |

### lk_spots — core dimensions

#### spot_type_id

| ID | Name |
|----|------|
| 1 | Single |
| 2 | Complex |
| 3 | Subspace |

#### spot_sector_id

| ID | Name |
|----|------|
| 9 | Industrial |
| 11 | Office |
| 13 | Retail |
| 15 | Land |

#### spot_modality_id

| ID | Name |
|----|------|
| 1 | Rent |
| 2 | Sale |
| 3 | Rent & Sale |

#### user_affiliation_id

| ID | Name |
|----|------|
| 0 | External User |
| 1 | Internal User |

#### user_industria_role_id

| ID | Name |
|----|------|
| 0 | Unknown |
| 1 | Tenant |
| 2 | Broker |
| 4 | Landlord |
| 5 | Developer |

#### spot_origin_id

| ID | Name |
|----|------|
| 1 | Spot2 |
| 2 | NocNok |
| 4 | Unknown |

#### spot_state_id (32 estados mexicanos + 1 extranjero)

| ID | Name |
|----|------|
| 1 | Aguascalientes |
| 2 | Baja California |
| 3 | Baja California Sur |
| 4 | Campeche |
| 5 | Chiapas |
| 6 | Chihuahua |
| 7 | Ciudad de Mexico |
| 8 | Coahuila de Zaragoza |
| 9 | Colima |
| 10 | Durango |
| 11 | Guanajuato |
| 12 | Guerrero |
| 13 | Hidalgo |
| 14 | Jalisco |
| 15 | Mexico |
| 16 | Michoacan de Ocampo |
| 17 | Morelos |
| 18 | Nayarit |
| 19 | Nuevo Leon |
| 20 | Oaxaca |
| 21 | Puebla |
| 22 | Queretaro |
| 23 | Quintana Roo |
| 24 | San Luis Potosi |
| 25 | Sinaloa |
| 26 | Sonora |
| 27 | Tabasco |
| 28 | Tamaulipas |
| 29 | Tlaxcala |
| 30 | Veracruz de Ignacio de la Llave |
| 31 | Yucatan |
| 32 | Zacatecas |
| 33 | Texas |

#### spot_municipality_id

916 values — too many to list. Query to get the full mapping:

```sql
SELECT DISTINCT spot_municipality_id, spot_municipality
FROM lk_spots
WHERE spot_municipality_id IS NOT NULL
ORDER BY spot_municipality_id
```

> **Note**: these values may grow as new types, sectors, states, municipalities, etc. are added to the Spot2.mx platform. The tables above reflect the current snapshot. When in doubt, query `lk_spots` or `lk_spot_status_dictionary` for the latest values.

---

## dm_spot_historical_metrics (reference only)

Pre-computed metrics table. Used **only for validation** — new metrics should query the source tables directly.

### Key columns

| Column | Type | Description |
|--------|------|-------------|
| `timeframe_id` | int | 1=Day, 2=Week, 3=Month, 4=Quarter, 5=Year |
| `timeframe` | varchar | 'Day', 'Week', 'Month', 'Quarter', 'Year' |
| `timeframe_timestamp` | timestamp | Period start date |
| `spot_count` | int | Number of distinct spots |
| `spot_total_sqm` | double | Sum of spot_area_in_sqm |
| All dimension columns from lk_spots | — | spot_type, spot_sector, spot_state, spot_municipality, spot_modality, etc. |
| All status names from dictionary | — | spot_status, spot_status_reason, spot_status_full |
