# Bronze layer — quick reference

Bronze assets extract **raw** data from source systems and write **Parquet** to S3. Use a single factory: `make_bronze_asset(...)` in `bronze/base.py`, one Python file per asset.

## How assets are discovered

The repo uses `load_from_defs_folder` in `dagster_pipeline/definitions.py`. Any new module under `defs/` that defines Dagster definitions is picked up automatically **if** it follows the project’s defs layout. Place new bronze files under:

`src/dagster_pipeline/defs/data_lakehouse/bronze/`

## Common steps (all sources)

| Step | Action | Notes |
|------|--------|--------|
| Add file | Create `raw_<prefix>_<name>_new.py` (or the name you pass as `asset_name`) calling `make_bronze_asset`. | See examples per source below. |
| IO manager | Bronze assets use `io_manager_key="s3_bronze"` (set inside `make_bronze_asset`). | Configured in `definitions.py` as resource `s3_bronze`. |
| Partitions | Default: **`daily_partitions`** on the asset. | Some pipelines (e.g. BT conversations) use **`partitioned=False`**; see `make_bronze_asset` / `_build_asset` in `base.py`. |
| Verify | `dagster asset list \| grep <asset_name>` | Adjust `-m` module if your entrypoint differs. |
| S3 path | `build_bronze_s3_key` → typically `s3://<bucket>/bronze/<asset_name>/...` | Exact bucket/env comes from shared config / env vars used in `shared.py`. |

## Sources (`source_type`)

Supported values (see `BronzeSourceType` in `bronze/base.py`):

1. **`mysql_prod`** — Spot2 Platform (MySQL Production)  
2. **`bigquery`**  
3. **`geospot_postgres`** — Geospot PostgreSQL  
4. **`chatbot_postgres`** — Chatbot PostgreSQL  
5. **`staging_postgres`** — Staging (Spot Chatbot PostgreSQL), when used  

### 1. MySQL Production (`mysql_prod`)

```python
from dagster_pipeline.defs.data_lakehouse.bronze.base import make_bronze_asset

raw_s2p_users_new = make_bronze_asset("mysql_prod", table_name="users")
```

- Optional: `where_clause`, `asset_name`, `description_suffix`.  
- Naming convention: **`raw_s2p_*_new`**.

### 2. BigQuery (`bigquery`)

```python
raw_bq_funnel_with_channel_new = make_bronze_asset(
    "bigquery",
    table_name="spot2-mx-ga4-bq.analitics_spot2.funnel_with_channel",
    asset_name="raw_bq_funnel_with_channel_new",
)
```

- `table_name` is full `project.dataset.table`.  
- Optional `asset_name` if the default derived name is not desired.  
- Convention: **`raw_bq_*_new`**.

### 3. Geospot PostgreSQL (`geospot_postgres`)

```python
raw_gs_lk_matches_visitors_to_leads_new = make_bronze_asset(
    "geospot_postgres",
    table_name="temp_matches_users_to_leads",
)
```

- Convention: **`raw_gs_*_new`**.

### 4. Chatbot PostgreSQL (`chatbot_postgres`)

- **Full table mirror:** `make_bronze_asset("chatbot_postgres", table_name="<table>")` → default name `raw_cb_<table>_new`.  
- **Custom SQL:** pass `query=`, and **required** `asset_name=` and `description=`.

```python
raw_cb_messages_new = make_bronze_asset(
    "chatbot_postgres",
    query=CB_MESSAGES_QUERY,
    asset_name="raw_cb_messages_new",
    description="Bronze: raw Chatbot PostgreSQL messages extract.",
)
```

- Convention: **`raw_cb_*_new`**.

## Naming conventions

| Prefix | Source |
|--------|--------|
| `raw_s2p_*` | MySQL Production (Spot2) |
| `raw_bq_*` | BigQuery |
| `raw_gs_*` | Geospot PostgreSQL |
| `raw_cb_*` | Chatbot PostgreSQL |

Prefer the **`_new`** suffix for governance-aligned tables to distinguish from legacy assets.

## S3 layout

Bronze writes through `write_polars_to_s3` + `build_bronze_s3_key`. Typical pattern:

`bronze/{asset_name}/data.parquet` (under the configured lakehouse bucket; see `shared.py`).

## Reference date / partitions

`resolve_stale_reference_date(context)` drives the logical **day** for extracts and downstream keys. Partitioned bronze runs usually pass a **partition key** (calendar day). Unpartitioned jobs still resolve a reference date for logging and paths—see BT conv bronze assets.

## Credentials / connectivity

Connection details are not hardcoded in these READMEs. They are resolved inside `query_bronze_source` / shared helpers (e.g. SSM parameters such as BigQuery or Geospot—see existing docs or ops runbooks).

## Verify

```bash
dagster asset list | grep raw_s2p_users_new
dagster asset materialize -m dagster_pipeline.definitions -s raw_s2p_users_new --partition 2024-12-01
```

Adjust `-m` / `--partition` if your deployment uses a different definitions module or partition policy.

## For LLM authors

- Read **`bronze/base.py`** for the full `make_bronze_asset` signature (`partitioned`, `tags`, `deps`, etc.).  
- Mirror an existing asset in the **same source family** (same `source_type` and naming prefix).  
- Do **not** change `conversation_analysis` unless the task explicitly allows it; governance extracts live under `data_lakehouse`.
