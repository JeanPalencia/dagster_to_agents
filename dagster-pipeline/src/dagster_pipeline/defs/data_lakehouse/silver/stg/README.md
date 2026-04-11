# Silver STG layer — quick reference

Silver **staging** assets read **bronze** Parquet from S3, transform (Polars), validate, and write **silver** Parquet to S3. Most S2P tables use **`make_silver_stg_asset`** in `silver/stg/base.py`. Specialized pipelines (e.g. BT conversations, `lk_spots`) use **custom `@dg.asset` modules**.

## Location

- **Generic STG factory:** `silver/stg/base.py` → `make_silver_stg_asset`  
- **This folder:** `silver/stg/*.py` — one module per asset (e.g. `stg_s2p_clients_new.py`)  
- **Reusable transforms:** `silver/utils.py` (`case_when`, `case_when_conditions`, `validate_and_clean_silver`, …)  
- **Domain modules:** `silver/modules/` (e.g. `lk_spots/`, `lk_projects/`) for larger pipelines  

## Option A — Template (recommended for simple `raw_* → stg_*`)

1. Copy the template (English placeholders in `_TEMPLATE.txt`):  
   `cp _TEMPLATE.txt stg_s2p_<table>_new.py`
2. Replace placeholders:
   - `{TABLE_NAME}` → logical table name in snake_case (e.g. `users`)
   - `{BRONZE_ASSET}` → upstream bronze asset name (e.g. `raw_s2p_users_new`)
3. Implement `_transform_s2p_<table>` only where needed.
4. **Keep bronze column names** unless the gold contract explicitly requires a rename (gold usually expects stable staging names).

## Option B — `make_silver_stg_asset` in a small file

For a thin pass-through or a short transform:

```python
from dagster_pipeline.defs.data_lakehouse.silver.stg.base import make_silver_stg_asset

def _transform(df):
    return df  # or return df.with_columns([...])

stg_s2p_example_new = make_silver_stg_asset(
    bronze_asset_name="raw_s2p_example_new",
    silver_asset_name="stg_s2p_example_new",
    transform_fn=_transform,
)
```

Read **`base.py`** for optional flags (e.g. `allow_row_loss`).

## Option C — Copy an existing asset

Copy a similar `stg_*_new.py` file and update:

- File name and asset function name  
- Bronze dependency name(s)  
- `build_silver_s3_key("stg_...", ...)`  
- `description` and log messages  
- Transform logic  

**Examples:** `stg_s2p_clients_new.py` (rich transforms), `stg_cb_messages_new.py` (thin bronze→silver).

## BT conversations (special case)

`stg_bt_conv_grouped_new`, `stg_bt_conv_with_events_new`, `stg_bt_conv_merged_new` live in **`stg_bt_conv_intermediate.py`**. They chain bronze messages + events + clients; they are **not** created with `make_silver_stg_asset`. Use that file as the template for BT conv silver changes.

## Typical `@dg.asset` shape (manual assets)

Prefer `make_silver_stg_asset` when possible. If you author a full `@dg.asset`, align with `silver/stg/base.py`:

```python
s3_key = build_silver_s3_key("stg_s2p_example_new", file_format="parquet")
write_polars_to_s3(df, s3_key, context, file_format="parquet")
```

`build_silver_s3_key` in `shared.py` takes **`table_name`** and optional **`file_format`** (no partition segment in the key path for the current layout).

## Generic helpers (`silver/utils.py`)

- **`case_when(column, mapping, default, alias)`** — equality mappings  
- **`case_when_conditions([(condition, value), ...], default, alias)`** — complex predicates  
- **`validate_and_clean_silver`** — NULL checks, type cleanup (see function docstring)  

Prefer these over huge `pl.when` chains for readability.

## Rules of thumb

1. **Do not drop bronze columns** in staging unless downstream gold no longer needs them. Prefer **`with_columns`** over **`select`** that omits columns.  
2. **Add** derived columns with clear suffixes (e.g. `*_text` for label expansions).  
3. **Naming:** prefer **`stg_*_new`** for governance-aligned tables.  
4. **IO:** `io_manager_key="s3_silver"` for standard STG assets.  

## S3 layout

Silver keys use **`build_silver_s3_key`** in `shared.py` — pattern like:

`silver/<stg_asset_name>/data.parquet`

Partitioned assets still use `daily_partitions` for orchestration; the key layout is defined in `build_silver_s3_key` (see implementation if it changes).

## Verify

```bash
dagster asset list | grep stg_s2p_clients_new
dagster asset materialize -m dagster_pipeline.definitions -s stg_s2p_clients_new --partition 2024-12-01
```

## For LLM authors

- Open **`silver/stg/base.py`** and one production file such as **`stg_s2p_clients_new.py`**.  
- If the bronze asset is **`partitioned=False`**, mirror that on the silver asset (see BT conv intermediate).  
- **`gold`** assets consume silver via **`read_silver_from_s3("<stg_name>", context)`** — keep `stg` names aligned with gold expectations.  
- **`conversation_analysis`** is legacy; lakehouse silver should stay under **`defs/data_lakehouse`**.
