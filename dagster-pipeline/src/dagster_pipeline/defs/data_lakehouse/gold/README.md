# Gold layer — quick reference

Gold assets build **curated / governance** tables: read from **silver** (and sometimes other gold), transform with **Polars**, attach **audit columns**, write **Parquet** to S3, and often **load to Geospot** (PostgreSQL) via the shared API.

## Location

- **Governance `_new` assets:** `gold/gold_*_new.py` (e.g. `gold_lk_leads_new`, `gold_bt_conv_conversations_new`)  
- **Shared helpers:** `gold/utils.py` (`add_audit_fields`, `match_conversations_to_clients`, `group_messages_into_conversations`, …)  
- **BT conv variables (legacy bridge):** `gold/bt_conv_variables/` — `config.py`, `vendor.py` (re-exports `conversation_analysis`), `phone_formats.py`, `dspy_schema.py`  
- **BT conv JSON flow:** `gold/conv_variables_flow.py`  
- **QA / diagnostics:** `gold/qa_gold.py`  

Legacy non-`_new` gold files (e.g. `gold_lk_leads.py`) may still exist for comparison; prefer **`_*_new`** for governance.

## Typical gold asset pattern

1. **`@dg.asset`**, `group_name="gold"`, **`io_manager_key="s3_gold"`**  
2. **`read_silver_from_s3("stg_...", context)`** (and other silvers as needed)  
3. **Transform** → Polars DataFrame matching the **target table contract** (column order matters for some loads)  
4. **`add_audit_fields(df, job_name="...")`** — sets `aud_inserted_*`, `aud_updated_*`, `aud_job`  
5. **`build_gold_s3_key(...)`** + **`write_polars_to_s3`**  
6. **`load_to_geospot(...)`** when the table is published (see existing assets for `mode`: `replace` vs `upsert`)  

Stale-input handling and notifications are copied from existing `gold_*_new.py` files (`read_silver_from_s3` metadata, `send_stale_data_notification`).

## Column order and schema

Many gold assets define a **tuple constant** (e.g. `LK_LEADS_LEGACY_COLUMN_ORDER`, `GOLD_LK_SPOTS_GOVERNANCE_ORDER`) so Parquet/COPY column order matches **Postgres / Geospot**. When adding columns:

- Extend the constant and any **final `select`/reorder** step.  
- Update **`docs/golden_tables/`** if your project keeps DDL docs there.  

## S3 IO manager mapping

`s3_gold_io_manager.py` maps some asset names to **logical table names** used in `build_gold_s3_key`. If you add a **new** gold asset that participates in the same IO pattern, you may need to extend **`GOLD_ASSET_TO_TABLE`**.

## Jobs and schedules

**`data_lakehouse/jobs.py`** defines `define_asset_job` entries (bronze/silver/gold chains, per-table jobs). New gold tables usually need:

- A **selection** (`AssetSelection.assets(...)`)  
- A **job** wired into schedules or sensors (e.g. `per_gold_chain_sensor`)  

**BT conversations** gold/bronze/silver jobs are often **unpartitioned**; most lk_* chains use **daily partitions**.

## BT conversations (high level)

- **Silver:** `silver/stg/stg_bt_conv_intermediate.py` → merged conversations  
- **Gold:** `gold_bt_conv_conversations_new.py` → funnel, projects, **`run_conv_variables_flow`**, upsert merge, Geospot  
- **Seniority:** reads **`lk_projects`** from Geospot in-process (`read_governance_from_db`) — run **`lk_projects`** gold before conv if you need `seniority_level`  

## For LLM authors

1. **Copy the closest `gold_*_new.py`** (same domain: leads, spots, conv, …).  
2. Keep **imports** from `shared.py` consistent (`read_silver_from_s3`, `load_to_geospot`, `resolve_stale_reference_date`).  
3. **`conversation_analysis`** reads **`bt_conv_conversations`** and loads its CSV output to **`bt_conv_conversations_bck`** only; BT conv reuses its logic through **`gold/bt_conv_variables/vendor.py`**.  
4. After adding assets, update **`jobs.py`** (and **`s3_gold_io_manager.GOLD_ASSET_TO_TABLE`** if applicable).  
5. Run **`dagster asset list`** / materialize a single asset to validate wiring.

## Verify

```bash
dagster asset list | grep gold_lk_leads_new
dagster asset materialize -m dagster_pipeline.definitions -s gold_lk_leads_new --partition 2024-12-01
```

Unpartitioned jobs omit `--partition` and use run config as required by your deployment.
