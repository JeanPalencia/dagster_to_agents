---
description: Contexto del pipeline Spot State Transitions (SST) para agentes IA
globs: dagster-pipeline/src/dagster_pipeline/defs/spot_state_transitions/**
alwaysApply: false
---

# Spot State Transitions (SST) Pipeline

Documentación completa en `dagster-pipeline/src/dagster_pipeline/defs/spot_state_transitions/README.md`.
Lineamientos de arquitectura en `dagster-pipeline/ARCHITECTURE.md`.

## Reglas rápidas

- **Layers**: Bronze (SQL inline) → STG (cast tipos) → Core (lógica pura) → Gold (audit + flags) → Publish (I/O)
- **STG 1:1**: cada `raw_*` tiene exactamente un `stg_*`. Los `rawi_*` no tienen STG.
- **Core sin I/O**: `core_sst_rebuild_history` usa funciones puras de `processing.py` (dedup, prev/next, sstd IDs).
- **processing.py**: contiene `TRANSITIONS_SCHEMA`, `FINAL_COLUMNS`, `FINAL_RENAME` y 4 funciones. Solo sirve a Core.
- **Naming**: groups = `sst_bronze`, `sst_silver`, `sst_gold`, `sst_publish`.
- **Job**: upstream de `lk_spot_status_history_to_s3` + `cleanup_storage`. Schedule diario 9:00 AM México.

## Flujo resumido

1. `rawi_gs_sst_watermark` → watermark dual (timestamp + max_id) desde GeoSpot
2. `rawi_s2p_sst_new_transitions` → nuevas transiciones MySQL (updated_at >= watermark OR id > max_id)
3. `raw_s2p_sst_affected_history` → historial completo MySQL de spots afectados (CTE con cut_date)
4. `raw_gs_sst_snapshots` → snapshots GeoSpot (source_id != 2) de spots afectados
5. `raw_gs_active_spot_ids` → todos los spot_id de lk_spots (independiente)
6-8. STG: cast a TRANSITIONS_SCHEMA / Int64
9. `core_sst_rebuild_history` → concat + dedup + prev/next + sstd IDs + rename
10. `gold_lk_spot_status_history` → hard-delete flag + audit fields
11. `lk_spot_status_history_to_s3` → CSV a S3

## Validación

Tests en `lakehouse-sdk/tests/spot_state_transitions/validate_sst_dagster.py`:
compara pipeline Pandas original vs Dagster/Polars row-by-row.
