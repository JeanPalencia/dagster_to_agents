#!/usr/bin/env python3
"""
Script incremental para actualizar el historial de estados de spots.
Versión 3 - Optimizado para procesar solo cambios incrementales.

Flujo:
1. Lee marca de agua desde GeoSpot (lk_spot_status_history)
2. Consulta MySQL solo por registros nuevos/editados (updated_at >= marca)
3. Identifica spots afectados
4. Reconstruye secuencia solo para esos spots
5. Genera final_spot_status_full_df_v3.parquet (solo cambios)

Uso:
    python update_spot_status_history_v3.py
"""

import pandas as pd
import numpy as np
import os
import sys
import warnings
import pymysql
import psycopg2
from dotenv import dotenv_values

warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")


# =============================================================================
# CONFIGURACIÓN
# =============================================================================
ANLYS_DIR = os.path.abspath(os.path.dirname(__file__))
OUT_DIR = os.path.join(ANLYS_DIR, "out")
ENV_MYSQL_FILE = os.path.join(ANLYS_DIR, ".env.mysql")
ENV_POSTGRES_FILE = os.path.join(ANLYS_DIR, ".env.postgres")

OUTPUT_PARQUET_V3 = os.path.join(OUT_DIR, "final_spot_status_full_df_v3.parquet")

MYSQL_CUT_DATE = "2025-09-12 13:28:00"


# =============================================================================
# QUERIES SQL
# =============================================================================

QUERY_NEW_TRANSITIONS = """
SELECT
  id AS source_sst_id,
  spot_id,
  state,
  reason,
  2 AS source_id,
  'spot_state_transitions' AS source,
  created_at AS source_sst_created_at,
  updated_at AS source_sst_updated_at
FROM spot_state_transitions
WHERE updated_at >= '{watermark}' OR id > {max_id}
ORDER BY spot_id, created_at, id
"""

QUERY_SPOTS_HISTORY = """
WITH ids_last_before AS (
  SELECT
    sst.id,
    ROW_NUMBER() OVER (
      PARTITION BY sst.spot_id
      ORDER BY sst.created_at DESC, sst.id DESC
    ) AS rn
  FROM spot_state_transitions sst
  WHERE sst.created_at < '{cut_date}'
    AND sst.spot_id IN ({spot_ids})
),
last_before_cut AS (
  SELECT sst.*
  FROM spot_state_transitions sst
  JOIN ids_last_before i ON i.id = sst.id
  WHERE i.rn = 1
),
post_cut AS (
  SELECT *
  FROM spot_state_transitions
  WHERE created_at >= '{cut_date}'
    AND spot_id IN ({spot_ids})
),
combined AS (
  SELECT * FROM last_before_cut
  UNION ALL
  SELECT * FROM post_cut
),
sequenced AS (
  SELECT
    c.*,
    LAG(c.state)  OVER (PARTITION BY c.spot_id ORDER BY c.created_at, c.id) AS prev_state,
    LAG(c.reason) OVER (PARTITION BY c.spot_id ORDER BY c.created_at, c.id) AS prev_reason
  FROM combined c
),
dedup AS (
  SELECT s.*
  FROM sequenced s
  WHERE
    s.prev_state IS NULL
    OR s.state <> s.prev_state
    OR NOT (s.reason <=> s.prev_reason)
)
SELECT
  id AS source_sst_id,
  spot_id,
  state,
  reason,
  2 AS source_id,
  'spot_state_transitions' AS source,
  created_at AS source_sst_created_at,
  updated_at AS source_sst_updated_at
FROM dedup
ORDER BY spot_id, created_at, id
"""


# =============================================================================
# FUNCIONES DE PROCESAMIENTO
# =============================================================================

def dedup_consecutive_runs(
    df: pd.DataFrame,
    *,
    spot_col: str = "spot_id",
    time_col: str = "source_sst_created_at",
    state_col: str = "state",
    reason_col: str = "reason",
):
    """
    Elimina repeticiones consecutivas de (state, reason) por spot_id en orden temporal,
    conservando el primer registro (más antiguo) de cada racha.
    """
    if df.empty:
        return df.copy(), df.copy(), df.copy(), df.copy()

    out = df.copy()

    out[time_col] = pd.to_datetime(out[time_col], errors="coerce")
    out = out.reset_index(drop=False).rename(columns={"index": "_row_id"})
    out = out.sort_values([spot_col, time_col, "_row_id"], kind="mergesort")

    state_norm  = out[state_col].astype("object").where(out[state_col].notna(), "__NA__")
    reason_norm = out[reason_col].astype("object").where(out[reason_col].notna(), "__NA__")
    pair = list(zip(state_norm, reason_norm))
    out["_pair"] = pair

    out["_prev_pair"] = out.groupby(spot_col, sort=False)["_pair"].shift(1)
    out["_is_change"] = (out["_pair"] != out["_prev_pair"]) | out["_prev_pair"].isna()

    out["_run_id"] = out.groupby(spot_col, sort=False)["_is_change"].cumsum().astype("Int64")

    out["row_in_run"] = out.groupby([spot_col, "_run_id"], sort=False).cumcount() + 1
    out["run_size"]   = out.groupby([spot_col, "_run_id"], sort=False)[spot_col].transform("size")

    keep_mask = out["row_in_run"] == 1
    cleaned_df = (
        out.loc[keep_mask]
           .drop(columns=["_pair", "_prev_pair", "_is_change", "_run_id", "row_in_run", "run_size"])
           .sort_values([spot_col, time_col, "_row_id"], kind="mergesort")
           .drop(columns=["_row_id"])
           .reset_index(drop=True)
    )

    review_df = out.drop(columns=["_prev_pair"]).rename(columns={"_run_id": "run_id"})
    dropped_df = out.loc[~keep_mask].copy()

    runs_sum = (
        out.groupby([spot_col, "_run_id"], as_index=False)
          .agg(
              from_ts=(time_col, "min"),
              to_ts=(time_col, "max"),
              run_size=("run_size", "first"),
              state_val=(state_col, "first"),
              reason_val=(reason_col, "first"),
          )
          .rename(columns={"_run_id": "run_id"})
          .sort_values([spot_col, "run_id"])
    )

    return cleaned_df, review_df, dropped_df, runs_sum


def ensure_int64_nullable(df, col):
    """Asegura que una columna sea Int64 nullable."""
    if col not in df.columns:
        df[col] = pd.Series([pd.NA] * len(df), dtype="Int64")
    else:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def add_prev_fields(
    df: pd.DataFrame,
    *,
    spot_col: str = "spot_id",
    time_col: str = "source_sst_created_at",
    state_col: str = "state",
    reason_col: str = "reason",
    minutes_as_int: bool = False,
) -> pd.DataFrame:
    """Añade campos de estado previo."""
    out = df.copy()

    out["_orig_pos"] = np.arange(len(out))
    out[time_col] = pd.to_datetime(out[time_col], errors="coerce")
    out = out.sort_values([spot_col, time_col, "_orig_pos"], kind="mergesort")

    g = out.groupby(spot_col, sort=False)
    out["prev_state"] = g[state_col].shift(1)
    out["prev_reason"] = g[reason_col].shift(1)
    out["prev_source_sst_created_at"] = g[time_col].shift(1)

    delta_min = (out[time_col] - out["prev_source_sst_created_at"]).dt.total_seconds() / 60.0
    if minutes_as_int:
        out["minutes_since_prev_state"] = (delta_min // 1).astype("Int64")
    else:
        out["minutes_since_prev_state"] = delta_min

    state_num       = pd.to_numeric(out[state_col], errors="coerce").astype("Int64")
    reason_num      = pd.to_numeric(out[reason_col], errors="coerce").astype("Int64")
    prev_state_num  = pd.to_numeric(out["prev_state"], errors="coerce").astype("Int64")
    prev_reason_num = pd.to_numeric(out["prev_reason"], errors="coerce").astype("Int64")

    out["state_full"] = (state_num * 100 + reason_num.fillna(0)).where(state_num.notna(), pd.NA).astype("Int64")
    out["prev_state_full"] = (prev_state_num * 100 + prev_reason_num.fillna(0)).where(prev_state_num.notna(), pd.NA).astype("Int64")

    out = (
        out.sort_values("_orig_pos")
           .drop(columns=["_orig_pos"])
           .reset_index(drop=True)
    )
    return out


def add_next_fields(
    df: pd.DataFrame,
    *,
    spot_col: str = "spot_id",
    time_col: str = "source_sst_created_at",
    state_col: str = "state",
    reason_col: str = "reason",
    minutes_as_int: bool = False,
) -> pd.DataFrame:
    """Añade campos de estado siguiente."""
    out = df.copy()

    out["_orig_pos"] = np.arange(len(out))
    out[time_col] = pd.to_datetime(out[time_col], errors="coerce")
    out = out.sort_values([spot_col, time_col, "_orig_pos"], kind="mergesort")

    g = out.groupby(spot_col, sort=False)
    out["next_state"] = g[state_col].shift(-1)
    out["next_reason"] = g[reason_col].shift(-1)
    out["next_source_sst_created_at"] = g[time_col].shift(-1)

    delta_min = (out["next_source_sst_created_at"] - out[time_col]).dt.total_seconds() / 60.0
    if minutes_as_int:
        out["minutes_until_next_state"] = (delta_min // 1).astype("Int64")
    else:
        out["minutes_until_next_state"] = delta_min

    state_num       = pd.to_numeric(out[state_col], errors="coerce").astype("Int64")
    reason_num      = pd.to_numeric(out[reason_col], errors="coerce").astype("Int64")
    next_state_num  = pd.to_numeric(out["next_state"], errors="coerce").astype("Int64")
    next_reason_num = pd.to_numeric(out["next_reason"], errors="coerce").astype("Int64")

    out["state_full"] = (state_num * 100 + reason_num.fillna(0)).where(state_num.notna(), pd.NA).astype("Int64")
    out["next_state_full"] = (next_state_num * 100 + next_reason_num.fillna(0)).where(next_state_num.notna(), pd.NA).astype("Int64")

    out = (
        out.sort_values("_orig_pos")
           .drop(columns=["_orig_pos"])
           .reset_index(drop=True)
    )
    return out


def add_sstd_ids(
    df: pd.DataFrame,
    *,
    status_col: str = "spot_status_id",
    reason_col: str = "spot_status_reason_id",
    prev_status_col: str = "prev_spot_status_id",
    prev_reason_col: str = "prev_spot_status_reason_id"
) -> pd.DataFrame:
    """Calcula y agrega los campos de ID de transición (sstd_*)."""
    out = df.copy()

    status = pd.to_numeric(out[status_col], errors='coerce').fillna(0).astype('int64')
    reason = pd.to_numeric(out[reason_col], errors='coerce').fillna(0).astype('int64')
    prev_status = pd.to_numeric(out[prev_status_col], errors='coerce').fillna(0).astype('int64')
    prev_reason = pd.to_numeric(out[prev_reason_col], errors='coerce').fillna(0).astype('int64')

    valid_mask = status > 0
    
    # A. sstd_status_final_id
    sstd_final = (status * 100000)
    out["sstd_status_final_id"] = sstd_final.where(valid_mask, pd.NA).astype("Int64")

    # B. sstd_status_id (excluye Public->Public)
    sstd_status = (status * 100000 + prev_status * 100)
    mask_exclude_simple = (status == 1) & (prev_status == 1)
    out["sstd_status_id"] = sstd_status.where(valid_mask & ~mask_exclude_simple, pd.NA).astype("Int64")

    # C. sstd_status_full_final_id
    sstd_full_final = (status * 100000 + reason * 1000)
    out["sstd_status_full_final_id"] = sstd_full_final.where(valid_mask, pd.NA).astype("Int64")

    # D. sstd_status_full_id (excluye auto-transiciones idénticas)
    full_current_val = (status * 100000 + reason * 1000)
    full_prev_val = (prev_status * 100000 + prev_reason * 1000)
    
    sstd_full = (status * 100000 + reason * 1000 + prev_status * 100 + prev_reason)
    mask_exclude_full = (full_current_val == full_prev_val)
    
    out["sstd_status_full_id"] = sstd_full.where(valid_mask & ~mask_exclude_full, pd.NA).astype("Int64")

    return out


# =============================================================================
# CONEXIONES
# =============================================================================

def get_postgres_connection():
    config = dotenv_values(ENV_POSTGRES_FILE)
    return psycopg2.connect(
        host=config.get('DB_HOST', 'localhost'),
        port=int(config.get('DB_PORT', 5432)),
        user=config.get('DB_USER'),
        password=config.get('DB_PASSWORD'),
        database=config.get('DB_NAME'),
    )


def get_mysql_connection():
    config = dotenv_values(ENV_MYSQL_FILE)
    return pymysql.connect(
        host=config.get('DB_HOST', 'localhost'),
        port=int(config.get('DB_PORT', 3306)),
        user=config.get('DB_USER'),
        password=config.get('DB_PASSWORD'),
        database=config.get('DB_NAME'),
        cursorclass=pymysql.cursors.DictCursor
    )


# =============================================================================
# LECTURA DESDE GEOSPOT
# =============================================================================

def get_watermark_from_geospot():
    """Obtiene la marca de agua (timestamp, max_id) desde GeoSpot."""
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT MAX(source_sst_updated_at), MAX(source_sst_id)
                FROM lk_spot_status_history
                WHERE source_id = 2
            """)
            result = cursor.fetchone()
            if result and result[0]:
                return pd.Timestamp(result[0]), int(result[1])
            return None, None
    finally:
        conn.close()


def get_snapshots_for_spots_from_geospot(spot_ids: set) -> pd.DataFrame:
    """Obtiene registros de snapshots desde GeoSpot para spots específicos."""
    if not spot_ids:
        return pd.DataFrame()

    conn = get_postgres_connection()
    try:
        ids_str = ",".join(str(int(sid)) for sid in spot_ids)
        query = f"""
            SELECT
                source_sst_id, spot_id,
                spot_status_id AS state,
                spot_status_reason_id AS reason,
                source_id, source,
                source_sst_created_at, source_sst_updated_at
            FROM lk_spot_status_history
            WHERE source_id != 2
              AND spot_id IN ({ids_str})
            ORDER BY spot_id, source_sst_created_at
        """
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    if df.empty:
        return df

    df["source_sst_id"] = df["source_sst_id"].astype("object").where(df["source_sst_id"].notna(), None)
    df["spot_id"] = pd.to_numeric(df["spot_id"], errors="coerce").astype("Int64")
    df["state"] = pd.to_numeric(df["state"], errors="coerce").astype("Int64")
    df["reason"] = pd.to_numeric(df["reason"], errors="coerce").astype("Int64")
    df["source_id"] = pd.to_numeric(df["source_id"], errors="coerce").astype("Int64")
    df["source"] = df["source"].astype("str")
    df["source_sst_created_at"] = pd.to_datetime(df["source_sst_created_at"])
    df["source_sst_updated_at"] = pd.to_datetime(df["source_sst_updated_at"])

    return df


# =============================================================================
# LECTURA DESDE MYSQL
# =============================================================================

def get_new_records_from_mysql(watermark: pd.Timestamp, max_id: int) -> pd.DataFrame:
    """Obtiene registros nuevos/editados desde la marca de agua o con id mayor al máximo conocido."""
    conn = get_mysql_connection()
    try:
        watermark_str = watermark.strftime("%Y-%m-%d %H:%M:%S")
        query = QUERY_NEW_TRANSITIONS.format(watermark=watermark_str, max_id=max_id)
        
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
    finally:
        conn.close()
    
    return _normalize_mysql_types(df)


def get_records_for_spots_from_mysql(spot_ids: set) -> pd.DataFrame:
    """Obtiene registros de MySQL para spots específicos."""
    if not spot_ids:
        return pd.DataFrame()
    
    conn = get_mysql_connection()
    try:
        ids_str = ",".join(str(int(sid)) for sid in spot_ids)
        query = QUERY_SPOTS_HISTORY.format(cut_date=MYSQL_CUT_DATE, spot_ids=ids_str)
        
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
    finally:
        conn.close()
    
    return _normalize_mysql_types(df)


def _normalize_mysql_types(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza tipos de datos de MySQL."""
    if df.empty:
        return df
    df["source_sst_id"] = df["source_sst_id"].astype('Int64')
    df["spot_id"] = df["spot_id"].astype('Int64')
    df["state"] = df["state"].astype('Int64')
    df["reason"] = df["reason"].astype('Int64')
    df["source_id"] = df["source_id"].astype('Int64')
    df["source"] = df["source"].astype('str')
    df["source_sst_created_at"] = pd.to_datetime(df["source_sst_created_at"])
    df["source_sst_updated_at"] = pd.to_datetime(df["source_sst_updated_at"])
    return df


# =============================================================================
# RECONSTRUCCIÓN
# =============================================================================

def rebuild_affected_spots(
    df_new_records: pd.DataFrame,
) -> pd.DataFrame:
    """Reconstruye la secuencia completa para los spots afectados."""
    affected_spot_ids = set(df_new_records["spot_id"].unique())
    
    print(f"    -> {len(affected_spot_ids)} spots afectados")
    
    print(f"    -> Consultando historial desde MySQL...")
    df_sst_affected = get_records_for_spots_from_mysql(affected_spot_ids)
    print(f"    -> {len(df_sst_affected)} registros de sst")
    
    print(f"    -> Consultando snapshots desde GeoSpot...")
    df_snapshots_affected = get_snapshots_for_spots_from_geospot(affected_spot_ids)
    print(f"    -> {len(df_snapshots_affected)} registros de snapshots")
    
    combined = pd.concat([df_snapshots_affected, df_sst_affected], ignore_index=True)
    combined = ensure_int64_nullable(combined, "source_sst_id")
    
    clean_df, _, _, _ = dedup_consecutive_runs(combined)
    clean_df = add_prev_fields(clean_df)
    clean_df = add_next_fields(clean_df)
    clean_df = add_sstd_ids(
        clean_df,
        status_col="state",
        reason_col="reason",
        prev_status_col="prev_state",
        prev_reason_col="prev_reason"
    )
    
    final_columns = [
        "source_sst_id", "spot_id", "state", "reason", "state_full",
        "prev_state", "prev_reason", "prev_state_full",
        "next_state", "next_reason", "next_state_full",
        "sstd_status_final_id", "sstd_status_id", "sstd_status_full_final_id", "sstd_status_full_id",
        "prev_source_sst_created_at", "next_source_sst_created_at",
        "minutes_since_prev_state", "minutes_until_next_state",
        "source_sst_created_at", "source_sst_updated_at", "source_id", "source"
    ]
    
    result = clean_df[final_columns].copy()
    result = result.rename(columns={
        "state": "spot_status_id",
        "reason": "spot_status_reason_id",
        "prev_state": "prev_spot_status_id",
        "prev_reason": "prev_spot_status_reason_id",
        "state_full": "spot_status_full_id",
        "prev_state_full": "prev_spot_status_full_id",
        "next_state": "next_spot_status_id",
        "next_reason": "next_spot_status_reason_id",
        "next_state_full": "next_spot_status_full_id"
    })
    
    print(f"    -> {len(result)} registros reconstruidos")
    return result


# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def update_incremental():
    """Ejecuta el pipeline incremental."""
    print("="*60)
    print("ACTUALIZACIÓN INCREMENTAL DE SPOT STATUS HISTORY (V3)")
    print("="*60)
    
    # 1. Marca de agua
    print("\n[1/6] Obteniendo marca de agua desde GeoSpot...")
    watermark, max_id = get_watermark_from_geospot()
    
    if watermark is None:
        print("    ERROR: No hay registros de spot_state_transitions en GeoSpot.")
        return None
    
    print(f"    -> Watermark: {watermark}  |  Max ID: {max_id:,}")
    
    # 2. Registros nuevos/editados
    print("\n[2/6] Consultando registros nuevos/editados desde MySQL...")
    df_new_records = get_new_records_from_mysql(watermark, max_id)
    
    if df_new_records.empty:
        print("    -> No hay cambios.")
        pd.DataFrame().to_parquet(OUTPUT_PARQUET_V3, index=False)
        print(f"    -> Guardado (vacío): {OUTPUT_PARQUET_V3}")
        return pd.DataFrame()
    
    print(f"    -> {len(df_new_records):,} registros nuevos/editados")
    
    # 3. Identificar spots afectados
    print("\n[3/6] Identificando spots afectados...")
    affected_spot_ids = set(df_new_records["spot_id"].unique())
    print(f"    -> {len(affected_spot_ids)} spots afectados")
    
    # 4. Reconstruir
    print("\n[4/6] Reconstruyendo spots afectados...")
    df_rebuilt = rebuild_affected_spots(df_new_records)
    
    # 5. Flag de hard-delete: consultar spots activos en GeoSpot (lk_spots)
    print("\n[5/6] Aplicando flag de hard-delete...")
    conn_pg = get_postgres_connection()
    try:
        with conn_pg.cursor() as cur:
            cur.execute("SELECT spot_id FROM lk_spots")
            spots_active = {row[0] for row in cur.fetchall()}
    finally:
        conn_pg.close()
    df_rebuilt["spot_hard_deleted_id"] = (
        df_rebuilt["spot_id"]
        .apply(lambda x: 0 if x in spots_active else 1)
        .astype("Int64")
    )
    df_rebuilt["spot_hard_deleted"] = (
        df_rebuilt["spot_hard_deleted_id"].map({0: "No", 1: "Yes"})
    )
    n_deleted = df_rebuilt["spot_hard_deleted_id"].sum()
    n_spots_deleted = df_rebuilt.loc[
        df_rebuilt["spot_hard_deleted_id"] == 1, "spot_id"
    ].nunique()
    print(f"    -> {n_spots_deleted:,} spots hard-deleted ({n_deleted:,} registros)")

    # 6. Guardar
    print("\n[6/6] Guardando...")
    df_rebuilt.to_parquet(OUTPUT_PARQUET_V3, index=False)
    print(f"    -> {OUTPUT_PARQUET_V3} ({len(df_rebuilt):,} registros)")
    
    print("\n" + "="*60)
    print("PROCESO COMPLETADO")
    print("="*60)
    
    return df_rebuilt


if __name__ == "__main__":
    update_incremental()
