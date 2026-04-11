#!/usr/bin/env python3
"""
Script autocontenido para calcular métricas históricas de spots
y guardarlas en out/dm_spot_historical_metrics.parquet.

Fuentes (todas desde GeoSpot/PostgreSQL):
  - lk_spot_status_history  (historial de estados)
  - lk_spots                (dimensiones del spot)
  - lk_spot_status_dictionary (nombres de status/reason/full)

Uso:
  python update_dm_spot_historical_metrics.py
"""

import gc
import os
import sys
import warnings

import pandas as pd
import psycopg2
from dotenv import dotenv_values

warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
ANLYS_DIR = os.path.abspath(os.path.dirname(__file__))
OUT_DIR = os.path.join(ANLYS_DIR, "out")
ENV_POSTGRES_FILE = os.path.join(ANLYS_DIR, ".env.postgres")

OUTPUT_PARQUET = os.path.join(OUT_DIR, "dm_spot_historical_metrics.parquet")


# =============================================================================
# CONEXIÓN
# =============================================================================

def get_postgres_connection():
    config = dotenv_values(ENV_POSTGRES_FILE)
    return psycopg2.connect(
        host=config.get("DB_HOST", "localhost"),
        port=int(config.get("DB_PORT", 5432)),
        user=config.get("DB_USER"),
        password=config.get("DB_PASSWORD"),
        database=config.get("DB_NAME"),
    )


# =============================================================================
# LECTURA DESDE GEOSPOT
# =============================================================================

def _ensure_int64(df):
    """Convierte todas las columnas *_id a Int64 nullable."""
    for col in df.columns:
        if col.endswith("_id"):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def load_history():
    conn = get_postgres_connection()
    try:
        df = pd.read_sql("SELECT * FROM lk_spot_status_history", conn)
    finally:
        conn.close()
    return _ensure_int64(df)


def load_spots():
    conn = get_postgres_connection()
    try:
        df = pd.read_sql("SELECT * FROM lk_spots", conn)
    finally:
        conn.close()
    return _ensure_int64(df)


def load_status_dict():
    conn = get_postgres_connection()
    try:
        df = pd.read_sql("SELECT * FROM lk_spot_status_dictionary", conn)
    finally:
        conn.close()
    return _ensure_int64(df)


# =============================================================================
# CÁLCULO DE MÉTRICAS
# =============================================================================

def compute_historical_metrics(
    df_history: pd.DataFrame,
    df_spots: pd.DataFrame,
    df_status_dict: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calcula métricas históricas de spots replicando la lógica de metrics.sql.

    Para cada registro de estado, determina en qué periodos temporales estuvo
    activo y agrega conteos y áreas por múltiples dimensiones.
    """

    # =========================================================================
    # PASO 1: JOIN inicial (equivalente a CTE sshe)
    # =========================================================================

    spot_dimension_cols = [
        "spot_id", "spot_area_in_sqm",
        "spot_type_id", "spot_type",
        "spot_sector_id", "spot_sector",
        "spot_state_id", "spot_state",
        "spot_municipality_id", "spot_municipality",
        "spot_modality_id", "spot_modality",
        "user_affiliation_id", "user_affiliation",
        "user_industria_role_id", "user_industria_role",
        "spot_origin_id", "spot_origin",
    ]

    spot_cols_available = [c for c in spot_dimension_cols if c in df_spots.columns]

    sshe = df_history.merge(
        df_spots[spot_cols_available],
        on="spot_id",
        how="inner",
    )

    # LEFT JOINs para nombres de status
    dict_status = df_status_dict[df_status_dict["spot_status_id"].notna()][
        ["spot_status_id", "spot_status"]
    ].drop_duplicates(subset=["spot_status_id"])

    sshe = sshe.merge(dict_status, on="spot_status_id", how="left")

    dict_reason = df_status_dict[df_status_dict["spot_status_reason_id"].notna()][
        ["spot_status_reason_id", "spot_status_reason"]
    ].drop_duplicates(subset=["spot_status_reason_id"])

    sshe = sshe.merge(dict_reason, on="spot_status_reason_id", how="left")

    dict_full = df_status_dict[df_status_dict["spot_status_full_id"].notna()][
        ["spot_status_full_id", "spot_status_full"]
    ].drop_duplicates(subset=["spot_status_full_id"])

    sshe = sshe.merge(dict_full, on="spot_status_full_id", how="left")

    for col in [c for c in sshe.columns if c.endswith("_id")]:
        sshe[col] = pd.to_numeric(sshe[col], errors="coerce").astype("Int64")

    # =========================================================================
    # PASO 2: Preparar datos temporales
    # =========================================================================

    sshe["source_sst_created_at"] = pd.to_datetime(sshe["source_sst_created_at"], errors="coerce")
    sshe["next_source_sst_created_at"] = pd.to_datetime(sshe["next_source_sst_created_at"], errors="coerce")

    sshe["state_start"] = sshe["source_sst_created_at"]
    sshe["state_end"] = sshe["next_source_sst_created_at"].fillna(pd.Timestamp.now())

    dimension_cols = [
        "spot_type_id", "spot_type",
        "spot_sector_id", "spot_sector",
        "spot_state_id", "spot_state",
        "spot_municipality_id", "spot_municipality",
        "spot_modality_id", "spot_modality",
        "user_affiliation_id", "user_affiliation",
        "user_industria_role_id", "user_industria_role",
        "spot_origin_id", "spot_origin",
        "spot_status_id", "spot_status",
        "spot_status_reason_id", "spot_status_reason",
        "spot_status_full_id", "spot_status_full",
    ]

    dimension_cols = [c for c in dimension_cols if c in sshe.columns]

    keep_cols = ["spot_id", "spot_area_in_sqm", "state_start", "state_end"] + dimension_cols
    sshe = sshe[keep_cols].copy()

    # =========================================================================
    # PASO 3: Procesar cada timeframe por separado
    # =========================================================================

    timeframes_config = [
        (1, "Day", "D"),
        (2, "Week", "W"),
        (3, "Month", "M"),
        (4, "Quarter", "Q"),
        (5, "Year", "Y"),
    ]

    all_results = []

    for tf_id, tf_name, period_code in timeframes_config:
        print(f"  Procesando timeframe: {tf_name}...")

        df_work = sshe.copy()

        df_work["period_start"] = df_work["state_start"].dt.to_period(period_code).dt.to_timestamp()
        df_work["period_end"] = df_work["state_end"].dt.to_period(period_code).dt.to_timestamp()

        if period_code == "D":
            df_work["n_periods"] = ((df_work["period_end"] - df_work["period_start"]).dt.days + 1).clip(lower=1)
        elif period_code == "W":
            df_work["n_periods"] = (((df_work["period_end"] - df_work["period_start"]).dt.days // 7) + 1).clip(lower=1)
        elif period_code == "M":
            df_work["n_periods"] = (
                (df_work["period_end"].dt.year - df_work["period_start"].dt.year) * 12
                + (df_work["period_end"].dt.month - df_work["period_start"].dt.month)
                + 1
            ).clip(lower=1)
        elif period_code == "Q":
            df_work["n_periods"] = (
                (df_work["period_end"].dt.year - df_work["period_start"].dt.year) * 4
                + (df_work["period_end"].dt.quarter - df_work["period_start"].dt.quarter)
                + 1
            ).clip(lower=1)
        elif period_code == "Y":
            df_work["n_periods"] = (
                df_work["period_end"].dt.year - df_work["period_start"].dt.year + 1
            ).clip(lower=1)

        max_periods = {"D": 3650, "W": 520, "M": 120, "Q": 40, "Y": 10}
        df_work["n_periods"] = df_work["n_periods"].clip(upper=max_periods[period_code])

        def generate_periods(row):
            start = row["period_start"]
            n = int(row["n_periods"])
            if period_code == "D":
                return [start + pd.Timedelta(days=i) for i in range(n)]
            elif period_code == "W":
                return [start + pd.Timedelta(weeks=i) for i in range(n)]
            elif period_code == "M":
                return [start + pd.DateOffset(months=i) for i in range(n)]
            elif period_code == "Q":
                return [start + pd.DateOffset(months=3 * i) for i in range(n)]
            elif period_code == "Y":
                return [start + pd.DateOffset(years=i) for i in range(n)]

        chunk_size = 50000
        chunks = []

        for i in range(0, len(df_work), chunk_size):
            chunk = df_work.iloc[i : i + chunk_size].copy()
            chunk["timeframe_timestamps"] = chunk.apply(generate_periods, axis=1)
            chunk = chunk.explode("timeframe_timestamps")
            chunk["timeframe_timestamp"] = chunk["timeframe_timestamps"]
            chunk = chunk.drop(columns=["timeframe_timestamps", "period_start", "period_end", "n_periods"])
            chunks.append(chunk)

        df_expanded = pd.concat(chunks, ignore_index=True)
        del chunks
        gc.collect()

        df_expanded["timeframe_id"] = tf_id
        df_expanded["timeframe"] = tf_name

        # Deduplicación por spot
        groupby_cols_per_spot = (
            ["timeframe_id", "timeframe", "timeframe_timestamp"]
            + dimension_cols
            + ["spot_id"]
        )

        per_spot = df_expanded.groupby(groupby_cols_per_spot, as_index=False, dropna=False).agg(
            spot_area_in_sqm=("spot_area_in_sqm", "max")
        )

        del df_expanded
        gc.collect()

        # Agregación final
        groupby_cols_final = (
            ["timeframe_id", "timeframe", "timeframe_timestamp"]
            + dimension_cols
        )

        tf_result = per_spot.groupby(groupby_cols_final, as_index=False, dropna=False).agg(
            spot_count=("spot_id", "count"),
            spot_total_sqm=("spot_area_in_sqm", "sum"),
        )

        all_results.append(tf_result)

        del per_spot, df_work, tf_result
        gc.collect()

        print(f"    -> {tf_name} completado")

    # =========================================================================
    # PASO 4: Combinar resultados
    # =========================================================================

    result = pd.concat(all_results, ignore_index=True)
    del all_results
    gc.collect()

    sort_cols = ["timeframe_id", "timeframe_timestamp"]
    for col in ["spot_type_id", "spot_sector_id", "spot_state_id", "spot_municipality_id"]:
        if col in result.columns:
            sort_cols.append(col)

    result = result.sort_values(sort_cols, na_position="last").reset_index(drop=True)

    for col in [c for c in result.columns if c.endswith("_id")]:
        result[col] = pd.to_numeric(result[col], errors="coerce").astype("Int64")

    return result


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("ACTUALIZACIÓN DE MÉTRICAS HISTÓRICAS DE SPOTS")
    print("=" * 60)

    # 1. Leer datos desde GeoSpot
    print("\n[1/3] Leyendo datos desde GeoSpot...")

    df_history = load_history()
    print(f"  -> lk_spot_status_history: {len(df_history):,} registros")

    df_spots = load_spots()
    print(f"  -> lk_spots: {len(df_spots):,} registros")

    df_status_dict = load_status_dict()
    print(f"  -> lk_spot_status_dictionary: {len(df_status_dict):,} registros")

    # 2. Calcular métricas
    print("\n[2/3] Calculando métricas históricas...")
    metrics_df = compute_historical_metrics(df_history, df_spots, df_status_dict)
    print(f"\n  -> {len(metrics_df):,} filas de métricas generadas")

    del df_history, df_spots, df_status_dict
    gc.collect()

    # 3. Guardar parquet
    print(f"\n[3/3] Guardando parquet...")
    metrics_df.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"  -> {OUTPUT_PARQUET}")

    for tf_id, tf_name in [(1, "Day"), (2, "Week"), (3, "Month"), (4, "Quarter"), (5, "Year")]:
        n = len(metrics_df[metrics_df["timeframe_id"] == tf_id])
        print(f"  -> {tf_name}: {n:,} filas")

    print(f"  -> Total: {len(metrics_df):,} filas")

    print("\n" + "=" * 60)
    print("PROCESO COMPLETADO")
    print("=" * 60)


if __name__ == "__main__":
    main()
