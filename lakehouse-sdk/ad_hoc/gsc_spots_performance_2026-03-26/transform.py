"""
Transformación: extrae spot_id y enriquece con datos de lk_spots (Geospot).

Patrones de URL cubiertos:
  - https://spot2.mx/spots/{slug}/{spot_id}
  - https://spot2.mx/spots/{spot_id}

Lee desde raw_gsc_spots.parquet (generado por query.py).
JOIN con lk_spots para agregar: spot_type, spot_sector, spot_status_full, spot_modality, spot_address.
Genera transformed_gsc_spots.parquet.
"""

import sys
import polars as pl
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "dagster-pipeline" / "src"))
from dagster_pipeline.defs.data_lakehouse.shared import _query_geospot_postgres_to_polars

DATA_DIR = Path(__file__).parent
RAW_PARQUET = DATA_DIR / "raw_gsc_spots.parquet"
OUT_PARQUET = DATA_DIR / "transformed_gsc_spots.parquet"
OUT_CSV = DATA_DIR / "transformed_gsc_spots.csv"

LK_SPOTS_QUERY = """
SELECT spot_id, spot_type, spot_sector, spot_status_full, spot_modality, spot_address
FROM lk_spots
"""


def main():
    df = pl.read_parquet(RAW_PARQUET)
    print(f"Registros leídos: {df.height:,}\n")

    # Extraer spot_id del segmento numérico después de /spots/
    df = df.with_columns(
        pl.col("url")
        .str.extract(r"/spots/(?:.+/)?(\d+)/?(?:\?.*)?$", 1)
        .cast(pl.Int64, strict=False)
        .alias("spot_id")
    )

    # --- Validación spot_id ---
    total = df.height
    nulls = df.filter(pl.col("spot_id").is_null()).height
    print(f"spot_id extraídos: {total - nulls:,} / {total:,} ({(total - nulls) / total * 100:.2f}%)")
    if nulls > 0:
        print(f"  Sin spot_id: {nulls} (quedan como null)\n")

    # --- JOIN con lk_spots ---
    print("Consultando lk_spots en Geospot...")
    df_spots = _query_geospot_postgres_to_polars(LK_SPOTS_QUERY)
    df_spots = df_spots.with_columns(pl.col("spot_id").cast(pl.Int64))
    print(f"  lk_spots: {df_spots.height:,} registros\n")

    # INNER JOIN: descarta IDs falsos (extraídos del slug) y URLs sin spot_id
    df_out = (
        df.join(df_spots, on="spot_id", how="inner")
        .select(
            "spot_id",
            "spot_type",
            "spot_sector",
            "spot_status_full",
            "spot_modality",
            "spot_address",
            "url",
            "impresiones",
            "clicks",
            "avg_posicion",
            "mes",
        )
    )

    dropped = total - df_out.height
    print(f"INNER JOIN con lk_spots:")
    print(f"  Registros finales: {df_out.height:,}")
    print(f"  Descartados:       {dropped:,} (IDs falsos o sin match)\n")

    df_out.write_parquet(OUT_PARQUET)
    df_out.write_csv(OUT_CSV)
    print(f"Guardado en: {OUT_PARQUET}")
    print(f"Guardado en: {OUT_CSV}\n")

    import pandas as pd
    pd.set_option("display.max_colwidth", 80)
    pd.set_option("display.width", 260)
    print(df_out.head(15).to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
