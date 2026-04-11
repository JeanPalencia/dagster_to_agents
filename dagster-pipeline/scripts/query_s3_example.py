#!/usr/bin/env python3
"""
Ejemplo de cómo consultar archivos Parquet en S3 directamente.

Uso:
    python scripts/query_s3_example.py
"""
import boto3
import polars as pl
import io

# Configuración
S3_BUCKET = "lakehouse-s2p"
REGION = "us-east-1"

# Cliente S3
s3 = boto3.client("s3", region_name=REGION)

# Ejemplo 1: Leer tabla bronze
print("=" * 60)
print("Ejemplo 1: Leer raw_s2p_clients desde bronze")
print("=" * 60)
s3_key_bronze = "bronze/raw_s2p_clients/data.parquet"
obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key_bronze)
df_bronze = pl.read_parquet(io.BytesIO(obj["Body"].read()))
print(f"Filas: {df_bronze.height:,}")
print(f"Columnas: {df_bronze.width}")
print(f"\nPrimeras 5 filas:")
print(df_bronze.head())

# Ejemplo 2: Leer tabla silver
print("\n" + "=" * 60)
print("Ejemplo 2: Leer stg_s2p_clients desde silver")
print("=" * 60)
s3_key_silver = "silver/stg_s2p_clients/data.parquet"
obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key_silver)
df_silver = pl.read_parquet(io.BytesIO(obj["Body"].read()))
print(f"Filas: {df_silver.height:,}")
print(f"Columnas: {df_silver.width}")
print(f"\nPrimeras 5 filas:")
print(df_silver.head())

# Ejemplo 3: Hacer queries con Polars
print("\n" + "=" * 60)
print("Ejemplo 3: Query - Contar por origin")
print("=" * 60)
if "origin" in df_silver.columns:
    result = df_silver.group_by("origin").agg(pl.count().alias("count"))
    print(result)

# Ejemplo 4: Filtrar datos
print("\n" + "=" * 60)
print("Ejemplo 4: Filtrar - Clientes con email")
print("=" * 60)
if "email" in df_silver.columns:
    with_email = df_silver.filter(pl.col("email").is_not_null())
    print(f"Clientes con email: {with_email.height:,} de {df_silver.height:,}")
