#!/usr/bin/env python3
"""
Script para consultar información de tablas en S3.

Uso:
    python scripts/query_s3_tables.py bronze raw_s2p_clients
    python scripts/query_s3_tables.py silver stg_s2p_clients
    python scripts/query_s3_tables.py gold lk_projects 2024-12-01
"""
import sys
from pathlib import Path

# Agregar el path del proyecto
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from dagster_pipeline.defs.data_lakehouse.shared import (
    get_table_info_from_s3,
    get_table_info_from_s3_gold,
    read_polars_from_s3,
    build_bronze_s3_key,
    build_silver_s3_key,
)


def print_table_info(info: dict):
    """Imprime información de una tabla de forma legible."""
    print(f"\n{'='*60}")
    print(f"S3 Path: {info['s3_path']}")
    print(f"{'='*60}")
    
    if not info.get("exists", False):
        print("❌ Archivo no encontrado en S3")
        if "error" in info:
            print(f"Error: {info['error']}")
        return
    
    print(f"✅ Archivo existe")
    print(f"📊 Filas: {info['rows']:,}")
    print(f"📋 Columnas: {info['columns']}")
    
    if "partition" in info:
        print(f"📅 Partición: {info['partition']}")
    
    if info['column_names']:
        print(f"\nColumnas ({len(info['column_names'])}):")
        for i, col in enumerate(info['column_names'], 1):
            print(f"  {i:2d}. {col}")


def main():
    if len(sys.argv) < 3:
        print("Uso:")
        print("  python scripts/query_s3_tables.py <layer> <table_name> [partition_key]")
        print("\nEjemplos:")
        print("  python scripts/query_s3_tables.py bronze raw_s2p_clients")
        print("  python scripts/query_s3_tables.py silver stg_s2p_clients")
        print("  python scripts/query_s3_tables.py gold lk_projects 2024-12-01")
        sys.exit(1)
    
    layer = sys.argv[1]
    table_name = sys.argv[2]
    partition_key = sys.argv[3] if len(sys.argv) > 3 else None
    
    try:
        if layer == "gold":
            if not partition_key:
                print("❌ Error: Para gold necesitas especificar partition_key")
                print("   Ejemplo: python scripts/query_s3_tables.py gold lk_projects 2024-12-01")
                sys.exit(1)
            info = get_table_info_from_s3_gold(table_name, partition_key)
        else:
            info = get_table_info_from_s3(layer, table_name)
        
        print_table_info(info)
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
