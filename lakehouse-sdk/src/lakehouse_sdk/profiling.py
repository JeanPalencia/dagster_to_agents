"""
Data profiling utilities for exploring DataFrames
"""
import polars as pl
from typing import Optional


def quick_profile(df: pl.DataFrame, name: str = "DataFrame") -> None:
    """
    Quick profiling of a DataFrame with basic statistics.
    
    Shows:
    - Dimensions (rows x columns)
    - Complete schema
    - Nulls per column
    - First 5 rows
    
    Args:
        df: Polars DataFrame
        name: Descriptive name for output
    
    Example:
        >>> quick_profile(df_clients, "Raw Clients")
    """
    print(f"\n{'='*60}")
    print(f"📊 {name}")
    print(f"{'='*60}")
    print(f"Rows: {df.height:,} | Columns: {df.width}")
    
    print(f"\n📋 Schema:")
    for col, dtype in df.schema.items():
        print(f"  {col}: {dtype}")
    
    print(f"\n🔍 Nulls per column:")
    null_counts = df.null_count()
    has_nulls = False
    for col in null_counts.columns:
        null_count = null_counts[col][0]
        if null_count > 0:
            has_nulls = True
            null_pct = (null_count / df.height * 100) if df.height > 0 else 0
            print(f"  ❌ {col}: {null_count:,} ({null_pct:.1f}%)")
    
    if not has_nulls:
        print("  ✅ No nulls found")
    
    print(f"\n📄 First 5 rows:")
    print(df.head())


def compare_schemas(
    df1: pl.DataFrame, 
    df2: pl.DataFrame, 
    name1: str = "DF1", 
    name2: str = "DF2"
) -> None:
    """
    Compares schemas between two DataFrames.
    
    Identifies:
    - Columns only in df1
    - Columns only in df2
    - Common columns
    - Data type differences
    
    Args:
        df1, df2: DataFrames to compare
        name1, name2: Descriptive names
    
    Example:
        >>> compare_schemas(df_old, df_new, "November", "December")
    """
    schema1 = set(df1.columns)
    schema2 = set(df2.columns)
    
    only_in_1 = schema1 - schema2
    only_in_2 = schema2 - schema1
    common = schema1 & schema2
    
    print(f"\n🔍 Schema Comparison: {name1} vs {name2}")
    print(f"{'='*60}")
    
    if only_in_1:
        print(f"\n❌ Columns only in {name1}:")
        for col in sorted(only_in_1):
            print(f"   - {col}")
    
    if only_in_2:
        print(f"\n❌ Columns only in {name2}:")
        for col in sorted(only_in_2):
            print(f"   - {col}")
    
    if not only_in_1 and not only_in_2:
        print(f"\n✅ Schemas have the same columns!")
    
    # Check type differences in common columns
    type_diffs = []
    for col in common:
        type1 = str(df1.schema[col])
        type2 = str(df2.schema[col])
        if type1 != type2:
            type_diffs.append((col, type1, type2))
    
    if type_diffs:
        print(f"\n⚠️  Type differences in common columns:")
        for col, type1, type2 in type_diffs:
            print(f"   - {col}: {type1} ({name1}) vs {type2} ({name2})")
    
    print(f"\n📊 Summary:")
    print(f"   Common columns: {len(common)}")
    print(f"   Only in {name1}: {len(only_in_1)}")
    print(f"   Only in {name2}: {len(only_in_2)}")


def advanced_profile(df: pl.DataFrame) -> pl.DataFrame:
    """
    Advanced profiling with detailed statistics per column.
    
    Returns DataFrame with:
    - column: column name
    - dtype: data type
    - null_count: number of nulls
    - null_pct: percentage of nulls
    - unique_count: number of unique values
    
    Args:
        df: DataFrame to analyze
    
    Returns:
        DataFrame with statistics per column
    
    Example:
        >>> stats = advanced_profile(df)
        >>> print(stats.filter(pl.col("null_pct") > 50))  # Columns with >50% nulls
    """
    stats = pl.DataFrame({
        "column": df.columns,
        "dtype": [str(dtype) for dtype in df.dtypes],
        "null_count": [df[col].null_count() for col in df.columns],
        "null_pct": [
            round(df[col].null_count() / df.height * 100, 2) if df.height > 0 else 0
            for col in df.columns
        ],
        "unique_count": [df[col].n_unique() for col in df.columns],
    })
    
    return stats

