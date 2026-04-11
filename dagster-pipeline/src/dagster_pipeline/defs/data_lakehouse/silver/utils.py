# defs/data_lakehouse/silver/utils.py
"""
Utilidades genéricas para assets Silver.

Funciones reutilizables para:
- Validaciones automáticas
- Transformaciones de tipos
- Renombramiento a snake_case
- CASE statements genéricos
"""
import dagster as dg
import polars as pl
from typing import Dict, List, Tuple, Optional, Union, Any


# ============ CASE Statement Helper ============

def case_when(
    column: Union[str, pl.Expr],
    mapping: Dict[Any, Any],
    default: Any = None,
    alias: Optional[str] = None,
) -> pl.Expr:
    """
    Crea un CASE WHEN genérico de forma más simple.
    
    Args:
        column: Nombre de columna (str) o expresión Polars
        mapping: Dict con {valor_origen: valor_destino}
        default: Valor por defecto (si no hay match)
        alias: Nombre de la columna resultante
    
    Returns:
        Expresión Polars con el CASE WHEN
    
    Example:
        # Antes (verboso):
        lead_origin_expr = (
            pl.when(pl.col("origin") == 1)
            .then(pl.lit("Spot2"))
            .when(pl.col("origin") == 2)
            .then(pl.lit("Hubspot"))
            .otherwise(pl.lit(None))
            .alias("lead_origin")
        )
        
        # Ahora (simple):
        lead_origin_expr = case_when(
            "origin",
            {1: "Spot2", 2: "Hubspot"},
            default=None,
            alias="lead_origin"
        )
    """
    if isinstance(column, str):
        col_expr = pl.col(column)
    else:
        col_expr = column
    
    expr = None
    for key, value in mapping.items():
        if expr is None:
            expr = pl.when(col_expr == key).then(pl.lit(value))
        else:
            expr = expr.when(col_expr == key).then(pl.lit(value))
    
    if default is not None:
        expr = expr.otherwise(pl.lit(default))
    else:
        expr = expr.otherwise(pl.lit(None))
    
    if alias:
        expr = expr.alias(alias)
    
    return expr


def case_when_conditions(
    conditions: List[Tuple[pl.Expr, Any]],
    default: Any = None,
    alias: Optional[str] = None,
) -> pl.Expr:
    """
    Crea un CASE WHEN con condiciones complejas (no solo igualdad).
    
    Args:
        conditions: Lista de tuplas (condición, valor)
        default: Valor por defecto
        alias: Nombre de la columna resultante
    
    Returns:
        Expresión Polars con el CASE WHEN
    
    Example:
        # Para condiciones complejas:
        lead_max_type_expr = case_when_conditions(
            [
                (pl.col("lead4_at").is_not_null(), "L4"),
                (pl.col("lead3_at").is_not_null(), "L3"),
                (pl.col("lead2_at").is_not_null(), "L2"),
            ],
            default="Others",
            alias="lead_max_type"
        )
    """
    expr = None
    for condition, value in conditions:
        if expr is None:
            expr = pl.when(condition).then(pl.lit(value))
        else:
            expr = expr.when(condition).then(pl.lit(value))
    
    if default is not None:
        expr = expr.otherwise(pl.lit(default))
    else:
        expr = expr.otherwise(pl.lit(None))
    
    if alias:
        expr = expr.alias(alias)
    
    return expr


# ============ Data Quality Checks ============

def check_nulls(
    df: pl.DataFrame,
    context: dg.AssetExecutionContext,
    threshold: float = 0.5,
) -> Dict[str, Tuple[bool, str]]:
    """
    Chequea columnas con muchos NULLs.
    
    Args:
        df: DataFrame a validar
        context: Contexto de Dagster
        threshold: Porcentaje de NULLs que se considera problemático (default: 0.5 = 50%)
    
    Returns:
        Dict con resultados: {columna: (passed, mensaje)}
    """
    results = {}
    for col in df.columns:
        null_count = df.select(pl.col(col).is_null().sum()).item()
        null_pct = null_count / df.height if df.height > 0 else 0
        
        if null_pct > threshold:
            results[col] = (
                False,
                f"Columna '{col}' tiene {null_pct*100:.1f}% NULLs (threshold: {threshold*100}%)"
            )
            context.log.warning(f"⚠️  {col}: {null_pct*100:.1f}% NULLs")
        else:
            results[col] = (True, f"Columna '{col}': {null_pct*100:.1f}% NULLs")
    
    return results


def check_primary_key(
    df: pl.DataFrame,
    context: dg.AssetExecutionContext,
    pk_column: str,
) -> Dict[str, Tuple[bool, str]]:
    """
    Chequea que la primary key sea válida (sin NULLs, sin duplicados).
    
    Args:
        df: DataFrame a validar
        context: Contexto de Dagster
        pk_column: Nombre de la columna que es primary key
    
    Returns:
        Dict con resultados: {check_name: (passed, mensaje)}
    """
    results = {}
    
    if pk_column not in df.columns:
        results["pk_exists"] = (False, f"Primary key '{pk_column}' no existe en el DataFrame")
        context.log.error(f"❌ Primary key '{pk_column}' no encontrada")
        return results
    
    # Chequeo de NULLs
    null_count = df.filter(pl.col(pk_column).is_null()).height
    if null_count > 0:
        results["pk_nulls"] = (False, f"Primary key '{pk_column}' tiene {null_count} valores NULL")
        context.log.warning(f"⚠️  {null_count} valores NULL en primary key '{pk_column}'")
    else:
        results["pk_nulls"] = (True, f"Primary key '{pk_column}' sin NULLs")
    
    # Chequeo de duplicados
    unique_count = df.select(pl.col(pk_column).n_unique()).item()
    duplicate_count = df.height - unique_count
    if duplicate_count > 0:
        results["pk_duplicates"] = (False, f"Primary key '{pk_column}' tiene {duplicate_count} duplicados")
        context.log.warning(f"⚠️  {duplicate_count} duplicados en primary key '{pk_column}'")
    else:
        results["pk_duplicates"] = (True, f"Primary key '{pk_column}' es única")
    
    return results


def fix_column_types(
    df: pl.DataFrame,
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """
    Fixes data types automatically.
    
    - Converts empty strings to NULL
    - Ensures booleans are actually boolean
    - Converts numeric strings to numbers when possible
    
    Args:
        df: DataFrame to fix
        context: Dagster context
    
    Returns:
        DataFrame with corrected types
    """
    conversions = []
    
    for col in df.columns:
        dtype = df.schema[col]
        
        # If string, convert empty strings to NULL
        if dtype == pl.Utf8:
            conversions.append(
                pl.when(pl.col(col) == "")
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )
        # Si es Int64 que debería ser boolean (0/1), convertir.
        # Guards:
        # 1. unique_vals must be non-empty (100% NULL → set([]).issubset({0,1}) is True in Python → wrong cast).
        # 2. Column name must not match numeric patterns: *_count, *_id, *_log, *_days, or explicit whitelist.
        #    These are counters/identifiers that happen to have values {0,1} but must stay Int64.
        elif dtype == pl.Int64:
            _numeric_name_suffixes = ("_count", "_id", "_log", "_days", "_total", "_num", "_size", "_amount")
            _numeric_columns_explicit: set[str] = {
                "user_days_since_last_log",
                "user_total_log_count",
                "user_days_since_last_spot_created",
                "user_days_since_last_spot_updated",
                "user_days_since_last_spot_activity",
            }
            _is_numeric_by_name = (
                col in _numeric_columns_explicit
                or any(col.endswith(s) for s in _numeric_name_suffixes)
            )
            if _is_numeric_by_name:
                pass  # always keep as Int64
            else:
                unique_vals = df.select(pl.col(col).unique().drop_nulls()).to_series().to_list()
                if unique_vals and set(unique_vals).issubset({0, 1}):
                    context.log.info(f"ℹ️  Converting column '{col}' from Int64 to Boolean")
                    conversions.append(
                        pl.col(col).cast(pl.Boolean).alias(col)
                    )
        else:
            # Keep column as is
            conversions.append(pl.col(col))
    
    if conversions:
        df = df.with_columns(conversions)
        context.log.info(f"✅ Data types fixed for {len(conversions)} columns")
    
    return df


def to_snake_case(name: str) -> str:
    """
    Converts a name to snake_case.
    
    Args:
        name: Name to convert
    
    Returns:
        Name in snake_case
    """
    import re
    # Insert _ before uppercase (except first)
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    # Insert _ between lowercase and uppercase
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()


def rename_to_snake_case(
    df: pl.DataFrame,
    context: dg.AssetExecutionContext,
    only_if_needed: bool = True,
) -> pl.DataFrame:
    """
    Renames columns to snake_case only when needed.
    
    Args:
        df: DataFrame to rename
        context: Dagster context
        only_if_needed: If True, only renames columns that are not in snake_case
    
    Returns:
        DataFrame with renamed columns
    """
    renames = {}
    
    for col in df.columns:
        snake_col = to_snake_case(col)
        if only_if_needed and col == snake_col:
            # Already in snake_case, do not rename
            continue
        if col != snake_col:
            renames[col] = snake_col
    
    if renames:
        df = df.rename(renames)
        context.log.info(f"✅ Renamed {len(renames)} columns to snake_case: {list(renames.keys())}")
    
    return df


def validate_input_bronze(
    df_bronze: pl.DataFrame,
    context: dg.AssetExecutionContext,
) -> dict:
    """
    Validates bronze input data BEFORE transformations.
    
    Args:
        df_bronze: Bronze DataFrame (input)
        context: Dagster context
    
    Returns:
        Dict with validation results: {
            "rows": int,
            "columns": int,
            "is_empty": bool,
            "has_data": bool
        }
    """
    context.log.info("🔍 Validating input data (bronze)...")
    
    results = {
        "rows": df_bronze.height,
        "columns": df_bronze.width,
        "is_empty": df_bronze.height == 0,
        "has_data": df_bronze.height > 0,
    }
    
    if results["is_empty"]:
        context.log.warning("⚠️  Bronze DataFrame is EMPTY - no data to transform")
    else:
        context.log.info(f"✅ Input bronze: {results['rows']:,} rows, {results['columns']} columns")
    
    return results


def validate_output_silver(
    df_silver: pl.DataFrame,
    df_bronze: pl.DataFrame,
    context: dg.AssetExecutionContext,
    allow_row_loss: bool = False,
    max_row_loss_pct: float = 0.0,
) -> dict:
    """
    Validates silver output data AFTER transformations.
    Ensures no critical data loss occurred.
    
    Args:
        df_silver: Silver DataFrame (output)
        df_bronze: Bronze DataFrame (original input)
        context: Dagster context
        allow_row_loss: If True, allows row loss (e.g. intentional filters)
        max_row_loss_pct: Maximum allowed loss percentage (0.0 = no loss)
    
    Returns:
        Dict with validation results: {
            "bronze_rows": int,
            "silver_rows": int,
            "rows_lost": int,
            "row_loss_pct": float,
            "data_loss_detected": bool,
            "validation_passed": bool
        }
    """
    context.log.info("🔍 Validating output data (silver)...")
    
    bronze_rows = df_bronze.height
    silver_rows = df_silver.height
    rows_lost = bronze_rows - silver_rows
    row_loss_pct = (rows_lost / bronze_rows * 100) if bronze_rows > 0 else 0.0
    
    results = {
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "rows_lost": rows_lost,
        "row_loss_pct": row_loss_pct,
        "data_loss_detected": False,
        "validation_passed": True,
    }
    
    # Validate row loss
    if rows_lost > 0:
        if not allow_row_loss:
            results["data_loss_detected"] = True
            results["validation_passed"] = False
            context.log.error(
                f"❌ DATA LOSS DETECTED: {rows_lost:,} rows lost "
                f"({row_loss_pct:.2f}%) - Expected 0% loss"
            )
        elif row_loss_pct > max_row_loss_pct:
            results["data_loss_detected"] = True
            results["validation_passed"] = False
            context.log.error(
                f"❌ EXCESSIVE DATA LOSS: {rows_lost:,} rows lost "
                f"({row_loss_pct:.2f}%) - Maximum allowed: {max_row_loss_pct}%"
            )
        else:
            context.log.warning(
                f"⚠️  Row loss allowed: {rows_lost:,} rows ({row_loss_pct:.2f}%)"
            )
    else:
        context.log.info(f"✅ No row loss: {silver_rows:,} rows (same as bronze)")
    
    # Validate not empty
    if silver_rows == 0 and bronze_rows > 0:
        results["data_loss_detected"] = True
        results["validation_passed"] = False
        context.log.error("❌ CRITICAL ERROR: Silver DataFrame is EMPTY but bronze had data")
    
    # Validate critical columns (if present in both)
    common_cols = set(df_bronze.columns) & set(df_silver.columns)
    if len(common_cols) == 0:
        context.log.warning("⚠️  No common columns between bronze and silver")
    else:
        context.log.info(f"✅ {len(common_cols)} common columns preserved")
    
    return results


def expr_mysql_numeric_int64(column: Union[str, pl.Expr]) -> pl.Expr:
    """Coerce ORM/MySQL numeric-ish values to Int64 for joins and CASE maps."""
    col = pl.col(column) if isinstance(column, str) else column
    return (
        pl.when(col.is_null())
        .then(pl.lit(None).cast(pl.Int64))
        .otherwise(col.cast(pl.Float64, strict=False).cast(pl.Int64))
    )


def expr_spot2_internal_email(email_col: str = "email") -> pl.Expr:
    """Trim + lower; True if email is @spot2.mx or @spot2-services.com."""
    e = pl.col(email_col).cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    return e.str.contains(r"@spot2\.mx$|@spot2-services\.com$")


def validate_and_clean_silver(
    df: pl.DataFrame,
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    """
    All-in-one function to validate and clean silver data AUTOMATICALLY.
    
    Runs all automatic validations and cleanups with no user configuration:
    - NULL check on all columns
    - Automatic type correction
    - Rename to snake_case when needed
    
    Note: Primary key validations must be done manually if required.
    
    Args:
        df: DataFrame to validate and clean
        context: Dagster context
    
    Returns:
        Validated and cleaned DataFrame
    """
    context.log.info("🔍 Starting automatic validation and cleanup...")
    
    # 1. NULL check (automatic, default threshold)
    null_results = check_nulls(df, context, threshold=0.5)
    passed = sum(1 for v in null_results.values() if v[0])
    context.log.info(f"✅ NULL checks: {passed}/{len(null_results)} passed")
    
    # 2. Type correction (automatic)
    df = fix_column_types(df, context)
    
    # 3. Rename to snake_case (automatic, only when needed)
    df = rename_to_snake_case(df, context, only_if_needed=True)
    
    context.log.info("✅ Validation and cleanup completed")
    
    return df
