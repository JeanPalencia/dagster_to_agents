"""
Asset loader with auto-discovery capabilities
"""
import sys
import inspect
import importlib
from pathlib import Path
from typing import Dict, Callable, Optional, Any
import polars as pl
from dagster import AssetExecutionContext

from .context import setup_import_paths, create_test_context, DAGSTER_PIPELINE_PATH


# Cache for loaded assets and execution results
_ASSETS_CACHE: Dict[str, Callable] = {}
_EXECUTION_CACHE: Dict[tuple, pl.DataFrame] = {}
_DEPS_CACHE: Dict[str, list[str]] = {}  # Cache for deps declared via @dg.asset(deps=[...])


def get_data_lakehouse_path() -> Path:
    """
    Retorna el path al directorio data_lakehouse en dagster-pipeline.
    
    Returns:
        Path al directorio data_lakehouse
    """
    return DAGSTER_PIPELINE_PATH / "dagster_pipeline" / "defs" / "data_lakehouse"


def discover_asset_files() -> list[Path]:
    """
    Descubre todos los archivos Python con assets en el data lakehouse.
    
    Busca en:
    - bronze/*.py
    - silver/stg/*.py
    - silver/core/*.py
    - gold/*.py
    - publish/*.py
    
    Returns:
        Lista de Paths a archivos Python con assets
    """
    lakehouse_path = get_data_lakehouse_path()
    asset_files = []
    
    # Directories to search
    search_dirs = [
        lakehouse_path / "bronze",
        lakehouse_path / "silver" / "stg",
        lakehouse_path / "silver" / "core",
        lakehouse_path / "gold",
        lakehouse_path / "publish",
    ]
    
    for search_dir in search_dirs:
        if search_dir.exists():
            # Find all .py files except __init__.py and shared.py
            for py_file in search_dir.glob("*.py"):
                if py_file.name not in ["__init__.py", "shared.py"]:
                    asset_files.append(py_file)
    
    return asset_files


def extract_assets_from_module(module) -> Dict[str, Callable]:
    """
    Extrae todas las funciones decoradas con @dg.asset de un módulo.
    
    Also extracts deps declared via @dg.asset(deps=[...]) and stores them
    in _DEPS_CACHE for later use by get_asset_dependencies().
    
    Args:
        module: Módulo Python importado
    
    Returns:
        Dict con {nombre_asset: función}
    """
    assets = {}
    
    for name, obj in inspect.getmembers(module):
        # Skip private members
        if name.startswith("_"):
            continue
        
        # Check if it's a Dagster AssetsDefinition
        if type(obj).__name__ == "AssetsDefinition":
            # Extract deps declared via @dg.asset(deps=[...])
            # These are dependencies that don't pass values as parameters
            try:
                # Dagster stores deps in asset_deps as a dict of {AssetKey: set of AssetKeys}
                if hasattr(obj, "asset_deps"):
                    for asset_key, dep_keys in obj.asset_deps.items():
                        if dep_keys:
                            # Convert AssetKeys to string names
                            dep_names = [
                                dep_key.path[-1] if hasattr(dep_key, "path") else str(dep_key)
                                for dep_key in dep_keys
                            ]
                            _DEPS_CACHE[name] = dep_names
            except Exception:
                pass  # Ignore errors in deps extraction
            
            # Extract the actual function from the AssetsDefinition
            # The op_def contains the compute_fn which is the original function
            if hasattr(obj, "op") and hasattr(obj.op, "compute_fn"):
                assets[name] = obj.op.compute_fn
            else:
                # Fallback: store the AssetsDefinition itself
                assets[name] = obj
            continue
        
        # Also check for regular functions that look like assets
        if inspect.isfunction(obj):
            try:
                sig = inspect.signature(obj)
                params = list(sig.parameters.keys())
                # Assets typically have 'context' as first parameter
                if params and params[0] == "context":
                    assets[name] = obj
            except (ValueError, TypeError):
                # Skip if we can't get signature
                pass
    
    return assets


def load_all_assets(force_reload: bool = False) -> Dict[str, Callable]:
    """
    Auto-descubre y carga TODOS los assets del data lakehouse.
    
    Recorre todos los archivos Python en:
    - bronze/
    - silver/stg/
    - silver/core/
    
    Y extrae todas las funciones que parecen ser assets de Dagster.
    
    Args:
        force_reload: Si True, fuerza recarga de assets (ignora cache)
    
    Returns:
        Dict con {nombre_asset: función_asset}
    
    Example:
        >>> assets = load_all_assets()
        >>> print(f"Assets disponibles: {list(assets.keys())}")
        >>> print(f"Total: {len(assets)} assets")
    """
    global _ASSETS_CACHE
    
    if _ASSETS_CACHE and not force_reload:
        return _ASSETS_CACHE
    
    # Setup import paths
    setup_import_paths()
    
    _ASSETS_CACHE = {}
    asset_files = discover_asset_files()
    
    for asset_file in asset_files:
        # Build module path relative to dagster_pipeline
        lakehouse_path = get_data_lakehouse_path()
        
        # Get relative path from lakehouse to the asset file
        relative_to_lakehouse = asset_file.relative_to(lakehouse_path)
        
        # Build full module path: dagster_pipeline.defs.data_lakehouse.bronze.raw_s2p_clients
        module_parts = ["dagster_pipeline", "defs", "data_lakehouse"]
        module_parts.extend(list(relative_to_lakehouse.parent.parts))
        module_parts.append(asset_file.stem)
        module_name = ".".join(module_parts)
        
        try:
            # Import the module
            module = importlib.import_module(module_name)
            
            # Extract assets from module
            module_assets = extract_assets_from_module(module)
            _ASSETS_CACHE.update(module_assets)
            
        except Exception as e:
            print(f"⚠️  Error loading assets from {asset_file.name}: {e}")
    
    return _ASSETS_CACHE


def load_asset(asset_name: str) -> Optional[Callable]:
    """
    Carga un asset específico por nombre.
    
    No necesitas conocer la ruta completa, solo el nombre del asset.
    
    Args:
        asset_name: Nombre del asset (e.g., "raw_s2p_clients", "stg_s2p_leads")
    
    Returns:
        Función del asset, o None si no se encuentra
    
    Example:
        >>> asset_func = load_asset("raw_s2p_clients")
        >>> context = create_test_context("2024-12-01")
        >>> df = asset_func(context)
    """
    assets = load_all_assets()
    return assets.get(asset_name)


def get_asset_dependencies(asset_func: Any, asset_name: str = None) -> list[str]:
    """
    Obtiene las dependencias de un asset inspeccionando sus parámetros
    y también los deps declarados via @dg.asset(deps=[...]).
    
    Args:
        asset_func: Función del asset o DecoratedOpFunction
        asset_name: Nombre del asset (para buscar deps declarados)
    
    Returns:
        Lista de nombres de assets de los que depende
    """
    deps = []
    
    # Handle DecoratedOpFunction from Dagster
    if hasattr(asset_func, "decorated_fn"):
        asset_func = asset_func.decorated_fn
    
    try:
        sig = inspect.signature(asset_func)
        params = list(sig.parameters.keys())
        
        # First param is always 'context', rest are dependencies
        if params and params[0] == "context":
            deps.extend(params[1:])
    except (ValueError, TypeError, AttributeError):
        pass
    
    # Also check for deps declared via @dg.asset(deps=[...])
    if asset_name and asset_name in _DEPS_CACHE:
        declared_deps = _DEPS_CACHE[asset_name]
        # Add declared deps that are not already in the list
        for dep in declared_deps:
            if dep not in deps:
                deps.append(dep)
    
    return deps


def execute_asset(
    asset_name: str,
    partition_key: Optional[str] = None,
    dependencies: Optional[Dict[str, pl.DataFrame]] = None,
    use_cache: bool = True
) -> pl.DataFrame:
    """
    Ejecuta un asset con resolución automática de dependencias.
    
    Args:
        asset_name: Nombre del asset a ejecutar
        partition_key: Fecha de partición (YYYY-MM-DD). Si None, usa ayer.
        dependencies: Dict con DataFrames de dependencias ya ejecutadas.
                     Si None, se ejecutan automáticamente.
        use_cache: Si True, usa resultados cacheados si existen
    
    Returns:
        DataFrame resultado del asset
    
    Raises:
        ValueError: Si el asset no existe
    
    Example:
        >>> # Execute bronze asset (without dependencies)
        >>> df_raw = execute_asset("raw_s2p_clients", "2024-12-01")
        >>> 
        >>> # Execute silver asset (resolves dependencies automatically)
        >>> df_leads = execute_asset("stg_s2p_leads", "2024-12-01")
        >>> 
        >>> # Or provide dependencies manually
        >>> df_leads = execute_asset(
        ...     "stg_s2p_leads",
        ...     "2024-12-01",
        ...     dependencies={"raw_s2p_clients": df_raw}
        ... )
    """
    global _EXECUTION_CACHE
    
    # Check cache
    cache_key = (asset_name, partition_key, tuple(sorted((dependencies or {}).keys())))
    if use_cache and cache_key in _EXECUTION_CACHE:
        print(f"🔄 Usando resultado cacheado para {asset_name}")
        return _EXECUTION_CACHE[cache_key]
    
    # Load asset
    asset_func = load_asset(asset_name)
    if asset_func is None:
        available = list(load_all_assets().keys())
        raise ValueError(
            f"Asset '{asset_name}' no encontrado. "
            f"Assets disponibles: {available}"
        )
    
    # Create context
    context = create_test_context(partition_key)
    
    # Get the actual callable function
    actual_func = asset_func
    if hasattr(asset_func, "decorated_fn"):
        actual_func = asset_func.decorated_fn
    
    # Get dependencies (both from function params and @dg.asset(deps=[...]))
    required_deps = get_asset_dependencies(asset_func, asset_name)
    
    # Separate parameter deps (need to pass as kwargs) from declared deps (just execute first)
    param_deps = []
    if hasattr(asset_func, "decorated_fn"):
        check_func = asset_func.decorated_fn
    else:
        check_func = asset_func
    try:
        sig = inspect.signature(check_func)
        params = list(sig.parameters.keys())
        if params and params[0] == "context":
            param_deps = params[1:]
    except (ValueError, TypeError, AttributeError):
        pass
    
    # Declared deps are those in required_deps but not in param_deps
    declared_deps = [d for d in required_deps if d not in param_deps]
    
    if required_deps:
        if dependencies is None:
            # Auto-resolve dependencies
            print(f"🔗 Resolviendo dependencias para {asset_name}: {required_deps}")
            dependencies = {}
            for dep_name in required_deps:
                dep_result = execute_asset(
                    dep_name, 
                    partition_key, 
                    use_cache=use_cache
                )
                # Only store in dependencies dict if it's a parameter dep (not declared dep)
                if dep_name in param_deps:
                    dependencies[dep_name] = dep_result
        
        # Execute with parameter dependencies only (declared deps just need to run first)
        if param_deps:
            result = actual_func(context, **dependencies)
        else:
            # No parameter deps, just declared deps - execute without kwargs
            result = actual_func(context)
    else:
        # Execute without dependencies
        result = actual_func(context)
    
    # Cache result
    if use_cache:
        _EXECUTION_CACHE[cache_key] = result
    
    return result


def clear_cache():
    """
    Limpia el cache de ejecuciones de assets.
    
    Útil cuando quieres re-ejecutar assets forzosamente o después de
    cambiar código de assets.
    
    Example:
        >>> clear_cache()
        >>> df = execute_asset("raw_s2p_clients", "2024-12-01")  # Re-ejecuta
    """
    global _EXECUTION_CACHE, _ASSETS_CACHE, _DEPS_CACHE
    _EXECUTION_CACHE.clear()
    _ASSETS_CACHE.clear()
    _DEPS_CACHE.clear()
    print("🗑️  Cache limpiado")


def list_assets() -> None:
    """
    Lista todos los assets disponibles con información básica.
    
    Example:
        >>> list_assets()
    """
    assets = load_all_assets()
    
    print(f"\n{'='*60}")
    print(f"📦 Assets Disponibles ({len(assets)})")
    print(f"{'='*60}")
    
    # Group by layer
    bronze_assets = [name for name in assets.keys() if name.startswith("raw_")]
    silver_stg_assets = [name for name in assets.keys() if name.startswith("stg_")]
    silver_core_assets = [name for name in assets.keys() if name.startswith("core_")]
    gold_assets = [name for name in assets.keys() if name.startswith("gold_")]
    publish_assets = [
        name for name in assets.keys() 
        if name.endswith("_to_s3") or name.endswith("_to_geospot")
    ]
    other_assets = [
        name for name in assets.keys() 
        if not name.startswith("raw_") 
        and not name.startswith("stg_")
        and not name.startswith("core_")
        and not name.startswith("gold_")
        and not name.endswith("_to_s3")
        and not name.endswith("_to_geospot")
    ]
    
    if bronze_assets:
        print(f"\n🥉 Bronze Layer ({len(bronze_assets)}):")
        for name in sorted(bronze_assets):
            deps = get_asset_dependencies(assets[name], name)
            deps_str = f" → {deps}" if deps else ""
            print(f"   - {name}{deps_str}")
    
    if silver_stg_assets:
        print(f"\n🥈 Silver STG Layer ({len(silver_stg_assets)}):")
        for name in sorted(silver_stg_assets):
            deps = get_asset_dependencies(assets[name], name)
            deps_str = f" → {deps}" if deps else ""
            print(f"   - {name}{deps_str}")
    
    if silver_core_assets:
        print(f"\n🥈 Silver Core Layer ({len(silver_core_assets)}):")
        for name in sorted(silver_core_assets):
            deps = get_asset_dependencies(assets[name], name)
            deps_str = f" → {deps}" if deps else ""
            print(f"   - {name}{deps_str}")
    
    if gold_assets:
        print(f"\n🥇 Gold Layer ({len(gold_assets)}):")
        for name in sorted(gold_assets):
            deps = get_asset_dependencies(assets[name], name)
            deps_str = f" → {deps}" if deps else ""
            print(f"   - {name}{deps_str}")
    
    if publish_assets:
        print(f"\n📤 Publish Layer ({len(publish_assets)}):")
        for name in sorted(publish_assets):
            deps = get_asset_dependencies(assets[name], name)
            deps_str = f" → {deps}" if deps else ""
            print(f"   - {name}{deps_str}")
    
    if other_assets:
        print(f"\n⚡ Other Assets ({len(other_assets)}):")
        for name in sorted(other_assets):
            deps = get_asset_dependencies(assets[name], name)
            deps_str = f" → {deps}" if deps else ""
            print(f"   - {name}{deps_str}")

