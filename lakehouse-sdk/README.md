# Lakehouse SDK

Independent SDK for testing, desarrollo y gestión administrativa del Data Lakehouse de Spot2.

## 🎯 Purpose

Este SDK proporciona un isolated environment for:

- **Probar assets** del data lakehouse sin afectar el pipeline productivo
- **Explorar transformaciones** complejas (joins, agregaciones) en notebooks
- **Validar datos** before promoting cambios a producción
- **Desarrollar nuevos assets** de forma iterativa

## 🚀 Setup

### Installation

```bash
cd lakehouse-sdk
uv sync
source .venv/bin/activate
```

### Verify installation

```bash
python -c "from lakehouse_sdk import load_all_assets; print('SDK OK')"
```

## 📚 Usage

### 1. Load Assets Automatically

```python
from lakehouse_sdk import load_all_assets, execute_asset

# Auto-discovers all assets del data lakehouse
assets = load_all_assets()
print(f"Assets available: {list(assets.keys())}")
# Output: ['raw_s2p_clients', 'raw_s2p_profiles', 'stg_s2p_leads', ...]
```

### 2. Execute Individual Assets

```python
from lakehouse_sdk import execute_asset, quick_profile

# Execute un asset specific
df_clients = execute_asset("raw_s2p_clients", partition_key="2024-12-01")
quick_profile(df_clients, "Raw Clients")

# Las dependencies are resolved automatically
df_leads = execute_asset("stg_s2p_leads", partition_key="2024-12-01")
```

### 3. Probar Transformaciones Compuestas

```python
# Cargar múltiples assets
df_clients = execute_asset("raw_s2p_clients", "2024-12-01")
df_leads = execute_asset("stg_s2p_leads", "2024-12-01")
df_profiles = execute_asset("raw_s2p_profiles", "2024-12-01")

# Experimentar con joins
df_joined = df_leads.join(
    df_profiles,
    left_on="lead_id",
    right_on="client_id",
    how="left"
)

# Analizar results
quick_profile(df_joined, "Leads + Profiles")

# Una vez validado, promover a un nuevo asset en dagster-pipeline
```

### 4. Validar Datos

```python
from lakehouse_sdk import validate_asset, ValidationRule, print_validation_results

# Definir reglas de validación
rules = [
    ValidationRule("id", lambda col: col.is_not_null(), "ID no debe ser null"),
    ValidationRule("id", lambda col: col.is_unique(), "ID debe ser único"),
    ValidationRule("email", lambda col: col.is_not_null(), "Email no debe ser null"),
]

# Execute validaciones
results = validate_asset(df_clients, rules)
print_validation_results(results)
```

### 5. Comparar Schemas

```python
from lakehouse_sdk import compare_schemas

# Comparar estructura entre versiones
df_old = execute_asset("raw_s2p_clients", "2024-11-01")
df_new = execute_asset("raw_s2p_clients", "2024-12-01")
compare_schemas(df_old, df_new, "Noviembre", "Diciembre")
```

## 🗂️ Structure

```
lakehouse-sdk/
├── src/lakehouse_sdk/
│   ├── __init__.py       # API pública
│   ├── loader.py         # Auto-discovery de assets
│   ├── context.py        # Testing context
│   ├── profiling.py      # Data profiling
│   └── validators.py     # Validaciones
├── notebooks/            # Notebooks organizeds por capa
│   ├── bronze/          # Testing of assets bronze
│   ├── silver/          # Testing of assets silver
│   └── gold/            # Testing of assets gold (futuro)
└── tests/               # Unit tests
```

## 🔑 Features Clave

1. **Auto-discovery**: No necesitas actualizar código cuando agregas nuevos assets
2. **Isolated Environment**: UV independiente del pipeline productivo
3. **Caching**: Evita re-ejecutar assets ya calculados en la sesión
4. **Dependency Resolution**: Ejecuta assets upstream automatically
5. **Interactive Testing**: Notebooks para exploración iterativa

## 📖 Ejemplos

Ver notebooks en:
- `notebooks/bronze/example_bronze.ipynb` - Testing of assets bronze
- `notebooks/silver/example_silver.ipynb` - Testing of assets silver (coming soon)

## 🔧 Development

### Execute tests

```bash
uv run pytest
```

### Abrir Jupyter Lab

```bash
uv run jupyter lab
```

## 🎯 Workflow Recommended

1. **Explorar** datos con `execute_asset()` y `quick_profile()`
2. **Experimentar** con transformaciones en notebooks
3. **Validar** results con `validate_asset()`
4. **Promover** transformaciones exitosas a assets en `dagster-pipeline/`
5. **Iterar** hasta lograr la transformación deseada

## 📝 Notes

- Este SDK **does not modify** el production code en `dagster-pipeline/`
- Los assets se ejecutan usando las mismas dependencias (AWS, MySQL) que producción
- Los results **no se persist** automatically (solo en memoria/notebooks)
- Para materializar en S3, usa el Dagster UI en `dagster-pipeline/`

## 🆘 Troubleshooting

### Error: "Module not found: dagster_pipeline"

Asegúrate que la ruta al pipeline productivo es correcta:

```python
from lakehouse_sdk.loader import DAGSTER_PIPELINE_PATH
print(DAGSTER_PIPELINE_PATH)
# Debe apuntar a: /home/luis/Spot2/dagster/dagster-pipeline/src
```

### Error: "Asset not found"

Lista all assets available:

```python
assets = load_all_assets()
print(list(assets.keys()))
```

## 📚 Referencias

- [Dagster Documentation](https://docs.dagster.io/)
- [Polars Documentation](https://pola-rs.github.io/polars/)
- [UV Documentation](https://docs.astral.sh/uv/)

