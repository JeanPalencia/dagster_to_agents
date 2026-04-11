# Lakehouse SDK - Quick Start Guide

## 1. Setup

```bash
cd lakehouse-sdk
uv sync
source .venv/bin/activate
```

## 2. Verify Installation

```bash
python -c "from lakehouse_sdk import load_all_assets; print('✅ SDK OK')"
```

## 3. Start Jupyter Lab

```bash
jupyter lab
```

## 4. Open Example Notebooks

- `notebooks/bronze/example_bronze.ipynb` - Bronze layer testing
- `notebooks/silver/example_silver.ipynb` - Silver layer testing

## 5. Basic Usage

### In a Python Script or Notebook

```python
from lakehouse_sdk import execute_asset, quick_profile, list_assets

# List all available assets
list_assets()

# Execute a bronze asset
df = execute_asset("raw_s2p_clients", partition_key="2024-12-01")
quick_profile(df)

# Execute a silver asset (dependencies resolved automatically)
df_leads = execute_asset("stg_s2p_leads", partition_key="2024-12-01")
quick_profile(df_leads)
```

### Test Joins Before Creating Assets

```python
# Load multiple assets
df_clients = execute_asset("raw_s2p_clients", "2024-12-01")
df_profiles = execute_asset("raw_s2p_profiles", "2024-12-01")

# Experiment with transformations
df_joined = df_clients.join(
    df_profiles, 
    left_on="id", 
    right_on="client_id",
    how="left"
)

# Once validated, promote to a new asset in dagster-pipeline
```

### Validate Data

```python
from lakehouse_sdk import validate_asset, ValidationRule, CommonRules, print_validation_results

rules = [
    ValidationRule("id", CommonRules.not_null(), "ID not null"),
    ValidationRule("id", CommonRules.unique(), "ID unique"),
]

results = validate_asset(df, rules)
print_validation_results(results)
```

## 6. Run Tests

```bash
uv run pytest
```

## Common Workflows

### 1. Explore New Data Source

```python
# Load bronze asset
df = execute_asset("raw_s2p_activity_log", "2024-12-01")

# Profile it
quick_profile(df)

# Advanced profiling
from lakehouse_sdk import advanced_profile
stats = advanced_profile(df)
print(stats.filter(pl.col("null_pct") > 10))  # Find columns with >10% nulls
```

### 2. Design New Transformation

```python
# Load dependencies
df_raw = execute_asset("raw_s2p_clients", "2024-12-01")

# Test transformation
df_transformed = df_raw.select([
    pl.col("id"),
    pl.col("email").str.to_lowercase().alias("email_lower"),
    # ... more transformations
])

# Validate
quick_profile(df_transformed)

# Once happy, create new asset in dagster-pipeline
```

### 3. Compare Schema Changes

```python
from lakehouse_sdk import compare_schemas

df_nov = execute_asset("raw_s2p_clients", "2024-11-01")
df_dec = execute_asset("raw_s2p_clients", "2024-12-01")

compare_schemas(df_nov, df_dec, "November", "December")
```

## Tips

- Use `clear_cache()` to force re-execution of assets
- All assets are cached during a session for performance
- The SDK does NOT modify production code in `dagster-pipeline/`
- Use notebooks for iterative development, then promote to assets

## Next Steps

1. Explore the example notebooks
2. Try executing your assets with real data
3. Experiment with joins and transformations
4. Create new assets in `dagster-pipeline/` based on your experiments

## Troubleshooting

### "Module not found: dagster_pipeline"

Make sure you're in the lakehouse-sdk directory and the relative path to dagster-pipeline is correct.

### "Asset not found"

List all assets to see what's available:

```python
from lakehouse_sdk import list_assets
list_assets()
```

