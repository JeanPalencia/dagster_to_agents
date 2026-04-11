#!/bin/bash
# Lakehouse SDK - Getting Started Script

echo "🚀 Lakehouse SDK - Setup and Verification"
echo "=========================================="
echo ""

# Check if in correct directory
if [ ! -f "pyproject.toml" ]; then
    echo "❌ Error: Run this script from the lakehouse-sdk directory"
    exit 1
fi

echo "1️⃣  Installing dependencies..."
uv sync

echo ""
echo "2️⃣  Running tests..."
uv run pytest tests/ -v

echo ""
echo "3️⃣  Verifying SDK..."
uv run python -c "
from lakehouse_sdk import load_all_assets, list_assets
assets = load_all_assets()
print(f'\n✅ SDK Successfully loaded {len(assets)} assets\n')
list_assets()
"

echo ""
echo "=========================================="
echo "✅ Lakehouse SDK is ready!"
echo ""
echo "📚 Next steps:"
echo "   1. Read README.md for full documentation"
echo "   2. Check QUICKSTART.md for quick examples"
echo "   3. Open Jupyter: uv run jupyter lab"
echo "   4. Try notebooks/bronze/example_bronze.ipynb"
echo ""
echo "🎯 Happy data exploring!"

