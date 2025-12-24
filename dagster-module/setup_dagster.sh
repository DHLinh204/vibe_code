#!/bin/bash
# Setup script for Dagster + dbt integration

set -e

echo "🚀 Setting up Dagster module with dbt integration..."

# Check if Python 3.10+ is available
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python version: $PYTHON_VERSION"

# Install dependencies
echo ""
echo "📦 Installing dependencies..."
pip install -e ".[dev]"

# Parse dbt project (generate manifest.json)
echo ""
echo "📝 Parsing dbt project..."
cd ../transform
if command -v dbt &> /dev/null; then
    dbt parse
    echo "✓ dbt project parsed successfully"
else
    echo "⚠️  dbt CLI not found. Run 'dbt parse' manually before running Dagster"
fi

# Return to dagster-module
cd ../dagster-module

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Ensure your dbt profiles.yml is configured correctly (~/.dbt/profiles.yml)"
echo "2. Test your dbt connection: cd ../transform && dbt debug"
echo "3. Start Dagster: dagster dev"
echo ""
