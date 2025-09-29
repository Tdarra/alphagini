#!/bin/bash

# Setup script for CCXT exchange testing

echo "🚀 Setting up virtual environment for CCXT testing..."

# Create virtual environment
python3 -m venv venv_ccxt

# Activate virtual environment
source venv_ccxt/bin/activate

# Upgrade pip
pip install --upgrade pip

echo "📦 Installing required packages..."

# Install required packages for testing
pip install \
    pandas \
    ccxt \
    yfinance \
    requests \
    google-cloud-bigquery \
    db-dtypes

echo "✅ Setup complete!"
echo ""
echo "To run tests:"
echo "1. source venv_ccxt/bin/activate"
echo "2. export COINAPI_KEY='your_key_here'     # Get free key from coinapi.io"
echo "3. python validate_coinapi_schema.py     # Test CoinAPI integration"
echo "4. python test_exchanges.py              # Test CCXT exchanges"
echo ""
echo "To deactivate when done:"
echo "deactivate"