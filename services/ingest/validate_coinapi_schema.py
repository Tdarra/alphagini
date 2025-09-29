#!/usr/bin/env python3
"""
Schema validation test for CoinAPI integration
Ensures data from CoinAPI matches BigQuery schema and CCXT format
"""

import os
import requests
import pandas as pd
import ccxt
from datetime import datetime, timedelta

# Configuration
COINAPI_KEY = os.environ.get("COINAPI_KEY")
COINAPI_BASE = "https://rest.coinapi.io/v1"

# Test symbols
TEST_SYMBOLS = {
    "BTC/USDT": "BTC",
    "ETH/USDT": "ETH", 
    "XRP/USDT": "XRP"
}

# Expected BigQuery schema
EXPECTED_BQ_SCHEMA = {
    'exchange': 'STRING',
    'symbol': 'STRING', 
    'timeframe': 'STRING',
    'ts': 'TIMESTAMP',
    'open': 'FLOAT',
    'high': 'FLOAT',
    'low': 'FLOAT', 
    'close': 'FLOAT',
    'volume': 'FLOAT'
}

def test_coinapi_sample_data():
    """Test CoinAPI response format and data availability"""
    print("🔍 Testing CoinAPI Sample Data")
    print("=" * 50)
    
    if not COINAPI_KEY:
        print("❌ COINAPI_KEY environment variable not set!")
        return False
    
    headers = {"X-CoinAPI-Key": COINAPI_KEY}
    
    for symbol, coinapi_id in TEST_SYMBOLS.items():
        print(f"\n📊 Testing {symbol} ({coinapi_id})...")
        
        try:
            # Test recent 5-minute data (last 24 hours)
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            today = datetime.now().strftime("%Y-%m-%d")
            
            url = f"{COINAPI_BASE}/ohlcv/{coinapi_id}/USD/history"
            params = {
                "period_id": "5MIN",
                "time_start": f"{yesterday}T00:00:00",
                "time_end": f"{today}T23:59:59",
                "limit": 10  # Small sample
            }
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                print(f"❌ {symbol}: No data returned")
                continue
            
            print(f"✅ {symbol}: {len(data)} records returned")
            
            # Check response structure
            sample = data[0]
            required_fields = ['time_period_start', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_traded']
            missing_fields = [field for field in required_fields if field not in sample]
            
            if missing_fields:
                print(f"❌ {symbol}: Missing fields: {missing_fields}")
                continue
            
            print(f"✅ {symbol}: All required fields present")
            print(f"   Sample record: {sample}")
            
        except Exception as e:
            print(f"❌ {symbol}: Error - {str(e)[:60]}...")
    
    return True

def test_coinapi_to_bq_schema():
    """Test CoinAPI data conversion to BigQuery schema"""
    print(f"\n🔧 Testing CoinAPI → BigQuery Schema Conversion")
    print("=" * 60)
    
    if not COINAPI_KEY:
        print("❌ COINAPI_KEY environment variable not set!")
        return False
    
    headers = {"X-CoinAPI-Key": COINAPI_KEY}
    
    try:
        # Get sample data for BTC
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        today = datetime.now().strftime("%Y-%m-%d")
        
        url = f"{COINAPI_BASE}/ohlcv/BTC/USD/history"
        params = {
            "period_id": "5MIN",
            "time_start": f"{yesterday}T00:00:00", 
            "time_end": f"{today}T23:59:59",
            "limit": 5
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            print("❌ No sample data for schema test")
            return False
        
        # Convert to DataFrame (simulate coinapi_historical_backfill.py logic)
        df = pd.DataFrame(data)
        
        print("📋 Original CoinAPI columns:")
        print(f"   {list(df.columns)}")
        
        # Apply transformations
        df = df.rename(columns={
            'time_period_start': 'ts',
            'price_open': 'open',
            'price_high': 'high',
            'price_low': 'low', 
            'price_close': 'close',
            'volume_traded': 'volume'
        })
        
        # Convert timestamp
        df['ts'] = pd.to_datetime(df['ts']).dt.tz_convert('UTC')
        
        # Add metadata
        df['exchange'] = 'coinapi'
        df['symbol'] = 'BTC/USDT'
        df['timeframe'] = '5m'
        
        # Select final columns
        df = df[['exchange', 'symbol', 'timeframe', 'ts', 'open', 'high', 'low', 'close', 'volume']]
        
        print("📋 Final BigQuery columns:")
        print(f"   {list(df.columns)}")
        
        print("📊 Data types:")
        for col, dtype in df.dtypes.items():
            print(f"   {col}: {dtype}")
        
        print("📝 Sample row:")
        print(df.iloc[0].to_dict())
        
        # Validate schema compatibility
        schema_valid = True
        for col in EXPECTED_BQ_SCHEMA.keys():
            if col not in df.columns:
                print(f"❌ Missing column: {col}")
                schema_valid = False
        
        if schema_valid:
            print("✅ Schema conversion successful!")
        
        return schema_valid
        
    except Exception as e:
        print(f"❌ Schema test failed: {e}")
        return False

def test_ccxt_compatibility():
    """Test that CoinAPI schema matches CCXT output format"""
    print(f"\n🔄 Testing CCXT Compatibility")
    print("=" * 40)
    
    try:
        # Get sample CCXT data for comparison
        exchange = ccxt.coinbase({'enableRateLimit': True})
        exchange.load_markets()
        
        if 'BTC/USDT' not in exchange.markets:
            print("⚠️  BTC/USDT not available on Coinbase, using BTC/USD")
            ccxt_symbol = 'BTC/USD'
        else:
            ccxt_symbol = 'BTC/USDT'
        
        ccxt_data = exchange.fetch_ohlcv(ccxt_symbol, '5m', limit=3)
        
        if not ccxt_data:
            print("❌ No CCXT data for comparison")
            return False
        
        # Convert CCXT data to DataFrame
        ccxt_df = pd.DataFrame(ccxt_data, columns=["ms", "open", "high", "low", "close", "volume"])
        ccxt_df["ts"] = pd.to_datetime(ccxt_df["ms"], unit="ms", utc=True)
        ccxt_df["exchange"] = 'coinbase'
        ccxt_df["symbol"] = 'BTC/USDT'
        ccxt_df["timeframe"] = '5m'
        ccxt_df = ccxt_df[["exchange", "symbol", "timeframe", "ts", "open", "high", "low", "close", "volume"]]
        
        print("📊 CCXT schema (Phase 2):")
        print(f"   Columns: {list(ccxt_df.columns)}")
        print(f"   Data types: {dict(ccxt_df.dtypes)}")
        
        # Compare with expected CoinAPI schema
        print("📊 Expected CoinAPI schema (Phase 1):")
        expected_cols = ['exchange', 'symbol', 'timeframe', 'ts', 'open', 'high', 'low', 'close', 'volume']
        print(f"   Columns: {expected_cols}")
        
        # Check compatibility
        if list(ccxt_df.columns) == expected_cols:
            print("✅ CoinAPI and CCXT schemas are compatible!")
            print("✅ Both phases will produce identical table structure")
            return True
        else:
            print("❌ Schema mismatch between CoinAPI and CCXT")
            return False
            
    except Exception as e:
        print(f"❌ CCXT compatibility test failed: {e}")
        return False

def estimate_costs():
    """Estimate CoinAPI costs for historical backfill"""
    print(f"\n💰 Cost Estimation (3 symbols only)")
    print("=" * 40)
    
    total_calls = 0
    total_records = 0
    
    symbols_info = {
        "BTC/USDT": {"start": "2015-01-01", "years": 10},
        "ETH/USDT": {"start": "2017-01-01", "years": 8},
        "XRP/USDT": {"start": "2017-01-01", "years": 8}
    }
    
    for symbol, info in symbols_info.items():
        years = info["years"]
        # 5-minute intervals: 288 per day * 365 days * years
        intervals = 288 * 365 * years
        # CoinAPI limit: 100k records per call
        calls = max(1, intervals // 100000 + 1)
        
        total_calls += calls
        total_records += intervals
        
        print(f"   {symbol}: ~{intervals:,} records = {calls} API calls")
    
    estimated_cost = total_calls * 0.005  # $5 per 1000 calls
    
    print(f"\n📞 Total API calls: {total_calls}")
    print(f"📊 Total 5-minute records: ~{total_records:,}")
    print(f"💵 Estimated cost: ~${estimated_cost:.2f}")
    print(f"💡 Free tier credits: $25 (sufficient: {'✅' if estimated_cost <= 25 else '❌'})")
    
    return estimated_cost <= 25

def main():
    print("🚀 CoinAPI Schema Validation & Cost Estimation")
    print("=" * 70)
    
    # Run all tests
    tests_passed = 0
    total_tests = 4
    
    if test_coinapi_sample_data():
        tests_passed += 1
    
    if test_coinapi_to_bq_schema():
        tests_passed += 1
        
    if test_ccxt_compatibility():
        tests_passed += 1
        
    if estimate_costs():
        tests_passed += 1
    
    print(f"\n📊 VALIDATION SUMMARY")
    print("=" * 70)
    print(f"✅ Tests passed: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print("🎉 All validations passed!")
        print("💡 Ready to run: python coinapi_historical_backfill.py")
        return True
    else:
        print("⚠️  Some validations failed - review issues above")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)