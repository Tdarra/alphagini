#!/usr/bin/env python3
"""
Validation test for historical_backfill.py
Tests data availability and row counts before running the actual backfill
"""

import yfinance as yf
import pandas as pd
from datetime import datetime

# Configuration (matching historical_backfill.py)
SYMBOL_MAP = {
    "BTC/USDT": "BTC-USD",
    "ETH/USDT": "ETH-USD", 
    "XRP/USDT": "XRP-USD",
    "SOL/USDT": "SOL-USD"
}

START_DATE = "2015-01-01"
END_DATE = "2025-09-22"

def test_yahoo_5min_data_availability():
    """Test if Yahoo Finance can provide 5-minute historical data"""
    print("🔍 Testing Yahoo Finance 5-Minute Data Availability")
    print("=" * 60)
    
    total_expected = 0
    results = []
    
    for symbol, yahoo_ticker in SYMBOL_MAP.items():
        print(f"\n📊 Testing {symbol} ({yahoo_ticker}) for 5-minute data...")
        
        try:
            # Test recent 5-minute data (last 7 days - Yahoo's typical limit for 5m)
            print(f"   🔍 Testing recent 5-minute data...")
            recent_5m = yf.download(yahoo_ticker, period="7d", interval="5m")
            
            if recent_5m.empty:
                print(f"❌ {yahoo_ticker} - No recent 5-minute data available")
                results.append({'symbol': symbol, 'status': 'NO_5MIN', 'rows': 0})
                continue
            
            print(f"✅ {yahoo_ticker} - Recent 5-minute data available ({len(recent_5m)} rows)")
            
            # Test how far back 5-minute data goes
            print(f"   🔍 Testing historical 5-minute data range...")
            
            # Try different historical periods to find the limit
            test_periods = ["30d", "60d", "90d", "1y", "2y", "5y", "max"]
            max_period_with_5m = None
            
            for period in test_periods:
                try:
                    test_data = yf.download(yahoo_ticker, period=period, interval="5m")
                    if not test_data.empty:
                        max_period_with_5m = period
                        earliest_5m = test_data.index.min()
                        latest_5m = test_data.index.max()
                        rows_5m = len(test_data)
                        print(f"   ✅ 5-minute data for {period}: {rows_5m:,} rows ({earliest_5m.date()} to {latest_5m.date()})")
                    else:
                        print(f"   ❌ No 5-minute data for {period}")
                        break
                except:
                    print(f"   ❌ Error testing {period}")
                    break
            
            if max_period_with_5m:
                # Calculate if we can get significant historical 5-minute data
                if earliest_5m.year <= 2020:  # If we can get 2020+ data
                    print(f"✅ {yahoo_ticker} - Substantial 5-minute history available")
                    
                    # Estimate 5-minute rows from earliest date to END_DATE
                    start_date = max(earliest_5m, datetime.strptime("2015-01-01", "%Y-%m-%d"))
                    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
                    
                    # 5-minute intervals: 288 per day (24 * 60 / 5)
                    days = (end_date - start_date).days
                    estimated_5m_rows = int(days * 288 * 0.9)  # 90% accounting for missing data
                    
                    print(f"📈 {yahoo_ticker} - Estimated 5-minute rows: ~{estimated_5m_rows:,}")
                    total_expected += estimated_5m_rows
                    
                    results.append({
                        'symbol': symbol, 
                        'yahoo_ticker': yahoo_ticker,
                        'status': 'SUCCESS_5MIN', 
                        'estimated_rows': estimated_5m_rows,
                        'earliest_5m': earliest_5m,
                        'max_period': max_period_with_5m
                    })
                else:
                    print(f"⚠️  {yahoo_ticker} - Limited 5-minute history (starts {earliest_5m.year})")
                    results.append({'symbol': symbol, 'status': 'LIMITED_5MIN', 'earliest': earliest_5m})
            else:
                print(f"❌ {yahoo_ticker} - No 5-minute historical data found")
                results.append({'symbol': symbol, 'status': 'NO_5MIN_HISTORY', 'rows': 0})
                
        except Exception as e:
            print(f"❌ {yahoo_ticker} - Error: {str(e)[:60]}...")
            results.append({'symbol': symbol, 'status': 'ERROR', 'rows': 0})
    
    return results, total_expected

def validate_5min_schema_compatibility():
    """Test that Yahoo Finance 5-minute data matches our BigQuery schema"""
    print(f"\n🔧 Testing 5-Minute Schema Compatibility")
    print("=" * 40)
    
    # Test with BTC as sample
    try:
        sample = yf.download("BTC-USD", period="7d", interval="5m")
        
        if sample.empty:
            print("❌ Cannot test schema - no 5-minute sample data")
            return False
        
        # Reset index to get Datetime as column
        sample = sample.reset_index()
        
        # Check required columns exist
        required_yahoo_cols = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
        missing_cols = [col for col in required_yahoo_cols if col not in sample.columns]
        
        if missing_cols:
            print(f"❌ Missing columns: {missing_cols}")
            return False
        
        # Test data type conversion for 5-minute data
        sample = sample.rename(columns={
            'Datetime': 'ts',
            'Open': 'open', 
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
        
        # Test timestamp conversion (5-minute data already has timezone info)
        if sample['ts'].dt.tz is None:
            sample['ts'] = sample['ts'].dt.tz_localize('UTC')
        else:
            sample['ts'] = sample['ts'].dt.tz_convert('UTC')
        
        print("✅ 5-minute schema compatibility confirmed")
        print(f"   Sample row count: {len(sample)}")
        print(f"   Date range: {sample['ts'].min()} to {sample['ts'].max()}")
        print(f"   Data types: {dict(sample.dtypes)}")
        print(f"   Sample intervals: {sample['ts'].diff().mode().iloc[0]} (should be 5 minutes)")
        
        return True
        
    except Exception as e:
        print(f"❌ 5-minute schema test failed: {e}")
        return False

def main():
    print("🚀 Historical Backfill Validation Test - 5-Minute Data")
    print("=" * 70)
    print(f"📅 Target date range: {START_DATE} to {END_DATE}")
    print(f"⏱️  Target timeframe: 5-minute intervals")
    print(f"🎯 Symbols: {list(SYMBOL_MAP.keys())}")
    print("=" * 70)
    
    # Test 5-minute data availability
    results, total_expected = test_yahoo_5min_data_availability()
    
    # Test 5-minute schema compatibility
    schema_ok = validate_5min_schema_compatibility()
    
    # Summary
    print(f"\n📊 5-MINUTE DATA VALIDATION SUMMARY")
    print("=" * 70)
    
    success_count = len([r for r in results if r['status'] == 'SUCCESS_5MIN'])
    limited_count = len([r for r in results if r['status'] == 'LIMITED_5MIN'])
    
    print(f"✅ Symbols with substantial 5-min history: {success_count}/{len(SYMBOL_MAP)}")
    print(f"⚠️  Symbols with limited 5-min history: {limited_count}/{len(SYMBOL_MAP)}")
    print(f"📈 Total estimated 5-minute rows: ~{total_expected:,}")
    print(f"🔧 5-minute schema compatibility: {'✅ PASS' if schema_ok else '❌ FAIL'}")
    
    # Detailed results
    print(f"\n📋 DETAILED RESULTS:")
    for result in results:
        symbol = result['symbol']
        status = result['status']
        
        if status == 'SUCCESS_5MIN':
            print(f"✅ {symbol}: 5-min data from {result['earliest_5m'].date()} (~{result['estimated_rows']:,} rows)")
        elif status == 'LIMITED_5MIN':
            print(f"⚠️  {symbol}: Limited 5-min data from {result['earliest'].date()}")
        elif status == 'NO_5MIN':
            print(f"❌ {symbol}: No 5-minute data available")
        elif status == 'NO_5MIN_HISTORY':
            print(f"❌ {symbol}: No historical 5-minute data")
        else:
            print(f"❌ {symbol}: {status}")
    
    if success_count > 0 and schema_ok:
        print(f"\n🎉 PARTIAL SUCCESS!")
        print(f"💡 {success_count} symbols have sufficient 5-minute historical data")
        print(f"💡 Consider proceeding with available symbols or using alternative data sources")
        return True
    elif limited_count > 0 and schema_ok:
        print(f"\n⚠️  LIMITED SUCCESS!")
        print(f"💡 {limited_count} symbols have some 5-minute data but limited history")
        print(f"💡 Consider shorter timeframes or alternative data sources")
        return False
    else:
        print(f"\n❌ VALIDATION FAILED!")
        print(f"💡 Yahoo Finance doesn't provide sufficient 5-minute historical data")
        print(f"💡 Consider alternative approaches:")
        print(f"   - Use daily data for historical backfill + 5-min for recent data")
        print(f"   - Use paid data providers (Alpha Vantage Premium, Polygon, etc.)")
        print(f"   - Use crypto-specific APIs (CoinAPI, CryptoCompare, etc.)")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)