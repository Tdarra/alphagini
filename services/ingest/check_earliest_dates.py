#!/usr/bin/env python3
"""
Check earliest available dates for each symbol on Yahoo Finance
This helps determine realistic start dates for historical backfill
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# Symbol mapping
SYMBOL_MAP = {
    "BTC/USDT": "BTC-USD",
    "ETH/USDT": "ETH-USD", 
    "XRP/USDT": "XRP-USD",
    "SOL/USDT": "SOL-USD"
}

def find_earliest_data(ticker: str, symbol: str):
    """Find the earliest available data for a given ticker"""
    print(f"\n🔍 Checking {symbol} ({ticker})...")
    
    try:
        # Try to get maximum historical data
        data = yf.download(ticker, start="2010-01-01", end="2025-12-31", interval="1d")
        
        if data.empty:
            print(f"❌ {ticker} - No data available")
            return None, None, 0
        
        # Get date range
        data = data.reset_index()
        earliest_date = data['Date'].min()
        latest_date = data['Date'].max()
        total_rows = len(data)
        
        print(f"✅ {ticker} - Data available")
        print(f"   📅 Earliest: {earliest_date.date()}")
        print(f"   📅 Latest: {latest_date.date()}")
        print(f"   📊 Total rows: {total_rows:,}")
        
        # Check if we have 2015+ data
        if earliest_date.year <= 2015:
            print(f"   🎉 Has 2015+ data!")
        else:
            print(f"   ⚠️  Data starts {earliest_date.year - 2015} years after 2015")
        
        return earliest_date, latest_date, total_rows
        
    except Exception as e:
        print(f"❌ {ticker} - Error: {str(e)[:60]}...")
        return None, None, 0

def calculate_optimal_start_dates():
    """Calculate the best start date for each symbol"""
    print("🚀 Yahoo Finance Data Availability Check")
    print("=" * 60)
    
    results = {}
    
    for symbol, yahoo_ticker in SYMBOL_MAP.items():
        earliest, latest, rows = find_earliest_data(yahoo_ticker, symbol)
        
        if earliest:
            results[symbol] = {
                'yahoo_ticker': yahoo_ticker,
                'earliest_date': earliest,
                'latest_date': latest,
                'total_rows': rows,
                'has_2015_data': earliest.year <= 2015
            }
        else:
            results[symbol] = {
                'yahoo_ticker': yahoo_ticker,
                'earliest_date': None,
                'latest_date': None,
                'total_rows': 0,
                'has_2015_data': False
            }
    
    # Summary and recommendations
    print(f"\n📊 SUMMARY & RECOMMENDATIONS")
    print("=" * 60)
    
    symbols_with_2015 = []
    symbols_without_2015 = []
    
    for symbol, data in results.items():
        if data['has_2015_data']:
            symbols_with_2015.append(symbol)
            print(f"✅ {symbol}: Use 2015-01-01 start date ({data['total_rows']:,} rows)")
        elif data['earliest_date']:
            symbols_without_2015.append(symbol)
            optimal_start = data['earliest_date'].strftime('%Y-%m-%d')
            print(f"⚠️  {symbol}: Use {optimal_start} start date ({data['total_rows']:,} rows)")
        else:
            print(f"❌ {symbol}: No Yahoo Finance data available")
    
    # Generate updated symbol mapping with optimal dates
    if symbols_with_2015 or symbols_without_2015:
        print(f"\n💡 UPDATED CODE FOR historical_backfill.py:")
        print("-" * 50)
        print("# Updated symbol mapping with optimal start dates")
        print("SYMBOL_CONFIG = {")
        
        for symbol, data in results.items():
            if data['earliest_date']:
                start_date = "2015-01-01" if data['has_2015_data'] else data['earliest_date'].strftime('%Y-%m-%d')
                print(f'    "{symbol}": {{"yahoo_ticker": "{data["yahoo_ticker"]}", "start_date": "{start_date}"}},')
        
        print("}")
        print()
        print("# In main(), use:")
        print("for symbol, config in SYMBOL_CONFIG.items():")
        print("    df = fetch_yahoo_historical(config['yahoo_ticker'], config['start_date'], END_DATE)")
    
    return results

def main():
    results = calculate_optimal_start_dates()
    
    # Count successful symbols
    available_symbols = len([r for r in results.values() if r['earliest_date'] is not None])
    total_symbols = len(results)
    
    print(f"\n🎯 FINAL SUMMARY:")
    print(f"Available symbols: {available_symbols}/{total_symbols}")
    
    if available_symbols > 0:
        print("✅ Ready to proceed with historical backfill using optimal dates")
    else:
        print("❌ Consider alternative data sources")

if __name__ == "__main__":
    main()