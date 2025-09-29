#!/usr/bin/env python3
"""
Local test script to validate CCXT exchanges for 2015+ historical data
Run this before gcloud builds to avoid unnecessary cloud costs
"""

import ccxt
import pandas as pd
from datetime import datetime

# Test symbols
SYMBOLS = ["BTC/USDT", "ETH/USDT", "XRP/USDT", "SOL/USDT"]

# Target date for historical data requirement
TARGET_DATE_2015 = datetime(2015, 1, 1)
TARGET_MS_2015 = int(TARGET_DATE_2015.timestamp() * 1000)

# Potential US-friendly exchanges to test
EXCHANGES_TO_TEST = [
    "coinbase",           # Coinbase
    "coinbasepro",        # Coinbase Pro (might be legacy)
    "coinbaseadvanced",   # Coinbase Advanced Trade
    "gemini",            # Gemini (US-based)
    "bittrex",           # Bittrex (US-based)
    "kraken",            # Kraken (for comparison)
]

def test_exchange_2015_data(exchange_id: str):
    """Test if exchange can provide data from 2015-01-01"""
    print(f"\n🔍 Testing {exchange_id} for 2015+ historical data...")
    
    try:
        # Check if exchange exists
        if not hasattr(ccxt, exchange_id):
            print(f"❌ {exchange_id} - Exchange not found in CCXT")
            return False, []
        
        # Create exchange instance
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({'enableRateLimit': True})
        
        # Load markets
        exchange.load_markets()
        print(f"✅ {exchange_id} - Exchange loaded successfully")
        
        symbols_with_2015_data = []
        
        # Test 2015 data availability for each symbol
        for symbol in SYMBOLS:
            if symbol not in exchange.markets:
                print(f"   ❌ {symbol} - Not available on {exchange_id}")
                continue
                
            try:
                print(f"   🔍 Testing {symbol} for 2015 data...")
                
                # Try to fetch data from 2015-01-01
                data_2015 = exchange.fetch_ohlcv(
                    symbol, 
                    '1d', 
                    since=TARGET_MS_2015, 
                    limit=10  # Get first 10 days of 2015
                )
                
                if data_2015 and len(data_2015) > 0:
                    first_date = pd.to_datetime(data_2015[0][0], unit='ms')
                    last_date = pd.to_datetime(data_2015[-1][0], unit='ms')
                    
                    # Check if data starts in 2015
                    if first_date.year == 2015:
                        print(f"   ✅ {symbol} - 2015 data available! ({first_date.date()} to {last_date.date()})")
                        symbols_with_2015_data.append(symbol)
                    else:
                        print(f"   ⚠️  {symbol} - Data starts from {first_date.date()} (not 2015)")
                else:
                    print(f"   ❌ {symbol} - No data from 2015")
                    
            except Exception as e:
                print(f"   ❌ {symbol} - Error: {str(e)[:80]}...")
        
        success = len(symbols_with_2015_data) > 0
        return success, symbols_with_2015_data
        
    except Exception as e:
        print(f"❌ {exchange_id} - Failed to initialize: {str(e)[:80]}...")
        return False, []

def main():
    print("🚀 CCXT Exchange Validation Test for 2015+ Historical Data")
    print("=" * 60)
    print(f"📅 Target: Data from {TARGET_DATE_2015.date()} for symbols: {SYMBOLS}")
    print("=" * 60)
    
    best_exchanges = []
    
    for exchange_id in EXCHANGES_TO_TEST:
        success, symbols_with_data = test_exchange_2015_data(exchange_id)
        
        if success:
            best_exchanges.append({
                'exchange': exchange_id,
                'symbols_count': len(symbols_with_data),
                'symbols': symbols_with_data
            })
    
    print(f"\n📊 RESULTS SUMMARY:")
    print("=" * 60)
    
    if best_exchanges:
        # Sort by number of symbols with 2015 data
        best_exchanges.sort(key=lambda x: x['symbols_count'], reverse=True)
        
        for result in best_exchanges:
            print(f"✅ {result['exchange']}: {result['symbols_count']}/4 symbols have 2015+ data")
            print(f"   Symbols: {result['symbols']}")
        
        # Recommend the best one
        best = best_exchanges[0]
        print(f"\n💡 RECOMMENDATION:")
        print(f"Update env.yaml with: ALPHAGINI_EXCHANGE: \"{best['exchange']}\"")
        
        if best['symbols_count'] == 4:
            print("🎉 Perfect! All symbols have 2015+ historical data!")
        else:
            print(f"⚠️  Only {best['symbols_count']}/4 symbols have full historical data")
            
    else:
        print("❌ No exchanges found with 2015+ historical data!")
        print("💡 Consider using a different data source (Alpha Vantage, Yahoo Finance, etc.)")

if __name__ == "__main__":
    main()