#!/usr/bin/env python3
"""
Historical Data Backfill using CoinAPI
Fetches 5-minute OHLCV data from 2015+ for crypto symbols

Requires CoinAPI key: https://www.coinapi.io/
Free tier: $25 credits (~5,000 API calls)
"""

import os
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import time

# Configuration
PROJECT = os.environ.get("ALPHAGINI_PROJECT", "alpha-gini")
DATASET = os.environ.get("ALPHAGINI_BQ_DATASET", "alphagini_marketdata") 
TABLE_ID = f"{PROJECT}.{DATASET}.ohlcv"

# CoinAPI Configuration
COINAPI_KEY = os.environ.get("COINAPI_KEY")  # Set this environment variable
COINAPI_BASE = "https://rest.coinapi.io/v1"

# Symbol mapping: Your symbols -> CoinAPI symbol IDs
SYMBOL_MAP = {
    "BTC/USDT": "BTC",      # Bitcoin
    "ETH/USDT": "ETH",      # Ethereum  
    "XRP/USDT": "XRP"       # Ripple
}

# Date ranges (CoinAPI has different start dates for different coins)
SYMBOL_START_DATES = {
    "BTC/USDT": "2015-01-01",   # Bitcoin has earliest data
    "ETH/USDT": "2017-01-01",   # Ethereum launched mid-2015, trading 2017
    "XRP/USDT": "2017-01-01"    # XRP trading started around 2017
}

END_DATE = "2025-09-22"  # Stop before recent data for Phase 2

def fetch_coinapi_ohlcv(symbol_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch 5-minute OHLCV data from CoinAPI"""
    print(f"📥 Fetching {symbol_id} from {start_date} to {end_date}...")
    
    if not COINAPI_KEY:
        raise ValueError("COINAPI_KEY environment variable not set")
    
    headers = {"X-CoinAPI-Key": COINAPI_KEY}
    
    try:
        # CoinAPI OHLCV endpoint for 5-minute data
        url = f"{COINAPI_BASE}/ohlcv/{symbol_id}/USD/history"
        params = {
            "period_id": "5MIN",      # 5-minute intervals
            "time_start": f"{start_date}T00:00:00",
            "time_end": f"{end_date}T23:59:59",
            "limit": 100000          # Max records per request
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if not data:
            print(f"❌ No data returned for {symbol_id}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Rename columns to match our schema
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
        
        # Select required columns
        df = df[['ts', 'open', 'high', 'low', 'close', 'volume']]
        
        print(f"✅ {symbol_id}: {len(df)} 5-minute records ({df['ts'].min()} to {df['ts'].max()})")
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API error for {symbol_id}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing {symbol_id}: {e}")
        return pd.DataFrame()

def load_to_bigquery(df: pd.DataFrame, symbol: str) -> int:
    """Load DataFrame to BigQuery"""
    if df.empty:
        return 0
    
    # Add metadata columns
    df['exchange'] = 'coinapi'
    df['symbol'] = symbol  # Use original symbol (BTC/USDT)
    df['timeframe'] = '5m'
    
    # Reorder columns to match schema
    df = df[['exchange', 'symbol', 'timeframe', 'ts', 'open', 'high', 'low', 'close', 'volume']]
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['exchange', 'symbol', 'timeframe', 'ts'])
    
    try:
        client = bigquery.Client()
        job = client.load_table_from_dataframe(df, TABLE_ID)
        job.result()  # Wait for completion
        
        print(f"📤 Loaded {len(df)} records for {symbol} to BigQuery")
        return len(df)
        
    except Exception as e:
        print(f"❌ BigQuery load error for {symbol}: {e}")
        return 0

def estimate_api_calls():
    """Estimate total API calls needed"""
    print("📊 Estimating API calls needed...")
    
    total_calls = 0
    for symbol, start_date in SYMBOL_START_DATES.items():
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(END_DATE, "%Y-%m-%d")
        days = (end - start).days
        
        # 5-minute intervals: 288 per day
        total_intervals = days * 288
        
        # CoinAPI returns up to 100k records per call
        calls_needed = max(1, total_intervals // 100000 + 1)
        total_calls += calls_needed
        
        print(f"   {symbol}: ~{total_intervals:,} intervals = {calls_needed} API calls")
    
    print(f"📞 Total estimated API calls: {total_calls}")
    print(f"💰 Estimated cost: ~${total_calls * 0.005:.2f} (at $5/1000 calls)")
    
    return total_calls

def main():
    print("🚀 CoinAPI Historical Data Backfill - 5 Minute")
    print("=" * 60)
    print(f"📅 Date ranges: Custom per symbol")
    print(f"⏱️  Timeframe: 5-minute intervals")
    print(f"🎯 Target table: {TABLE_ID}")
    print(f"📊 Symbols: {list(SYMBOL_MAP.keys())}")
    print("=" * 60)
    
    if not COINAPI_KEY:
        print("❌ COINAPI_KEY environment variable not set!")
        print("💡 Get your free API key at: https://www.coinapi.io/")
        print("💡 Then run: export COINAPI_KEY='your_key_here'")
        return
    
    # Estimate costs
    estimate_api_calls()
    
    # Confirm before proceeding
    response = input("\nProceed with historical backfill? (y/N): ")
    if response.lower() != 'y':
        print("❌ Aborted by user")
        return
    
    total_loaded = 0
    
    for symbol, coinapi_symbol in SYMBOL_MAP.items():
        start_date = SYMBOL_START_DATES[symbol]
        print(f"\n🔄 Processing {symbol} ({coinapi_symbol})...")
        print(f"📅 Date range: {start_date} to {END_DATE}")
        
        # Fetch historical data
        df = fetch_coinapi_ohlcv(coinapi_symbol, start_date, END_DATE)
        
        if not df.empty:
            # Load to BigQuery
            loaded = load_to_bigquery(df, symbol)
            total_loaded += loaded
            
            # Rate limiting (CoinAPI allows 100 requests/sec but be conservative)
            time.sleep(0.1)
        else:
            print(f"⚠️  Skipping {symbol} - no data available")
    
    print(f"\n✅ CoinAPI Historical Backfill Complete!")
    print(f"📤 Total 5-minute rows loaded: {total_loaded:,}")
    print(f"\n🔄 Next step: Use ccxt_ingest.py for ongoing updates (Phase 2)")

if __name__ == "__main__":
    main()