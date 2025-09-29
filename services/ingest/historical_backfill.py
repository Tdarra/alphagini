#!/usr/bin/env python3
"""
Historical Data Backfill (2015-01-01 to recent)
Uses Yahoo Finance for free historical OHLCV data

Run this ONCE to populate historical data, then use ccxt_ingest.py for ongoing updates
"""

import os
import pandas as pd
import yfinance as yf
from google.cloud import bigquery
from datetime import datetime, timedelta
import time

# Configuration
PROJECT = os.environ.get("ALPHAGINI_PROJECT", "alpha-gini")
DATASET = os.environ.get("ALPHAGINI_BQ_DATASET", "alphagini_marketdata") 
TABLE_ID = f"{PROJECT}.{DATASET}.ohlcv"

# Symbol mapping: Your symbols -> Yahoo Finance tickers
SYMBOL_MAP = {
    "BTC/USDT": "BTC-USD",
    "ETH/USDT": "ETH-USD", 
    "XRP/USDT": "XRP-USD",
    "SOL/USDT": "SOL-USD"
}

# Date range for backfill
START_DATE = "2015-01-01"
END_DATE = "2025-09-22"  # Stop before recent data to avoid overlap with ongoing updates

def fetch_yahoo_historical(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch historical daily OHLCV from Yahoo Finance"""
    print(f"📥 Fetching {ticker} from {start_date} to {end_date}...")
    
    try:
        # Download data
        data = yf.download(ticker, start=start_date, end=end_date, interval="1d")
        
        if data.empty:
            print(f"❌ No data returned for {ticker}")
            return pd.DataFrame()
        
        # Reset index to get Date as column
        data = data.reset_index()
        
        # Rename columns to match our schema
        data = data.rename(columns={
            'Date': 'ts',
            'Open': 'open', 
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
        
        # Select only needed columns
        data = data[['ts', 'open', 'high', 'low', 'close', 'volume']]
        
        # Convert timestamp to UTC (Yahoo gives market timezone)
        data['ts'] = pd.to_datetime(data['ts']).dt.tz_localize('UTC')
        
        print(f"✅ {ticker}: {len(data)} daily records ({data['ts'].min().date()} to {data['ts'].max().date()})")
        return data
        
    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")
        return pd.DataFrame()

def load_to_bigquery(df: pd.DataFrame, symbol: str) -> int:
    """Load DataFrame to BigQuery"""
    if df.empty:
        return 0
    
    # Add metadata columns
    df['exchange'] = 'yahoo_finance'
    df['symbol'] = symbol  # Use original symbol (BTC/USDT)
    df['timeframe'] = '1d'
    
    # Reorder columns to match schema
    df = df[['exchange', 'symbol', 'timeframe', 'ts', 'open', 'high', 'low', 'close', 'volume']]
    
    # Remove any duplicate timestamps
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

def validate_expected_rows():
    """Calculate expected number of rows for validation"""
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    days = (end - start).days
    
    # Account for weekends (crypto trades 7 days, but Yahoo might have gaps)
    expected_per_symbol = days * 0.95  # 95% to account for missing data
    total_expected = expected_per_symbol * len(SYMBOL_MAP)
    
    print(f"📊 Expected rows: ~{int(total_expected):,} total ({int(expected_per_symbol):,} per symbol)")
    return int(total_expected)

def main():
    print("🚀 Historical Data Backfill")
    print("=" * 50)
    print(f"📅 Date range: {START_DATE} to {END_DATE}")
    print(f"🎯 Target table: {TABLE_ID}")
    print(f"📊 Symbols: {list(SYMBOL_MAP.keys())}")
    print("=" * 50)
    
    validate_expected_rows()
    
    total_loaded = 0
    
    for symbol, yahoo_ticker in SYMBOL_MAP.items():
        print(f"\n🔄 Processing {symbol} ({yahoo_ticker})...")
        
        # Fetch historical data
        df = fetch_yahoo_historical(yahoo_ticker, START_DATE, END_DATE)
        
        if not df.empty:
            # Load to BigQuery
            loaded = load_to_bigquery(df, symbol)
            total_loaded += loaded
            
            # Rate limiting (be nice to Yahoo)
            time.sleep(1)
        else:
            print(f"⚠️  Skipping {symbol} - no data available")
    
    print(f"\n✅ Historical Backfill Complete!")
    print(f"📤 Total rows loaded: {total_loaded:,}")
    print(f"\n🔄 Next step: Use ccxt_ingest.py for ongoing updates")

if __name__ == "__main__":
    main()