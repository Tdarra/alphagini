import os
import time
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta

# CoinGecko API (free tier: 10-50 calls/minute)
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# Symbol mapping
SYMBOL_MAP = {
    "BTC/USDT": "bitcoin",
    "ETH/USDT": "ethereum", 
    "XRP/USDT": "ripple",
    "SOL/USDT": "solana"
}

def fetch_historical_daily(coin_id: str, start_date: str, end_date: str):
    """Fetch daily OHLCV from CoinGecko"""
    url = f"{COINGECKO_BASE}/coins/{coin_id}/ohlc"
    params = {
        "vs_currency": "usd",
        "days": "max"  # Get maximum history
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close"])
    df["ts"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["volume"] = 0  # CoinGecko free tier doesn't include volume
    
    # Filter date range
    df = df[(df["ts"] >= start_date) & (df["ts"] <= end_date)]
    return df[["ts", "open", "high", "low", "close", "volume"]]

def backfill_historical_data():
    """Backfill historical daily data from CoinGecko"""
    bq = bigquery.Client()
    
    for symbol, coin_id in SYMBOL_MAP.items():
        print(f"Fetching {symbol} ({coin_id})...")
        
        try:
            df = fetch_historical_daily(coin_id, "2015-01-01", "2025-09-22")
            
            # Format for BigQuery
            df["exchange"] = "coingecko"
            df["symbol"] = symbol
            df["timeframe"] = "1d"
            df = df[["exchange", "symbol", "timeframe", "ts", "open", "high", "low", "close", "volume"]]
            
            # Load to BigQuery
            table_id = f"{os.environ['ALPHAGINI_PROJECT']}.{os.environ.get('ALPHAGINI_BQ_DATASET', 'alphagini_marketdata')}.ohlcv"
            job = bq.load_table_from_dataframe(df, table_id)
            job.result()
            
            print(f"Loaded {len(df)} daily records for {symbol}")
            time.sleep(6)  # Rate limit: 10 calls/minute
            
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")

if __name__ == "__main__":
    ...
    # backfill_historical_data()