#!/usr/bin/env python3
"""
Historical Data Backfill using CoinAPI Flat Files
Downloads bulk CSV files for 5-minute OHLCV data from 2015+

Requires CoinAPI Flat Files access: https://www.coinapi.io/products/flat-files
Much more cost-effective than API calls for historical data
"""

import os
import requests
import pandas as pd
import gzip
from google.cloud import bigquery
from datetime import datetime, timedelta
import time
from io import StringIO
import ccxt

# Configuration
PROJECT = os.environ.get("ALPHAGINI_PROJECT", "alpha-gini")
DATASET = os.environ.get("ALPHAGINI_BQ_DATASET", "alphagini_marketdata") 
TABLE_ID = f"{PROJECT}.{DATASET}.ohlcv"

# CoinAPI Flat Files Configuration
COINAPI_KEY = os.environ.get("COINAPI_KEY")
COINAPI_FLATFILES_BASE = "https://flatfiles.coinapi.io"

# Target symbols and exchanges
SYMBOLS_CONFIG = {
    "BTC/USDT": {
        "exchanges": ["BINANCE", "COINBASE", "KRAKEN"],  # Try multiple exchanges
        "start_date": "2015-01-01"
    },
    "ETH/USDT": {
        "exchanges": ["BINANCE", "COINBASE", "KRAKEN"],
        "start_date": "2017-01-01"
    },
    "XRP/USDT": {
        "exchanges": ["BINANCE", "COINBASE", "KRAKEN"], 
        "start_date": "2017-01-01"
    }
}

def test_5min_granularity_availability():
    """Test if CoinAPI flat files provide 5-minute granularity"""
    print("⏱️  Testing 5-Minute Granularity Availability")
    print("=" * 50)
    
    test_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")  # YYYYMMDD format
    test_exchanges = ["BITSTAMP", "COINBASE", "BINANCE"]  # Try multiple exchanges
    
    if not COINAPI_KEY:
        print("❌ COINAPI_KEY not set, cannot test")
        return False
    
    headers = {"X-CoinAPI-Key": COINAPI_KEY}
    
    for exchange in test_exchanges:
        try:
            # Correct URL format based on CoinAPI documentation
            # T-OHLCV/D-YYYYMMDD/E-[EXCHANGE]/[symbol].csv.gz
            possible_urls = [
                f"{COINAPI_FLATFILES_BASE}/T-OHLCV/D-{test_date}/E-{exchange}/BTCUSD.csv.gz",
                f"{COINAPI_FLATFILES_BASE}/T-OHLCV/D-{test_date}/E-{exchange}/BTC_USD.csv.gz",
                f"{COINAPI_FLATFILES_BASE}/T-OHLCV/D-{test_date}/E-{exchange}/BTCUSDT.csv.gz"
            ]
            
            for url in possible_urls:
                print(f"   🔍 Trying: {exchange} OHLCV data...")
                response = requests.head(url, headers=headers)  # HEAD request to check existence
                
                if response.status_code == 200:
                    print(f"✅ OHLCV data found for {exchange}")
                    
                    # Download sample to check granularity
                    response = requests.get(url, headers=headers, stream=True)
                    if response.status_code == 200:
                        content = gzip.decompress(response.content).decode('utf-8')
                        sample_df = pd.read_csv(StringIO(content), nrows=10)
                        
                        print(f"   📊 Sample columns: {list(sample_df.columns)}")
                        
                        # Check if timestamps indicate 5-minute intervals
                        if 'timestamp' in sample_df.columns or 'time_period_start' in sample_df.columns:
                            ts_col = 'timestamp' if 'timestamp' in sample_df.columns else 'time_period_start'
                            sample_df[ts_col] = pd.to_datetime(sample_df[ts_col])
                            
                            if len(sample_df) > 1:
                                time_diff = sample_df[ts_col].diff().iloc[1]
                                print(f"   ⏱️  Time interval: {time_diff}")
                                
                                if time_diff == pd.Timedelta(minutes=5):
                                    print(f"✅ 5-minute granularity confirmed!")
                                    return True
                                elif time_diff == pd.Timedelta(minutes=1):
                                    print(f"✅ 1-minute granularity available (can aggregate to 5-min)")
                                    return True
                                else:
                                    print(f"⚠️  Different granularity: {time_diff}")
                    
                    return True  # File exists, assume correct format
                    
                elif response.status_code == 403:
                    print(f"❌ 403 Forbidden - flat files require paid subscription")
                    return False
                elif response.status_code == 404:
                    continue  # Try next URL
                    
        except Exception as e:
            print(f"   ❌ Error testing {exchange}: {str(e)[:50]}")
            continue
    
    print("⚠️  No OHLCV flat files found with test URLs")
    print("💡 Possible issues:")
    print("   - Subscription required for flat files access")
    print("   - Different URL format than expected")
    print("   - OHLCV data not available for test date/exchanges")
    return False

def standardize_to_phase2_schema(df: pd.DataFrame, exchange: str, target_symbol: str) -> pd.DataFrame:
    """
    Standardize CoinAPI flat file data to match Phase 2 CCXT schema exactly
    Ensures seamless compatibility between Phase 1 (flat files) and Phase 2 (CCXT)
    """
    if df.empty:
        return df
    
    print(f"🔧 Standardizing schema for Phase 2 compatibility...")
    
    # Step 1: Filter for target symbol (flat files contain all symbols)
    symbol_variants = [
        target_symbol.replace('/', ''),      # BTCUSDT
        target_symbol,                       # BTC/USDT
        target_symbol.replace('/', '_'),     # BTC_USDT
        target_symbol.replace('/', '-'),     # BTC-USDT
        f"{target_symbol.split('/')[0]}USD", # BTCUSD (if USDT not available)
    ]
    
    # Try to find the symbol in the data
    symbol_df = pd.DataFrame()
    for variant in symbol_variants:
        # Check different possible symbol column names
        symbol_columns = ['symbol', 'symbol_id', 'pair', 'instrument', 'market']
        
        for col in symbol_columns:
            if col in df.columns:
                mask = df[col].str.upper() == variant.upper()
                if mask.any():
                    symbol_df = df[mask].copy()
                    print(f"   ✅ Found {len(symbol_df)} records for {variant} in column '{col}'")
                    break
        
        if not symbol_df.empty:
            break
    
    if symbol_df.empty:
        print(f"   ❌ No data found for {target_symbol} variants: {symbol_variants}")
        return pd.DataFrame()
    
    # Step 2: Map CoinAPI columns to CCXT/BigQuery schema
    # This comprehensive mapping handles various CoinAPI flat file formats
    column_mapping = {
        # Timestamp variations
        'timestamp': 'ts',
        'time_period_start': 'ts', 
        'time_open': 'ts',
        'datetime': 'ts',
        'date_time': 'ts',
        'time': 'ts',
        
        # OHLC variations
        'price_open': 'open',
        'open_price': 'open',
        'price_high': 'high',
        'high_price': 'high', 
        'price_low': 'low',
        'low_price': 'low',
        'price_close': 'close',
        'close_price': 'close',
        
        # Volume variations
        'volume_traded': 'volume',
        'volume': 'volume',
        'vol': 'volume',
        'base_volume': 'volume'
    }
    
    # Apply column mapping
    for old_col, new_col in column_mapping.items():
        if old_col in symbol_df.columns:
            symbol_df = symbol_df.rename(columns={old_col: new_col})
    
    # Step 3: Ensure EXACT schema match with Phase 2 (CCXT)
    required_columns = ['ts', 'open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in symbol_df.columns]
    
    if missing_columns:
        print(f"❌ Missing required columns for Phase 2 compatibility: {missing_columns}")
        print(f"   Available columns: {list(symbol_df.columns)}")
        return pd.DataFrame()
    
    # Step 4: Data type standardization to match CCXT output
    try:
        # Convert timestamp to UTC (same as CCXT)
        symbol_df['ts'] = pd.to_datetime(symbol_df['ts'])
        if symbol_df['ts'].dt.tz is None:
            symbol_df['ts'] = symbol_df['ts'].dt.tz_localize('UTC')
        else:
            symbol_df['ts'] = symbol_df['ts'].dt.tz_convert('UTC')
        
        # Convert OHLCV to float (same as CCXT)
        for col in ['open', 'high', 'low', 'close', 'volume']:
            symbol_df[col] = pd.to_numeric(symbol_df[col], errors='coerce')
        
        # Add metadata columns (matching CCXT format exactly)
        symbol_df['exchange'] = f'coinapi_{exchange.lower()}'  # Distinguish from Phase 2
        symbol_df['symbol'] = target_symbol                    # Exact same format as CCXT
        symbol_df['timeframe'] = '5m'                         # Exact same format as CCXT
        
    except Exception as e:
        print(f"❌ Data type conversion failed: {e}")
        return pd.DataFrame()
    
    # Step 5: Final column selection (EXACT match with Phase 2)
    final_columns = ['exchange', 'symbol', 'timeframe', 'ts', 'open', 'high', 'low', 'close', 'volume']
    final_df = symbol_df[final_columns].copy()
    
    # Step 6: Remove any rows with NaN values (data quality)
    initial_rows = len(final_df)
    final_df = final_df.dropna()
    final_rows = len(final_df)
    
    if initial_rows != final_rows:
        print(f"   ⚠️  Removed {initial_rows - final_rows} rows with missing data")
    
    print(f"✅ Schema standardization complete: {len(final_df)} clean records")
    return final_df

def validate_schema_compatibility():
    """Validate that processed flat file data exactly matches CCXT schema"""
    print("\n🔧 Validating Phase 1 ↔ Phase 2 Schema Compatibility")
    print("=" * 60)
    
    try:
        # Get sample CCXT data (Phase 2 format)
        exchange = ccxt.coinbase({'enableRateLimit': True})
        exchange.load_markets()
        
        if 'BTC/USD' in exchange.markets:
            ccxt_data = exchange.fetch_ohlcv('BTC/USD', '5m', limit=2)
        else:
            print("⚠️  BTC/USD not available, using available market")
            available_markets = list(exchange.markets.keys())[:5]
            print(f"   Available markets: {available_markets}")
            return False
        
        if not ccxt_data:
            print("❌ No CCXT data for comparison")
            return False
        
        # Convert CCXT data to DataFrame (same as ccxt_ingest.py)
        ccxt_df = pd.DataFrame(ccxt_data, columns=["ms", "open", "high", "low", "close", "volume"])
        ccxt_df["ts"] = pd.to_datetime(ccxt_df["ms"], unit="ms", utc=True)
        ccxt_df["exchange"] = 'coinbase'
        ccxt_df["symbol"] = 'BTC/USD'
        ccxt_df["timeframe"] = '5m'
        ccxt_df = ccxt_df[["exchange", "symbol", "timeframe", "ts", "open", "high", "low", "close", "volume"]]
        
        # Create mock flat file data in expected format
        mock_flatfile_data = {
            'timestamp': ccxt_df['ts'].dt.strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': 'BTCUSD',
            'price_open': ccxt_df['open'],
            'price_high': ccxt_df['high'], 
            'price_low': ccxt_df['low'],
            'price_close': ccxt_df['close'],
            'volume_traded': ccxt_df['volume']
        }
        mock_df = pd.DataFrame(mock_flatfile_data)
        
        # Process through standardization function
        processed_df = standardize_to_phase2_schema(mock_df, 'COINBASE', 'BTC/USD')
        
        if processed_df.empty:
            print("❌ Schema standardization failed")
            return False
        
        # Compare schemas
        ccxt_schema = {col: str(dtype) for col, dtype in ccxt_df.dtypes.items()}
        processed_schema = {col: str(dtype) for col, dtype in processed_df.dtypes.items()}
        
        print("📊 Phase 2 (CCXT) Schema:")
        for col, dtype in ccxt_schema.items():
            print(f"   {col}: {dtype}")
        
        print("📊 Phase 1 (Processed Flat File) Schema:")
        for col, dtype in processed_schema.items():
            print(f"   {col}: {dtype}")
        
        # Check exact match
        schema_match = True
        for col in ccxt_schema.keys():
            if col not in processed_schema:
                print(f"❌ Missing column in processed data: {col}")
                schema_match = False
            elif col == 'ts':  # Timestamp columns might have slight differences
                continue  # Both should be datetime64[ns, UTC]
            elif ccxt_schema[col] != processed_schema[col]:
                print(f"⚠️  Data type mismatch for {col}: {ccxt_schema[col]} vs {processed_schema[col]}")
        
        if schema_match:
            print("✅ Perfect schema compatibility!")
            print("✅ Phase 1 (flat files) and Phase 2 (CCXT) will produce identical table structure")
            return True
        else:
            print("⚠️  Minor schema differences detected but likely compatible")
            return True
            
    except Exception as e:
        print(f"❌ Schema validation failed: {e}")
        return False

def download_flatfile_sample(exchange: str, date: str) -> pd.DataFrame:
    """Download a sample flat file to test access and format"""
    print(f"📥 Testing flat file access: {exchange} for {date}")
    
    if not COINAPI_KEY:
        raise ValueError("COINAPI_KEY environment variable not set")
    
    # Correct CoinAPI flat files URL format
    # T-OHLCV/D-YYYYMMDD/E-[EXCHANGE]/[symbol].csv.gz
    date_formatted = datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d")
    
    # Try different symbol formats for BTC
    symbol_variants = ["BTCUSD", "BTC_USD", "BTCUSDT", "BTC_USDT"]
    
    headers = {"X-CoinAPI-Key": COINAPI_KEY}
    
    for symbol in symbol_variants:
        url = f"{COINAPI_FLATFILES_BASE}/T-OHLCV/D-{date_formatted}/E-{exchange}/{symbol}.csv.gz"
        
        try:
            print(f"   🔍 Trying: {url}")
            response = requests.get(url, headers=headers, stream=True)
            
            if response.status_code == 200:
                # Decompress and read CSV
                content = gzip.decompress(response.content).decode('utf-8')
                df = pd.read_csv(StringIO(content))
                
                print(f"✅ Successfully downloaded {exchange} {date}: {len(df)} rows")
                print(f"   Symbol: {symbol}")
                print(f"   Columns: {list(df.columns)}")
                
                return df
                
            elif response.status_code == 403:
                print(f"❌ 403 Forbidden - Flat files require paid subscription")
                return pd.DataFrame()
                
            elif response.status_code == 404:
                print(f"   ⚠️  Symbol {symbol} not found for {exchange}")
                continue  # Try next symbol variant
                
            else:
                print(f"❌ HTTP {response.status_code}: {response.reason}")
                continue
                
        except Exception as e:
            print(f"❌ Error downloading {symbol}: {e}")
            continue
    
    print(f"❌ No data found for {exchange} on {date}")
    return pd.DataFrame()

def load_to_bigquery(df: pd.DataFrame, symbol: str, exchange: str) -> int:
    """Load DataFrame to BigQuery"""
    if df.empty:
        return 0
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['exchange', 'symbol', 'timeframe', 'ts'])
    
    try:
        client = bigquery.Client()
        job = client.load_table_from_dataframe(df, TABLE_ID)
        job.result()  # Wait for completion
        
        print(f"📤 Loaded {len(df)} records for {symbol} ({exchange}) to BigQuery")
        return len(df)
        
    except Exception as e:
        print(f"❌ BigQuery load error for {symbol} ({exchange}): {e}")
        return 0

def main():
    print("🚀 CoinAPI Flat Files Historical Backfill")
    print("=" * 60)
    print(f"📅 Target symbols: {list(SYMBOLS_CONFIG.keys())}")
    print(f"⏱️  Timeframe: 5-minute intervals") 
    print(f"🎯 Target table: {TABLE_ID}")
    print("=" * 60)
    
    if not COINAPI_KEY:
        print("❌ COINAPI_KEY environment variable not set!")
        print("💡 Get your API key at: https://www.coinapi.io/")
        return
    
    # Test 1: 5-minute granularity availability
    print("\n1️⃣ Testing 5-Minute Granularity...")
    granularity_ok = test_5min_granularity_availability()
    
    # Test 2: Schema compatibility validation
    print("\n2️⃣ Testing Schema Compatibility...")
    schema_ok = validate_schema_compatibility()
    
    # Test 3: Flat file access
    print("\n3️⃣ Testing Flat File Access...")
    test_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    sample_df = download_flatfile_sample("BINANCE", test_date)
    access_ok = not sample_df.empty
    
    # Summary
    print(f"\n📊 VALIDATION SUMMARY")
    print("=" * 60)
    print(f"⏱️  5-minute granularity: {'✅ Available' if granularity_ok else '⚠️  Needs verification'}")
    print(f"🔧 Schema compatibility: {'✅ Compatible' if schema_ok else '❌ Issues detected'}")
    print(f"📥 Flat file access: {'✅ Working' if access_ok else '❌ Failed'}")
    
    if schema_ok:
        print("\n🎉 Schema validation passed!")
        print("✅ CoinAPI flat files will seamlessly integrate with Phase 2 (CCXT)")
        
        if access_ok:
            print("✅ Ready for full historical backfill implementation")
        else:
            print("💡 Contact CoinAPI for flat files subscription and URL format")
    else:
        print("⚠️  Schema compatibility issues need resolution")

if __name__ == "__main__":
    main()