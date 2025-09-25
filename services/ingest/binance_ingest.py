import os, time, math
import pandas as pd
import ccxt
from google.cloud import bigquery

PROJECT = os.environ["ALPHAGINI_PROJECT"]
DATASET = os.environ.get("ALPHAGINI_BQ_DATASET","alphagini_marketdata")
TABLE = f"{PROJECT}.{DATASET}.ohlcv"

SYMBOLS = os.environ.get("ALPHAGINI_SYMBOLS","BTC/USDT,ETH/USDT").split(",")
TIMEFRAMES = os.environ.get("ALPHAGINI_TIMEFRAMES","1h,1d").split(",")
BACKFILL_DAYS = int(os.environ.get("ALPHAGINI_BACKFILL_DAYS","365"))
INCREMENTAL_LIMIT = int(os.environ.get("ALPHAGINI_INCREMENTAL_LIMIT","1500"))  # max klines per call

def _ms(dt) -> int:
    """
    Convert a datetime-like to epoch milliseconds in UTC.
    Handles both tz-naive and tz-aware inputs.
    """
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None or ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.value / 1e6)

def fetch_binance(symbol, timeframe, since_ms=None, limit=1000):
    ex = ccxt.binance({"enableRateLimit": True, "options": {"adjustForTimeDifference": True}})
    rows = []
    while True:
        try:
            batch = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=limit)
        except ccxt.NetworkError:
            time.sleep(2); continue
        except ccxt.ExchangeError as e:
            raise e
        if not batch: break
        rows += batch
        # paginate forward
        last = batch[-1][0]
        if since_ms is not None and (len(batch) < limit): break
        since_ms = last + ex.parse_timeframe(timeframe)*1000
        if len(batch) < limit: break
        # be polite with free tier
        time.sleep(ex.rateLimit/1000.0)
        # safety cap to avoid unbounded loops
        if len(rows) >= 1_000_000: break
    df = pd.DataFrame(rows, columns=["ms","open","high","low","close","volume"])
    if df.empty: return df
    df["ts"] = pd.to_datetime(df["ms"], unit="ms", utc=True)
    df["exchange"] = "binance"
    df["symbol"] = symbol
    df["timeframe"] = timeframe
    return df[["exchange","symbol","timeframe","ts","open","high","low","close","volume"]]

def last_ts_in_bq(client, symbol, timeframe):
    q = f"""
    SELECT MAX(ts) AS ts FROM `{TABLE}`
    WHERE exchange='binance' AND symbol=@s AND timeframe=@tf
    """
    job = client.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("s","STRING",symbol),
            bigquery.ScalarQueryParameter("tf","STRING",timeframe),
        ]))
    row = list(job.result())[0]
    return row.ts  # None if no rows

def upsert(client, df: pd.DataFrame):
    if df.empty: return
    job = client.load_table_from_dataframe(df, TABLE)
    job.result()

def run():
    bq = bigquery.Client()
    for sym in SYMBOLS:
        for tf in TIMEFRAMES:
            # incremental: start from last ts, else backfill N days
            last = last_ts_in_bq(bq, sym, tf)
            if last:
                #  last comes back tz-aware from BigQuery; normalize to UTC then to ms
                since_ms = _ms(last)
                df = fetch_binance(sym, tf, since_ms=since_ms, limit=INCREMENTAL_LIMIT)
            else:
                # use an explicit UTC start time (naive -> localize)
                start = pd.Timestamp.now(tz="UTC").floor("T") - pd.Timedelta(days=BACKFILL_DAYS)
                df = fetch_binance(sym, tf, since_ms=_ms(start), limit=INCREMENTAL_LIMIT)
            # basic QA
            if not df.empty:
                df = df.drop_duplicates(subset=["exchange","symbol","timeframe","ts"]).sort_values("ts")
                df = df[df["open"].notna() & df["close"].notna()]
                upsert(bq, df)

if __name__ == "__main__":
    run()
