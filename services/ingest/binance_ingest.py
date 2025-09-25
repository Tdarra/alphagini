import os
import time
import logging
from typing import Optional

import pandas as pd
import ccxt
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Forbidden


# ========= Logging setup =========
logging.basicConfig(
    level=os.environ.get("ALPHAGINI_LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("alphagini.ingest")


# ========= Env/config =========
PROJECT = os.environ["ALPHAGINI_PROJECT"]  # e.g., "my-project-123"
DATASET = os.environ.get("ALPHAGINI_BQ_DATASET", "alphagini_marketdata")
TABLE_ID = f"{PROJECT}.{DATASET}.ohlcv"

SYMBOLS = [s.strip() for s in os.environ["ALPHAGINI_SYMBOLS"].split(",") if s.strip()]
TIMEFRAMES = [t.strip() for t in os.environ.get("ALPHAGINI_TIMEFRAMES", "5m").split(",") if t.strip()]

BACKFILL_DAYS = int(os.environ.get("ALPHAGINI_BACKFILL_DAYS", "3650"))
PAGE_LIMIT = int(os.environ.get("ALPHAGINI_INCREMENTAL_LIMIT", "1000"))  # max candles per Binance call
MAX_PAGES = int(os.environ.get("ALPHAGINI_MAX_PAGES", "0"))  # 0 = unlimited
EXTRA_SLEEP_MS = int(os.environ.get("ALPHAGINI_SLEEP_MS", "0"))  # add sleep per page to be polite (ms)


# ========= Helpers =========
def _ms(dt) -> int:
    """Convert a datetime-like to epoch milliseconds in UTC; supports tz-naive/aware."""
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None or ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.value / 1e6)


def ensure_table(bq: bigquery.Client):
    """Create dataset/table if missing (idempotent) and log current row count."""
    # Dataset
    try:
        bq.get_dataset(f"{PROJECT}.{DATASET}")
    except NotFound:
        log.info(f"Creating dataset {PROJECT}.{DATASET}")
        bq.create_dataset(bigquery.Dataset(f"{PROJECT}.{DATASET}"), exists_ok=True)

    # Table
    try:
        bq.get_table(TABLE_ID)
    except NotFound:
        log.info(f"Creating table {TABLE_ID}")
        schema = [
            bigquery.SchemaField("exchange", "STRING"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("timeframe", "STRING"),
            bigquery.SchemaField("ts", "TIMESTAMP"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("volume", "FLOAT"),
        ]
        table = bigquery.Table(TABLE_ID, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="ts")
        bq.create_table(table)

    # Row count snapshot
    try:
        q = f"SELECT COUNT(*) AS rows FROM `{TABLE_ID}`"
        rows = list(bq.query(q).result())[0].rows
        log.info(f"BigQuery table ready: {TABLE_ID} | existing_rows={rows}")
    except Exception as e:
        log.warning(f"Could not query row count for {TABLE_ID}: {e}")


def check_permissions(bq: bigquery.Client):
    """Quick check that our identity can write to the table (dataEditor + jobUser)."""
    try:
        # Dry-run a trivial query to ensure we can start jobs
        bq.query("SELECT 1").result()
    except Forbidden as e:
        log.error("BigQuery job submission forbidden. Ensure the job's Service Account has "
                  "'roles/bigquery.jobUser' and 'roles/bigquery.dataEditor'.")
        raise e


def fetch_and_load_symbol_tf(
    bq: bigquery.Client,
    ex: ccxt.binance,
    symbol: str,
    timeframe: str,
):
    """Fetch OHLCV in pages and append each page to BigQuery with verbose logs."""
    # Resume from last timestamp if present
    last = last_ts_in_bq(bq, symbol, timeframe)
    if last:
        since_ms = _ms(last)
        log.info(f"{symbol} {timeframe} | resume_from={last} ({since_ms} ms)")
    else:
        start = pd.Timestamp.now(tz="UTC").floor("T") - pd.Timedelta(days=BACKFILL_DAYS)
        since_ms = _ms(start)
        log.info(f"{symbol} {timeframe} | backfill_from={start} ({since_ms} ms)")

    pages = 0
    total_rows = 0

    while True:
        # Optional cap to avoid very long single-execution backfills
        if MAX_PAGES and pages >= MAX_PAGES:
            log.info(f"{symbol} {timeframe} | hit MAX_PAGES={MAX_PAGES}, stopping this run.")
            break

        try:
            batch = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=PAGE_LIMIT)
        except ccxt.NetworkError as e:
            log.warning(f"{symbol} {timeframe} | network error: {e}; retrying after 2s")
            time.sleep(2)
            continue
        except ccxt.ExchangeError as e:
            log.error(f"{symbol} {timeframe} | exchange error: {e}; aborting this pair.")
            break

        if not batch:
            log.info(f"{symbol} {timeframe} | no more data. Done.")
            break

        pages += 1
        # Convert and clean *this page only*
        df = pd.DataFrame(batch, columns=["ms", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_datetime(df["ms"], unit="ms", utc=True)
        df["exchange"], df["symbol"], df["timeframe"] = "binance", symbol, timeframe
        df = df[["exchange", "symbol", "timeframe", "ts", "open", "high", "low", "close", "volume"]]
        before = len(df)
        df = df.drop_duplicates(subset=["exchange", "symbol", "timeframe", "ts"]).sort_values("ts")
        df = df[df["open"].notna() & df["close"].notna()]
        after = len(df)

        first_ts = df["ts"].iloc[0]
        last_ts = df["ts"].iloc[-1]
        log.info(f"{symbol} {timeframe} | page={pages} fetched={before} cleaned={after} "
                 f"range=[{first_ts} .. {last_ts}]")

        # Load this page right away
        load_job = bq.load_table_from_dataframe(df, TABLE_ID)
        load_job.result()
        # After load, fetch updated table count for this symbol/timeframe (cheap via partition scan)
        try:
            q = f"""
            SELECT COUNT(*) AS rows
            FROM `{TABLE_ID}`
            WHERE exchange='binance' AND symbol=@s AND timeframe=@tf
            """
            job = bq.query(q, job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("s", "STRING", symbol),
                    bigquery.ScalarQueryParameter("tf", "STRING", timeframe),
                ]
            ))
            sym_rows = list(job.result())[0].rows
            log.info(f"{symbol} {timeframe} | page={pages} loaded_rows={after} total_rows_for_pair={sym_rows}")
        except Exception as e:
            log.warning(f"{symbol} {timeframe} | unable to fetch pair row count: {e}")

        total_rows += after

        # Advance window
        last_ms = int(df["ts"].iloc[-1].timestamp() * 1000)
        since_ms = last_ms + ex.parse_timeframe(timeframe) * 1000

        # Respect rate limit + optional extra politeness
        time.sleep(max(ex.rateLimit / 1000.0, 0.001) + (EXTRA_SLEEP_MS / 1000.0))

        # If Binance returned a short page, we're likely done
        if before < PAGE_LIMIT:
            log.info(f"{symbol} {timeframe} | short page ({before}<{PAGE_LIMIT}); stopping.")
            break

    log.info(f"{symbol} {timeframe} | completed pages={pages}, rows_loaded={total_rows}")


def last_ts_in_bq(client: bigquery.Client, symbol: str, timeframe: str):
    q = f"""
    SELECT MAX(ts) AS ts FROM `{TABLE_ID}`
    WHERE exchange='binance' AND symbol=@s AND timeframe=@tf
    """
    job = client.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("s", "STRING", symbol),
                bigquery.ScalarQueryParameter("tf", "STRING", timeframe),
            ]
        ),
    )
    row = list(job.result())[0]
    return row.ts  # None if no rows


# ========= Main =========
def run():
    log.info(f"Starting ingest → {TABLE_ID}")
    log.info(f"Symbols={SYMBOLS} | Timeframes={TIMEFRAMES} | backfill_days={BACKFILL_DAYS} "
             f"| page_limit={PAGE_LIMIT} | max_pages={MAX_PAGES} | extra_sleep_ms={EXTRA_SLEEP_MS}")

    bq = bigquery.Client()
    ensure_table(bq)
    check_permissions(bq)

    # Create a single Binance client (reused across all loops)
    ex = ccxt.binance({"enableRateLimit": True, "options": {"adjustForTimeDifference": True}})
    _ = ex.load_markets()  # warm-up & verify connectivity

    for sym in SYMBOLS:
        for tf in TIMEFRAMES:
            fetch_and_load_symbol_tf(bq, ex, sym, tf)

    log.info("Ingest complete.")


if __name__ == "__main__":
    run()
