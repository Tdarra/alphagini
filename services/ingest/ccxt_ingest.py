import os
import time
import logging
from typing import Optional, List

import pandas as pd
import ccxt
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Forbidden


# ================= Logging =================
logging.basicConfig(
    level=os.environ.get("ALPHAGINI_LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("alphagini.ingest")


# ================= Env / Config =================
PROJECT = os.environ["ALPHAGINI_PROJECT"]
DATASET = os.environ.get("ALPHAGINI_BQ_DATASET", "alphagini_marketdata")
TABLE_ID = f"{PROJECT}.{DATASET}.ohlcv"

# Static list from env.yaml (required)
SYMBOLS = [s.strip() for s in os.environ["ALPHAGINI_SYMBOLS"].split(",") if s.strip()]
TIMEFRAMES = [t.strip() for t in os.environ.get("ALPHAGINI_TIMEFRAMES", "5m").split(",") if t.strip()]

# Exchange (default to Kraken; file name is legacy)
EXCHANGE_ID = os.environ.get("ALPHAGINI_EXCHANGE", "kraken")

# Backfill controls
BACKFILL_DAYS = int(os.environ.get("ALPHAGINI_BACKFILL_DAYS", "3650"))
PAGE_LIMIT = int(os.environ.get("ALPHAGINI_INCREMENTAL_LIMIT", "720"))  # Kraken ≈ 720 max per call
MAX_PAGES = int(os.environ.get("ALPHAGINI_MAX_PAGES", "0"))  # 0 = unlimited
EXTRA_SLEEP_MS = int(os.environ.get("ALPHAGINI_SLEEP_MS", "0"))

# Start-point overrides
FORCE_FROM = os.environ.get("ALPHAGINI_FORCE_FROM", "").strip()          # e.g. "2015-01-01T00:00:00Z"
BACKFILL_START = os.environ.get("ALPHAGINI_BACKFILL_START", "").strip()  # e.g. "2015-01-01T00:00:00Z"


# ================= Helpers =================
def _ms(dt) -> int:
    """Convert datetime-like to epoch ms in UTC (handles tz-naive/aware)."""
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None or ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.value / 1e6)


def ensure_table(bq: bigquery.Client):
    """Create dataset/table if missing; log current row count."""
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
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="ts"
        )
        bq.create_table(table)

    # Row count snapshot
    try:
        q = f"SELECT COUNT(*) AS rows FROM `{TABLE_ID}`"
        rows = list(bq.query(q).result())[0].rows
        log.info(f"BigQuery table ready: {TABLE_ID} | existing_rows={rows}")
    except Exception as e:
        log.warning(f"Could not query row count for {TABLE_ID}: {e}")


def check_permissions(bq: bigquery.Client):
    try:
        bq.query("SELECT 1").result()
    except Forbidden as e:
        log.error(
            "BigQuery submission forbidden. Grant the job's Service Account "
            "'roles/bigquery.jobUser' and dataset-level 'roles/bigquery.dataEditor' (for writes)."
        )
        raise e


def last_ts_in_bq(client: bigquery.Client, symbol: str, timeframe: str):
    q = f"""
    SELECT MAX(ts) AS ts FROM `{TABLE_ID}`
    WHERE exchange=@ex AND symbol=@s AND timeframe=@tf
    """
    job = client.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ex", "STRING", EXCHANGE_ID),
                bigquery.ScalarQueryParameter("s", "STRING", symbol),
                bigquery.ScalarQueryParameter("tf", "STRING", timeframe),
            ]
        ),
    )
    row = list(job.result())[0]
    return row.ts  # None if empty


def choose_start_ms(last_ts_in_table, timeframe: str) -> int:
    """
    Decide since_ms for paging, honoring overrides.
      1) ALPHAGINI_FORCE_FROM (always wins)
      2) resume from last_ts_in_table + one bar
      3) ALPHAGINI_BACKFILL_START (first-time backfill)
      4) BACKFILL_DAYS fallback
    """
    if FORCE_FROM:
        return _ms(pd.Timestamp(FORCE_FROM))

    bar_ms = ccxt.Exchange.parse_timeframe(timeframe) * 1000

    if last_ts_in_table is not None:
        return _ms(last_ts_in_table) + bar_ms

    if BACKFILL_START:
        return _ms(pd.Timestamp(BACKFILL_START))

    start = pd.Timestamp.now(tz="UTC").floor("T") - pd.Timedelta(days=BACKFILL_DAYS)
    return _ms(start)


def get_exchange() -> ccxt.Exchange:
    cls = getattr(ccxt, EXCHANGE_ID)
    ex = cls({"enableRateLimit": True, "options": {"adjustForTimeDifference": True}})
    ex.load_markets()
    return ex


def load_page_to_bq(bq: bigquery.Client, df: pd.DataFrame, symbol: str, timeframe: str) -> int:
    if df.empty:
        return 0
    df = df.drop_duplicates(subset=["exchange", "symbol", "timeframe", "ts"]).sort_values("ts")
    df = df[df["open"].notna() & df["close"].notna()]
    job = bq.load_table_from_dataframe(df, TABLE_ID)
    job.result()
    return len(df)


# ================= Main fetch/load =================
def fetch_and_load_symbol_tf(bq: bigquery.Client, ex: ccxt.Exchange, symbol: str, timeframe: str):
    last = last_ts_in_bq(bq, symbol, timeframe)
    since_ms = choose_start_ms(last, timeframe)

    # log start mode
    if FORCE_FROM:
        log.info(f"{symbol} {timeframe} | force_from={pd.Timestamp(FORCE_FROM)} ({since_ms} ms)")
    elif last is not None:
        log.info(f"{symbol} {timeframe} | resume_from={last} ({since_ms} ms)")
    elif BACKFILL_START:
        log.info(f"{symbol} {timeframe} | backfill_from={BACKFILL_START} ({since_ms} ms)")
    else:
        log.info(
            f"{symbol} {timeframe} | backfill_days={BACKFILL_DAYS} start_ms={since_ms}"
        )

    pages = 0
    total_rows = 0
    bar_ms = ex.parse_timeframe(timeframe) * 1000

    while True:
        if MAX_PAGES and pages >= MAX_PAGES:
            log.info(f"{symbol} {timeframe} | hit MAX_PAGES={MAX_PAGES}, stopping.")
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
        df = pd.DataFrame(batch, columns=["ms", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_datetime(df["ms"], unit="ms", utc=True)
        df["exchange"], df["symbol"], df["timeframe"] = EXCHANGE_ID, symbol, timeframe
        df = df[["exchange", "symbol", "timeframe", "ts", "open", "high", "low", "close", "volume"]]

        first_ts = df["ts"].iloc[0]
        last_ts = df["ts"].iloc[-1]
        loaded = load_page_to_bq(bq, df, symbol, timeframe)
        total_rows += loaded

        log.info(
            f"{symbol} {timeframe} | page={pages} fetched={len(batch)} loaded={loaded} "
            f"range=[{first_ts} .. {last_ts}] total_loaded={total_rows}"
        )

        # advance window by one bar after last row
        since_ms = int(last_ts.timestamp() * 1000) + bar_ms

        # rate limit + optional extra sleep
        time.sleep(max(ex.rateLimit / 1000.0, 0.001) + (EXTRA_SLEEP_MS / 1000.0))

        # stop if we've effectively caught up to current time (one bar lag)
        now_ms = int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)
        if since_ms >= now_ms - bar_ms:
            log.info(f"{symbol} {timeframe} | reached current time; stopping.")
            break

    log.info(f"{symbol} {timeframe} | completed pages={pages}, rows_loaded={total_rows}")


def run():
    log.info(
        f"Starting ingest → {TABLE_ID} | exchange={EXCHANGE_ID} | "
        f"symbols={SYMBOLS} | timeframes={TIMEFRAMES} | "
        f"page_limit={PAGE_LIMIT} max_pages={MAX_PAGES} extra_sleep_ms={EXTRA_SLEEP_MS}"
    )

    bq = bigquery.Client()
    ensure_table(bq)
    check_permissions(bq)

    ex = get_exchange()

    for sym in SYMBOLS:
        if sym not in ex.markets:
            log.warning(f"{sym} not listed on {EXCHANGE_ID}; skipping.")
            continue
        for tf in TIMEFRAMES:
            fetch_and_load_symbol_tf(bq, ex, sym, tf)

    log.info("Ingest complete.")


if __name__ == "__main__":
    run()
