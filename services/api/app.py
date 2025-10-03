import hashlib, json, os, time
import logging
from dataclasses import asdict, dataclass
from typing import Dict, List

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from google.cloud import bigquery

PROJECT = os.environ.get("ALPHAGINI_PROJECT")
DATASET_MD = os.environ.get("ALPHAGINI_BQ_DATASET", "alphagini_marketdata")
TABLE_OHLCV = f"{PROJECT}.{DATASET_MD}.ohlcv"

DATASET_EXP = os.environ.get("ALPHAGINI_EXP_DATASET", "alphagini_experiments")
TABLE_BT = f"{PROJECT}.{DATASET_EXP}.backtests"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alphagini.api")

app = FastAPI(title="alphagini-api", version="0.1")

# Allow browser UI by default
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ---------- Models ----------
class BacktestRequest(BaseModel):
    symbol: str
    timeframe: str = Field(pattern=r"^\d+[smhdw]|[smhdw]$", default="5m")
    start: str
    end: str
    model: str = Field(pattern=r"^(naive|sma)$", default="naive")  # keep simple & fast
    strategy: str = Field(pattern=r"^(buy_hold|sma_cross)$", default="buy_hold")
    cash_start: float = 100_000.0
    # strategy params (optional)
    sma_fast: int = 10
    sma_slow: int = 30

@dataclass
class EquityMetrics:
    sharpe: float
    win_rate: float
    max_drawdown: float
    abs_return_usd: float
    rel_return: float

# ---------- Utils ----------
def bq() -> bigquery.Client:
    return bigquery.Client(project=PROJECT)

def periods_per_year(tf: str) -> int:
    # approximate for intraday
    mult = int(tf[:-1]) if tf[:-1].isdigit() else 1
    unit = tf[-1]
    if unit == "m": return int((365*24*60)/mult)
    if unit == "h": return int((365*24)/mult)
    if unit == "d": return int(365/mult)
    if unit == "w": return int(52/mult)
    return 365

def load_ohlcv(symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame:
    client = bq()
    q = f"""
      SELECT ts, open, high, low, close, volume
      FROM `{TABLE_OHLCV}`
      WHERE symbol=@s AND timeframe=@tf
        AND ts BETWEEN @start AND @end
      ORDER BY ts
    """
    job = client.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("s","STRING", symbol),
                bigquery.ScalarQueryParameter("tf","STRING", timeframe),
                bigquery.ScalarQueryParameter("start","TIMESTAMP", start),
                bigquery.ScalarQueryParameter("end","TIMESTAMP", end),
            ]
        ),
    )
    df = job.result().to_dataframe(create_bqstorage_client=False)
    if df.empty:
        raise HTTPException(status_code=404, detail="No data for given window")
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.set_index("ts").sort_index()
    return df

# ---------- Models (predictions) ----------
def model_predict(df: pd.DataFrame, model: str, sma_fast=10, sma_slow=30) -> pd.Series:
    y = df["close"].astype(float)
    if model == "naive":
        return y.shift(1).fillna(y.iloc[0])
    if model == "sma":
        return y.rolling(sma_fast, min_periods=1).mean()
    raise ValueError("unknown model")

# ---------- Strategies ----------
def equity_buy_hold(df: pd.DataFrame, cash_start: float) -> pd.Series:
    price = df["close"].astype(float)
    units = cash_start / price.iloc[0]
    return units * price

def equity_sma_cross(df: pd.DataFrame, cash_start: float, sma_fast=10, sma_slow=30) -> pd.Series:
    price = df["close"].astype(float)

    fast = price.rolling(sma_fast, min_periods=1).mean()
    slow = price.rolling(sma_slow, min_periods=1).mean()
    long = (fast > slow).astype(int)
    long = long.shift(1).fillna(0)  # act on next bar

    equity: list[float] = [cash_start]
    units = 0.0
    cash = cash_start

    # one equity value per bar after the first
    for p, pos in zip(price.iloc[1:].values, long.iloc[1:].values):
        if pos and units == 0.0:      # enter long
            units = cash / p
            cash = 0.0
        elif (not pos) and units > 0: # exit long
            cash = units * p
            units = 0.0

        equity.append(cash + units * p)

    # equity has exactly len(price) values now
    return pd.Series(equity, index=price.index)

# ---------- Metrics ----------
def metrics_from_equity(eq: pd.Series, ppyr: int) -> EquityMetrics:
    rets = eq.pct_change().dropna()
    if len(rets) == 0:
        return EquityMetrics(0.0, 0.0, 0.0, float(eq.iloc[-1]-eq.iloc[0]), float(eq.iloc[-1]/eq.iloc[0]-1))
    sharpe = float(np.sqrt(ppyr) * (rets.mean() / (rets.std() + 1e-9)))
    win_rate = float((rets > 0).mean())
    roll_max = eq.cummax()
    dd = (eq/roll_max - 1.0).min()
    mdd = float(dd)
    abs_ret = float(eq.iloc[-1] - eq.iloc[0])
    rel_ret = float(eq.iloc[-1]/eq.iloc[0] - 1.0)
    return EquityMetrics(sharpe, win_rate, mdd, abs_ret, rel_ret)

# ---------- Persistence / Cache ----------
def cache_key(req: BacktestRequest) -> str:
    payload = {
        "symbol": req.symbol, "tf": req.timeframe, "start": req.start, "end": req.end,
        "model": req.model, "strategy": req.strategy, "cash": req.cash_start,
        "sma_fast": req.sma_fast, "sma_slow": req.sma_slow,
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode()).hexdigest()

def fetch_cached(req: BacktestRequest):
    client = bq()
    q = f"""
      SELECT id, metrics_json
      FROM `{TABLE_BT}`
      WHERE symbol=@s AND timeframe=@tf AND start_ts=@st AND end_ts=@en
        AND model=@m AND strategy=@str AND id=@id
      ORDER BY requested_at DESC LIMIT 1
    """
    job = client.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("s","STRING", req.symbol),
                bigquery.ScalarQueryParameter("tf","STRING", req.timeframe),
                bigquery.ScalarQueryParameter("st","TIMESTAMP", req.start),
                bigquery.ScalarQueryParameter("en","TIMESTAMP", req.end),
                bigquery.ScalarQueryParameter("m","STRING", req.model),
                bigquery.ScalarQueryParameter("str","STRING", req.strategy),
                bigquery.ScalarQueryParameter("id","STRING", cache_key(req)),
            ]
        ),
    )
    rows = list(job.result())
    return None if not rows else json.loads(rows[0].metrics_json)

def persist_result(req: BacktestRequest, metrics: Dict, duration_ms: int):
    client = bq()
    row = {
        "id": cache_key(req),
        "requested_at": pd.Timestamp.utcnow().isoformat(),
        "symbol": req.symbol,
        "timeframe": req.timeframe,
        "start_ts": req.start,
        "end_ts": req.end,
        "model": req.model,
        "strategy": req.strategy,
        "params_json": json.dumps({"sma_fast": req.sma_fast, "sma_slow": req.sma_slow}),
        "metrics_json": json.dumps(metrics),
        "duration_ms": duration_ms,
    }
    client.insert_rows_json(TABLE_BT, [row])

# ---------- Endpoints ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/symbols")
def symbols():
    logger.info("Fetching symbols from %s", TABLE_OHLCV)
    client = bq()
    q = f"""
      SELECT symbol, timeframe, MIN(ts) first_ts, MAX(ts) last_ts, COUNT(*) row_count
      FROM `{TABLE_OHLCV}`
      GROUP BY symbol, timeframe
      ORDER BY symbol, timeframe
    """
    rows = [dict(r) for r in client.query(q).result()]
    logger.info("Returning %d symbols", len(rows))
    return rows

@app.post("/backtest")
def backtest(req: BacktestRequest):
    payload = req.model_dump()
    logger.info("Received backtest request: %s", json.dumps(payload, sort_keys=True))
    t0 = time.time()
    cache_id = cache_key(req)
    # try cache first
    cached = fetch_cached(req)
    if cached:
        logger.info("Cache hit for request %s", cache_id)
        cached["_cached"] = True
        logger.info("Returning cached metrics: %s", json.dumps(cached, sort_keys=True))
        return {"summary": req.model_dump(), "metrics": cached, "equity_curve": []}

    df = load_ohlcv(req.symbol, req.timeframe, req.start, req.end)
    logger.info(
        "Loaded %d OHLCV rows for %s %s between %s and %s",
        len(df),
        req.symbol,
        req.timeframe,
        req.start,
        req.end,
    )

    # forecast series (we calculate RMSE vs close)
    preds = model_predict(df, req.model, req.sma_fast, req.sma_slow)
    err = df["close"].astype(float) - preds
    rmse = float(np.sqrt(np.mean(err**2)))

    # equity
    if req.strategy == "buy_hold":
        eq = equity_buy_hold(df, req.cash_start)
    else:
        eq = equity_sma_cross(df, req.cash_start, req.sma_fast, req.sma_slow)

    m = metrics_from_equity(eq, periods_per_year(req.timeframe))
    metrics = {
        "sharpe": m.sharpe,
        "win_rate": m.win_rate,
        "max_drawdown": m.max_drawdown,
        "abs_return_usd": m.abs_return_usd,
        "rel_return": m.rel_return,
        "rmse": rmse,
    }
    duration_ms = int((time.time() - t0) * 1000)
    logger.info("Computed metrics in %d ms: %s", duration_ms, json.dumps(metrics, sort_keys=True))

    # persist
    persist_result(req, metrics, duration_ms)
    logger.info("Persisted backtest result for %s", cache_id)

    curve = [{"ts": str(ts), "equity": float(val)} for ts, val in eq.items()]
    response_payload = {"summary": req.model_dump(), "metrics": metrics, "equity_curve": curve}
    logger.info("Sending backtest response with %d equity points", len(response_payload["equity_curve"]))
    return response_payload
