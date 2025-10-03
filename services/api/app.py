import hashlib, json, os, time
from dataclasses import asdict, dataclass
from typing import Dict, List, Any
import logging

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

app = FastAPI(title="alphagini-api", version="0.1")

logger = logging.getLogger("alphagini")
logger.setLevel(logging.INFO)


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

def log(logs: List[str], msg: str):
    m = str(msg)
    logs.append(m)
    logger.info(m)

def periods_per_year(tf: str) -> int:
    # approximate for intraday
    mult = int(tf[:-1]) if tf[:-1].isdigit() else 1
    unit = tf[-1]
    if unit == "m": return int((365*24*60)/mult)
    if unit == "h": return int((365*24)/mult)
    if unit == "d": return int(365/mult)
    if unit == "w": return int(52/mult)
    return 365

def load_ohlcv(symbol: str, timeframe: str, start: str, end: str, logs: List[str]) -> pd.DataFrame:
    # ... your BQ query build ...
    log(logs, f"load_ohlcv: symbol={symbol}, timeframe={timeframe}, start={start}, end={end}")
    df = ...  # <== existing query to BQ that returns columns ['ts','open','high','low','close','volume']
    if df.empty:
        log(logs, "load_ohlcv: BQ returned 0 rows")
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").set_index("ts")
    log(logs, f"load_ohlcv: rows={len(df)}, first={df.index.min()}, last={df.index.max()}")
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

def equity_sma_cross(price: pd.Series,
                     cash_start: float,
                     sma_fast: int,
                     sma_slow: int,
                     logs: List[str]) -> pd.Series:
    """
    price: pd.Series indexed by timestamp, float
    returns cumulative equity series aligned 1:1 with price index
    """
    log(logs, f"equity_sma_cross: len(price)={len(price)}, fast={sma_fast}, slow={sma_slow}")

    price = price.astype(float)
    price = price.sort_index()

    # SMAs
    fast = price.rolling(window=sma_fast, min_periods=sma_fast).mean()
    slow = price.rolling(window=sma_slow, min_periods=sma_slow).mean()

    # Position: 1 when fast>slow else 0. Shift(1) so we trade on next bar open.
    pos = (fast > slow).astype(int).shift(1).fillna(0)

    # P&L from close-to-close returns
    rets = price.pct_change().fillna(0)

    # Strategy returns: position * market returns
    strat = (pos * rets).fillna(0)

    equity = (1.0 + strat).cumprod() * float(cash_start)

    # logs for sanity
    cross_count = int(((fast > slow).astype(int).diff() != 0).sum())
    log(logs, f"SMA signals={cross_count}, first_ts={price.index.min()}, last_ts={price.index.max()}")
    log(logs, f"NaNs: fast={int(fast.isna().sum())}, slow={int(slow.isna().sum())}")

    # ensure alignment is exact
    if not equity.index.equals(price.index):
        log(logs, "WARNING: index misalignment detected; reindexing equity to price.")
        equity = equity.reindex(price.index)

    # if all NaN due to very short window, fill with starting cash
    equity = equity.fillna(method="ffill").fillna(float(cash_start))

    log(logs, f"equity len={len(equity)}, min={equity.min():.2f}, max={equity.max():.2f}")
    return equity

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
    client = bq()
    q = f"""
      SELECT symbol, timeframe, MIN(ts) first_ts, MAX(ts) last_ts, COUNT(*) row_count
      FROM `{TABLE_OHLCV}`
      GROUP BY symbol, timeframe
      ORDER BY symbol, timeframe
    """
    return [dict(r) for r in client.query(q).result()]

@app.post("/backtest")
def backtest(req: BacktestRequest):
    logs: List[str] = []
    df = load_ohlcv(req.symbol, req.timeframe, req.start, req.end, logs)
    if df.empty:
        return JSONResponse(status_code=404, content={"detail": "No data for given window.", "logs": logs})

    price = df["close"]
    eq = equity_sma_cross(price, req.cash_start, req.sma_fast, req.sma_slow, logs)

    # prepare series for the UI
    # return ISO strings to avoid timezone surprises in the browser
    ts_iso = [ts.isoformat() for ts in eq.index.to_pydatetime()]
    equity_vals = eq.round(2).tolist()

    # simple metrics (extend as you like)
    ret_abs = float(equity_vals[-1] - req.cash_start)
    ret_rel = float(equity_vals[-1] / req.cash_start - 1.0)

    payload: Dict[str, Any] = {
        "summary": {
            "symbol": req.symbol,
            "timeframe": req.timeframe,
            "start": req.start,
            "end": req.end,
            "bars": len(ts_iso),
        },
        "metrics": {
            "abs_return": ret_abs,
            "rel_return": ret_rel,
        },
        "series": {
            "ts": ts_iso,
            "equity": equity_vals,
        },
        "logs": logs[:2000],  # cap to keep responses reasonable
    }
    return payload
