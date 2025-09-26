from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Literal
import os, math
import pandas as pd
from google.cloud import bigquery
from statsmodels.tsa.arima.model import ARIMA
import numpy as np

app = FastAPI(title="alphagini API")

PROJECT = os.environ["ALPHAGINI_PROJECT"]
DATASET = os.environ.get("ALPHAGINI_BQ_DATASET","alphagini_marketdata")
TABLE = f"{PROJECT}.{DATASET}.ohlcv"

class BacktestRequest(BaseModel):
    symbol: str              # e.g., "BTC/USD"
    timeframe: Literal["5m","1h","1d"]
    start: str               # ISO, e.g., "2023-01-01T00:00:00Z"
    end: str                 # ISO, e.g., "2023-12-31T00:00:00Z"
    model: Literal["arima","naive","sma"] = "arima"
    strategy: Literal["buy_hold","sma_cross"] = "buy_hold"
    cash_start: float = 100000.0

def load_ohlcv(symbol, timeframe, start, end) -> pd.DataFrame:
    bq = bigquery.Client()
    q = f"""
    SELECT ts, open, high, low, close, volume
    FROM `{TABLE}`
    WHERE symbol=@s AND timeframe=@tf AND ts BETWEEN @start AND @end
    ORDER BY ts
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("s","STRING",symbol),
            bigquery.ScalarQueryParameter("tf","STRING",timeframe),
            bigquery.ScalarQueryParameter("start","TIMESTAMP",start),
            bigquery.ScalarQueryParameter("end","TIMESTAMP",end),
        ]
    ))
    df = job.result().to_dataframe(create_bqstorage_client=False)
    if df.empty:
        raise HTTPException(404, "No data for given window.")
    return df

def metrics_from_equity(equity: pd.Series, freq_per_year: int) -> dict:
    rets = equity.pct_change().dropna()
    # annualized sharpe (risk-free ~0 for crypto)
    sharpe = (rets.mean() * freq_per_year) / (rets.std(ddof=0) * math.sqrt(freq_per_year)) if len(rets)>1 else float("nan")
    cumret = equity.iloc[-1] / equity.iloc[0] - 1.0
    # max drawdown
    roll_max = equity.cummax()
    drawdown = (equity/roll_max - 1.0).min()
    win_rate = (rets > 0).mean() if len(rets) else float("nan")
    return {"sharpe": float(sharpe), "cum_return": float(cumret), "max_drawdown": float(drawdown), "win_rate": float(win_rate)}

def periods_per_year(tf: str) -> int:
    return {"5m": 365*24*12, "1h": 365*24, "1d": 365}[tf]

def model_predict(df: pd.DataFrame, model: str) -> pd.Series:
    y = df["close"].astype(float).values
    if model == "naive":
        preds = np.r_[y[0], y[:-1]]
    elif model == "sma":
        preds = pd.Series(y).rolling(10, min_periods=1).mean().values
    else:  # "arima" baseline
        # small ARIMA for a quick baseline; production would tune/order-select
        try:
            m = ARIMA(y, order=(1,1,1)).fit(method_kwargs={"warn_convergence": False})
            fc = m.predict(start=1, end=len(y)-1)
            preds = np.r_[y[0], fc]
        except Exception:
            preds = np.r_[y[0], y[:-1]]
    return pd.Series(preds, index=df.index)

def strategy_equity(df: pd.DataFrame, strategy: str, cash_start: float) -> pd.Series:
    px = df["close"].astype(float)
    if strategy == "sma_cross":
        sma_fast = px.rolling(10, min_periods=1).mean()
        sma_slow = px.rolling(30, min_periods=1).mean()
        signal = (sma_fast > sma_slow).astype(int)  # 1 long, 0 flat
    else:
        signal = pd.Series(1, index=px.index)       # buy & hold
    rets = signal.shift(1).fillna(0) * px.pct_change().fillna(0)
    equity = (1 + rets).cumprod() * cash_start
    return equity

@app.post("/backtest")
def backtest(req: BacktestRequest):
    df = load_ohlcv(req.symbol, req.timeframe, req.start, req.end)
    df = df.set_index(pd.to_datetime(df["ts"], utc=True))
    preds = model_predict(df, req.model)
    # quick model error metrics
    err = df["close"].astype(float) - preds
    rmse = float(np.sqrt(np.mean(err**2)))
    # compare to ARIMA baseline
    base = model_predict(df, "arima")
    rmse_base = float(np.sqrt(np.mean((df["close"].astype(float) - base)**2)))
    # strategy equity & metrics
    equity = strategy_equity(df, req.strategy, req.cash_start)
    m = metrics_from_equity(equity, periods_per_year(req.timeframe))
    # relative/absolute returns on $100k
    abs_ret = float(equity.iloc[-1] - req.cash_start)
    rel_ret = float(equity.iloc[-1] / req.cash_start - 1.0)
    return {
        "summary": {
            "symbol": req.symbol, "timeframe": req.timeframe,
            "start": req.start, "end": req.end,
            "model": req.model, "strategy": req.strategy
        },
        "metrics": {
            **m, "abs_return_usd": abs_ret, "rel_return": rel_ret,
            "rmse": rmse, "rmse_arima_baseline": rmse_base
        },
        "equity_curve": [
            {"ts": str(ts), "equity": float(val)} for ts, val in equity.items()
        ],
    }
