from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, List
from datetime import datetime

ModelName = Literal["arima","prophet","xgb","lstm"]
StrategyName = Literal["buy_hold","sma_cross","rsi_meanrev","breakout"]

class BacktestRequest(BaseModel):
    exchange: str = "binance"
    symbol: str = "BTC/USDT"
    timeframe: str = "1h"
    start: datetime
    end: datetime
    lookback_days: int = 180
    model: ModelName
    model_params: Dict = {}
    strategy: StrategyName
    strategy_params: Dict = {}
    initial_cash: float = 100_000.0

class EquityPoint(BaseModel):
    ts: datetime
    equity: float

class Metrics(BaseModel):
    sharpe: float; sortino: float; max_dd: float
    win_rate: float; trades: int
    abs_return: float; rel_return: float; cagr: float

class ModelMetrics(BaseModel):
    rmse: float; mae: float; mape: float
    baseline_model: str = "arima"; baseline_rmse: float

class BacktestResponse(BaseModel):
    run_id: str
    equity_curve: List[EquityPoint]
    metrics: Metrics
    model_metrics: ModelMetrics
