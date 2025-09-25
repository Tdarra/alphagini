import numpy as np
import pandas as pd
from ..models import arima, prophet, xgb, lstm
from ..strategies import buy_hold, sma_cross, rsi_meanrev
from dataclasses import dataclass

MODELS = {"arima": arima.Model, "prophet": prophet.Model, "xgb": xgb.Model, "lstm": lstm.Model}
STRATS = {"buy_hold": buy_hold.Strategy, "sma_cross": sma_cross.Strategy, "rsi_meanrev": rsi_meanrev.Strategy}

@dataclass
class Result:
    equity_series: list[tuple[pd.Timestamp,float]]
    perf_metrics: dict
    y_true: np.ndarray
    y_pred: np.ndarray

def run_backtest(df: pd.DataFrame, req):
    # df with index=ts, columns [open,high,low,close,volume]
    price = df["close"].copy()
    lookback = int(req.lookback_days * 24*60 / _tf_minutes(req.timeframe))
    train = price.iloc[:lookback]; test = price.iloc[lookback:]

    model = MODELS[req.model](**req.model_params)
    model.fit(train.to_frame(name="close"))
    forecast = model.predict(len(test))  # returns np.array aligned to test.index
    y_true = test.values; y_pred = forecast

    # strategy consumes price & forecast and yields positions (0..1)
    strat = STRATS[req.strategy](**req.strategy_params)
    pos = strat.generate_positions(price=test, forecast=pd.Series(y_pred, index=test.index))

    ret = test.pct_change().fillna(0.0)
    strat_ret = (pos.shift(1).fillna(0.0) * ret)  # next-bar execution
    equity = (1 + strat_ret).cumprod() * req.initial_cash

    perf = _perf_metrics(strat_ret, equity, req.initial_cash)
    return Result(equity_series=list(zip(equity.index, equity.values)), perf_metrics=perf, y_true=y_true, y_pred=y_pred)

def _tf_minutes(tf: str) -> int:
    if tf.endswith("m"): return int(tf[:-1])
    if tf.endswith("h"): return int(tf[:-1]) * 60
    if tf.endswith("d"): return int(tf[:-1]) * 1440
    raise ValueError(tf)

def _perf_metrics(returns: pd.Series, equity: pd.Series, initial_cash: float) -> dict:
    rf = 0.0
    ann = np.sqrt(365*24*60/_tf_minutes("60"))  # approximate if 1h; refine per tf
    sharpe = (returns.mean() - rf) / (returns.std() + 1e-9) * np.sqrt(365*24)
    downside = returns[returns < 0].std() + 1e-9
    sortino = (returns.mean() - rf) / downside * np.sqrt(365*24)
    roll_max = equity.cummax()
    dd = (equity/roll_max - 1).min()
    wins = (returns > 0).sum(); trades = (returns != 0).sum()
    abs_ret = equity.iloc[-1] - initial_cash
    rel_ret = equity.iloc[-1] / initial_cash - 1
    years = max((equity.index[-1]-equity.index[0]).days,1)/365
    cagr = (equity.iloc[-1]/initial_cash)**(1/years) - 1 if years>0 else np.nan
    return dict(sharpe=float(sharpe), sortino=float(sortino), max_dd=float(dd),
                win_rate=float(wins/max(trades,1)), trades=int(trades),
                abs_return=float(abs_ret), rel_return=float(rel_ret), cagr=float(cagr))
