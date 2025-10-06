#!/usr/bin/env python3
"""
Local runner for alphagini that reuses production code from services/api.

- Loads OHLCV from a CSV (ccxt/BQ schema)
- Calls strategies & (optional) model training defined in services/api/*
- Prints JSON metrics to stdout

Usage examples are at the bottom of this file.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import argparse
import inspect
import json
import os
from typing import Optional, Tuple, Dict
import numpy as np
import pandas as pd
from services.api.app import equity_sma_cross, equity_buy_hold, metrics_from_equity

# -----------------------------
# Import your existing API code
# -----------------------------
def _import_first(*paths):
    last_err = None
    for p in paths:
        try:
            mod = __import__(p, fromlist=["*"])
            return mod
        except Exception as e:
            last_err = e
    raise ImportError(f"Could not import any of: {paths}. Last error: {last_err}")


# Try strategies module first, then app.py (both patterns showed up in your repo/history)
# api_mod = _import_first("services.api.strategies", "services.api.app")
api_mod = _import_first("services.api.app")


# Grab strategy functions if present
equity_buy_hold = getattr(api_mod, "equity_buy_hold", None)
equity_sma_cross = getattr(api_mod, "equity_sma_cross", None)

# Optional metrics helper if you already have one in services/api
metrics_from_equity = getattr(api_mod, "metrics_from_equity", None)

# Optional training helpers (only used if you actually defined them)
train_arima = getattr(api_mod, "train_arima", None)
train_naive = getattr(api_mod, "train_naive", None)


# -----------------------------
# CSV loading / schema helpers
# -----------------------------
def load_ohlcv_csv(
    csv_path: str,
    symbol: Optional[str],
    timeframe: Optional[str],
    start: Optional[str],
    end: Optional[str],
) -> Tuple[pd.DataFrame, str, str]:
    """
    Accepts ccxt/BQ-like schema:
      exchange,symbol,timeframe,ts,open,high,low,close,volume
    Reduced schema is fine (ts, open, high, low, close[, volume]) if you pass --symbol/--timeframe.

    Returns df indexed by UTC ts with columns: open,high,low,close,volume.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    df = pd.read_csv(csv_path)
    low = {c.lower(): c for c in df.columns}

    def need(col):
        if col not in low:
            raise ValueError(f"CSV missing required column '{col}'")

    need("ts"); need("open"); need("high"); need("low"); need("close")
    df.rename(columns={low["ts"]: "ts"}, inplace=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    if "volume" in low:
        df.rename(columns={low["volume"]: "volume"}, inplace=True)
    else:
        df["volume"] = np.nan

    # normalize optional symbol/timeframe, allow CLI override
    if "symbol" in low:
        df.rename(columns={low["symbol"]: "symbol"}, inplace=True)
    else:
        df["symbol"] = symbol or "UNKNOWN/UNKNOWN"

    if "timeframe" in low:
        df.rename(columns={low["timeframe"]: "timeframe"}, inplace=True)
    else:
        df["timeframe"] = timeframe or "5m"

    if symbol:
        df = df[df["symbol"] == symbol]
    if timeframe:
        df = df[df["timeframe"] == timeframe]
    if start:
        df = df[df["ts"] >= pd.Timestamp(start, tz="UTC")]
    if end:
        df = df[df["ts"] < pd.Timestamp(end, tz="UTC")]

    if df.empty:
        raise ValueError("No rows after filtering (check symbol/timeframe/start/end).")

    df = df.sort_values("ts").set_index("ts")
    return df[["open", "high", "low", "close", "volume"]], df["symbol"].iloc[0], df["timeframe"].iloc[0]


# -----------------------------
# Metrics (use your function if present)
# -----------------------------
def _basic_metrics(eq: pd.Series, cash_start: float) -> Dict[str, float]:
    eq = eq.astype(float)
    rets = eq.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    total = float(eq.iloc[-1] / eq.iloc[0] - 1.0) if len(eq) else 0.0
    sr = float(0.0)
    if rets.std(ddof=0) > 0:
        # simple annualization using daily scale; your metrics fn may do this more precisely
        sr = float((rets.mean() / rets.std(ddof=0)) * np.sqrt(365))
    return {
        "final_equity": float(eq.iloc[-1]) if len(eq) else float(cash_start),
        "abs_return": total,
        "sharpe": sr,
        "win_rate": float((rets > 0).mean()),
    }


def compute_metrics(eq: pd.Series, cash_start: float) -> Dict[str, float]:
    if callable(metrics_from_equity):
        # Use your implementation if it exists
        return metrics_from_equity(eq, cash_start)  # type: ignore[arg-type]
    return _basic_metrics(eq, cash_start)


# -----------------------------
# Backtest runner that calls YOUR functions
# -----------------------------
def run_backtest(
    df: pd.DataFrame,
    symbol: str,
    timeframe: str,
    strategy: str,
    cash_start: float,
    sma_fast: int,
    sma_slow: int,
) -> Dict:
    close = df["close"].astype(float)

    if strategy == "buy_hold":
        if not callable(equity_buy_hold):
            raise RuntimeError("equity_buy_hold not found in services/api")
        # Inspect signature to decide whether your function expects (series, cash) or (df, cash, ...)
        sig = inspect.signature(equity_buy_hold)
        if len(sig.parameters) == 2:
            eq = equity_buy_hold(close, cash_start)  # type: ignore[misc]
        else:
            eq = equity_buy_hold(df, cash_start)     # type: ignore[misc]
    elif strategy == "sma_cross":
        if not callable(equity_sma_cross):
            raise RuntimeError("equity_sma_cross not found in services/api")
        sig = inspect.signature(equity_sma_cross)
        # Support both (series, cash, fast, slow) and (df, cash, fast, slow)
        if len(sig.parameters) >= 4:
            # decide whether first param should be series or df by name
            first = list(sig.parameters.keys())[0]
            if first in ("price", "close", "series"):
                eq = equity_sma_cross(close, cash_start, sma_fast, sma_slow)  # type: ignore[misc]
            else:
                eq = equity_sma_cross(df, cash_start, sma_fast, sma_slow)     # type: ignore[misc]
        else:
            # very old variant: (series, cash) only
            eq = equity_sma_cross(close, cash_start)  # type: ignore[misc]
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    # Make sure it's a pandas Series aligned to time index
    if not isinstance(eq, pd.Series):
        eq = pd.Series(eq, index=close.index)  # tolerate list/array returns
    if not eq.index.equals(close.index):
        eq = eq.reindex(close.index).ffill().bfill()

    metrics = compute_metrics(eq, cash_start)

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "bars": int(len(close)),
        "start": str(close.index[0]),
        "end": str(close.index[-1]),
        "strategy": strategy,
        "cash_start": cash_start,
        "metrics": metrics,
    }


# -----------------------------
# Optional: call your training funcs if present
# -----------------------------
def run_training(
    df: pd.DataFrame,
    model: str,
    p: int,
    d: int,
    q: int,
    train_ratio: float,
) -> Dict:
    close = df["close"].astype(float)

    if model.lower() == "arima":
        if not callable(train_arima):
            raise RuntimeError("train_arima not found in services/api (add it there to use locally)")
        res = train_arima(close, order=(p, d, q), train_ratio=train_ratio)  # your function/return type
        # Try to normalize result to a dict
        if hasattr(res, "__dict__"):
            out = {**res.__dict__}
        else:
            out = dict(res)
        out.setdefault("model", "arima")
        return out
    elif model.lower() == "naive":
        if callable(train_naive):
            res = train_naive(close, train_ratio=train_ratio)
            return res.__dict__ if hasattr(res, "__dict__") else dict(res)
        # If you don’t expose a naive trainer, let the user know
        raise RuntimeError("naive training not implemented in services/api")
    else:
        raise ValueError(f"Unknown model: {model}")


# -----------------------------
# CLI
# -----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="alphagini local runner (reuses services/api code)")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("backtest", help="Run a strategy backtest using services/api strategies.")
    b.add_argument("--csv", required=True, help="Path to OHLCV CSV")
    b.add_argument("--symbol", help="Symbol to filter, e.g. BTC/USD")
    b.add_argument("--timeframe", help="Timeframe to filter, e.g. 5m")
    b.add_argument("--start", help="Start ISO8601 (UTC), e.g. 2024-08-01T00:00:00Z")
    b.add_argument("--end", help="End ISO8601 (UTC), exclusive")
    b.add_argument("--strategy", default="sma_cross", choices=["buy_hold", "sma_cross"])
    b.add_argument("--cash", type=float, default=100000)
    b.add_argument("--sma-fast", dest="sma_fast", type=int, default=10)
    b.add_argument("--sma-slow", dest="sma_slow", type=int, default=30)

    t = sub.add_parser("train", help="Train a model using services/api training functions (if present).")
    t.add_argument("--csv", required=True, help="Path to OHLCV CSV")
    t.add_argument("--symbol", help="Symbol to filter")
    t.add_argument("--timeframe", help="Timeframe to filter")
    t.add_argument("--start", help="Start ISO8601 (UTC)")
    t.add_argument("--end", help="End ISO8601 (UTC)")
    t.add_argument("--model", default="arima", choices=["arima", "naive"])
    t.add_argument("--p", type=int, default=1)
    t.add_argument("--d", type=int, default=1)
    t.add_argument("--q", type=int, default=1)
    t.add_argument("--train-ratio", type=float, default=0.8)

    p.add_argument("--print-head", action="store_true", help="Print first 3 rows after filtering (debug)")
    return p


def main():
    args = build_parser().parse_args()

    df, sym, tf = load_ohlcv_csv(args.csv, args.symbol, args.timeframe, args.start, args.end)
    if getattr(args, "print_head", False):
        print(df.head(3).to_string())

    if args.cmd == "backtest":
        out = run_backtest(
            df=df,
            symbol=sym,
            timeframe=tf,
            strategy=args.strategy,
            cash_start=float(args.cash),
            sma_fast=int(args.sma_fast),
            sma_slow=int(args.sma_slow),
        )
        print(json.dumps(out, indent=2))
    elif args.cmd == "train":
        out = run_training(
            df=df,
            model=args.model,
            p=args.p,
            d=args.d,
            q=args.q,
            train_ratio=args.train_ratio,
        )
        print(json.dumps(out, indent=2))
    else:
        raise SystemExit("unknown subcommand")


if __name__ == "__main__":
    # Allow running from repo root without installing the package:
    #   PYTHONPATH=. python tools/local_cli.py ...
    main()
