#!/usr/bin/env python3
import pandas as pd
import numpy as np
from typing import Iterable, Literal, Optional

PriceBasis = Literal["close", "hlc3", "ohlc4", "vwap"]

def _check_cols(df: pd.DataFrame, needed: Iterable[str]) -> None:
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

def _typical_price(df: pd.DataFrame, basis: PriceBasis) -> pd.Series:
    if basis == "close":
        return df["close"].astype(float)
    if basis == "hlc3":
        return (df["high"] + df["low"] + df["close"]) / 3.0
    if basis == "ohlc4":
        return (df["open"] + df["high"] + df["low"] + df["close"]) / 4.0
    if basis == "vwap":
        if "vwap" not in df.columns:
            raise ValueError("vwap column not present; choose another price_basis")
        return df["vwap"].astype(float)
    raise ValueError(f"unsupported basis: {basis}")

def _aggregate_chunk(chunk: pd.DataFrame) -> dict:
    """Aggregate contiguous time bars into one bar (OHLCV)."""
    if chunk.empty:
        return {}
    return {
        "open": float(chunk["open"].iloc[0]),
        "high": float(chunk["high"].max()),
        "low": float(chunk["low"].min()),
        "close": float(chunk["close"].iloc[-1]),
        "volume": float(chunk["volume"].sum()),
        "n_src_bars": int(len(chunk)),
        "ts_open": pd.Timestamp(chunk.index[0]),
        "ts_close": pd.Timestamp(chunk.index[-1]),
    }

def time_to_volume_bars(
    df: pd.DataFrame,
    vol_threshold: float,
    keep_tail: bool = True,
) -> pd.DataFrame:
    """
    Build volume bars by accumulating consecutive time bars until cumulative
    volume >= vol_threshold, then emitting an aggregated bar.

    Input df: indexed by UTC timestamp, columns: open, high, low, close, volume
    """
    _check_cols(df, ["open", "high", "low", "close", "volume"])
    df = df.sort_index()

    bars = []
    start = 0
    cum = 0.0

    vols = df["volume"].astype(float).values
    n = len(df)

    for i in range(n):
        cum += vols[i]
        if cum >= vol_threshold:
            chunk = df.iloc[start : i + 1]
            bars.append(_aggregate_chunk(chunk))
            start = i + 1
            cum = 0.0

    if keep_tail and start < n:
        chunk = df.iloc[start:n]
        bars.append(_aggregate_chunk(chunk))

    out = pd.DataFrame(bars)
    if len(out):
        out.index = out["ts_close"]  # bar timestamp = last source bar time
        out = out.drop(columns=["ts_open", "ts_close"])
    return out

def time_to_dollar_bars(
    df: pd.DataFrame,
    dollar_threshold: float,
    price_basis: PriceBasis = "hlc3",
    keep_tail: bool = True,
) -> pd.DataFrame:
    """
    Build dollar bars by accumulating (price_basis * volume) across time bars.
    """
    _check_cols(df, ["open", "high", "low", "close", "volume"])
    df = df.sort_index()
    basis = _typical_price(df, price_basis).astype(float)
    dv = (basis * df["volume"].astype(float)).values

    bars = []
    start = 0
    cum = 0.0
    n = len(df)

    for i in range(n):
        cum += dv[i]
        if cum >= dollar_threshold:
            chunk = df.iloc[start : i + 1]
            bars.append(_aggregate_chunk(chunk))
            start = i + 1
            cum = 0.0

    if keep_tail and start < n:
        chunk = df.iloc[start:n]
        bars.append(_aggregate_chunk(chunk))

    out = pd.DataFrame(bars)
    if len(out):
        out.index = out["ts_close"]
        out = out.drop(columns=["ts_open", "ts_close"])
    return out

def time_to_tick_bars(
    df: pd.DataFrame,
    ticks_per_bar: int,
    trades_col: str = "trades",
    keep_tail: bool = True,
) -> pd.DataFrame:
    """
    Build tick bars by accumulating the per-bar trade count column.
    Requires df[trades_col] to exist (e.g., 'num_trades' from some exchanges).
    """
    if trades_col not in df.columns:
        raise ValueError(
            f"Column '{trades_col}' not found. True tick bars need a trade count per bar. "
            "Standard OHLCV lacks this; fetch tick/trade data or a 'trades' count."
        )
    _check_cols(df, ["open", "high", "low", "close", "volume"])
    df = df.sort_index()

    ticks = df[trades_col].astype(float).values
    bars, start, cum = [], 0, 0.0
    n = len(df)

    for i in range(n):
        cum += ticks[i]
        if cum >= ticks_per_bar:
            chunk = df.iloc[start : i + 1]
            agg = _aggregate_chunk(chunk)
            agg["n_ticks"] = float(cum)
            bars.append(agg)
            start = i + 1
            cum = 0.0

    if keep_tail and start < n:
        chunk = df.iloc[start:n]
        agg = _aggregate_chunk(chunk)
        agg["n_ticks"] = float(cum)
        bars.append(agg)

    out = pd.DataFrame(bars)
    if len(out):
        out.index = out["ts_close"]
        out = out.drop(columns=["ts_open", "ts_close"])
    return out

