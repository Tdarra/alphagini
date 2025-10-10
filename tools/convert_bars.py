#!/usr/bin/env python3
import argparse
import os
import pandas as pd
from typing import Optional

from bars import time_to_volume_bars, time_to_dollar_bars, time_to_tick_bars  # same folder

def load_ohlcv(csv_path: str, symbol: Optional[str], timeframe: Optional[str]) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)
    df = pd.read_csv(csv_path)
    # normalize columns
    df.columns = [c.lower() for c in df.columns]
    need = ["ts", "open", "high", "low", "close", "volume"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"CSV missing required column '{c}'")
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    if "symbol" not in df.columns and symbol:
        df["symbol"] = symbol
    if "timeframe" not in df.columns and timeframe:
        df["timeframe"] = timeframe
    return df.sort_values("ts").set_index("ts")

def main():
    p = argparse.ArgumentParser(description="Convert time OHLCV into volume/dollar/tick bars.")
    p.add_argument("--csv", required=True, help="Input OHLCV CSV")
    p.add_argument("--out", required=True, help="Output CSV path")
    p.add_argument("--symbol", help="Symbol override if missing in CSV")
    p.add_argument("--timeframe", help="Timeframe override if missing in CSV")

    sub = p.add_subparsers(dest="kind", required=True)

    v = sub.add_parser("volume")
    v.add_argument("--vol-threshold", type=float, required=True, help="Base-asset volume per bar (e.g., 200.0)")

    d = sub.add_parser("dollar")
    d.add_argument("--usd-threshold", type=float, required=True, help="Dollar turnover per bar (e.g., 5e6)")
    d.add_argument("--price-basis", default="hlc3", choices=["close","hlc3","ohlc4","vwap"])

    t = sub.add_parser("tick")
    t.add_argument("--ticks-per-bar", type=int, required=True, help="Target number of trades per bar")
    t.add_argument("--trades-col", default="trades", help="Column with trade counts (default: trades)")

    args = p.parse_args()

    df = load_ohlcv(args.csv, args.symbol, args.timeframe)

    if args.kind == "volume":
        out = time_to_volume_bars(df, args.vol_threshold)
    elif args.kind == "dollar":
        out = time_to_dollar_bars(df, args.usd_threshold, price_basis=args.price_basis)
    else:
        out = time_to_tick_bars(df, args.ticks_per_bar, trades_col=args.trades_col)

    out.to_csv(args.out, index_label="ts")
    print(f"Wrote {len(out)} bars to {args.out}")

if __name__ == "__main__":
    main()

