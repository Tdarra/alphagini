"use client";

import React from "react";

type SymbolInfo = {
  symbol: string;
  timeframe: string;
  first_ts: string;
  last_ts: string;
  rows: number;
};

const API_URL = process.env.NEXT_PUBLIC_API_URL!;

function toISOStringZ(local: string) {
  // local is "YYYY-MM-DDTHH:MM" (no seconds), interpret as local time, convert to Z
  const d = new Date(local);
  return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().replace(/\.\d{3}Z$/, "Z");
}

export default function Page() {
  const [symbols, setSymbols] = React.useState<SymbolInfo[]>([]);
  const [symbolsLoading, setSymbolsLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const [symbol, setSymbol] = React.useState("");
  const [timeframe, setTimeframe] = React.useState("5m");
  const [start, setStart] = React.useState(""); // datetime-local
  const [end, setEnd] = React.useState("");
  const [cashStart, setCashStart] = React.useState(100000);
  const [running, setRunning] = React.useState(false);
  const [result, setResult] = React.useState<any>(null);

  React.useEffect(() => {
    (async () => {
      setSymbolsLoading(true);
      setError(null);
      try {
        const r = await fetch(`${API_URL}/symbols`);
        if (!r.ok) throw new Error(await r.text());
        const data: SymbolInfo[] = await r.json();
        setSymbols(data);
        if (data.length) setSymbol(data[0].symbol);
        // default window: last 3 days
        const now = new Date();
        const before = new Date(now.getTime() - 3 * 24 * 60 * 60 * 1000);
        const toLocal = (d: Date) =>
          new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
        setStart(toLocal(before));
        setEnd(toLocal(now));
      } catch (e: any) {
        setError(`Failed to load symbols: ${e.message || e}`);
      } finally {
        setSymbolsLoading(false);
      }
    })();
  }, []);

  async function runBacktest() {
    setRunning(true);
    setError(null);
    setResult(null);
    try {
      const payload = {
        symbol,
        timeframe,
        start: toISOStringZ(start),
        end: toISOStringZ(end),
        model: "sma",
        strategy: "sma_cross",
        cash_start: cashStart,
        sma_fast: 10,
        sma_slow: 30,
      };
      const r = await fetch(`${API_URL}/backtest`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      setResult(data);
    } catch (e: any) {
      setError(`Backtest failed: ${e.message || e}`);
    } finally {
      setRunning(false);
    }
  }

  return (
    <main style={{ maxWidth: 960, margin: "40px auto", padding: 16 }}>
      <h1 style={{ fontSize: 28, fontWeight: 700, marginBottom: 16 }}>alphagini — Backtesting</h1>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        <label>
          <div>Symbol</div>
          <select
            value={symbol}
            disabled={symbolsLoading}
            onChange={(e) => setSymbol(e.target.value)}
            style={{ width: "100%", padding: 8 }}
          >
            {symbols.map((s) => (
              <option key={`${s.symbol}-${s.timeframe}`} value={s.symbol}>
                {s.symbol}
              </option>
            ))}
          </select>
        </label>

        <label>
          <div>Timeframe</div>
          <select
            value={timeframe}
            onChange={(e) => setTimeframe(e.target.value)}
            style={{ width: "100%", padding: 8 }}
          >
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="30m">30m</option>
            <option value="1h">1h</option>
            <option value="4h">4h</option>
            <option value="1d">1d</option>
          </select>
        </label>

        <label>
          <div>Start (local)</div>
          <input
            type="datetime-local"
            value={start}
            onChange={(e) => setStart(e.target.value)}
            style={{ width: "100%", padding: 8 }}
          />
        </label>

        <label>
          <div>End (local)</div>
          <input
            type="datetime-local"
            value={end}
            onChange={(e) => setEnd(e.target.value)}
            style={{ width: "100%", padding: 8 }}
          />
        </label>

        <label>
          <div>Starting cash ($)</div>
          <input
            type="number"
            min={0}
            step={1000}
            value={cashStart}
            onChange={(e) => setCashStart(Number(e.target.value))}
            style={{ width: "100%", padding: 8 }}
          />
        </label>
      </div>

      <div style={{ marginTop: 16, display: "flex", gap: 12 }}>
        <button
          onClick={runBacktest}
          disabled={running || !symbol || !start || !end}
          style={{
            padding: "10px 16px",
            background: running ? "#aaa" : "#111",
            color: "#fff",
            borderRadius: 8,
          }}
        >
          {running ? "Running…" : "Run"}
        </button>
      </div>

      {error && (
        <div style={{ marginTop: 16, color: "#b00020", whiteSpace: "pre-wrap" }}>{error}</div>
      )}

      {result && (
        <pre style={{ marginTop: 16, background: "#f5f5f5", padding: 12, borderRadius: 8 }}>
          {JSON.stringify(result.summary ?? result, null, 2)}
        </pre>
      )}

      <p style={{ marginTop: 24, color: "#666" }}>
        Tip: date-times are interpreted in your local timezone then converted to UTC for the API.
      </p>
    </main>
  );
}
