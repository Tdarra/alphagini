"use client";

import React from "react";

import EquityChart from "../components/EquityChart";

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
  const [model, setModel] = React.useState("sma");
  const [strategy, setStrategy] = React.useState("sma_cross");
  const [running, setRunning] = React.useState(false);
  const [result, setResult] = React.useState<any>(null);
  const [logs, setLogs] = React.useState<string[]>([]);

  const appendLog = React.useCallback((message: string, data?: unknown) => {
    const timestamp = new Date().toLocaleTimeString();
    let serialized = "";
    if (data !== undefined) {
      try {
        serialized = `\n${JSON.stringify(data, null, 2)}`;
      } catch (err) {
        serialized = `\n${String(data)}`;
      }
    }
    const line = `[${timestamp}] ${message}${serialized}`;
    console.log(`[alphagini-web] ${message}`, data);
    setLogs((prev) => [...prev.slice(-99), line]);
  }, []);

  React.useEffect(() => {
    (async () => {
      setSymbolsLoading(true);
      setError(null);
      try {
        const symbolsEndpoint = `${API_URL}/symbols`;
        appendLog("Requesting symbol metadata from API", { endpoint: symbolsEndpoint });
        const r = await fetch(symbolsEndpoint);
        if (!r.ok) throw new Error(await r.text());
        const data: SymbolInfo[] = await r.json();
        appendLog("Received symbol metadata", data);
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
        const message = `Failed to load symbols: ${e.message || e}`;
        appendLog(message);
        setError(message);
      } finally {
        setSymbolsLoading(false);
      }
    })();
  }, [appendLog]);

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
        model,
        strategy,
        cash_start: cashStart,
        sma_fast: 10,
        sma_slow: 30,
      };
      const endpoint = `${API_URL}/backtest`;
      appendLog("Submitting backtest request", { endpoint, payload });
      const r = await fetch(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!r.ok) {
        const errorText = await r.text();
        appendLog("Backtest request failed", { endpoint, status: r.status, body: errorText });
        throw new Error(errorText || `Request failed with status ${r.status}`);
      }
      const data = await r.json();
      appendLog("Received backtest response", { endpoint, data });
      setResult(data);
    } catch (e: any) {
      const message = `Backtest failed: ${e.message || e}`;
      appendLog(message);
      setError(message);
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
          <div>Model</div>
          <select
            value={model}
            onChange={(e) => setModel(e.target.value)}
            style={{ width: "100%", padding: 8 }}
          >
            <option value="naive">Naive (previous close)</option>
            <option value="sma">Simple moving average</option>
          </select>
        </label>

        <label>
          <div>Strategy</div>
          <select
            value={strategy}
            onChange={(e) => setStrategy(e.target.value)}
            style={{ width: "100%", padding: 8 }}
          >
            <option value="buy_hold">Buy &amp; hold</option>
            <option value="sma_cross">SMA crossover</option>
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
        <ResultSection result={result} />
      )}

      <div style={{ marginTop: 24 }}>
        <h2 style={{ fontSize: 18, marginBottom: 8 }}>Debug logs</h2>
        <pre
          style={{
            background: "#111827",
            color: "#d1d5db",
            padding: 12,
            borderRadius: 8,
            border: "1px solid #1f2937",
            whiteSpace: "pre-wrap",
            maxHeight: 240,
            overflowY: "auto",
            fontSize: 13,
          }}
        >
          {logs.length ? logs.join("\n\n") : "Run a backtest to view debug logs."}
        </pre>
      </div>

      <p style={{ marginTop: 24, color: "#666" }}>
        Tip: date-times are interpreted in your local timezone then converted to UTC for the API.
      </p>
    </main>
  );
}

type BacktestResult = {
  summary?: Record<string, unknown>;
  metrics?: Record<string, unknown>;
  equity_curve?: { ts: string; equity: number }[];
};

function ResultSection({ result }: { result: BacktestResult }) {
  const metrics = result.metrics ?? {};
  const metricLabels: Record<string, string> = {
    sharpe: "Sharpe Ratio",
    win_rate: "Win Rate",
    max_drawdown: "Max Drawdown",
    abs_return_usd: "Absolute Return ($)",
    rel_return: "Relative Return",
    rmse: "RMSE",
  };
  const metricOrder = Object.keys(metricLabels) as (keyof typeof metricLabels)[];

  const formatMetric = React.useCallback((key: string, value: number) => {
    if (value === null || value === undefined) return "—";
    if (key === "abs_return_usd") {
      return new Intl.NumberFormat(undefined, {
        style: "currency",
        currency: "USD",
        maximumFractionDigits: 0,
      }).format(value);
    }
    if (key === "win_rate" || key === "rel_return" || key === "max_drawdown") {
      return `${(value * 100).toFixed(2)}%`;
    }
    if (key === "sharpe") {
      return value.toFixed(2);
    }
    if (key === "rmse") {
      return value.toFixed(4);
    }
    return value.toString();
  }, []);

  const equityCurve = React.useMemo(() => {
    const points = result.equity_curve ?? [];
    return {
      ts: points.map((p) => p.ts),
      equity: points.map((p) => p.equity),
    };
  }, [result.equity_curve]);

  const hasMetrics = metricOrder.some((key) => typeof metrics[key] === "number");

  return (
    <div style={{ marginTop: 24, display: "flex", flexDirection: "column", gap: 24 }}>
      {hasMetrics && (
        <div>
          <h2 style={{ fontSize: 18, marginBottom: 12 }}>Backtest metrics</h2>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
              gap: 12,
            }}
          >
            {metricOrder.map((key) => {
              const raw = metrics[key];
              if (typeof raw !== "number") return null;
              return (
                <div
                  key={key}
                  style={{
                    background: "#0f172a",
                    color: "#e2e8f0",
                    borderRadius: 8,
                    padding: "12px 16px",
                    border: "1px solid #1e293b",
                  }}
                >
                  <div style={{ fontSize: 12, textTransform: "uppercase", color: "#94a3b8" }}>
                    {metricLabels[key] ?? key}
                  </div>
                  <div style={{ fontSize: 20, fontWeight: 600, marginTop: 4 }}>
                    {formatMetric(key, raw)}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div>
        <h2 style={{ fontSize: 18, marginBottom: 12 }}>Equity curve</h2>
        <div
          style={{
            background: "#0f172a",
            borderRadius: 8,
            border: "1px solid #1e293b",
            padding: 16,
          }}
        >
          <EquityChart ts={equityCurve.ts} equity={equityCurve.equity} />
        </div>
      </div>

      <div>
        <h2 style={{ fontSize: 18, marginBottom: 8 }}>Raw response</h2>
        <pre
          style={{
            background: "#0f172a",
            color: "#e2e8f0",
            padding: 12,
            borderRadius: 8,
            border: "1px solid #1e293b",
            whiteSpace: "pre-wrap",
          }}
        >
          {JSON.stringify(result.summary ?? result, null, 2)}
        </pre>
      </div>
    </div>
  );
}
