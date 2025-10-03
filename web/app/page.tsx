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
  const [logs, setLogs] = React.useState<string[]>([]);
  const log = (...parts: any[]) => {
    setLogs((prev) => [
      ...prev,
      parts
        .map((p) => (typeof p === "string" ? p : JSON.stringify(p)))
        .join(" "),
    ]);
  };


    React.useEffect(() => {
    (async () => {
      setSymbolsLoading(true);
      setError(null);
      try {
        const r = await fetch(`${API_URL}/symbols`);
        if (!r.ok) throw new Error(await r.text());
        const data: SymbolInfo[] = await r.json();
        setSymbols(data);
        log("symbols loaded:", data.length);  // <-- NEW
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
        log("symbols error:", e?.message || e);  // <-- NEW
      } finally {
        setSymbolsLoading(false);
      }
    })();
  }, []);


  async function runBacktest() {
    setRunning(true);
    setError(null);
    setResult(null);
    setLogs([]); // clear old logs
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

      // Client-side breadcrumbs
      const url = `${API_URL}/backtest`;
      log("POST", url);
      log("payload", payload);
      console.log("[backtest] POST", url, payload);

      const r = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });

      log("status", String(r.status));
      const ct = r.headers.get("content-type") || "(none)";
      log("content-type", ct);
      console.log("[backtest] status:", r.status, "| content-type:", ct);

      // Read body as text first so we can always inspect it
      const text = await r.text();
      const preview = text.length > 500 ? text.slice(0, 500) + "…(truncated)" : text;
      log("body-preview", preview);
      console.log("[backtest] body-preview:", preview);

      if (!r.ok) {
        throw new Error(text || `HTTP ${r.status}`);
      }

      // Safely parse JSON
      let data: any = null;
      try {
        data = text ? JSON.parse(text) : null;
      } catch (err: any) {
        log("json-parse-error", err?.message || String(err));
        console.error("[backtest] json-parse-error:", err);
        throw new Error("API returned non-JSON (see Debug logs)");
      }

      // If the server sends a logs array, surface it
      if (Array.isArray(data?.logs)) {
        setLogs((prev) => [...prev, ...data.logs]);
      }

      // Helpful counts + key list
      log("keys", data ? Object.keys(data) : []);
      if (Array.isArray(data?.equity)) log("equity points", data.equity.length);
      if (Array.isArray(data?.prices)) log("prices points", data.prices.length);
      console.log("[backtest] keys:", data ? Object.keys(data) : []);
      if (Array.isArray(data?.equity)) console.log("[backtest] equity points:", data.equity.length);
      if (Array.isArray(data?.prices)) console.log("[backtest] prices points:", data.prices.length);

      setResult(data);
    } catch (e: any) {
      const msg = e?.message || String(e);
      setError(`Backtest failed: ${msg}`);
      log("exception", msg);
      console.error("[backtest] exception:", e);
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
      {logs.length > 0 && (
        <div style={{ marginTop: 12 }}>
          <div style={{ fontWeight: 600, marginBottom: 6 }}>Debug logs</div>
          <pre
            style={{
              background: "#0b1020",
              color: "#e6f1ff",
              padding: 12,
              borderRadius: 8,
              whiteSpace: "pre-wrap",
              maxHeight: 260,
              overflow: "auto",
              fontSize: 13,
            }}
          >
            {logs.join("\n")}
          </pre>
        </div>
      )}
      <p style={{ marginTop: 24, color: "#666" }}>
        Tip: date-times are interpreted in your local timezone then converted to UTC for the API.
      </p>
    </main>
  );
}
