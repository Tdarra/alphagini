"use client";
import { useEffect, useMemo, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

type SymbolInfo = { symbol: string; timeframe: string; first_ts: string; last_ts: string; rows: number; };

export default function Home() {
  const API = process.env.NEXT_PUBLIC_API_URL!;
  const [symbols, setSymbols] = useState<SymbolInfo[]>([]);
  const [symbol, setSymbol] = useState<string>("");
  const [timeframe, setTimeframe] = useState<string>("5m");
  const [start, setStart] = useState<string>("");
  const [end, setEnd] = useState<string>("");
  const [model, setModel] = useState<"naive"|"sma">("sma");
  const [strategy, setStrategy] = useState<"buy_hold"|"sma_cross">("sma_cross");
  const [cash, setCash] = useState<number>(100000);
  const [fast, setFast] = useState<number>(10);
  const [slow, setSlow] = useState<number>(30);
  const [metrics, setMetrics] = useState<any>(null);
  const [curve, setCurve] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetch(`${API}/symbols`).then(r => r.json()).then((rows: SymbolInfo[]) => {
      setSymbols(rows);
      if (rows.length) {
        setSymbol(rows[0].symbol);
        setTimeframe(rows[0].timeframe);
        setStart(rows[0].first_ts);
        setEnd(rows[0].last_ts);
      }
    });
  }, [API]);

  const doRun = async () => {
    setLoading(true);
    const body = {
      symbol, timeframe, start, end, model, strategy,
      cash_start: cash, sma_fast: fast, sma_slow: slow
    };
    const r = await fetch(`${API}/backtest`, { method:"POST", headers:{ "content-type":"application/json" }, body: JSON.stringify(body) });
    const j = await r.json();
    setMetrics(j.metrics);
    setCurve(j.equity_curve?.map((d:any)=>({ ts: d.ts, equity: d.equity })) ?? []);
    setLoading(false);
  };

  const rows = useMemo(()=>symbols.filter(s=>s.symbol===symbol), [symbols, symbol]);

  return (
    <main className="p-6 max-w-6xl mx-auto">
      <h1 className="text-2xl font-semibold mb-4">alphagini — Backtesting</h1>

      <div className="grid grid-cols-2 gap-4 mb-6">
        <label>Symbol
          <select className="border p-2 w-full" value={symbol} onChange={e=>setSymbol(e.target.value)}>
            {symbols.map((s,i)=><option key={i} value={s.symbol}>{s.symbol}</option>)}
          </select>
        </label>
        <label>Timeframe
          <input className="border p-2 w-full" value={timeframe} onChange={e=>setTimeframe(e.target.value)} />
        </label>
        <label>Start (UTC)
          <input className="border p-2 w-full" value={start} onChange={e=>setStart(e.target.value)} />
        </label>
        <label>End (UTC)
          <input className="border p-2 w-full" value={end} onChange={e=>setEnd(e.target.value)} />
        </label>
        <label>Model
          <select className="border p-2 w-full" value={model} onChange={e=>setModel(e.target.value as any)}>
            <option value="naive">naive</option>
            <option value="sma">sma</option>
          </select>
        </label>
        <label>Strategy
          <select className="border p-2 w-full" value={strategy} onChange={e=>setStrategy(e.target.value as any)}>
            <option value="buy_hold">buy_hold</option>
            <option value="sma_cross">sma_cross</option>
          </select>
        </label>
        <label>Cash start
          <input type="number" className="border p-2 w-full" value={cash} onChange={e=>setCash(Number(e.target.value))}/>
        </label>
        <label>Fast/Slow (SMA)
          <div className="flex gap-2">
            <input type="number" className="border p-2 w-full" value={fast} onChange={e=>setFast(Number(e.target.value))}/>
            <input type="number" className="border p-2 w-full" value={slow} onChange={e=>setSlow(Number(e.target.value))}/>
          </div>
        </label>
      </div>

      <button onClick={doRun} disabled={loading} className="border px-4 py-2">{loading? "Running…" : "Run backtest"}</button>

      {metrics && (
        <div className="mt-6 grid grid-cols-5 gap-4 text-sm">
          <div>Sharpe<br/><b>{metrics.sharpe.toFixed(2)}</b></div>
          <div>Win rate<br/><b>{(metrics.win_rate*100).toFixed(1)}%</b></div>
          <div>Max DD<br/><b>{(metrics.max_drawdown*100).toFixed(1)}%</b></div>
          <div>Abs Return<br/><b>${metrics.abs_return_usd.toLocaleString()}</b></div>
          <div>Rel Return<br/><b>{(metrics.rel_return*100).toFixed(2)}%</b></div>
        </div>
      )}

      <div className="mt-6 h-80 border">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={curve}>
            <XAxis dataKey="ts" hide />
            <YAxis />
            <Tooltip />
            <Line type="monotone" dataKey="equity" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </main>
  );
}
