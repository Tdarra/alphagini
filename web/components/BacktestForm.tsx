import { useState } from "react";
import { runBacktest } from "@/lib/api";

export default function BacktestForm({ onResult }:{ onResult:(r:any)=>void }) {
  const [loading,setLoading]=useState(false);
  async function submit(e:any){
    e.preventDefault(); setLoading(true);
    const form = new FormData(e.currentTarget);
    const payload = Object.fromEntries(form.entries());
    const res = await runBacktest(payload);
    onResult(res); setLoading(false);
  }
  return (
    <form onSubmit={submit} className="grid md:grid-cols-6 gap-3">
      <input name="symbol" defaultValue="BTC/USDT" className="input"/>
      <select name="model" className="input">
        <option>arima</option><option>prophet</option><option>xgb</option><option>lstm</option>
      </select>
      <select name="strategy" className="input">
        <option>buy_hold</option><option>sma_cross</option><option>rsi_meanrev</option>
      </select>
      <input type="datetime-local" name="start" className="input"/>
      <input type="datetime-local" name="end" className="input"/>
      <button className="btn" disabled={loading}>{loading?"Running…":"Run backtest"}</button>
      <style jsx>{`
        .input{border:1px solid #e5e7eb; padding:.5rem; border-radius:.5rem}
        .btn{background:black;color:white;padding:.5rem 1rem;border-radius:.5rem}
      `}</style>
    </form>
  );
}
