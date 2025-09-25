"use client";
import { useState } from "react";
import BacktestForm from "@/components/BacktestForm";
import MetricsCards from "@/components/MetricsCards";

export default function BacktestPage() {
  const [result, setResult] = useState<any>(null);
  return (
    <div className="p-6 space-y-6">
      <h1 className="text-3xl font-semibold">Crypto Backtesting</h1>
      <BacktestForm onResult={setResult} />
      {result && <MetricsCards data={result} />}
      {result?.equity_curve && (
        <div className="mt-6">
          <EquityChart points={result.equity_curve} />
        </div>
      )}
    </div>
  );
}

function EquityChart({ points }: { points: {ts:string,equity:number}[] }) {
  // lightweight, client-only chart with recharts
  // (import lazily to keep snippet short)
  const Recharts = require("recharts");
  const { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } = Recharts;
  return (
    <div className="h-80">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points}>
          <XAxis dataKey="ts" hide/>
          <YAxis />
          <Tooltip />
          <Line type="monotone" dataKey="equity" dot={false}/>
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
