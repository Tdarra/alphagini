"use client";

import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip } from "recharts";

type Point = { t: Date; equity: number };

export default function EquityChart({ ts, equity }: { ts: string[]; equity: number[] }) {
  if (!ts?.length || !equity?.length || ts.length !== equity.length) {
    return (
      <div className="rounded border border-neutral-700 p-4 text-sm text-neutral-400">
        No chart data (ts={ts?.length ?? 0}, equity={equity?.length ?? 0}).
      </div>
    );
  }

  const data: Point[] = ts.map((iso, i) => ({ t: new Date(iso), equity: equity[i] }));

  return (
    <div className="h-80 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data}>
          <XAxis
            dataKey="t"
            tickFormatter={(d: Date) => d.toISOString().slice(5, 16)}
            type="number"
            domain={["dataMin", "dataMax"]}
            scale="time"
          />
          <YAxis />
          <Tooltip
            labelFormatter={(label) => new Date(label).toLocaleString()}
            formatter={(v) => [v, "Equity"]}
          />
          <Area dataKey="equity" type="monotone" strokeWidth={1.5} fillOpacity={0.15} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

