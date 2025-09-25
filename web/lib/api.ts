export async function runBacktest(payload:any){
    const body = {
      exchange: "binance",
      symbol: payload.symbol,
      timeframe: "1h",
      start: new Date(payload.start).toISOString(),
      end: new Date(payload.end).toISOString(),
      lookback_days: 180,
      model: payload.model,
      strategy: payload.strategy,
      initial_cash: 100000
    };
    const res = await fetch(process.env.NEXT_PUBLIC_API_URL+"/backtest/run",{
      method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)
    });
    if(!res.ok) throw new Error(await res.text());
    return await res.json();
  }
  