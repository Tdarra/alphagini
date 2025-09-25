from fastapi import APIRouter, HTTPException
from ..schemas import BacktestRequest, BacktestResponse, EquityPoint, Metrics, ModelMetrics
from ..data.bigquery import load_ohlcv
from ..backtester.engine import run_backtest
from ..backtester.metrics import model_errors, baseline_arima_rmse
import uuid

router = APIRouter()

@router.post("/run", response_model=BacktestResponse)
def run(req: BacktestRequest):
    df = load_ohlcv(req.exchange, req.symbol, req.timeframe, req.start, req.end)
    if df.empty: raise HTTPException(404, "No data in range")
    result = run_backtest(df=df, req=req)  # returns equity_series, perf_metrics, y_true, y_pred
    rmse, mae, mape = model_errors(result.y_true, result.y_pred)
    base_rmse = baseline_arima_rmse(result.y_true)
    return BacktestResponse(
        run_id=str(uuid.uuid4()),
        equity_curve=[EquityPoint(ts=t, equity=e) for t,e in result.equity_series],
        metrics=Metrics(**result.perf_metrics),
        model_metrics=ModelMetrics(rmse=rmse, mae=mae, mape=mape, baseline_rmse=base_rmse)
    )
