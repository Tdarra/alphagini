from fastapi import FastAPI
from routers import backtest

app = FastAPI(title="alphagini", version="0.1.0")
app.include_router(backtest.router, prefix="/backtest", tags=["backtest"])

@app.get("/health")
def health(): return {"ok": True}
