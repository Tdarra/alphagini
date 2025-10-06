# alphagini

Crypto price forecasting & backtesting on GCP with a FastAPI backend, Next.js dashboard, and BigQuery as the market-data store.

Pick models/strategies, select a time window, and view metrics like Sharpe, win rate, absolute/relative return, RMSE, and baseline vs ARIMA.

Data schema follows ccxt-style OHLCV for easy continuous refresh.

## 🚦 Quick Start (local, no GCP)
**From repo root**
```
./scripts/setup_local.sh       # creates .venv_local and installs local deps
source ./activate_local.sh     # activates venv and sets PYTHONPATH=.
```
**Run a local backtest against a CSV (no BigQuery, no Cloud Run)**
```
python tools/local_cli.py backtest \
  --csv services/ingest/normalized/coinbase_BTC_USD_5m_ccxt.csv \
  --symbol BTC/USD --timeframe 5m \
  --start 2025-08-01T00:00:00Z --end 2025-08-03T00:00:00Z \
  --strategy sma_cross --cash 100000 --sma-fast 10 --sma-slow 30
```

The CLI reuses the same strategy/metrics functions that power the API (imports from services/api), so results match what you’ll deploy.

### 🗂️ Repo Structure
```
alphagini/
├─ services/
│  ├─ api/
│  │  ├─ app.py            # FastAPI: /health, /symbols, /backtest, BQ access
│  │  ├─ strategies.py     # Strategy helpers (e.g., SMA cross, buy/hold)
│  │  └─ ...               # (pydantic models, utils, etc.)
│  └─ ingest/
│     ├─ binance_ingest.py # ccxt-based historical loader (initial)
│     ├─ kraken_ingest.py  # ccxt-based alt (used after Binance 451)
│     └─ normalized/       # CSVs normalized to ccxt schema for BQ
├─ web/
│  ├─ app/page.tsx         # Next.js App Router UI
│  └─ package.json         # "start": "next start -p $PORT"
├─ tools/
│  ├─ local_cli.py         # Local runner (backtest/train) using services/api code
│  └─ requirements-local.txt
├─ scripts/
│  ├─ setup_local.sh       # Creates .venv_local and installs local deps
│  └─ setup_local.ps1
├─ activate_local.sh       # Activates venv and sets PYTHONPATH=.
└─ README.md
```
### 🧰 Local Development
1. Environment

```
./scripts/setup_local.sh
source ./activate_local.sh
python --version
```

This creates .venv_local, installs pandas/numpy/fastapi (for import compatibility), and exports PYTHONPATH=. so from services.api ... works locally.

We include google-cloud-bigquery & db-dtypes in local deps because app.py imports them—local runs won’t hit GCP, but imports succeed.

2. CSV Schema (ccxt-style)

Columns expected (case-insensitive):

Required: ts, open, high, low, close

Optional: volume, symbol, timeframe, exchange

ts must be UTC (e.g., 2024-08-01T00:00:00Z).
If your CSV lacks symbol/timeframe, pass them via CLI flags.

3. Local CLI

Backtest:
```
python tools/local_cli.py backtest \
  --csv services/ingest/normalized/coinbase_ETH_USD_5m_ccxt.csv \
  --symbol ETH/USD --timeframe 5m \
  --start 2025-08-01T00:00:00Z --end 2025-08-02T00:00:00Z \
  --strategy buy_hold --cash 100000
  ```

Train (only if corresponding trainer is implemented in services/api):
```
python tools/local_cli.py train \
  --csv services/ingest/normalized/coinbase_BTC_USD_5m_ccxt.csv \
  --symbol BTC/USD --timeframe 5m \
  --start 2025-08-01T00:00:00Z --end 2025-08-07T00:00:00Z \
  --model arima --p 1 --d 1 --q 1 --train-ratio 0.8
```

The CLI imports your strategies/metrics from services/api so you’re testing the real code, not a duplicate implementation.

### 🧪 Run the API Locally (no GCP)
**From repo root**
`source ./activate_local.sh`

**Minimal env (only needed if your app reads these locally)**
```
export ALPHAGINI_PROJECT="local-dev"
export ALPHAGINI_BQ_DATASET="alphagini_marketdata"
export ALPHAGINI_EXP_DATASET="alphagini_experiments"
```

**Start FastAPI locally**
`uvicorn services.api.app:app --host 0.0.0.0 --port 8080 --reload`

**In another shell**
`curl http://localhost:8080/health`


Then point the web app at your local API:

**web/.env.local**
`NEXT_PUBLIC_API_URL=http://localhost:8080`

### 🌐 Dashboard (Next.js)
**From repo root**
```
cd web
npm install            # only first time
echo "NEXT_PUBLIC_API_URL=http://localhost:8080" > .env.local
npm run dev            # http://localhost:3000
```


Symbol: auto-loads from /symbols

Timeframe: dropdown (e.g., 5m/15m/30m/1h/4h/1d)

Dates: local datetime pickers are converted to UTC ISO for the API

Run: posts to /backtest and renders metrics + chart

Debug logs: a collapsible box under the chart shows client and server breadcrumbs (API returns a logs: [] array; UI appends it)

If the UI calls /undefined/symbols, you didn’t inject NEXT_PUBLIC_API_URL at build time—see the GCP section below.

## ☁️ GCP Deployment (Cloud Run + BigQuery)
Datasets & Table

Market data (read): TBD
Schema (ccxt-like): exchange STRING, symbol STRING, timeframe STRING, ts TIMESTAMP, open FLOAT64, high FLOAT64, low FLOAT64, close FLOAT64, volume FLOAT64

Experiments (write): alphagini_experiments (your backtests, metrics, etc.)

Deploy API (alphagini-api):
```
export PROJECT_ID="alpha-gini"
export REGION="us-central1"
```
**Build image (Cloud Build)**
`gcloud builds submit services/api --tag gcr.io/$PROJECT_ID/alphagini-api:latest`

**Deploy to Cloud Run**
```
gcloud run deploy alphagini-api \
  --image gcr.io/$PROJECT_ID/alphagini-api:latest \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars ALPHAGINI_PROJECT=$PROJECT_ID,\
ALPHAGINI_BQ_DATASET=alphagini_marketdata,\
ALPHAGINI_EXP_DATASET=alphagini_experiments
```


IAM for the service account (SA):
```
SA=$(gcloud run services describe alphagini-api --region $REGION \
  --format='value(spec.template.spec.serviceAccountName)')
```
**Run queries**
```
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA" --role="roles/bigquery.jobUser"
```

**Read market data**
```
bq --project_id="$PROJECT_ID" update --dataset \
  --add_iam_member="roles/bigquery.dataViewer:serviceAccount:${SA}" \
  "$PROJECT_ID:alphagini_marketdata"
```

**Write experiments (if applicable)**
```
bq --project_id="$PROJECT_ID" update --dataset \
  --add_iam_member="roles/bigquery.dataEditor:serviceAccount:${SA}" \
  "$PROJECT_ID:alphagini_experiments"
```


**Verify:**
```
API_URL=$(gcloud run services describe alphagini-api --region $REGION --format='value(status.url)')
curl "$API_URL/health"
curl "$API_URL/symbols"
```


If you see ValueError: Please install db-dtypes, add db-dtypes to your API image (we already do in local setup).
If /symbols 500s: check dataset/table names and SA roles above.

Deploy Web (alphagini-web) with Buildpacks (no Dockerfile)

Next.js inlines NEXT_PUBLIC_API_URL at build time. You must pass it during the build:

`API_URL=$(gcloud run services describe alphagini-api --region $REGION --format='value(status.url)')`

**Ensure web/package.json has: "start": "next start -p $PORT"**
```
gcloud run deploy alphagini-web \
  --source web \
  --region $REGION \
  --allow-unauthenticated \
  --set-build-env-vars NEXT_PUBLIC_API_URL="$API_URL" \
  --set-env-vars       NEXT_PUBLIC_API_URL="$API_URL"
```


Open the printed alphagini-web URL. Network calls should hit GET {API_URL}/symbols and POST {API_URL}/backtest.

**Common gotchas:**

/undefined/symbols → you didn’t pass `--set-build-env-vars NEXT_PUBLIC_API_URL=...`

“container failed to listen on $PORT” → set start to next start -p $PORT

BQ “Access Denied” → missing roles/bigquery.jobUser or dataset-level dataViewer/dataEditor

Symbols dropdown empty → wrong dataset/table name; verify ALPHAGINI_BQ_DATASET and ohlcv table

### 🔁 Ingestion

services/ingest/* contains jobs to backfill/refresh the OHLCV table:

ccxt via Binance/Kraken (we switched to Kraken after Binance 451/regional restrictions)

or paid API (CoinAPI flat files) pre-normalized to ccxt schema, then loaded into BQ.

Keep large raw CSVs out of Git (GitHub’s 100MB hard cap). Use GCS, or commit only normalized samples under services/ingest/normalized/.

### 🔌 API Endpoints
```
GET /health → { ok: true }

GET /symbols → array of { symbol, timeframe, first_ts, last_ts, rows }

POST /backtest → body:

{
  "symbol": "BTC/USD",
  "timeframe": "5m",
  "start": "2025-09-01T00:00:00Z",
  "end":   "2025-09-02T00:00:00Z",
  "model": "sma",
  "strategy": "sma_cross",
  "cash_start": 100000,
  "sma_fast": 10,
  "sma_slow": 30
}

```
response:
```
{
  "summary": { "bars": 576, "...": "..." },
  "metrics": { "abs_return": 0.0123, "rel_return": 0.0123, "...": "..." },
  "series":  { "ts": ["...ISO..."], "equity": [100000, ...] },
  "logs":    ["debug breadcrumbs ..."]
}
```
## 🧑‍💻 Contributing

Create feature branches from main; open PRs early.

Do not commit large datasets; prefer GCS or .gitignore them.
If you accidentally commit >100MB files, use git filter-repo to rewrite history (we already cleaned prior CSV commits).

Add/extend strategies in services/api/strategies.py.

Keep local tests green via tools/local_cli.py before you push.

🧭 Troubleshooting

UI “Running…” forever → open browser Network tab; if calls go to /undefined/..., rebuild alphagini-web with --set-build-env-vars NEXT_PUBLIC_API_URL=....

500 on /backtest → check Cloud Run logs; common: pandas length mismatch (fixed in SMA), BQ permissions, or empty data window.

No chart → API returned empty series; expand the Debug logs pane in the UI (server returns logs: []) to see load window, SMA params, counts, etc.

db-dtypes error → make sure db-dtypes is in the API image (and installed locally for import).

## 📄 Environment Variables (summary)
```
API (alphagini-api)

ALPHAGINI_PROJECT — GCP Project ID (e.g., alpha-gini)

ALPHAGINI_BQ_DATASET — dataset with OHLCV table (e.g., alphagini_marketdata)

ALPHAGINI_EXP_DATASET — dataset for experiment outputs (e.g., alphagini_experiments)
```
Web (alphagini-web)

`NEXT_PUBLIC_API_URL` — required at build time (and runtime), the public URL of alphagini-api

Get it with:
`gcloud run services describe alphagini-api --region us-central1 --format='value(status.url)'`

## 🧭 Make it yours

Add more models (Prophet, LSTM) under services/api/... and surface them in web/app/page.tsx.

Expand metrics: drawdowns, exposure, turnover, fees/slippage simulation.

Schedule refresh via Cloud Scheduler → Cloud Run Jobs (ingestion) → BigQuery.
