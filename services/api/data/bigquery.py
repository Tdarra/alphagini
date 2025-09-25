import pandas as pd
from google.cloud import bigquery
from datetime import datetime

def load_ohlcv(exchange:str, symbol:str, timeframe:str, start:datetime, end:datetime) -> pd.DataFrame:
    client = bigquery.Client()
    q = """
    SELECT ts, open, high, low, close, volume
    FROM `PROJECT.marketdata.ohlcv`
    WHERE exchange=@exchange AND symbol=@symbol AND timeframe=@tf
      AND ts BETWEEN @start AND @end
    ORDER BY ts
    """
    job = client.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("exchange","STRING",exchange),
            bigquery.ScalarQueryParameter("symbol","STRING",symbol),
            bigquery.ScalarQueryParameter("tf","STRING",timeframe),
            bigquery.ScalarQueryParameter("start","TIMESTAMP",start),
            bigquery.ScalarQueryParameter("end","TIMESTAMP",end),
        ]
    ))
    df = job.result().to_dataframe()
    return df.set_index("ts")
