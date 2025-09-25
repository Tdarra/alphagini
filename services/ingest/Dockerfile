FROM mirror.gcr.io/library/python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python","binance_ingest.py"]
