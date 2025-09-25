FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim

# Install Python & pip
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*
    
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python","binance_ingest.py"]
