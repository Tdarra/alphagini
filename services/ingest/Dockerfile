# Google-hosted base to avoid Docker Hub rate limits
FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim

# Install Python + venv
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

# Create and use a virtualenv 
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Upgrade pip first (in venv), then install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -U pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY . .

CMD ["python","binance_ingest.py"]
