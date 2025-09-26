FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip python3-venv && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -U pip setuptools wheel && pip install --no-cache-dir -r requirements.txt
COPY app.py .
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8080"]
