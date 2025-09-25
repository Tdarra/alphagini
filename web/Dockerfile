FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim

# Install Python & pip
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*
    
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
ENV PORT=8080
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8080"]
