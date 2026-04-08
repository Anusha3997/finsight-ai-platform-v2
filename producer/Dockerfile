# ── producer/Dockerfile ──────────────────────────────────
# Lightweight Python image 
FROM python:3.11-slim

# Keeps Python from buffering stdout/stderr (you see logs immediately)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first (Docker layer cache: only re-runs if requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Run the producer — it loops and pushes to Kafka on an interval
CMD ["python", "producer.py"]