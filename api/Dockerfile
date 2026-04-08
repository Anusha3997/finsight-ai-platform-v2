
# ── api/Dockerfile ───────────────────────────────────────
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create api directory and copy files there
RUN mkdir api
COPY api/ ./api/

# Expose FastAPI port
EXPOSE 8000

# Run with uvicorn
# --host 0.0.0.0 makes it reachable from outside the container
# --reload is for development — remove in production
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
