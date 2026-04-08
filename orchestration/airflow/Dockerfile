# ── orchestration/airflow/Dockerfile ──────────────────────
FROM apache/airflow:2.8.1-python3.11

# Switch to root to install system packages
USER root

# Install system dependencies (if needed)
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set working directory
WORKDIR /opt/airflow
