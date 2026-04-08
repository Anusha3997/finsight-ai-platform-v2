# ── orchestration/airflow/Dockerfile ─────────────────────
# Official Airflow image 
FROM apache/airflow:2.8.1-python3.11

# Installing any extra Python packages your DAGs need
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
