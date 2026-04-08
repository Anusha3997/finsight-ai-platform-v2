# ── spark/Dockerfile ─────────────────────────────────────
# Used Official Apache Spark image with Python support
FROM apache/spark:3.5.0-python3

# Switching to root to install extra Python packages
USER root

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Installing Python dependencies on top of the base Spark image
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the job
COPY spark_job.py .


ADD https://jdbc.postgresql.org/download/postgresql-42.7.1.jar /opt/spark/jars/

# Submit the Spark job in local mode (no separate cluster needed for this project)
CMD ["spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "spark_job.py"]