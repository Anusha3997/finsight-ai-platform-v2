

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ── DAG default args ──────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "finsight",
    "depends_on_past":  False,        # don't wait for yesterday's run
    "email_on_failure": False,        # set True + add email in production
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    # If a run is still going after 1 hour something is wrong — kill it
    "execution_timeout": timedelta(hours=1),
}


# ─────────────────────────────────────────────────────────
#  TASK FUNCTIONS
# ─────────────────────────────────────────────────────────

def check_kafka_health(**context):

    import socket
    host, port = "kafka", 9092
    try:
        sock = socket.create_connection((host, port), timeout=10)
        sock.close()
        logger.info(f"Kafka is reachable at {host}:{port}")
    except OSError as e:
        raise RuntimeError(f"Kafka not reachable at {host}:{port}: {e}")


def check_postgres_health(**context):
    
    hook = PostgresHook(postgres_conn_id="finsight_postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()

    for table in ["stock_prices", "forecasts", "risk_metrics"]:
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
            (table,)
        )
        exists = cursor.fetchone()[0]
        if not exists:
            raise RuntimeError(
                f"Table '{table}' does not exist. "
                f"Run the Spark job once manually to create tables."
            )
        logger.info(f"Table '{table}' exists ✓")

    cursor.close()
    conn.close()


def trigger_producer(**context):
   
    import os
    tickers = os.getenv("TICKERS", "AAPL,MSFT,GOOGL,TSLA")
    logger.info(
        f"Daily ETL window open. Producer is running for tickers: {tickers}. "
        f"Kafka topic: {os.getenv('KAFKA_TOPIC', 'stock-prices')}"
    )


def wait_for_kafka_messages(**context):
    
    import time
    from kafka import KafkaConsumer
    import json
    import os

    broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    topic  = os.getenv("KAFKA_TOPIC",  "stock-prices")
    today  = datetime.utcnow().date().isoformat()

    logger.info(f"Waiting for messages dated {today} on topic '{topic}'")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset="latest",
        consumer_timeout_ms=5000,      # stop iterating after 5s of no messages
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    deadline = time.time() + 600       # wait up to 10 minutes
    found    = False

    while time.time() < deadline:
        for message in consumer:
            msg_date = message.value.get("date", "")
            if msg_date == today:
                logger.info(f"Found today's message: {message.value['ticker']} {msg_date}")
                found = True
                break
        if found:
            break
        logger.info("No today's messages yet — waiting 30s...")
        time.sleep(30)

    consumer.close()

    if not found:
        logger.warning(
            "No messages with today's date found after 10 minutes. "
            "Proceeding anyway — Spark will process whatever is in the topic."
        )


def trigger_spark_job(**context):
    
    import subprocess
    import os

    logger.info("Triggering Spark job via spark-submit...")

    result = subprocess.run(
        [
            "docker", "exec", "finsight-spark",
            "spark-submit",
            "--master", "local[*]",
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "/app/spark_job.py",
        ],
        capture_output=True,
        text=True,
        timeout=3600,   # 1 hour max
    )

    if result.returncode != 0:
        logger.error(f"Spark job failed:\n{result.stderr}")
        raise RuntimeError(f"spark-submit exited with code {result.returncode}")

    logger.info(f"Spark job completed successfully:\n{result.stdout[-2000:]}")


def verify_data_written(**context):
    
    from datetime import timedelta

    hook   = PostgresHook(postgres_conn_id="finsight_postgres")
    conn   = hook.get_conn()
    cursor = conn.cursor()

    # Check stock_prices has recent data (within last 7 days — weekends/holidays)
    cursor.execute("""
        SELECT COUNT(*), MAX(date)
        FROM   stock_prices
        WHERE  date >= CURRENT_DATE - INTERVAL '7 days'
    """)
    count, max_date = cursor.fetchone()
    logger.info(f"stock_prices: {count} recent rows, latest date: {max_date}")
    if count == 0:
        raise RuntimeError("No recent rows in stock_prices — Spark may have failed silently")

    # Check forecasts exist
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM forecasts")
    forecast_tickers = cursor.fetchone()[0]
    logger.info(f"forecasts: {forecast_tickers} tickers covered")
    if forecast_tickers == 0:
        raise RuntimeError("No rows in forecasts table")

    # Check risk_metrics exist
    cursor.execute("SELECT COUNT(*) FROM risk_metrics")
    risk_count = cursor.fetchone()[0]
    logger.info(f"risk_metrics: {risk_count} tickers covered")
    if risk_count == 0:
        raise RuntimeError("No rows in risk_metrics table")

    cursor.close()
    conn.close()
    logger.info("Data verification passed ✓")


def notify_success(**context):
    run_date = context["ds"]                           # YYYY-MM-DD
    dag_id   = context["dag"].dag_id
    logger.info(f"DAG '{dag_id}' completed successfully for {run_date}")


def notify_failure(context):
    
    task_id  = context["task_instance"].task_id
    dag_id   = context["dag"].dag_id
    run_date = context["ds"]
    logger.error(
        f"DAG '{dag_id}' FAILED at task '{task_id}' for run date {run_date}"
    )


# ─────────────────────────────────────────────────────────
#  DAG DEFINITION
# ─────────────────────────────────────────────────────────
with DAG(
    dag_id="finsight_daily_etl",
    description="Daily pipeline: Stooq → Kafka → Spark → Postgres",
    default_args=DEFAULT_ARGS,
    # Run every day at 6:00 AM UTC
    # Cron format: minute hour day month weekday
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,          # don't backfill missed runs
    max_active_runs=1,      # only one run at a time
    tags=["finsight", "etl", "daily"],
    # Call notify_failure on any task failure
    on_failure_callback=notify_failure,
) as dag:

    # ── Task 1: Check Kafka ───────────────────────────────
    t_check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    # ── Task 2: Check Postgres ────────────────────────────
    t_check_postgres = PythonOperator(
        task_id="check_postgres_health",
        python_callable=check_postgres_health,
    )

    # ── Task 3: Trigger producer window ──────────────────
    t_trigger_producer = PythonOperator(
        task_id="trigger_producer",
        python_callable=trigger_producer,
    )

    # ── Task 4: Wait for Kafka messages ──────────────────
    t_wait_kafka = PythonOperator(
        task_id="wait_for_kafka_messages",
        python_callable=wait_for_kafka_messages,
    )

    # ── Task 5: Run Spark job ─────────────────────────────
    t_spark = PythonOperator(
        task_id="trigger_spark_job",
        python_callable=trigger_spark_job,
        # Give Spark extra time — don't use the default 1hr DAG timeout
        execution_timeout=timedelta(hours=2),
    )

    # ── Task 6: Verify data ───────────────────────────────
    t_verify = PythonOperator(
        task_id="verify_data_written",
        python_callable=verify_data_written,
    )

    # ── Task 7a: Success notification ────────────────────
    t_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # ── Task 7b: Failure notification ────────────────────
    # TriggerRule.ONE_FAILED = runs if ANY upstream task failed
    t_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
        op_kwargs={"context": {}},
    )

    # ── Dependency chain ──────────────────────────────────
    (
        t_check_kafka
        >> t_check_postgres
        >> t_trigger_producer
        >> t_wait_kafka
        >> t_spark
        >> t_verify
        >> [t_success, t_failure]   # both listen — only correct one fires
    )