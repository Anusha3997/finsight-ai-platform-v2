"""
spark_job.py
This is the main Spark job that runs continuously to process incoming stock price data.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, DateType, TimestampType,
)
from sklearn.linear_model import LinearRegression
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",   "kafka:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC",    "stock-prices")
POSTGRES_HOST  = os.getenv("POSTGRES_HOST",  "postgres")
POSTGRES_PORT  = os.getenv("POSTGRES_PORT",  "5432")
POSTGRES_USER  = os.getenv("POSTGRES_USER",  "finsight")
POSTGRES_PASS  = os.getenv("POSTGRES_PASSWORD", "finsight_pass")
POSTGRES_DB    = os.getenv("POSTGRES_DB",    "finsight_db")

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
JDBC_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver":   "org.postgresql.Driver",
}

# SQLAlchemy URL for pandas upserts 
SQLALCHEMY_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# How many future days to forecast
FORECAST_HORIZON_DAYS = 30

# Risk-free rate for Sharpe ratio (annualised, e.g. US T-bill ~5%)
RISK_FREE_RATE = float(os.getenv("RISK_FREE_RATE", "0.05"))


# ── Kafka message schema ──────────────────────────────────
PRICE_SCHEMA = StructType([
    StructField("ticker", StringType(),  nullable=False),
    StructField("date",   StringType(),  nullable=False),  # "YYYY-MM-DD"
    StructField("open",   DoubleType(),  nullable=True),
    StructField("high",   DoubleType(),  nullable=True),
    StructField("low",    DoubleType(),  nullable=True),
    StructField("close",  DoubleType(),  nullable=False),
    StructField("volume", DoubleType(),  nullable=True),
])


# ─────────────────────────────────────────────────────────
#  1.  SPARK SESSION
# ─────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("finsight-pipeline")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config(
            "spark.jars",
            "/opt/bitnami/spark/jars/postgresql-42.7.1.jar",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") 
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────
#  2.  READ FROM KAFKA
# ─────────────────────────────────────────────────────────
def read_kafka_stream(spark: SparkSession) -> DataFrame:

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


# ─────────────────────────────────────────────────────────
#  3.  PARSE JSON PAYLOAD
# ─────────────────────────────────────────────────────────
def parse_messages(raw_df: DataFrame) -> DataFrame:

    return (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                PRICE_SCHEMA,
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
        .withColumn("date", F.to_date(F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ss")))
        .filter(F.col("ticker").isNotNull() & F.col("close").isNotNull())
    )


# ─────────────────────────────────────────────────────────
#  4.  WRITE RAW PRICES TO POSTGRES
# ─────────────────────────────────────────────────────────
def write_prices_to_postgres(batch_df: DataFrame, batch_id: int):
    """
    foreachBatch handler — called by Spark for every micro-batch.
    Writes raw OHLCV rows to stock_prices with upsert (no duplicates).
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty, skipping prices write")
        return

    # Convert to pandas for SQLAlchemy upsert
    pdf: pd.DataFrame = batch_df.select(
        "ticker", "date", "open", "high", "low", "close", "volume"
    ).toPandas()

    # Deduplicate within the batch itself before upserting
    pdf = pdf.drop_duplicates(subset=["ticker", "date"])

    engine = create_engine(SQLALCHEMY_URL)
    rows = pdf.to_dict(orient="records")

    with engine.begin() as conn:
        stmt = pg_insert(__stock_prices_table(conn)).values(rows)
        stmt = stmt.on_conflict_do_nothing(index_elements=["ticker", "date"])
        conn.execute(stmt)

    logger.info(f"Batch {batch_id}: wrote {len(rows)} price rows to Postgres")

    # After writing prices, trigger compute for each ticker in this batch
    tickers = pdf["ticker"].unique().tolist()
    _compute_and_store(pdf, tickers)


def __stock_prices_table(conn):
    from sqlalchemy import Table, MetaData
    meta = MetaData()
    meta.reflect(bind=conn, only=["stock_prices"])
    return meta.tables["stock_prices"]


# ─────────────────────────────────────────────────────────
#  5.  COMPUTE: FORECAST + RISK  (pandas / sklearn)
# ─────────────────────────────────────────────────────────
def _compute_and_store(batch_pdf: pd.DataFrame, tickers: list[str]):

    engine = create_engine(SQLALCHEMY_URL)

    for ticker in tickers:
        logger.info(f"Computing forecast + risk for {ticker}")
        try:
            history = _load_history(engine, ticker)
            if len(history) < 30:
                logger.warning(f"{ticker}: only {len(history)} rows, need ≥30. Skipping.")
                continue

            _store_forecast(engine, ticker, history)
            _store_risk_metrics(engine, ticker, history)

        except Exception as e:
            logger.error(f"Failed to compute for {ticker}: {e}", exc_info=True)


def _load_history(engine, ticker: str) -> pd.DataFrame:
    query = text("""
        SELECT date, close, open, high, low, volume
        FROM   stock_prices
        WHERE  ticker = :ticker
        ORDER  BY date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"ticker": ticker})
    df["date"] = pd.to_datetime(df["date"])
    return df


# ── 5a. Linear Regression Forecast ───────────────────────
def _store_forecast(engine, ticker: str, history: pd.DataFrame):

    df = history.copy().reset_index(drop=True)

    X = np.arange(len(df)).reshape(-1, 1)
    y = df["close"].values

    model = LinearRegression()
    model.fit(X, y)

    # Generate future indices
    last_idx   = len(df)
    last_date  = df["date"].iloc[-1]
    future_X   = np.arange(last_idx, last_idx + FORECAST_HORIZON_DAYS).reshape(-1, 1)
    predictions = model.predict(future_X)

    # Build records
    rows = []
    for i, pred_close in enumerate(predictions):
        future_date = last_date + timedelta(days=i + 1)
        rows.append({
            "ticker":          ticker,
            "forecast_date":   future_date.date(),
            "predicted_close": round(float(pred_close), 4),
            "model":           "linear_regression",
            "computed_at":     datetime.utcnow(),
        })

    forecast_df = pd.DataFrame(rows)

    with engine.begin() as conn:
        # Delete existing forecasts for this ticker before inserting fresh ones
        conn.execute(
            text("DELETE FROM forecasts WHERE ticker = :ticker"),
            {"ticker": ticker},
        )
        forecast_df.to_sql(
            "forecasts",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )

    logger.info(f"{ticker}: stored {len(rows)}-day forecast")


# ── 5b. Risk Metrics ──────────────────────────────────────
def _store_risk_metrics(engine, ticker: str, history: pd.DataFrame):
    """
    Compute risk metrics from the full price history and upsert into risk_metrics.

    Metrics:
      daily_volatility       — std dev of daily returns
      annualised_volatility  — daily_vol × sqrt(252)
      max_drawdown           — largest peak-to-trough decline (as %)
      sharpe_ratio           — (mean_daily_return - daily_risk_free) / daily_vol
      avg_daily_return       — mean of daily % returns
    """
    df = history.copy().sort_values("date").reset_index(drop=True)

    # Daily returns: (close_t / close_{t-1}) - 1
    df["daily_return"] = df["close"].pct_change()
    returns = df["daily_return"].dropna()

    if len(returns) < 2:
        logger.warning(f"{ticker}: not enough return data for risk metrics")
        return

    # ── Volatility ────────────────────────────────────────
    daily_vol       = float(returns.std())
    annual_vol      = daily_vol * (252 ** 0.5)  # 252 trading days/year

    # ── Max Drawdown ──────────────────────────────────────
    # Rolling maximum of close price
    rolling_max     = df["close"].cummax()
    drawdown        = (df["close"] - rolling_max) / rolling_max
    max_drawdown    = float(drawdown.min())  # most negative value

    # ── Sharpe Ratio ──────────────────────────────────────
    # Convert annual risk-free rate to daily
    daily_rf        = RISK_FREE_RATE / 252
    excess_returns  = returns - daily_rf
    sharpe          = float(
        excess_returns.mean() / excess_returns.std() * (252 ** 0.5)
    ) if excess_returns.std() != 0 else 0.0

    # ── Average Daily Return ──────────────────────────────
    avg_daily_return = float(returns.mean())

    row = {
        "ticker":               ticker,
        "daily_volatility":     round(daily_vol,        6),
        "annual_volatility":    round(annual_vol,       6),
        "max_drawdown":         round(max_drawdown,     6),
        "sharpe_ratio":         round(sharpe,           6),
        "avg_daily_return":     round(avg_daily_return, 6),
        "computed_at":          datetime.utcnow(),
        "data_from":            df["date"].min().date(),
        "data_to":              df["date"].max().date(),
        "num_trading_days":     len(df),
    }

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM risk_metrics WHERE ticker = :ticker"),
            {"ticker": ticker},
        )
        pd.DataFrame([row]).to_sql(
            "risk_metrics",
            conn,
            if_exists="append",
            index=False,
            method="multi",
        )

    logger.info(
        f"{ticker}: sharpe={sharpe:.2f} | "
        f"annual_vol={annual_vol:.2%} | "
        f"max_drawdown={max_drawdown:.2%}"
    )


# ─────────────────────────────────────────────────────────
#  6.  ENSURE TABLES EXIST
# ─────────────────────────────────────────────────────────
def ensure_tables(engine):
   
    ddl = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id          BIGSERIAL PRIMARY KEY,
        ticker      VARCHAR(20)    NOT NULL,
        date        DATE           NOT NULL,
        open        DOUBLE PRECISION,
        high        DOUBLE PRECISION,
        low         DOUBLE PRECISION,
        close       DOUBLE PRECISION NOT NULL,
        volume      DOUBLE PRECISION,
        UNIQUE (ticker, date)
    );

    CREATE TABLE IF NOT EXISTS forecasts (
        id               BIGSERIAL PRIMARY KEY,
        ticker           VARCHAR(20)      NOT NULL,
        forecast_date    DATE             NOT NULL,
        predicted_close  DOUBLE PRECISION NOT NULL,
        model            VARCHAR(50),
        computed_at      TIMESTAMP        NOT NULL,
        UNIQUE (ticker, forecast_date)
    );

    CREATE TABLE IF NOT EXISTS risk_metrics (
        id                  BIGSERIAL PRIMARY KEY,
        ticker              VARCHAR(20)      NOT NULL UNIQUE,
        daily_volatility    DOUBLE PRECISION,
        annual_volatility   DOUBLE PRECISION,
        max_drawdown        DOUBLE PRECISION,
        sharpe_ratio        DOUBLE PRECISION,
        avg_daily_return    DOUBLE PRECISION,
        num_trading_days    INTEGER,
        data_from           DATE,
        data_to             DATE,
        computed_at         TIMESTAMP        NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_prices_ticker_date  ON stock_prices  (ticker, date DESC);
    CREATE INDEX IF NOT EXISTS idx_forecasts_ticker    ON forecasts     (ticker, forecast_date DESC);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Tables verified / created")


# ─────────────────────────────────────────────────────────
#  7.  MAIN
# ─────────────────────────────────────────────────────────
def main():
    logger.info("Starting Finsight Spark job")

    # 1. Make sure Postgres tables exist
    engine = create_engine(SQLALCHEMY_URL)
    ensure_tables(engine)

    # 2. Build Spark session
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 3. Read from Kafka
    raw_stream = read_kafka_stream(spark)

    # 4. Parse messages
    parsed_stream = parse_messages(raw_stream)

    # 5. Write via foreachBatch
    #    foreachBatch gives us a regular DataFrame (not streaming)
    #    so we can use pandas / sklearn inside it
    query = (
        parsed_stream
        .writeStream
        .foreachBatch(write_prices_to_postgres)
        .option("checkpointLocation", "/tmp/finsight-checkpoint")
        # processingTime trigger: run a micro-batch every 60 seconds
        .trigger(processingTime="60 seconds")
        .start()
    )

    logger.info("Streaming query started — waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()