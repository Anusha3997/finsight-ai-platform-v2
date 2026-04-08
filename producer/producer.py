"""
producer.py
───────────
Fetches stock prices from yfinance and pushes each row
as a JSON message onto the Kafka topic `stock-prices`.

"""
 
from __future__ import annotations
 
import json
import logging
import os
import time
from datetime import date
 
from kafka import KafkaProducer
from kafka.errors import KafkaError
 
from yfinance_client import fetch_daily_prices
 
# ── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)
 
# ── Config from environment ───────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "stock-prices")
TICKERS      = [t.strip().upper() for t in os.getenv("TICKERS", "AAPL,MSFT,GOOGL,TSLA").split(",")]
INTERVAL_SEC = int(os.getenv("FETCH_INTERVAL_SECONDS", "60"))
 
 
# ── Serializer ────────────────────────────────────────────
def _serialize(obj):
    """JSON serializer — handles date objects which json.dumps can't."""
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")
 
 
# ── Producer factory ──────────────────────────────────────
def build_producer() -> KafkaProducer:

    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, default=_serialize).encode("utf-8"),
                # Ensures all in-sync replicas acknowledge the write
                acks="all",
                retries=3,
            )
            logger.info(f"Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except KafkaError as e:
            logger.warning(f"Kafka not ready (attempt {attempt}/5): {e}")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 5 attempts")
 
 
# ── Core publish function ─────────────────────────────────
def publish_ticker(producer: KafkaProducer, ticker: str) -> int:
    
    logger.info(f"Fetching prices for {ticker}")
    try:
        df = fetch_daily_prices(ticker)
    except Exception as e:
        logger.error(f"Failed to fetch {ticker}: {e}",exc_info=True)
        return 0
 
    # Normalize columns just in case
    df.columns = [c.strip().lower() for c in df.columns]
    sent = 0
 
    for row in df.to_dict(orient="records"):
        message = {
            "ticker": ticker.upper(),
            "date":   row.get("date"),
            "open":   float(row.get("open") or 0),
            "high":   float(row.get("high") or 0),
            "low":    float(row.get("low")  or 0),
            "close":  float(row.get("close") or 0),
            "volume": float(row.get("volume") or 0),
        }
 
        # Use ticker as the Kafka partition key
        producer.send(
            KAFKA_TOPIC,
            key=ticker.encode("utf-8"),
            value=message,
        )
        sent += 1
 
    # Flush ensures all buffered messages are actually sent
    producer.flush()
    logger.info(f"Published {sent} rows for {ticker} → topic: {KAFKA_TOPIC}")
    return sent
 
 
# ── Main loop ─────────────────────────────────────────────
def main():
    logger.info(f"Starting producer | tickers={TICKERS} | interval={INTERVAL_SEC}s")
    producer = build_producer()
 
    while True:
        for ticker in TICKERS:
            publish_ticker(producer, ticker)
        logger.info(f"Sleeping {INTERVAL_SEC}s before next fetch...")
        time.sleep(INTERVAL_SEC)
 
 
if __name__ == "__main__":
    main()
 
