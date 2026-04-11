# Finsight AI Platform v2
Real-time stock analytics pipeline — prices, forecasts, and risk metrics from a single ticker search.

## What it does
Enter any stock ticker (AAPL, MSFT, TSLA, GOOGL) and Finsight shows:
- Candlestick price chart with 1W / 1M / 3M / 6M / 1Y range selector
- 7-day Linear Regression forecast with target price projection
- Risk metrics — Sharpe ratio, annual volatility, max drawdown
- OHLCV summary — close, open, high, volume at a glance

## Why v2?
This is a complete architectural overhaul of Version 1

**Ingestion:** Direct Stooq API call on request -> Kafka producer streaming continuously
**Processing:** Computed in FastAPI on every request -> Pre-computed by PySpark, served instantly
**ML:** Linear Regression in API layer -> Linear Regression in Spark job
**Orchestration:** Manual / none -> Airflow DAG — daily scheduled pipeline
**Load time:** Slow — computed on demand -> Fast — results already in Postgres
**Scalability:** Single ticker at a time -> Multiple tickers processed in parallel


**The core insight** : In v1, every time a user searched a ticker the API fetched data, ran regression, and computed risk metrics in real time — this was slow and didn't scale. 
In v2, Spark pre-computes everything on a schedule and stores results. The API just reads from Postgres — sub-100ms response times regardless of how complex the computation was.

## UI Screenshots
**Price history** — candlestick chart with OHLCV metrics

**7-day forecast** — Linear Regression with target price

**Risk metrics** — Sharpe ratio, volatility, max drawdown

## Tech Stack
Data source - yfinance
Message queue - Apache Kafka + Zookeeper
Stream processing - PySpark Structured Streaming
Machine learning - Scikit-learn Linear Regression
Database - PostgreSQL + SQLAlchemy
Orchestration - Apache Airflow
API - FastAPI + Pydantic
UI - Streamlit
Infrastructure - Docker + Docker Compose
Testing- Pytest + SQLite fixtures

## Risk Metrics Explained
- Sharpe Ratio- Risk-adjusted return. > 1.0 = strong, < 0 = poor
- Annual VolatilityStd dev of daily returns × √252. Higher = more price swings
- Max DrawdownLargest peak-to-trough decline ever. e.g. -0.34 = fell 34% from its high
- Avg Daily ReturnMean daily % change over the full history

## How to run locally
**Prerequisites**
Docker Desktop
Docker Compose

**Steps**
```bash #1. Clone the repo
git clone https://github.com/Anusha3997/finsight-ai-platform-v2.git
cd finsight-ai-platform-v2

# 2. Set up environment variables
cp .env.example .env

# 3. Start infrastructure
docker-compose up zookeeper kafka postgres -d

# 4. Start producer (fetches from Stooq, pushes to Kafka)
docker-compose up producer -d

# 5. Start Spark job (consumes Kafka, computes ML + risk, writes to Postgres)
docker-compose up spark -d

# 6. Start API
docker-compose up api -d

# 7. Start UI
docker-compose up ui -d
```

## ServiceURL
**Streamlit UI:** http://localhost:8501
**FastAPI docshttp:** //localhost:8000/docs
**Airflowhttp:** //localhost:8080



## What I'd improve next
- Better forecast model — swap Linear Regression for Facebook Prophet or ARIMA to handle seasonality and trend changepoints more realistically
- Real-time WebSocket — push live price updates to the UI instead of polling
- Redis caching — cache frequent ticker lookups to reduce Postgres load
- Great Expectations — add data quality checks between Spark and Postgres
- Cloud deployment — migrate Kafka/Spark to AWS MSK/EMR, Airflow to MWAA


## Related
v1 — Direct ingestion pipeline — Stooq → FastAPI → Postgres → Streamlit (no Kafka/Spark)
