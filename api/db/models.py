"""
api/db/models.py
────────────────
SQLAlchemy ORM models.

These mirror EXACTLY the three tables that spark_job.py creates:
  • stock_prices   — raw OHLCV rows
  • forecasts      — Linear Regression predictions
  • risk_metrics   — volatility, drawdown, Sharpe ratio

The API never writes to these tables — Spark does.
The API only reads from them via these models.
"""

from datetime import date, datetime

from sqlalchemy import (BigInteger, Date, DateTime, Double,Index, Integer, String, UniqueConstraint)
from sqlalchemy.orm import Mapped, mapped_column

from api.db.base import Base


# ─────────────────────────────────────────────────────────
#  1.  Stock Prices  (raw OHLCV from Kafka → Spark)
# ─────────────────────────────────────────────────────────
class StockPrice(Base):
    __tablename__ = "stock_prices"

    id:     Mapped[int]   = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str]   = mapped_column(String(20), nullable=False)
    date:   Mapped[date]  = mapped_column(Date,       nullable=False)
    open:   Mapped[float | None] = mapped_column(Double)
    high:   Mapped[float | None] = mapped_column(Double)
    low:    Mapped[float | None] = mapped_column(Double)
    close:  Mapped[float]        = mapped_column(Double, nullable=False)
    volume: Mapped[float | None] = mapped_column(Double)

    __table_args__ = (
        # Matches the UNIQUE constraint Spark uses for upserts
        UniqueConstraint("ticker", "date", name="uq_stock_prices_ticker_date"),
        # Fast lookups by ticker sorted by date descending
        Index("idx_prices_ticker_date", "ticker", "date"),
    )

    def __repr__(self) -> str:
        return f"<StockPrice {self.ticker} {self.date} close={self.close}>"


# ─────────────────────────────────────────────────────────
#  2.  Forecasts  (Linear Regression predictions from Spark)
# ─────────────────────────────────────────────────────────
class Forecast(Base):
    __tablename__ = "forecasts"

    id:              Mapped[int]      = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker:          Mapped[str]      = mapped_column(String(20), nullable=False)
    forecast_date:   Mapped[date]     = mapped_column(Date,       nullable=False)
    predicted_close: Mapped[float]    = mapped_column(Double,     nullable=False)
    model:           Mapped[str|None] = mapped_column(String(50)) # e.g. "linear_regression"
    computed_at:     Mapped[datetime] = mapped_column(DateTime,   nullable=False)

    __table_args__ = (
        UniqueConstraint("ticker", "forecast_date", name="uq_forecasts_ticker_date"),
        Index("idx_forecasts_ticker", "ticker", "forecast_date"),
    )

    def __repr__(self) -> str:
        return f"<Forecast {self.ticker} {self.forecast_date} pred={self.predicted_close}>"


# ─────────────────────────────────────────────────────────
#  3.  Risk Metrics  (computed by Spark from price history)
# ─────────────────────────────────────────────────────────
class RiskMetric(Base):
    __tablename__ = "risk_metrics"

    id:               Mapped[int]      = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker:           Mapped[str]      = mapped_column(String(20), nullable=False, unique=True)

    # Volatility
    daily_volatility: Mapped[float | None] = mapped_column(Double)
    annual_volatility: Mapped[float | None] = mapped_column(Double)

    # Drawdown
    max_drawdown:     Mapped[float | None] = mapped_column(Double)

    # Return metrics
    sharpe_ratio:     Mapped[float | None] = mapped_column(Double)
    avg_daily_return: Mapped[float | None] = mapped_column(Double)

    # Metadata — useful for the UI to show "computed 3 hours ago"
    computed_at:      Mapped[datetime]     = mapped_column(DateTime, nullable=False)
    data_from:        Mapped[date | None]  = mapped_column(Date)
    data_to:          Mapped[date | None]  = mapped_column(Date)
    num_trading_days: Mapped[int | None]   = mapped_column(Integer)

    def __repr__(self) -> str:
        return f"<RiskMetric {self.ticker} sharpe={self.sharpe_ratio:.2f}>"