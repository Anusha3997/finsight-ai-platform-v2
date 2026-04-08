"""
api/services/ingest_service.py
───────────────────────────────
Queries raw OHLCV price data from Postgres.

This service does ONE thing: read stock_prices rows.
No computation, no Kafka, no Spark — just SQL.
"""

from datetime import date

from sqlalchemy.orm import Session

from api.db.models import StockPrice
from api.schemas.prices import PriceListResponse, PriceOut


def get_prices(
    db:         Session,
    ticker:     str,
    start_date: date | None = None,
    end_date:   date | None = None,
    limit:      int         = 365,
) -> PriceListResponse:
    """
    Fetch OHLCV rows for a ticker, optionally filtered by date range.

    Args:
        db:         SQLAlchemy session (injected by FastAPI)
        ticker:     Stock symbol e.g. "AAPL"
        start_date: Only return rows on or after this date
        end_date:   Only return rows on or before this date
        limit:      Max rows to return (default 1 year of daily data)

    Returns:
        PriceListResponse with ticker, count, and list of price rows
    """
    ticker = ticker.upper().strip()

    query = (
        db.query(StockPrice)
        .filter(StockPrice.ticker == ticker)
        .order_by(StockPrice.date.asc())
    )

    if start_date:
        query = query.filter(StockPrice.date >= start_date)
    if end_date:
        query = query.filter(StockPrice.date <= end_date)

    rows = query.limit(limit).all()

    return PriceListResponse(
        ticker=ticker,
        count=len(rows),
        prices=[PriceOut.model_validate(row) for row in rows],
    )


def get_latest_price(db: Session, ticker: str) -> PriceOut | None:
    """
    Return the single most recent price row for a ticker.
    Useful for the UI to show the current price header.
    """
    ticker = ticker.upper().strip()

    row = (
        db.query(StockPrice)
        .filter(StockPrice.ticker == ticker)
        .order_by(StockPrice.date.desc())
        .first()
    )

    return PriceOut.model_validate(row) if row else None


def get_available_tickers(db: Session) -> list[str]:
    """
    Return a sorted list of all tickers that have price data.
    Useful for a ticker search / dropdown in the UI.
    """
    rows = (
        db.query(StockPrice.ticker)
        .distinct()
        .order_by(StockPrice.ticker.asc())
        .all()
    )
    return [row.ticker for row in rows]