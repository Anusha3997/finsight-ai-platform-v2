"""
api/services/forecast_service.py
──────────────────────────────────
Queries pre-computed forecast data from Postgres.

Spark already ran Linear Regression and stored predictions.
This service just reads them back out.
"""

from sqlalchemy.orm import Session

from api.db.models import Forecast
from api.schemas.forecast import ForecastPoint, ForecastResponse


def get_forecast(
    db:     Session,
    ticker: str,
) -> ForecastResponse | None:
    """
    Fetch all forecast points for a ticker.

    Returns None if no forecast has been computed yet
    (i.e. Spark hasn't processed this ticker yet).

    Args:
        db:     SQLAlchemy session
        ticker: Stock symbol e.g. "AAPL"

    Returns:
        ForecastResponse with all predicted dates + prices,
        or None if no data exists.
    """
    ticker = ticker.upper().strip()

    rows = (
        db.query(Forecast)
        .filter(Forecast.ticker == ticker)
        .order_by(Forecast.forecast_date.asc())
        .all()
    )

    if not rows:
        return None

    # All rows for the same ticker share the same computed_at and model
    # — grab metadata from the first row
    first = rows[0]

    return ForecastResponse(
        ticker=ticker,
        computed_at=first.computed_at,
        horizon=len(rows),
        model=first.model,
        forecasts=[ForecastPoint.model_validate(row) for row in rows],
    )