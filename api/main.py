"""
api/main.py
────────────
FastAPI application entry point.

"""

from datetime import date

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from api.core.config import settings
from api.db.session import get_db
from api.schemas.forecast import ForecastResponse
from api.schemas.prices import PriceListResponse, PriceOut
from api.schemas.risk import RiskMetricResponse
from api.services.forecast_service import get_forecast
from api.services.ingest_service import (
    get_available_tickers,
    get_latest_price,
    get_prices,
)
from api.services.risk_service import get_all_risk_metrics, get_risk_metrics

# ── App ───────────────────────────────────────────────────
app = FastAPI(
    title=settings.APP_TITLE,
    version=settings.APP_VERSION,
    description=(
        "Finsight AI Platform API — serves stock prices, "
        "Linear Regression forecasts, and risk metrics."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS ──────────────────────────────────────────────────
# Allows the UI (React/Vue on localhost:3000) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────
#  HEALTH
# ─────────────────────────────────────────────────────────
@app.get("/health", tags=["health"], summary="Health check")
def health():
    """
    Used by Docker healthcheck and load balancers.
    Returns 200 if the API is running.
    """
    return {"status": "ok", "version": settings.APP_VERSION}


# ─────────────────────────────────────────────────────────
#  PRICES
# ─────────────────────────────────────────────────────────
@app.get(
    "/prices",
    response_model=PriceListResponse,
    tags=["prices"],
    summary="Get historical OHLCV prices for a ticker",
)
def prices(
    ticker:     str        = Query(..., description="Stock symbol e.g. AAPL", example="AAPL"),
    start_date: date | None = Query(None, description="Filter from this date (YYYY-MM-DD)"),
    end_date:   date | None = Query(None, description="Filter to this date (YYYY-MM-DD)"),
    limit:      int         = Query(365,  description="Max rows to return", ge=1, le=5000),
    db:         Session     = Depends(get_db),
) -> PriceListResponse:
    result = get_prices(db, ticker, start_date, end_date, limit)

    if result.count == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No price data found for ticker '{ticker.upper()}'. "
                   f"Check the ticker symbol or wait for the next Spark run.",
        )
    return result


@app.get(
    "/prices/latest",
    response_model=PriceOut,
    tags=["prices"],
    summary="Get the most recent price for a ticker",
)
def latest_price(
    ticker: str     = Query(..., description="Stock symbol e.g. AAPL", example="AAPL"),
    db:     Session = Depends(get_db),
) -> PriceOut:
    result = get_latest_price(db, ticker)

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No price data found for '{ticker.upper()}'.",
        )
    return result


@app.get(
    "/prices/tickers",
    response_model=list[str],
    tags=["prices"],
    summary="List all available tickers",
)
def available_tickers(db: Session = Depends(get_db)) -> list[str]:
    """
    Returns every ticker that has price data in Postgres.
    Useful for a search dropdown in the UI.
    """
    tickers = get_available_tickers(db)
    if not tickers:
        raise HTTPException(
            status_code=404,
            detail="No tickers found. The pipeline may not have run yet.",
        )
    return tickers


# ─────────────────────────────────────────────────────────
#  FORECAST
# ─────────────────────────────────────────────────────────
@app.get(
    "/forecast",
    response_model=ForecastResponse,
    tags=["forecast"],
    summary="Get Linear Regression price forecast for a ticker",
)
def forecast(
    ticker: str     = Query(..., description="Stock symbol e.g. AAPL", example="AAPL"),
    db:     Session = Depends(get_db),
) -> ForecastResponse:
    """
    Returns pre-computed 30-day price forecast.
    Forecasts are computed by the Spark job and refreshed on every pipeline run.
    """
    result = get_forecast(db, ticker)

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No forecast found for '{ticker.upper()}'. "
                   f"Spark may not have processed this ticker yet.",
        )
    return result


# ─────────────────────────────────────────────────────────
#  RISK METRICS
# ─────────────────────────────────────────────────────────
@app.get(
    "/risk",
    response_model=RiskMetricResponse,
    tags=["risk"],
    summary="Get risk metrics for a ticker",
)
def risk(
    ticker: str     = Query(..., description="Stock symbol e.g. AAPL", example="AAPL"),
    db:     Session = Depends(get_db),
) -> RiskMetricResponse:
    """
    Returns pre-computed risk metrics:
    - daily_volatility / annual_volatility
    - max_drawdown
    - sharpe_ratio
    - avg_daily_return

    All metrics are computed by the Spark job from full price history.
    """
    result = get_risk_metrics(db, ticker)

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No risk metrics found for '{ticker.upper()}'. "
                   f"Spark may not have processed this ticker yet.",
        )
    return result


@app.get(
    "/risk/all",
    response_model=list[RiskMetricResponse],
    tags=["risk"],
    summary="Get risk metrics for all tickers",
)
def all_risk(db: Session = Depends(get_db)) -> list[RiskMetricResponse]:
    """
    Returns risk metrics for every ticker, sorted by Sharpe ratio descending.
    Useful for a portfolio overview / comparison table in the UI.
    """
    results = get_all_risk_metrics(db)

    if not results:
        raise HTTPException(
            status_code=404,
            detail="No risk metrics found. The pipeline may not have run yet.",
        )
    return results


# ─────────────────────────────────────────────────────────
#  SUMMARY  (convenience endpoint for the UI)
# ─────────────────────────────────────────────────────────
@app.get(
    "/summary",
    tags=["summary"],
    summary="Get prices + forecast + risk in one call",
)
def summary(
    ticker: str     = Query(..., description="Stock symbol e.g. AAPL", example="AAPL"),
    db:     Session = Depends(get_db),
) -> dict:
    """
    Combines latest price, forecast, and risk metrics into a single response.
    The UI can call this one endpoint instead of three separate ones.
    """
    ticker = ticker.upper().strip()

    latest   = get_latest_price(db, ticker)
    forecast = get_forecast(db, ticker)
    risk     = get_risk_metrics(db, ticker)

    if not latest and not forecast and not risk:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for '{ticker}'. "
                   f"Check the ticker symbol or wait for the next Spark run.",
        )

    return {
        "ticker":       ticker,
        "latest_price": latest,
        "forecast":     forecast,
        "risk":         risk,
    }