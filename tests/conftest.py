"""
tests/conftest.py
──────────────────
Shared pytest fixtures.

Every test file can use these by just declaring them as arguments —
pytest injects them automatically. No imports needed in test files.

Fixtures provided:
  db_session   — in-memory SQLite session (no real Postgres needed)
  sample_prices  — list of StockPrice ORM objects
  sample_forecast — list of Forecast ORM objects
  sample_risk    — a RiskMetric ORM object
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from api.db.base import Base
from api.db.models import Forecast, RiskMetric, StockPrice


# ── In-memory SQLite engine ───────────────────────────────
# SQLite is used instead of real Postgres so tests run without Docker.
# The schema is identical — SQLAlchemy handles dialect differences.
@pytest.fixture(scope="function")
def db_session() -> Session:
    """
    Creates a fresh in-memory SQLite database for each test function.
    Tables are created, test runs, then everything is torn down.
    'scope=function' means each test gets a clean slate.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)

    TestingSession = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    session = TestingSession()

    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)


# ── Sample data fixtures ──────────────────────────────────
@pytest.fixture
def sample_prices(db_session: Session) -> list[StockPrice]:
    """
    Insert 60 days of fake AAPL price data into the test DB.
    Returns the list of inserted ORM objects.
    """
    base_date  = date(2024, 1, 1)
    base_price = 180.0
    rows = []

    for i in range(60):
        price = StockPrice(
            ticker=  "AAPL",
            date=    base_date + timedelta(days=i),
            open=    base_price + i * 0.1,
            high=    base_price + i * 0.1 + 2.0,
            low=     base_price + i * 0.1 - 2.0,
            close=   base_price + i * 0.1,
            volume=  1_000_000.0 + i * 1000,
        )
        rows.append(price)

    db_session.add_all(rows)
    db_session.commit()
    return rows


@pytest.fixture
def sample_forecast(db_session: Session) -> list[Forecast]:
    """
    Insert 30 days of fake AAPL forecast data.
    """
    base_date   = date(2024, 3, 1)
    computed_at = datetime(2024, 3, 1, 6, 0, 0)
    rows = []

    for i in range(30):
        rows.append(Forecast(
            ticker=          "AAPL",
            forecast_date=   base_date + timedelta(days=i + 1),
            predicted_close= 190.0 + i * 0.2,
            model=           "linear_regression",
            computed_at=     computed_at,
        ))

    db_session.add_all(rows)
    db_session.commit()
    return rows


@pytest.fixture
def sample_risk(db_session: Session) -> RiskMetric:
    """
    Insert one fake AAPL risk metric row.
    """
    row = RiskMetric(
        ticker=            "AAPL",
        daily_volatility=  0.012,
        annual_volatility= 0.190,
        max_drawdown=      -0.25,
        sharpe_ratio=      1.35,
        avg_daily_return=  0.0008,
        computed_at=       datetime(2024, 3, 1, 6, 0, 0),
        data_from=         date(2024, 1, 1),
        data_to=           date(2024, 3, 1),
        num_trading_days=  60,
    )
    db_session.add(row)
    db_session.commit()
    return row