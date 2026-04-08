"""
tests/test_risk_service.py
───────────────────────────
Tests for risk_service.py.
"""

from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from api.services.risk_service import get_all_risk_metrics, get_risk_metrics
from api.db.models import RiskMetric
from datetime import datetime, date


class TestGetRiskMetrics:

    def test_returns_metrics_for_valid_ticker(self, db_session, sample_risk):
        result = get_risk_metrics(db_session, "AAPL")

        assert result is not None
        assert result.ticker == "AAPL"

    def test_returns_correct_sharpe_ratio(self, db_session, sample_risk):
        result = get_risk_metrics(db_session, "AAPL")

        assert result.sharpe_ratio == pytest.approx(1.35, rel=1e-4)

    def test_returns_correct_max_drawdown(self, db_session, sample_risk):
        result = get_risk_metrics(db_session, "AAPL")

        # max_drawdown is negative (a loss)
        assert result.max_drawdown == pytest.approx(-0.25, rel=1e-4)
        assert result.max_drawdown < 0

    def test_returns_correct_volatility(self, db_session, sample_risk):
        result = get_risk_metrics(db_session, "AAPL")

        assert result.daily_volatility  == pytest.approx(0.012, rel=1e-4)
        assert result.annual_volatility == pytest.approx(0.190, rel=1e-4)

    def test_returns_none_for_unknown_ticker(self, db_session):
        result = get_risk_metrics(db_session, "FAKE")

        assert result is None

    def test_ticker_is_case_insensitive(self, db_session, sample_risk):
        result = get_risk_metrics(db_session, "aapl")

        assert result is not None
        assert result.ticker == "AAPL"

    def test_metadata_fields_present(self, db_session, sample_risk):
        result = get_risk_metrics(db_session, "AAPL")

        assert result.computed_at  is not None
        assert result.data_from    is not None
        assert result.data_to      is not None
        assert result.num_trading_days == 60


class TestGetAllRiskMetrics:

    def test_returns_list(self, db_session, sample_risk):
        results = get_all_risk_metrics(db_session)

        assert isinstance(results, list)
        assert len(results) == 1

    def test_returns_empty_list_when_no_data(self, db_session):
        results = get_all_risk_metrics(db_session)

        assert results == []

    def test_sorted_by_sharpe_descending(self, db_session):
        """Insert two tickers and verify ordering."""
        db_session.add_all([
            RiskMetric(
                ticker="LOW_SHARPE",
                sharpe_ratio=0.5,
                computed_at=datetime(2024, 3, 1),
                daily_volatility=0.02,
                annual_volatility=0.32,
                max_drawdown=-0.40,
                avg_daily_return=0.0003,
            ),
            RiskMetric(
                ticker="HIGH_SHARPE",
                sharpe_ratio=2.1,
                computed_at=datetime(2024, 3, 1),
                daily_volatility=0.01,
                annual_volatility=0.16,
                max_drawdown=-0.10,
                avg_daily_return=0.0009,
            ),
        ])
        db_session.commit()

        results = get_all_risk_metrics(db_session)

        assert results[0].ticker == "HIGH_SHARPE"
        assert results[1].ticker == "LOW_SHARPE"