"""
tests/test_forecast_service.py
────────────────────────────────
Tests for forecast_service.py.

Tests the service functions directly against the in-memory
SQLite DB — no HTTP layer, no mocking needed.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy.orm import Session

from api.services.forecast_service import get_forecast


class TestGetForecast:

    def test_returns_forecast_for_valid_ticker(self, db_session, sample_forecast):
        result = get_forecast(db_session, "AAPL")

        assert result is not None
        assert result.ticker == "AAPL"

    def test_forecast_has_correct_horizon(self, db_session, sample_forecast):
        result = get_forecast(db_session, "AAPL")

        # sample_forecast fixture inserts 30 points
        assert result.horizon == 30
        assert len(result.forecasts) == 30

    def test_forecast_dates_are_ascending(self, db_session, sample_forecast):
        result = get_forecast(db_session, "AAPL")
        dates = [f.forecast_date for f in result.forecasts]

        assert dates == sorted(dates)

    def test_forecast_has_model_name(self, db_session, sample_forecast):
        result = get_forecast(db_session, "AAPL")

        assert result.model == "linear_regression"

    def test_forecast_has_predicted_close(self, db_session, sample_forecast):
        result = get_forecast(db_session, "AAPL")

        for point in result.forecasts:
            assert point.predicted_close > 0

    def test_returns_none_for_unknown_ticker(self, db_session):
        # No data inserted for FAKE
        result = get_forecast(db_session, "FAKE")

        assert result is None

    def test_ticker_is_case_insensitive(self, db_session, sample_forecast):
        # Service should upper() the ticker
        result_upper = get_forecast(db_session, "AAPL")
        result_lower = get_forecast(db_session, "aapl")

        assert result_upper is not None
        assert result_lower is not None
        assert result_upper.horizon == result_lower.horizon