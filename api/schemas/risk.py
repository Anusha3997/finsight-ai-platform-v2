"""
api/schemas/risk.py
────────────────────
Pydantic schemas for risk metric endpoints.
"""

from datetime import date, datetime

from pydantic import BaseModel, Field, ConfigDict


class RiskMetricResponse(BaseModel):
    ticker: str

    # ── Volatility ────────────────────────────────────────
    daily_volatility:  float | None = Field(
        None,
        description="Std dev of daily returns"
    )
    annual_volatility: float | None = Field(
        None,
        description="Daily volatility × √252 (annualised)"
    )

    # ── Drawdown ──────────────────────────────────────────
    max_drawdown: float | None = Field(
        None,
        description="Largest peak-to-trough decline, e.g. -0.34 means -34%"
    )

    # ── Return metrics ────────────────────────────────────
    sharpe_ratio:     float | None = Field(
        None,
        description="Annualised Sharpe ratio (excess return / volatility)"
    )
    avg_daily_return: float | None = Field(
        None,
        description="Mean daily percentage return"
    )

    # ── Metadata ──────────────────────────────────────────
    computed_at:      datetime      = Field(description="When Spark last computed these metrics")
    data_from:        date | None   = Field(None, description="Earliest date in the price history used")
    data_to:          date | None   = Field(None, description="Latest date in the price history used")
    num_trading_days: int | None    = Field(None, description="Number of trading days analysed")

    model_config = ConfigDict(from_attributes=True)