"""
api/services/risk_service.py
─────────────────────────────
Queries pre-computed risk metrics from Postgres.

Spark already computed volatility, drawdown, and Sharpe ratio.
This service reads them back and adds one convenience field:
a human-readable risk_level label for the UI.
"""

from sqlalchemy.orm import Session

from api.db.models import RiskMetric
from api.schemas.risk import RiskMetricResponse


# ── Risk level thresholds (Sharpe ratio based) ────────────
# Sharpe > 1.0  → LOW risk   (good return per unit of risk)
# Sharpe 0–1.0  → MEDIUM risk
# Sharpe < 0    → HIGH risk   (negative risk-adjusted return)
def _risk_label(sharpe: float | None) -> str:
    if sharpe is None:
        return "UNKNOWN"
    if sharpe >= 1.0:
        return "LOW"
    if sharpe >= 0.0:
        return "MEDIUM"
    return "HIGH"


def get_risk_metrics(
    db:     Session,
    ticker: str,
) -> RiskMetricResponse | None:
    """
    Fetch risk metrics for a single ticker.

    Returns None if Spark hasn't computed metrics yet.

    Args:
        db:     SQLAlchemy session
        ticker: Stock symbol e.g. "AAPL"

    Returns:
        RiskMetricResponse or None
    """
    ticker = ticker.upper().strip()

    row = (
        db.query(RiskMetric)
        .filter(RiskMetric.ticker == ticker)
        .first()
    )

    if not row:
        return None

    return RiskMetricResponse.model_validate(row)


def get_all_risk_metrics(db: Session) -> list[RiskMetricResponse]:
    """
    Fetch risk metrics for ALL tickers.
    Useful for a dashboard overview table showing all stocks side by side.
    Results are sorted by Sharpe ratio descending (best first).
    """
    rows = (
        db.query(RiskMetric)
        .order_by(RiskMetric.sharpe_ratio.desc().nulls_last())
        .all()
    )
    return [RiskMetricResponse.model_validate(row) for row in rows]