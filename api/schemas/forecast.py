"""
api/schemas/forecast.py
────────────────────────
Pydantic schemas for forecast endpoints.
"""

from datetime import date, datetime

from pydantic import BaseModel, Field, ConfigDict


# ── Single forecast point ─────────────────────────────────
class ForecastPoint(BaseModel):
    forecast_date:   date
    predicted_close: float
    model:           str | None = None

    model_config = ConfigDict(from_attributes=True)


# ── Full response ─────────────────────────────────────────
class ForecastResponse(BaseModel):
    ticker:      str
    computed_at: datetime             = Field(description="When Spark last ran this forecast")
    horizon:     int                  = Field(description="Number of forecast days")
    model:       str | None           = None
    forecasts:   list[ForecastPoint]

    model_config = ConfigDict(from_attributes=True)