"""
api/schemas/prices.py
─────────────────────
Pydantic schemas for stock price endpoints.

Separating schemas from models is important:
  - models.py  = how data is stored  (SQLAlchemy, tied to DB shape)
  - schemas.py = how data is served  (Pydantic, tied to API contract)

This means you can change your DB schema without breaking
the API contract, and vice versa.
"""

from datetime import date

from pydantic import BaseModel, Field, ConfigDict


# ── Single price row ──────────────────────────────────────
class PriceOut(BaseModel):
    ticker: str
    date:   date
    open:   float | None = None
    high:   float | None = None
    low:    float | None = None
    close:  float
    volume: float | None = None

    # orm_mode (v2 syntax) — allows constructing from a SQLAlchemy model
    # instance directly: PriceOut.model_validate(db_row)
    model_config = ConfigDict(from_attributes=True)


# ── List response — what the endpoint actually returns ────
class PriceListResponse(BaseModel):
    ticker:      str
    count:       int                  = Field(description="Number of rows returned")
    prices:      list[PriceOut]

    model_config = ConfigDict(from_attributes=True)