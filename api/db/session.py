"""
api/db/session.py
─────────────────
SQLAlchemy engine + session factory.

No Spark. No Kafka. Just a Postgres connection.

Usage in a FastAPI route:
    from api.db.session import get_db

    @router.get("/prices")
    def get_prices(db: Session = Depends(get_db)):
        return db.query(StockPrice).all()
"""

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from api.core.config import settings

# ── Engine ────────────────────────────────────────────────
# pool_pre_ping=True means SQLAlchemy checks the connection is alive
# before using it — prevents "connection closed" errors after idle time
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    # Keep a small pool — this is a read-heavy, low-concurrency API
    pool_size=5,
    max_overflow=10,
)

# ── Session factory ───────────────────────────────────────
# autocommit=False — we control transactions explicitly
# autoflush=False  — don't flush to DB until we call commit()
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)


# ── FastAPI dependency ────────────────────────────────────
def get_db() -> Generator[Session, None, None]:
    """
    Yields a database session and guarantees it is closed
    after the request finishes — even if an exception is raised.

    Inject into any route with:
        db: Session = Depends(get_db)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()