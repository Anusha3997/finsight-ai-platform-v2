"""
tests/test_health.py
─────────────────────
Tests for the /health endpoint.
Uses FastAPI's TestClient — no real server needed.
"""

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def test_health_returns_200():
    response = client.get("/health")
    assert response.status_code == 200


def test_health_returns_ok_status():
    response = client.get("/health")
    data = response.json()
    assert data["status"] == "ok"


def test_health_returns_version():
    response = client.get("/health")
    data = response.json()
    assert "version" in data